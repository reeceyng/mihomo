package outboundgroup

import (
	"math"
	"sort"
	"sync/atomic"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// rttLogMid and rttBeta are the shared log-logistic parameters used by both
// computeScore and coldStartScore so the two paths stay in sync.
var (
	rttLogMid = math.Log(250) // midpoint: 250 ms
	rttBeta   = 2.0           // steepness
)

const (
	// passiveBootstrapN is the number of samples collected before establishing
	// the passive RTT baseline. Using a median of N samples prevents a single
	// outlier from polluting the baseline.
	passiveBootstrapN = 3
	// passiveAlpha is the EMA smoothing factor for passive RTT tracking.
	// 0.3 (up from 0.2) improves convergence speed for degradation detection.
	passiveAlpha = 0.3
	// passiveDegradeRatio and passiveDegradeMinMs define the degradation trigger:
	// current sample must exceed ratio*baseline AND exceed the absolute minimum.
	passiveDegradeRatio = 2.0
	passiveDegradeMinMs = 150.0
)

// selectAlpha returns the EMA smoothing factor, using 0.5 for the first few
// samples to converge faster from cold start.
func selectAlpha(base float64, count int) float64 {
	if count < 3 {
		return 0.5
	}
	return base
}

// proxyStats holds RTT metrics for a single proxy node.
// rttEMA/rttCount are updated by active probes only (health checks, retests).
// passiveRTTEMA/passiveRTTCount track passive traffic RTT for degradation detection only.
type proxyStats struct {
	rttEMA   float64 // EMA of ln(rtt_ms) — active probes only
	rttCount int

	passiveRTTEMA       float64   // EMA of passive RTT in ms (for degradation detection)
	passiveRTTCount     int
	passiveRTTBootstrap []float64 // cold-start buffer; nil after bootstrap completes

	lastActivity time.Time
	lastRetestAt time.Time // per-node retest cooldown
	activeConns  atomic.Int64
}

// computeScore calculates a score for proxy based on RTT only. Lower = better.
// Must be called with statsMu read-locked.
func (s *Smart) computeScore(proxy C.Proxy) float64 {
	st, exists := s.stats[proxy.Name()]

	if !exists || st.rttCount == 0 {
		return s.coldStartScore(proxy)
	}

	// Short-circuit for clearly poor nodes: ln(5000ms) ≈ 8.52, logLogistic output ≈ 1.0.
	// Avoids unnecessary floating-point work for timed-out or penalised nodes.
	if st.rttEMA > 8.5 {
		return 1.0
	}

	// RTT: log-logistic midpoint 200ms -> [0,1]
	return logLogisticLog(st.rttEMA, rttLogMid, rttBeta)
}

// coldStartScore returns a score based on health check data for proxies
// with no passive metrics yet.
func (s *Smart) coldStartScore(proxy C.Proxy) float64 {
	delay := proxy.LastDelayForTestUrl(s.testUrl)
	if !proxy.AliveForTestUrl(s.testUrl) || !isValidDelay(delay) {
		return math.MaxFloat64
	}
	return logLogisticLog(math.Log(float64(delay)), rttLogMid, rttBeta)
}

// isValidDelay returns true if the delay represents a successful measurement.
func isValidDelay(delay uint16) bool {
	return delay > 0 && delay != 0xffff
}

// logLogisticLog is the log-logistic CDF for pre-logged values.
func logLogisticLog(logValue, logMid, beta float64) float64 {
	return 1.0 / (1.0 + math.Exp(-beta*(logValue-logMid)))
}

func (s *Smart) getOrCreateStatsUnlocked(name string) *proxyStats {
	st, ok := s.stats[name]
	if !ok {
		st = &proxyStats{
			lastActivity: time.Now(),
		}
		s.stats[name] = st
	}
	return st
}

// updateEMA applies an EMA update to the stats and returns the new EMA and count.
// Must be called with statsMu held.
func (st *proxyStats) updateEMA(logMs float64, alpha float64, now time.Time) (ema float64, count int) {
	st.rttEMA = alpha*logMs + (1-alpha)*st.rttEMA
	st.rttCount++
	st.lastActivity = now
	return st.rttEMA, st.rttCount
}

// clampMs converts a duration to milliseconds, clamping to a minimum of 1.
func clampMs(d time.Duration) float64 {
	ms := float64(d.Milliseconds())
	if ms < 1 {
		return 1
	}
	return ms
}

// logRTTChange logs an RTT sample when the EMA shift is significant (>5% in ln space).
func logRTTChange(proxyName string, ms, prevEMA, ema float64, count int) {
	if math.Abs(ema-prevEMA) > 0.05 {
		log.Debugln("[Smart] %s RTT sample=%.0fms ema=%.1fms (n=%d)",
			proxyName, ms, math.Exp(ema), count)
	}
}

// recordRTT updates the RTT EMA from a latency measurement.
// penalty=true bypasses jitter suppression (used for dead-node / dial-failure penalties).
func (s *Smart) recordRTT(proxyName string, duration time.Duration, penalty bool) {
	ms := clampMs(duration)
	logMs := math.Log(ms)
	now := time.Now()

	s.statsMu.Lock()
	st := s.getOrCreateStatsUnlocked(proxyName)

	if st.rttCount == 0 {
		st.rttEMA = logMs
		st.rttCount = 1
		st.lastActivity = now
		s.statsMu.Unlock()
		return
	}

	rising := logMs > st.rttEMA

	// Non-penalty rising RTT: run jitter detection BEFORE updating the EMA so
	// that no goroutine ever observes a jitter-polluted score.
	if rising && !penalty {
		if s.jitter.isSuppressed(now) {
			s.statsMu.Unlock()
			return
		}
		prevEMA := st.rttEMA
		s.statsMu.Unlock()

		// jitter.recordRise has its own mutex; call it outside statsMu.
		activeNodes := len(s.GetProxies(false))
		triggered, rollbacks := s.jitter.recordRise(proxyName, now, prevEMA, logMs, activeNodes)
		if triggered {
			s.applyJitterRollbacks(rollbacks)
			log.Warnln("[Smart] %s local network jitter detected, suppressing EMA updates for %v",
				s.Name(), s.jitter.suppressDur)
			return
		}

		// Not a jitter event: apply the rising EMA update.
		// Re-check both direction and that no other goroutine has already
		// updated the EMA while we were outside the lock (TOCTOU guard).
		s.statsMu.Lock()
		if logMs > st.rttEMA && st.rttEMA == prevEMA {
			alpha := selectAlpha(s.alphaUp, st.rttCount)
			st.updateEMA(logMs, alpha, now)
		}
		ema, count := st.rttEMA, st.rttCount
		s.statsMu.Unlock()
		logRTTChange(proxyName, ms, prevEMA, ema, count)
		return
	}

	// Falling RTT or penalty: update EMA directly under the lock.
	base := s.alphaDown
	if rising { // penalty + rising
		base = s.alphaUp
	}
	prevLogEMA := st.rttEMA
	alpha := selectAlpha(base, st.rttCount)
	ema, count := st.updateEMA(logMs, alpha, now)
	s.statsMu.Unlock()

	logRTTChange(proxyName, ms, prevLogEMA, ema, count)
}

// applyJitterRollbacks restores EMA values for nodes affected by jitter.
func (s *Smart) applyJitterRollbacks(rollbacks []rollbackEntry) {
	s.statsMu.Lock()
	for _, rb := range rollbacks {
		if st, ok := s.stats[rb.name]; ok && st.rttEMA > rb.prevEMA+0.18 {
			st.rttEMA = rb.prevEMA
		}
	}
	s.statsMu.Unlock()
}

// recordPassiveRTT records a passive RTT sample from user traffic.
// It does NOT update the scoring EMA. Instead it maintains a separate
// passive baseline and triggers a targeted node retest when the current
// sample exceeds 2x the historical passive average.
//
// The first passiveBootstrapN samples are buffered and their median is used
// as the initial baseline, preventing a single outlier from polluting it.
func (s *Smart) recordPassiveRTT(proxyName string, rtt time.Duration) {
	ms := clampMs(rtt)

	s.statsMu.Lock()
	st := s.getOrCreateStatsUnlocked(proxyName)
	st.lastActivity = time.Now()

	// Cold-start: collect passiveBootstrapN samples, then use median as baseline.
	if st.passiveRTTCount < passiveBootstrapN {
		st.passiveRTTBootstrap = append(st.passiveRTTBootstrap, ms)
		st.passiveRTTCount++
		if st.passiveRTTCount == passiveBootstrapN {
			sorted := append([]float64(nil), st.passiveRTTBootstrap...)
			sort.Float64s(sorted)
			st.passiveRTTEMA = sorted[len(sorted)/2]
			st.passiveRTTBootstrap = nil // release buffer
		}
		s.statsMu.Unlock()
		return
	}

	prevEMA := st.passiveRTTEMA
	alpha := selectAlpha(passiveAlpha, st.passiveRTTCount)
	st.passiveRTTEMA = alpha*ms + (1-alpha)*st.passiveRTTEMA
	st.passiveRTTCount++
	s.statsMu.Unlock()

	// Trigger retest when passive RTT significantly exceeds historical baseline.
	if ms > passiveDegradeRatio*prevEMA && ms > passiveDegradeMinMs {
		log.Debugln("[Smart] %s passive RTT degradation: %.0fms > 2x %.0fms, scheduling retest for same-IP nodes",
			proxyName, ms, prevEMA)
		s.triggerSameIPRetest(proxyName)
	}
}

// ProxyStatsSnapshot is a serializable snapshot of a proxy's metrics.
type ProxyStatsSnapshot struct {
	Name           string    `json:"name"`
	RTTMs          float64   `json:"rttMs"`
	PassiveRTTMs   float64   `json:"passiveRttMs,omitempty"`
	ActiveConns    int64     `json:"activeConns"`
	Score          float64   `json:"score"`
	LastActivity   time.Time `json:"lastActivity"`
}

// SmartStats returns per-proxy snapshots sorted by score ascending (best first).
func (s *Smart) SmartStats() []ProxyStatsSnapshot {
	proxies := s.GetProxies(false)

	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	result := make([]ProxyStatsSnapshot, 0, len(proxies))
	for _, p := range proxies {
		name := p.Name()
		snap := ProxyStatsSnapshot{
			Name:  name,
			Score: s.computeScore(p),
		}
		if st, ok := s.stats[name]; ok {
			if st.rttCount > 0 {
				snap.RTTMs = math.Exp(st.rttEMA)
			}
			if st.passiveRTTCount > 0 {
				snap.PassiveRTTMs = st.passiveRTTEMA
			}
			snap.ActiveConns = st.activeConns.Load()
			snap.LastActivity = st.lastActivity
		}
		result = append(result, snap)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score < result[j].Score
	})
	return result
}
