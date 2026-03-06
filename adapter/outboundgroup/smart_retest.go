package outboundgroup

import (
	"context"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

func (s *Smart) expectedStatusRanges() utils.IntRanges[uint16] {
	r, _ := utils.NewUnsignedRanges[uint16](s.expectedStatus)
	return r
}

// testAndRecord runs a URL test on the proxy and records the result.
func (s *Smart) testAndRecord(ctx context.Context, p C.Proxy, expectedStatus utils.IntRanges[uint16]) {
	name := p.Name()
	delay, err := p.URLTest(ctx, s.testUrl, expectedStatus)
	if err == nil && isValidDelay(delay) {
		s.recordRTT(name, time.Duration(delay)*time.Millisecond, false)
	} else if err != nil {
		s.recordRTT(name, s.retryTimeout, true)
	}
}

// findProxy returns the proxy with the given name, or nil if not found.
func (s *Smart) findProxy(name string) C.Proxy {
	for _, p := range s.GetProxies(false) {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

type retestCandidate struct {
	proxy        C.Proxy
	lastActivity time.Time
	prevEMA      float64 // EMA before retest; 0 means no prior data
	hasData      bool    // true if rttCount > 0 before retest
}

// selectiveRetest tests proxies with no RTT data or stale activity.
func (s *Smart) selectiveRetest() {
	s.retestMu.Lock()
	if time.Now().Before(s.nextRetestAt) {
		s.retestMu.Unlock()
		return
	}
	// Block re-entry while retest is running; adaptRetestInterval will set the real value.
	s.nextRetestAt = time.Now().Add(5 * time.Minute)
	s.retestMu.Unlock()

	s.retestSingle.Do(func() (struct{}, error) {
		// M3: ensure adaptRetestInterval is always called, even if a panic occurs,
		// so nextRetestAt is never left stuck at the 5-minute sentinel value.
		adapted := false
		defer func() {
			if !adapted {
				s.adaptRetestInterval(false)
			}
		}()

		proxies := s.GetProxies(false)
		candidates := make([]retestCandidate, 0, len(proxies))

		// Build candidates and snapshot EMA values in a single pass.
		s.statsMu.RLock()
		for _, p := range proxies {
			st, exists := s.stats[p.Name()]
			if !exists || st.rttCount == 0 || time.Since(st.lastActivity) > 10*time.Minute {
				c := retestCandidate{proxy: p}
				if exists {
					c.lastActivity = st.lastActivity
					c.hasData = st.rttCount > 0
					c.prevEMA = st.rttEMA
				}
				candidates = append(candidates, c)
			}
		}
		s.statsMu.RUnlock()

		if len(candidates) == 0 {
			return struct{}{}, nil
		}

		// Cap: 30% of total, clamped to [3, 10]
		maxRetest := clampInt(len(proxies)*3/10, 3, 10)
		if len(candidates) > maxRetest {
			sort.SliceStable(candidates, func(i, j int) bool {
				ti, tj := candidates[i].lastActivity, candidates[j].lastActivity
				if !ti.Equal(tj) {
					return ti.Before(tj)
				}
				// Secondary key: name ensures deterministic order when lastActivity is equal (e.g. zero value).
				return candidates[i].proxy.Name() < candidates[j].proxy.Name()
			})
			candidates = candidates[:maxRetest]
		}

		log.Debugln("[Smart] %s selective retest: %d/%d proxies", s.Name(), len(candidates), len(proxies))

		// M4: guard against TestTimeout=0 which would make the context expire immediately.
		testTimeout := s.TestTimeout
		if testTimeout <= 0 {
			testTimeout = 5000
		}
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(testTimeout)*time.Millisecond)
		defer cancel()

		expectedStatus := s.expectedStatusRanges()

		var wg sync.WaitGroup
		jitterMs := clampInt(len(candidates)*50, 50, 200)
		for _, c := range candidates {
			p := c.proxy
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(rand.Intn(jitterMs)) * time.Millisecond)
				s.testAndRecord(ctx, p, expectedStatus)
			}()
		}
		wg.Wait()

		adapted = true
		s.adaptRetestInterval(s.shouldResetBackoff(candidates))

		return struct{}{}, nil
	})
}

// triggerRetest is the callback passed to onDialFailed.
func (s *Smart) triggerRetest() {
	// Reset both the timer and the backoff so a dial failure always triggers
	// an immediate retest at the base interval, regardless of how far the
	// exponential backoff has grown.
	s.retestMu.Lock()
	s.nextRetestAt = time.Now()
	s.retestBackoff = s.retestInterval
	s.retestMu.Unlock()
	s.selectiveRetest()
	// Reset after retest completes so the new EMA data informs the next selection.
	s.fastSingle.Reset()
}

// shouldResetBackoff checks whether the retest produced a significant change:
// any node's RTT EMA shifted >~30%, or any previously-unknown node now has data.
// Must be called without statsMu held.
func (s *Smart) shouldResetBackoff(candidates []retestCandidate) bool {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	for _, c := range candidates {
		name := c.proxy.Name()
		st, ok := s.stats[name]
		if !ok {
			continue
		}
		if c.hasData && math.Abs(st.rttEMA-c.prevEMA) > 0.26 {
			return true
		}
		if !c.hasData && st.rttCount > 0 {
			return true
		}
	}
	return false
}

// adaptRetestInterval adjusts the next retest time using exponential backoff.
// If significantChange is true (any node RTT shifted >~30%), the backoff resets
// to the initial interval so the strategy stays responsive during network churn.
func (s *Smart) adaptRetestInterval(significantChange bool) {
	s.retestMu.Lock()
	defer s.retestMu.Unlock()

	// M2: guard against int64 overflow before doubling; cap unconditionally.
	next := s.retestBackoff
	if next <= 3*time.Minute/2 {
		next *= 2
	} else {
		next = 3 * time.Minute
	}
	if significantChange {
		next = s.retestInterval
	}
	s.retestBackoff = next
	s.nextRetestAt = time.Now().Add(next)

	log.Debugln("[Smart] %s next retest in %v (significantChange=%v)", s.Name(), next, significantChange)
}

// triggerSameIPRetest retests the given node and all nodes sharing the same host IP.
func (s *Smart) triggerSameIPRetest(proxyName string) {
	target := s.findProxy(proxyName)
	if target == nil {
		s.triggerNodeRetest(proxyName)
		return
	}
	host, _, err := net.SplitHostPort(target.Addr())
	if err != nil {
		s.triggerNodeRetest(proxyName)
		return
	}

	var matched []string
	for _, p := range s.GetProxies(false) {
		if h, _, e := net.SplitHostPort(p.Addr()); e == nil && h == host {
			matched = append(matched, p.Name())
			s.triggerNodeRetest(p.Name())
		}
	}
	log.Debugln("[Smart] %s passive degradation on %s (host=%s), retesting %d nodes: %v",
		s.Name(), proxyName, host, len(matched), matched)
}

// triggerNodeRetest immediately retests a single proxy node.
// Each node is subject to a 5-second cooldown to prevent retest storms.
func (s *Smart) triggerNodeRetest(proxyName string) {
	now := time.Now()

	s.statsMu.Lock()
	st, ok := s.stats[proxyName]
	if !ok || now.Sub(st.lastRetestAt) < 5*time.Second {
		s.statsMu.Unlock()
		return
	}
	st.lastRetestAt = now
	s.statsMu.Unlock()

	target := s.findProxy(proxyName)
	if target == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(s.TestTimeout)*time.Millisecond)
		defer cancel()

		expectedStatus := s.expectedStatusRanges()
		delay, err := target.URLTest(ctx, s.testUrl, expectedStatus)
		if err == nil && isValidDelay(delay) {
			s.recordRTT(proxyName, time.Duration(delay)*time.Millisecond, false)
			log.Debugln("[Smart] %s node retest %s: %dms", s.Name(), proxyName, delay)
		} else {
			s.recordRTT(proxyName, s.retryTimeout, true)
			log.Debugln("[Smart] %s node retest %s: failed (%v)", s.Name(), proxyName, err)
		}
		s.fastSingle.Reset()
	}()
}
