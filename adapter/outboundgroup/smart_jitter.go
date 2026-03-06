package outboundgroup

import (
	"sync"
	"time"
)

// rttRiseEvent records a single RTT rise event with rollback info.
type rttRiseEvent struct {
	proxyName string
	at        time.Time
	prevEMA   float64 // EMA before this rise, used for rollback
	newLogRTT float64 // log(RTT) of the rising sample, used for amplitude check
}

// rollbackEntry describes a node that needs its EMA restored.
type rollbackEntry struct {
	name    string
	prevEMA float64
}

type jitterDetector struct {
	mu            sync.Mutex
	riseWindow    []rttRiseEvent
	suppressUntil time.Time
	window        time.Duration
	suppressDur   time.Duration
}

func newJitterDetector() jitterDetector {
	return jitterDetector{
		window:      10 * time.Second,
		suppressDur: 30 * time.Second,
	}
}

// isSuppressed reports whether jitter suppression is currently active.
func (j *jitterDetector) isSuppressed(now time.Time) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return now.Before(j.suppressUntil)
}

// minJitterRiseThreshold is the minimum average rise in ln(RTT) space required
// to treat concurrent node rises as network-wide jitter rather than normal variance.
// 0.18 ≈ e^0.18 ≈ 1.20, meaning each node must have risen ~20% on average.
const minJitterRiseThreshold = 0.18

// recordRise records an RTT rise event. If enough distinct nodes have risen
// within the detection window AND the average rise amplitude is significant,
// it triggers suppression and returns the rollback list.
func (j *jitterDetector) recordRise(proxyName string, now time.Time, prevEMA, newLogRTT float64, activeNodes int) (triggered bool, rollbacks []rollbackEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Prune events outside the window.
	cutoff := now.Add(-j.window)
	valid := j.riseWindow[:0]
	for _, e := range j.riseWindow {
		if e.at.After(cutoff) {
			valid = append(valid, e)
		}
	}
	j.riseWindow = append(valid, rttRiseEvent{
		proxyName: proxyName,
		at:        now,
		prevEMA:   prevEMA,
		newLogRTT: newLogRTT,
	})

	// Dynamic threshold: max(3, activeNodes*0.4).
	// Scales with pool size so larger groups require more nodes to trigger,
	// while small pools (<=7 nodes) always require at least 3.
	thresh := int(float64(activeNodes) * 0.4)
	if thresh < 3 {
		thresh = 3
	}

	// Build earliest-EMA-per-node map; also serves as distinct node count.
	earliest := make(map[string]rttRiseEvent, len(j.riseWindow))
	for _, e := range j.riseWindow {
		if prev, ok := earliest[e.proxyName]; !ok || e.at.Before(prev.at) {
			earliest[e.proxyName] = e
		}
	}
	if len(earliest) < thresh {
		return false, nil
	}

	// Amplitude guard: require the average rise across nodes to be significant.
	// This prevents small normal fluctuations from triggering jitter suppression.
	var totalRise float64
	for _, e := range earliest {
		if rise := e.newLogRTT - e.prevEMA; rise > 0 {
			totalRise += rise
		}
	}
	if totalRise/float64(len(earliest)) < minJitterRiseThreshold {
		return false, nil
	}

	// Triggered: activate suppression.
	j.suppressUntil = now.Add(j.suppressDur)

	rollbacks = make([]rollbackEntry, 0, len(earliest))
	for name, e := range earliest {
		rollbacks = append(rollbacks, rollbackEntry{name: name, prevEMA: e.prevEMA})
	}

	// Clear window after trigger.
	j.riseWindow = j.riseWindow[:0]

	return true, rollbacks
}
