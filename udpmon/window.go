package udpmon

import (
	"sort"
	"sync"
	"time"
)

// Window is a fixed-capacity raw measurement ring. Samples are final probe
// outcomes only: a probe that was never executed because of local overload is
// absent rather than being converted into network loss.
type Window struct {
	mu       sync.RWMutex
	buf      []Sample
	next     int
	count    int
	period   time.Duration
	interval time.Duration
}

func NewWindow(period, interval time.Duration, hardMax int) *Window {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	capacity := int(period/interval) + 2
	if capacity < 16 {
		capacity = 16
	}
	if hardMax > 0 && capacity > hardMax {
		capacity = hardMax
	}
	return &Window{buf: make([]Sample, capacity), period: period, interval: interval}
}

func (w *Window) Add(sample Sample) {
	w.mu.Lock()
	w.buf[w.next] = sample
	w.next = (w.next + 1) % len(w.buf)
	if w.count < len(w.buf) {
		w.count++
	}
	w.mu.Unlock()
}

func (w *Window) Snapshot(now time.Time) []Sample {
	w.mu.RLock()
	out := make([]Sample, 0, w.count)
	cutoff := now.Add(-w.period)
	start := w.next - w.count
	if start < 0 {
		start += len(w.buf)
	}
	for i := 0; i < w.count; i++ {
		s := w.buf[(start+i)%len(w.buf)]
		if !s.At.IsZero() && !s.At.Before(cutoff) {
			out = append(out, s)
		}
	}
	w.mu.RUnlock()
	return out
}

func AggregatePath(nodeID, receiverID string, samples []Sample, interval time.Duration, receiver ReceiverHealth, thresholds Thresholds, now time.Time) PathAggregate {
	expected := 0
	if interval > 0 && len(samples) > 0 {
		first := samples[0].At
		span := now.Sub(first)
		if span < 0 {
			span = 0
		}
		expected = int(span/interval) + 1
	}
	if expected < thresholds.MinSamples {
		expected = thresholds.MinSamples
	}

	rtts := make([]time.Duration, 0, len(samples))
	diffs := make([]time.Duration, 0, len(samples))
	sent, received := 0, 0
	var lastValid time.Time
	var previous time.Duration
	havePrevious := false
	for _, s := range samples {
		switch s.Status {
		case SampleOK:
			sent++
			received++
			rtts = append(rtts, s.RTT)
			if havePrevious {
				d := s.RTT - previous
				if d < 0 {
					d = -d
				}
				diffs = append(diffs, d)
			}
			previous = s.RTT
			havePrevious = true
			if s.At.After(lastValid) {
				lastValid = s.At
			}
		case SampleLoss:
			sent++
		case SampleInvalid:
			// Locally invalid observations never become VPN packet loss.
		}
	}
	coverage := 0.0
	if expected > 0 {
		coverage = float64(sent) / float64(expected)
		if coverage > 1 {
			coverage = 1
		}
	}
	loss := 0.0
	if sent > 0 {
		loss = float64(sent-received) / float64(sent)
	}
	result := PathAggregate{
		NodeID: nodeID, ReceiverID: receiverID, Sent: sent, Received: received,
		Loss: loss, Coverage: coverage, LastValid: lastValid, SampleCount: len(samples),
		Expected: expected, HealthyInput: receiver.Usable(thresholds.ReceiverConfidence),
	}
	if !result.HealthyInput || sent < thresholds.MinSamples || coverage < thresholds.MinCoverage || lastValid.IsZero() || now.Sub(lastValid) > thresholds.StaleAfter {
		result.State = StateUnknown
		return result
	}
	if len(rtts) > 0 {
		sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
		result.MedianRTT = percentileDuration(rtts, 0.50)
		result.P90RTT = percentileDuration(rtts, 0.90)
		result.P95RTT = percentileDuration(rtts, 0.95)
	}
	if len(diffs) > 0 {
		sort.Slice(diffs, func(i, j int) bool { return diffs[i] < diffs[j] })
		result.Jitter = percentileDuration(diffs, 0.50)
	}
	switch {
	case loss >= thresholds.OfflineLoss:
		result.State = StateOffline
	case loss >= thresholds.DegradedLoss || result.Jitter >= thresholds.Jitter:
		result.State = StateDegraded
	default:
		result.State = StateOnline
	}
	return result
}

func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1)*p + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

type weightedDuration struct {
	value  time.Duration
	weight float64
}

func weightedMedian(values []weightedDuration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i].value < values[j].value })
	total := 0.0
	for _, v := range values {
		total += v.weight
	}
	half := total / 2
	seen := 0.0
	for _, v := range values {
		seen += v.weight
		if seen >= half {
			return v.value
		}
	}
	return values[len(values)-1].value
}

func AggregateNode(nodeID string, paths []PathAggregate, health map[string]ReceiverHealth, thresholds Thresholds, enabled, supported bool) NodeAggregate {
	result := NodeAggregate{NodeID: nodeID, Paths: paths, TotalReceivers: len(paths)}
	if !enabled {
		result.State = StateDisabled
		return result
	}
	if !supported {
		result.State = StateUnsupported
		return result
	}

	var rttValues, p95Values, jitterValues []weightedDuration
	losses := make([]float64, 0, len(paths))
	known, online, degraded, offline := 0, 0, 0, 0
	for _, p := range paths {
		h := health[p.ReceiverID]
		if !h.Usable(thresholds.ReceiverConfidence) {
			continue
		}
		result.HealthyReceivers++
		if p.State == StateUnknown {
			continue
		}
		known++
		switch p.State {
		case StateOnline:
			online++
		case StateDegraded:
			degraded++
		case StateOffline:
			offline++
		}
		weight := h.Confidence * p.Coverage
		if weight <= 0 {
			weight = 0.01
		}
		if p.Received > 0 {
			rttValues = append(rttValues, weightedDuration{p.MedianRTT, weight})
			p95Values = append(p95Values, weightedDuration{p.P95RTT, weight})
			jitterValues = append(jitterValues, weightedDuration{p.Jitter, weight})
			if p.LastValid.After(result.LastValid) {
				result.LastValid = p.LastValid
			}
		}
		losses = append(losses, p.Loss)
	}

	if result.HealthyReceivers == 0 || known == 0 {
		result.State = StateUnknown
		return result
	}
	result.RTT = weightedMedian(rttValues)
	result.P95 = weightedMedian(p95Values)
	result.Jitter = weightedMedian(jitterValues)
	result.ReceiverSuccessRatio = float64(online+degraded) / float64(known)
	if len(losses) > 0 {
		sortedLoss := append([]float64(nil), losses...)
		sort.Float64s(sortedLoss)
		result.LossConsensus = sortedLoss[(len(sortedLoss)-1)/2]
		result.LossWorstHealthy = sortedLoss[len(sortedLoss)-1]
		result.ReceiverDisagreement = sortedLoss[len(sortedLoss)-1] - sortedLoss[0]
	}
	if result.HealthyReceivers == 1 {
		result.Confidence = "single_receiver"
	} else {
		result.Confidence = "multi_receiver"
	}

	switch {
	case offline == known:
		result.State = StateOffline
	case offline > 0 || degraded > 0 || (online > 0 && known < result.HealthyReceivers) || result.ReceiverDisagreement >= thresholds.Disagreement:
		result.State = StateDegraded
	case online == known:
		result.State = StateOnline
	default:
		result.State = StateUnknown
	}
	return result
}
