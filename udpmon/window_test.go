package udpmon

import (
	"testing"
	"time"
)

func TestWeightedMedianDoesNotChooseMinimumOrMean(t *testing.T) {
	got := weightedMedian([]weightedDuration{
		{value: 10 * time.Millisecond, weight: 0.1},
		{value: 40 * time.Millisecond, weight: 0.8},
		{value: 200 * time.Millisecond, weight: 0.1},
	})
	if got != 40*time.Millisecond {
		t.Fatalf("weighted median=%s want=40ms", got)
	}
}

func TestReceiverFailureCannotMakeNodeOffline(t *testing.T) {
	thresholds := DefaultThresholds()
	paths := []PathAggregate{{NodeID: "n", ReceiverID: "r", State: StateOffline, Coverage: 1}}
	health := map[string]ReceiverHealth{"r": {ID: "r", State: ReceiverUnreachable, Confidence: 0}}
	got := AggregateNode("n", paths, health, thresholds, true, true)
	if got.State != StateUnknown {
		t.Fatalf("state=%s want UNKNOWN", got.State)
	}
}

func TestRegionalFailureIsDegradedNotAveragedAway(t *testing.T) {
	thresholds := DefaultThresholds()
	paths := []PathAggregate{
		{NodeID: "n", ReceiverID: "a", State: StateOnline, Loss: 0, Coverage: 1},
		{NodeID: "n", ReceiverID: "b", State: StateOffline, Loss: 1, Coverage: 1},
	}
	health := map[string]ReceiverHealth{
		"a": {ID: "a", State: ReceiverHealthy, Confidence: 1},
		"b": {ID: "b", State: ReceiverHealthy, Confidence: 1},
	}
	got := AggregateNode("n", paths, health, thresholds, true, true)
	if got.State != StateDegraded || got.LossWorstHealthy != 1 {
		t.Fatalf("aggregate=%+v", got)
	}
}

func TestCompactBlockPreservesLossAndMissing(t *testing.T) {
	start := time.Unix(100, 0)
	b := NewCompactBlock(start, 100*time.Millisecond, time.Second)
	b.Put(Sample{At: start, RTT: 31 * time.Millisecond, Status: SampleOK})
	b.Put(Sample{At: start.Add(200 * time.Millisecond), Status: SampleLoss})
	if b.Slots[1] != CompactMissing || b.Slots[2] != CompactLoss {
		t.Fatalf("slots=%v", b.Slots[:3])
	}
	decoded, err := UnmarshalCompactBlock(b.MarshalBinary())
	if err != nil {
		t.Fatal(err)
	}
	samples := decoded.Samples()
	if len(samples) != 2 || samples[0].Status != SampleOK || samples[1].Status != SampleLoss {
		t.Fatalf("samples=%+v", samples)
	}
}
