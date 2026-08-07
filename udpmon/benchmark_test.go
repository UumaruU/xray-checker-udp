package udpmon

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkScheduler10Hz(b *testing.B) {
	scenarios := []struct {
		name      string
		nodes     int
		receivers int
	}{
		{"100x1", 100, 1},
		{"500x3", 500, 3},
		{"700x3", 700, 3},
		{"1000x3", 1000, 3},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			count := scenario.nodes * scenario.receivers
			paths := make([]*RuntimePath, 0, count)
			for n := 0; n < scenario.nodes; n++ {
				for r := 0; r < scenario.receivers; r++ {
					paths = append(paths, &RuntimePath{Config: PathConfig{
						NodeID: "n" + strconv.Itoa(n), ReceiverID: "r" + strconv.Itoa(r), ProbeInterval: 100 * time.Millisecond,
					}})
				}
			}
			scheduler := NewScheduler(count*2, time.Millisecond)
			now := time.Unix(1700000000, 0)
			scheduler.SetPaths(paths, now)
			b.ReportAllocs()
			b.ReportMetric(float64(count*10), "target-probes/s")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now = now.Add(100 * time.Millisecond)
				scheduler.schedule(now)
				for len(scheduler.jobs) > 0 {
					<-scheduler.jobs
				}
			}
		})
	}
}

func BenchmarkAggregatePath300Samples(b *testing.B) {
	now := time.Unix(1700000000, 0)
	samples := make([]Sample, 300)
	for i := range samples {
		samples[i] = Sample{At: now.Add(time.Duration(i-299) * 100 * time.Millisecond), Sequence: uint64(i), RTT: time.Duration(20+i%7) * time.Millisecond, Status: SampleOK}
	}
	health := ReceiverHealth{ID: "r", State: ReceiverHealthy, Confidence: 1}
	thresholds := DefaultThresholds()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = AggregatePath("n", "r", samples, 100*time.Millisecond, health, thresholds, now)
	}
}
