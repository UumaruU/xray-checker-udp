package udpmon

import (
	"testing"
	"time"
)

func TestSchedulerPhasesPaths(t *testing.T) {
	s := NewScheduler(128, 10*time.Millisecond)
	now := time.Unix(100, 0)
	paths := make([]*RuntimePath, 20)
	seen := map[time.Duration]bool{}
	for i := range paths {
		paths[i] = &RuntimePath{Config: PathConfig{NodeID: string(rune('a' + i)), ReceiverID: "r", ProbeInterval: 100 * time.Millisecond}}
		seen[phaseFor(paths[i])] = true
	}
	if len(seen) < 2 {
		t.Fatal("all paths received the same scheduler phase")
	}
	s.SetPaths(paths, now)
}

func TestSchedulerQueueIsBounded(t *testing.T) {
	s := NewScheduler(1, 10*time.Millisecond)
	now := time.Unix(100, 0)
	paths := []*RuntimePath{
		{Config: PathConfig{NodeID: "a", ReceiverID: "r", ProbeInterval: 100 * time.Millisecond}},
		{Config: PathConfig{NodeID: "b", ReceiverID: "r", ProbeInterval: 100 * time.Millisecond}},
	}
	for _, p := range paths {
		p.nextDue = now
	}
	s.mu.Lock()
	s.paths = paths
	s.mu.Unlock()
	s.schedule(now)
	if got := len(s.jobs); got != 1 {
		t.Fatalf("queue=%d want=1", got)
	}
	if s.Stats().Dropped == 0 {
		t.Fatal("expected a local scheduler drop when queue is full")
	}
}
