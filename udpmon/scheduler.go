package udpmon

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type PathConfig struct {
	NodeID        string
	ReceiverID    string
	SocksAddress  string
	Target        string
	KeyVersion    uint16
	Key           []byte
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
}

type RuntimePath struct {
	Config PathConfig
	Window *Window
	seq    atomic.Uint64
	nextDue time.Time
}

type ProbeJob struct {
	Path *RuntimePath
	Due  time.Time
}

type SchedulerStats struct {
	Scheduled uint64
	Dropped   uint64
	LagNS     int64
	Queue     int
}

// Scheduler uses one ticker for all paths and a bounded worker queue. Paths are
// deterministically phased inside their interval so a subscription/config reload
// cannot create a synchronized 100ms burst.
type Scheduler struct {
	mu        sync.RWMutex
	paths     []*RuntimePath
	jobs      chan ProbeJob
	tick      time.Duration
	scheduled atomic.Uint64
	dropped   atomic.Uint64
	lagNS     atomic.Int64
}

func NewScheduler(queueCapacity int, tick time.Duration) *Scheduler {
	if queueCapacity < 1 {
		queueCapacity = 8192
	}
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	return &Scheduler{jobs: make(chan ProbeJob, queueCapacity), tick: tick}
}

func phaseFor(path *RuntimePath) time.Duration {
	interval := path.Config.ProbeInterval
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(path.Config.NodeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(path.Config.ReceiverID))
	return time.Duration(h.Sum64() % uint64(interval))
}

func (s *Scheduler) SetPaths(paths []*RuntimePath, now time.Time) {
	for _, path := range paths {
		interval := path.Config.ProbeInterval
		if interval <= 0 {
			interval = 100 * time.Millisecond
			path.Config.ProbeInterval = interval
		}
		path.nextDue = now.Add(phaseFor(path))
	}
	s.mu.Lock()
	s.paths = paths
	s.mu.Unlock()
}

func (s *Scheduler) Jobs() <-chan ProbeJob { return s.jobs }

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.schedule(now)
		}
	}
}

func (s *Scheduler) schedule(now time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, path := range s.paths {
		if now.Before(path.nextDue) {
			continue
		}
		due := path.nextDue
		interval := path.Config.ProbeInterval
		path.nextDue = path.nextDue.Add(interval)
		// Never generate catch-up storms. A missed local scheduling opportunity is
		// an absent measurement, not a synthetic packet loss event.
		if now.Sub(due) > interval {
			path.nextDue = now.Add(interval)
			s.dropped.Add(1)
			continue
		}
		select {
		case s.jobs <- ProbeJob{Path: path, Due: due}:
			s.scheduled.Add(1)
		default:
			s.dropped.Add(1)
		}
	}
}

func (s *Scheduler) SetLag(lag time.Duration) { s.lagNS.Store(int64(lag)) }

func (s *Scheduler) Stats() SchedulerStats {
	return SchedulerStats{
		Scheduled: s.scheduled.Load(), Dropped: s.dropped.Load(), LagNS: s.lagNS.Load(), Queue: len(s.jobs),
	}
}
