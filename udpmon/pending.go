package udpmon

import (
	"sync"
	"time"
)

type PendingProbe struct {
	ProbeID  uint64
	Sequence uint64
	SentAt   time.Time
	Deadline time.Time
	Nonce    [12]byte
	Path     *RuntimePath
}

type pendingShard struct {
	mu sync.RWMutex
	m  map[uint64]PendingProbe
}

// PendingTracker combines sharded lookup with a bounded expiration wheel.
// Expiration is driven by one Runtime ticker; no probe owns a timer.
type PendingTracker struct {
	shards     []pendingShard
	wheelMu    sync.Mutex
	wheel      [][]uint64
	tick       time.Duration
	maxPending int
	count      int
}

func NewPendingTracker(maxPending int, tick time.Duration, wheelSlots int) *PendingTracker {
	if maxPending < 1 {
		maxPending = 65536
	}
	if tick <= 0 {
		tick = 25 * time.Millisecond
	}
	if wheelSlots < 16 {
		wheelSlots = 256
	}
	p := &PendingTracker{
		shards: make([]pendingShard, 64), wheel: make([][]uint64, wheelSlots),
		tick: tick, maxPending: maxPending,
	}
	for i := range p.shards {
		p.shards[i].m = make(map[uint64]PendingProbe)
	}
	return p
}

func (p *PendingTracker) shard(id uint64) *pendingShard {
	return &p.shards[id%uint64(len(p.shards))]
}

func (p *PendingTracker) Put(entry PendingProbe) bool {
	p.wheelMu.Lock()
	if p.count >= p.maxPending {
		p.wheelMu.Unlock()
		return false
	}
	p.count++
	slot := int((entry.Deadline.UnixNano() / p.tick.Nanoseconds()) % int64(len(p.wheel)))
	p.wheel[slot] = append(p.wheel[slot], entry.ProbeID)
	p.wheelMu.Unlock()

	shard := p.shard(entry.ProbeID)
	shard.mu.Lock()
	shard.m[entry.ProbeID] = entry
	shard.mu.Unlock()
	return true
}

func (p *PendingTracker) Peek(id uint64) (PendingProbe, bool) {
	shard := p.shard(id)
	shard.mu.RLock()
	entry, ok := shard.m[id]
	shard.mu.RUnlock()
	return entry, ok
}

func (p *PendingTracker) Take(id uint64) (PendingProbe, bool) {
	shard := p.shard(id)
	shard.mu.Lock()
	entry, ok := shard.m[id]
	if ok {
		delete(shard.m, id)
	}
	shard.mu.Unlock()
	if ok {
		p.wheelMu.Lock()
		p.count--
		p.wheelMu.Unlock()
	}
	return entry, ok
}

// Expire returns probes whose deadline has passed. IDs from the same wheel slot
// that are not due yet are reinserted, which makes the wheel safe across clock
// boundaries without scanning every pending probe.
func (p *PendingTracker) Expire(now time.Time) []PendingProbe {
	slot := int((now.UnixNano() / p.tick.Nanoseconds()) % int64(len(p.wheel)))
	p.wheelMu.Lock()
	ids := p.wheel[slot]
	p.wheel[slot] = nil
	p.wheelMu.Unlock()

	expired := make([]PendingProbe, 0, len(ids))
	for _, id := range ids {
		entry, ok := p.Peek(id)
		if !ok {
			continue
		}
		if entry.Deadline.After(now) {
			p.wheelMu.Lock()
			next := int((entry.Deadline.UnixNano() / p.tick.Nanoseconds()) % int64(len(p.wheel)))
			p.wheel[next] = append(p.wheel[next], id)
			p.wheelMu.Unlock()
			continue
		}
		if taken, ok := p.Take(id); ok {
			expired = append(expired, taken)
		}
	}
	return expired
}

func (p *PendingTracker) Len() int {
	p.wheelMu.Lock()
	n := p.count
	p.wheelMu.Unlock()
	return n
}

func (p *PendingTracker) Clear() []PendingProbe {
	var out []PendingProbe
	for i := range p.shards {
		shard := &p.shards[i]
		shard.mu.Lock()
		for _, entry := range shard.m {
			out = append(out, entry)
		}
		clear(shard.m)
		shard.mu.Unlock()
	}
	p.wheelMu.Lock()
	for i := range p.wheel {
		p.wheel[i] = nil
	}
	p.count = 0
	p.wheelMu.Unlock()
	return out
}

func (p *PendingTracker) CancelNode(nodeID string) []PendingProbe {
	var out []PendingProbe
	for i := range p.shards {
		shard := &p.shards[i]
		shard.mu.Lock()
		for id, entry := range shard.m {
			if entry.Path != nil && entry.Path.Config.NodeID == nodeID {
				out = append(out, entry)
				delete(shard.m, id)
			}
		}
		shard.mu.Unlock()
	}
	if len(out) > 0 {
		p.wheelMu.Lock()
		p.count -= len(out)
		if p.count < 0 {
			p.count = 0
		}
		p.wheelMu.Unlock()
	}
	return out
}
