package udpmon

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"xray-checker/udpmon/protocol"
	"xray-checker/udpmon/socks5udp"
)

type RuntimeConfig struct {
	Workers          int
	QueueCapacity    int
	SchedulerTick    time.Duration
	ExpirationTick   time.Duration
	MaxPending       int
	WindowDuration   time.Duration
	WindowHardMax    int
	HandshakeTimeout time.Duration
}

func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		Workers: 4, QueueCapacity: 8192, SchedulerTick: 10 * time.Millisecond,
		ExpirationTick: 25 * time.Millisecond, MaxPending: 65536,
		WindowDuration: 30 * time.Second, WindowHardMax: 3000,
		HandshakeTimeout: 3 * time.Second,
	}
}

type RuntimeStats struct {
	Scheduler        SchedulerStats
	Pending          int
	Sent             uint64
	Received         uint64
	TimedOut         uint64
	Invalid          uint64
	AssociationError uint64
	ActiveAssociations int
}

type nodeAssociation struct {
	mu        sync.Mutex
	assoc     *socks5udp.Association
	backoff   time.Duration
	nextRetry time.Time
}

type Runtime struct {
	cfg       RuntimeConfig
	scheduler *Scheduler
	pending   *PendingTracker

	pathsMu sync.RWMutex
	paths   map[string]*RuntimePath

	assocMu sync.Mutex
	assocs  map[string]*nodeAssociation

	probeID atomic.Uint64
	noncePrefix [4]byte

	sent      atomic.Uint64
	received  atomic.Uint64
	timedOut  atomic.Uint64
	invalid   atomic.Uint64
	assocErr  atomic.Uint64

	OnSample func(nodeID, receiverID string, sample Sample)
}

func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.ExpirationTick <= 0 {
		cfg.ExpirationTick = 25 * time.Millisecond
	}
	var prefix [4]byte
	if _, err := rand.Read(prefix[:]); err != nil {
		return nil, err
	}
	return &Runtime{
		cfg: cfg,
		scheduler: NewScheduler(cfg.QueueCapacity, cfg.SchedulerTick),
		pending: NewPendingTracker(cfg.MaxPending, cfg.ExpirationTick, 512),
		paths: make(map[string]*RuntimePath), assocs: make(map[string]*nodeAssociation),
		noncePrefix: prefix,
	}, nil
}

func pathKey(nodeID, receiverID string) string { return nodeID + "\x00" + receiverID }

// SetPaths atomically replaces the active node×receiver set. Exclusions and
// All/Selected/Automatic receiver selection are resolved by the control plane
// before paths reach the hot scheduler.
func (r *Runtime) SetPaths(configs []PathConfig) {
	now := time.Now()
	paths := make([]*RuntimePath, 0, len(configs))
	newMap := make(map[string]*RuntimePath, len(configs))
	r.pathsMu.RLock()
	old := r.paths
	for _, cfg := range configs {
		key := pathKey(cfg.NodeID, cfg.ReceiverID)
		path := old[key]
		if path == nil || path.Config.ProbeInterval != cfg.ProbeInterval {
			path = &RuntimePath{Config: cfg, Window: NewWindow(r.cfg.WindowDuration, cfg.ProbeInterval, r.cfg.WindowHardMax)}
		} else {
			path.Config = cfg
		}
		paths = append(paths, path)
		newMap[key] = path
	}
	r.pathsMu.RUnlock()
	r.pathsMu.Lock()
	r.paths = newMap
	r.pathsMu.Unlock()
	r.scheduler.SetPaths(paths, now)

	// Associations for removed nodes are closed immediately; remaining nodes keep
	// their persistent SOCKS control/UDP sockets across receiver-list changes.
	activeNodes := make(map[string]bool, len(configs))
	for _, cfg := range configs {
		activeNodes[cfg.NodeID] = true
	}
	r.assocMu.Lock()
	for nodeID, state := range r.assocs {
		if activeNodes[nodeID] {
			continue
		}
		state.mu.Lock()
		if state.assoc != nil {
			_ = state.assoc.Close()
			state.assoc = nil
		}
		state.mu.Unlock()
		delete(r.assocs, nodeID)
	}
	r.assocMu.Unlock()
}

func (r *Runtime) Run(ctx context.Context) {
	go r.scheduler.Run(ctx)
	for i := 0; i < r.cfg.Workers; i++ {
		go r.worker(ctx)
	}
	go r.expirer(ctx)
	<-ctx.Done()
	r.pending.Clear()
	r.closeAssociations()
}

func (r *Runtime) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-r.scheduler.Jobs():
			now := time.Now()
			if lag := now.Sub(job.Due); lag > 0 {
				r.scheduler.SetLag(lag)
			}
			r.sendProbe(ctx, job.Path, now)
		}
	}
}

func (r *Runtime) sendProbe(ctx context.Context, path *RuntimePath, now time.Time) {
	assoc, err := r.association(ctx, path.Config.NodeID, path.Config.SocksAddress)
	if err != nil {
		// No packet left the local Checker/Xray infrastructure, so this is an
		// absent/invalid observation, never network LOSS.
		r.invalid.Add(1)
		return
	}
	probeID := r.probeID.Add(1)
	sequence := path.seq.Add(1)
	var nonce [12]byte
	copy(nonce[:4], r.noncePrefix[:])
	binary.BigEndian.PutUint64(nonce[4:], probeID)
	msg := protocol.Message{
		Type: protocol.TypeProbeRequest, KeyVersion: path.Config.KeyVersion,
		ReceiverID: protocol.IDFromString(path.Config.ReceiverID), NodeID: protocol.IDFromString(path.Config.NodeID),
		ProbeID: probeID, Sequence: sequence, SentAtMS: now.UnixMilli(), Nonce: nonce,
	}
	var packetBuf [protocol.MaxPacketSize]byte
	packet, err := protocol.Encode(packetBuf[:0], msg, path.Config.Key)
	if err != nil {
		r.invalid.Add(1)
		return
	}
	entry := PendingProbe{
		ProbeID: probeID, Sequence: sequence, SentAt: now,
		Deadline: now.Add(path.Config.ProbeTimeout), Nonce: nonce, Path: path,
	}
	if !r.pending.Put(entry) {
		// Capacity pressure means the probe is not sent; the graph naturally gets
		// less dense instead of fabricating a local-drop data point.
		return
	}
	if err := assoc.WriteTo(packet, path.Config.Target); err != nil {
		_, _ = r.pending.Take(probeID)
		r.invalid.Add(1)
		r.invalidateAssociation(path.Config.NodeID, assoc)
		return
	}
	r.sent.Add(1)
}

func (r *Runtime) association(ctx context.Context, nodeID, socksAddress string) (*socks5udp.Association, error) {
	r.assocMu.Lock()
	state := r.assocs[nodeID]
	if state == nil {
		state = &nodeAssociation{backoff: 250 * time.Millisecond}
		r.assocs[nodeID] = state
	}
	r.assocMu.Unlock()

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.assoc != nil {
		return state.assoc, nil
	}
	if time.Now().Before(state.nextRetry) {
		return nil, errors.New("SOCKS5 association reconnect is in backoff")
	}
	assoc, err := socks5udp.Dial(ctx, socksAddress, r.cfg.HandshakeTimeout)
	if err != nil {
		state.nextRetry = time.Now().Add(state.backoff)
		state.backoff *= 2
		if state.backoff > 30*time.Second {
			state.backoff = 30 * time.Second
		}
		r.assocErr.Add(1)
		return nil, err
	}
	state.assoc = assoc
	state.backoff = 250 * time.Millisecond
	state.nextRetry = time.Time{}
	go r.readLoop(ctx, nodeID, assoc)
	return assoc, nil
}

func (r *Runtime) readLoop(ctx context.Context, nodeID string, assoc *socks5udp.Association) {
	buffer := make([]byte, 2048)
	for {
		_, payload, err := assoc.ReadFrom(buffer)
		if err != nil {
			if ctx.Err() == nil {
				r.invalidateAssociation(nodeID, assoc)
			}
			return
		}
		if len(payload) < protocol.MinPacketSize {
			r.invalid.Add(1)
			continue
		}
		probeID := binary.BigEndian.Uint64(payload[24:32])
		pending, ok := r.pending.Peek(probeID)
		if !ok || pending.Path == nil {
			continue
		}
		msg, err := protocol.Decode(payload, pending.Path.Config.Key)
		if err != nil || msg.Type != protocol.TypeProbeResponse || msg.ProbeID != pending.ProbeID || msg.Sequence != pending.Sequence || msg.Nonce != pending.Nonce || msg.ReceiverID != protocol.IDFromString(pending.Path.Config.ReceiverID) || msg.NodeID != protocol.IDFromString(pending.Path.Config.NodeID) {
			r.invalid.Add(1)
			continue
		}
		pending, ok = r.pending.Take(probeID)
		if !ok {
			continue
		}
		now := time.Now()
		sample := Sample{At: now, Sequence: pending.Sequence, RTT: now.Sub(pending.SentAt), Status: SampleOK}
		r.received.Add(1)
		r.record(pending.Path, sample)
	}
}

func (r *Runtime) expirer(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ExpirationTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, pending := range r.pending.Expire(now) {
				r.timedOut.Add(1)
				r.record(pending.Path, Sample{At: now, Sequence: pending.Sequence, Status: SampleLoss})
			}
		}
	}
}

func (r *Runtime) record(path *RuntimePath, sample Sample) {
	if path == nil {
		return
	}
	path.Window.Add(sample)
	if r.OnSample != nil {
		r.OnSample(path.Config.NodeID, path.Config.ReceiverID, sample)
	}
}

func (r *Runtime) invalidateAssociation(nodeID string, assoc *socks5udp.Association) {
	r.assocMu.Lock()
	state := r.assocs[nodeID]
	r.assocMu.Unlock()
	if state != nil {
		state.mu.Lock()
		if state.assoc == assoc {
			_ = state.assoc.Close()
			state.assoc = nil
			state.nextRetry = time.Now().Add(state.backoff)
		}
		state.mu.Unlock()
	}
	for _, pending := range r.pending.CancelNode(nodeID) {
		r.invalid.Add(1)
		// Keep this out of packet-loss statistics. It is retained internally only
		// as an invalid observation and renders as a graph gap.
		r.record(pending.Path, Sample{At: time.Now(), Sequence: pending.Sequence, Status: SampleInvalid})
	}
}

func (r *Runtime) closeAssociations() {
	r.assocMu.Lock()
	defer r.assocMu.Unlock()
	for _, state := range r.assocs {
		state.mu.Lock()
		if state.assoc != nil {
			_ = state.assoc.Close()
			state.assoc = nil
		}
		state.mu.Unlock()
	}
	clear(r.assocs)
}

func (r *Runtime) PathSnapshot(nodeID, receiverID string, now time.Time) []Sample {
	r.pathsMu.RLock()
	path := r.paths[pathKey(nodeID, receiverID)]
	r.pathsMu.RUnlock()
	if path == nil {
		return nil
	}
	return path.Window.Snapshot(now)
}

func (r *Runtime) Stats() RuntimeStats {
	r.assocMu.Lock()
	active := 0
	for _, state := range r.assocs {
		state.mu.Lock()
		if state.assoc != nil {
			active++
		}
		state.mu.Unlock()
	}
	r.assocMu.Unlock()
	return RuntimeStats{
		Scheduler: r.scheduler.Stats(), Pending: r.pending.Len(), Sent: r.sent.Load(),
		Received: r.received.Load(), TimedOut: r.timedOut.Load(), Invalid: r.invalid.Load(),
		AssociationError: r.assocErr.Load(), ActiveAssociations: active,
	}
}
