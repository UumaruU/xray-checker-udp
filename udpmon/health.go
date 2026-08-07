package udpmon

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"xray-checker/udpmon/protocol"
)

type directReceiver struct {
	id          string
	target      string
	keyVersion  uint16
	key         []byte
	heartbeatTS int64
	healthJSON  string
	nextDue     time.Time
	window      [60]bool
	next        int
	count       int
	rtt         time.Duration
	lastSeen    time.Time
}

type directPending struct {
	receiverID string
	probeID    uint64
	sentAt     time.Time
	deadline   time.Time
	nonce      [12]byte
	key        []byte
}

type reportedPeer struct {
	ID         string  `json:"id"`
	Sent       int     `json:"sent"`
	Received   int     `json:"received"`
	Loss       float64 `json:"loss"`
	RTTMS      float64 `json:"rttMs"`
	LastSeenMS int64   `json:"lastSeenMs"`
}

type reportedHealth struct {
	Peers []reportedPeer `json:"peers"`
}

// DirectHealthMonitor checks Checker→Receiver independently from proxied VLESS
// traffic and folds Receiver↔Receiver observations into Receiver health. It never
// decides VLESS state itself.
type DirectHealthMonitor struct {
	mu          sync.RWMutex
	receivers   map[string]*directReceiver
	interval    time.Duration
	pendingMu   sync.Mutex
	pending     map[uint64]directPending
	probeID     atomic.Uint64
	noncePrefix [4]byte
}

func NewDirectHealthMonitor() (*DirectHealthMonitor, error) {
	m := &DirectHealthMonitor{receivers: make(map[string]*directReceiver), interval: time.Second, pending: make(map[uint64]directPending)}
	if _, err := rand.Read(m.noncePrefix[:]); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *DirectHealthMonitor) SetConfig(cfg ControlConfig) error {
	interval := time.Duration(cfg.Settings.DirectReceiverIntervalMS) * time.Millisecond
	if interval < 200*time.Millisecond {
		interval = time.Second
	}
	now := time.Now()
	next := make(map[string]*directReceiver, len(cfg.Receivers))
	m.mu.RLock()
	for _, item := range cfg.Receivers {
		if !item.Enabled {
			continue
		}
		key, err := base64.StdEncoding.DecodeString(item.Key)
		if err != nil || len(key) < 16 {
			m.mu.RUnlock()
			return ErrInvalidReceiverKey
		}
		r := m.receivers[item.ID]
		if r == nil {
			r = &directReceiver{id: item.ID}
		}
		r.target = net.JoinHostPort(item.Host, strconv.Itoa(item.UDPPort))
		r.keyVersion = item.KeyVersion
		r.key = key
		r.heartbeatTS = item.HeartbeatTS
		r.healthJSON = item.Health
		if r.nextDue.IsZero() {
			r.nextDue = now.Add(healthPhase(item.ID, interval))
		}
		next[item.ID] = r
	}
	m.mu.RUnlock()
	m.mu.Lock()
	m.receivers = next
	m.interval = interval
	m.mu.Unlock()
	return nil
}

var ErrInvalidReceiverKey = &receiverKeyError{}

type receiverKeyError struct{}

func (*receiverKeyError) Error() string { return "invalid Receiver data-plane key" }

func healthPhase(id string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return time.Duration(protocol.IDFromString(id) % uint64(interval))
}

func (m *DirectHealthMonitor) Run(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() { <-ctx.Done(); _ = conn.Close() }()
	go m.readLoop(ctx, conn)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			m.schedule(conn, now)
			m.expire(now)
		}
	}
}

func (m *DirectHealthMonitor) schedule(conn *net.UDPConn, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, receiver := range m.receivers {
		if now.Before(receiver.nextDue) {
			continue
		}
		receiver.nextDue = receiver.nextDue.Add(m.interval)
		if now.Sub(receiver.nextDue) > m.interval {
			receiver.nextDue = now.Add(m.interval)
		}
		probeID := m.probeID.Add(1)
		var nonce [12]byte
		copy(nonce[:4], m.noncePrefix[:])
		binary.BigEndian.PutUint64(nonce[4:], probeID)
		msg := protocol.Message{
			Type: protocol.TypePeerRequest, KeyVersion: receiver.keyVersion,
			ReceiverID: protocol.IDFromString(receiver.id), NodeID: protocol.IDFromString("checker-direct"),
			ProbeID: probeID, Sequence: probeID, SentAtMS: now.UnixMilli(), Nonce: nonce,
		}
		var packetBuf [protocol.MaxPacketSize]byte
		packet, err := protocol.Encode(packetBuf[:0], msg, receiver.key)
		if err != nil {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", receiver.target)
		if err != nil {
			continue
		}
		m.pendingMu.Lock()
		if len(m.pending) >= 4096 {
			m.pendingMu.Unlock()
			continue
		}
		m.pending[probeID] = directPending{receiverID: receiver.id, probeID: probeID, sentAt: now, deadline: now.Add(2 * time.Second), nonce: nonce, key: receiver.key}
		m.pendingMu.Unlock()
		if _, err := conn.WriteToUDP(packet, addr); err != nil {
			m.pendingMu.Lock()
			delete(m.pending, probeID)
			m.pendingMu.Unlock()
		}
	}
}

func (m *DirectHealthMonitor) readLoop(ctx context.Context, conn *net.UDPConn) {
	var buf [protocol.MaxPacketSize]byte
	for {
		n, _, err := conn.ReadFromUDP(buf[:])
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if n < protocol.MinPacketSize {
			continue
		}
		probeID := binary.BigEndian.Uint64(buf[24:32])
		m.pendingMu.Lock()
		pending, ok := m.pending[probeID]
		m.pendingMu.Unlock()
		if !ok {
			continue
		}
		msg, err := protocol.Decode(buf[:n], pending.key)
		if err != nil || msg.Type != protocol.TypePeerResponse || msg.ProbeID != pending.probeID || msg.Nonce != pending.nonce {
			continue
		}
		m.pendingMu.Lock()
		if _, ok := m.pending[probeID]; !ok {
			m.pendingMu.Unlock()
			continue
		}
		delete(m.pending, probeID)
		m.pendingMu.Unlock()
		m.record(pending.receiverID, true, time.Since(pending.sentAt), time.Now())
	}
}

func (m *DirectHealthMonitor) expire(now time.Time) {
	var lost []string
	m.pendingMu.Lock()
	for id, pending := range m.pending {
		if !pending.deadline.After(now) {
			lost = append(lost, pending.receiverID)
			delete(m.pending, id)
		}
	}
	m.pendingMu.Unlock()
	for _, id := range lost {
		m.record(id, false, 0, now)
	}
}

func (m *DirectHealthMonitor) record(id string, ok bool, rtt time.Duration, now time.Time) {
	m.mu.Lock()
	r := m.receivers[id]
	if r != nil {
		r.window[r.next] = ok
		r.next = (r.next + 1) % len(r.window)
		if r.count < len(r.window) {
			r.count++
		}
		if ok {
			r.rtt = rtt
			r.lastSeen = now
		}
	}
	m.mu.Unlock()
}

func receiverDirectLoss(r *directReceiver) float64 {
	if r.count == 0 {
		return 1
	}
	received := 0
	for i := 0; i < r.count; i++ {
		if r.window[i] {
			received++
		}
	}
	return float64(r.count-received) / float64(r.count)
}

func peerReports(targetID string, receivers map[string]*directReceiver, now time.Time) (success float64, reports int) {
	for sourceID, source := range receivers {
		if sourceID == targetID || source.healthJSON == "" {
			continue
		}
		var health reportedHealth
		if json.Unmarshal([]byte(source.healthJSON), &health) != nil {
			continue
		}
		for _, p := range health.Peers {
			if p.ID != targetID || p.Sent == 0 || p.LastSeenMS == 0 {
				continue
			}
			if now.Sub(time.UnixMilli(p.LastSeenMS)) > 15*time.Second {
				continue
			}
			success += 1 - p.Loss
			reports++
		}
	}
	if reports > 0 {
		success /= float64(reports)
	}
	return success, reports
}

func (m *DirectHealthMonitor) Snapshot(now time.Time) map[string]ReceiverHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]ReceiverHealth, len(m.receivers))
	ids := make([]string, 0, len(m.receivers))
	for id := range m.receivers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := m.receivers[id]
		directLoss := receiverDirectLoss(r)
		heartbeatAge := time.Duration(1<<63 - 1)
		if r.heartbeatTS > 0 {
			heartbeatAge = now.Sub(time.Unix(r.heartbeatTS, 0))
		}
		peerSuccess, peerCount := peerReports(id, m.receivers, now)
		h := ReceiverHealth{ID: id, HeartbeatAge: heartbeatAge, DirectRTT: r.rtt, DirectLoss: directLoss, PeerSuccess: peerSuccess}
		directHealthy := r.count >= 3 && directLoss <= 0.20 && !r.lastSeen.IsZero() && now.Sub(r.lastSeen) <= 5*time.Second
		peerHealthy := peerCount == 0 || peerSuccess >= 0.70
		switch {
		case heartbeatAge <= 15*time.Second && directHealthy && peerHealthy:
			h.State, h.Confidence = ReceiverHealthy, 1.0
		case heartbeatAge > 15*time.Second && r.count >= 3 && directLoss >= 0.95 && peerCount > 0 && peerSuccess < 0.20:
			h.State, h.Confidence = ReceiverUnreachable, 0
		case r.count == 0 && heartbeatAge > 15*time.Second:
			h.State, h.Confidence = ReceiverUnknown, 0
		default:
			h.State, h.Confidence = ReceiverDegraded, 0.40
		}
		out[id] = h
	}
	return out
}
