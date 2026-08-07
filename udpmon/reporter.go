package udpmon

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type reportSample struct {
	nodeID     string
	receiverID string
	interval   time.Duration
	sample     Sample
}

type liveBlockKey struct {
	nodeID     string
	receiverID string
	startMS    int64
	intervalUS uint32
}

type LiveBlock struct {
	NodeID     string `json:"nodeId"`
	ReceiverID string `json:"receiverId"`
	StartMS    int64  `json:"startMs"`
	IntervalUS uint32 `json:"intervalUs"`
	Slots      string `json:"slots"`
}

type PanelReport struct {
	Revision        int64                     `json:"revision"`
	GeneratedAtMS   int64                     `json:"generatedAtMs"`
	Blocks          []LiveBlock               `json:"blocks"`
	Paths           []PathAggregate           `json:"paths"`
	Nodes           []NodeAggregate           `json:"nodes"`
	ReceiverHealth  map[string]ReceiverHealth `json:"receiverHealth"`
	Runtime         RuntimeStats              `json:"runtime"`
	ReporterQueue   int                       `json:"reporterQueue"`
	ReporterDropped uint64                    `json:"reporterDropped"`
	Load            LoadStatus                `json:"load"`
}

type Reporter struct {
	url               string
	token             string
	runtime           *Runtime
	health            *DirectHealthMonitor
	http              *http.Client
	input             chan reportSample
	dropped           atomic.Uint64
	sendErrors        atomic.Uint64
	configApplyErrors atomic.Uint64
	cfgMu             sync.RWMutex
	cfg               ControlConfig
	loadMu            sync.RWMutex
	load              LoadStatus
	states            *StateTracker
}

func NewReporter(panelURL, token string, runtime *Runtime, health *DirectHealthMonitor) *Reporter {
	return &Reporter{
		url: strings.TrimRight(panelURL, "/") + "/api/v1/udp/batch", token: token,
		runtime: runtime, health: health, http: &http.Client{Timeout: 5 * time.Second},
		input:  make(chan reportSample, 65536),
		states: NewStateTracker(),
	}
}

func (r *Reporter) UpdateConfig(cfg ControlConfig) {
	r.cfgMu.Lock()
	r.cfg = cfg
	r.cfgMu.Unlock()
}

func (r *Reporter) UpdateLoad(status LoadStatus) { r.loadMu.Lock(); r.load = status; r.loadMu.Unlock() }

func (r *Reporter) Ingest(nodeID, receiverID string, interval time.Duration, sample Sample) {
	select {
	case r.input <- reportSample{nodeID: nodeID, receiverID: receiverID, interval: interval, sample: sample}:
	default:
		r.dropped.Add(1)
	}
}

func (r *Reporter) Run(ctx context.Context) {
	blocks := make(map[liveBlockKey]*CompactBlock)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastFlush := time.Now()
	var lastAggregate time.Time
	var cachedPaths []PathAggregate
	var cachedNodes []NodeAggregate
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-r.input:
			start := item.sample.At.Truncate(time.Second)
			key := liveBlockKey{nodeID: item.nodeID, receiverID: item.receiverID, startMS: start.UnixMilli(), intervalUS: uint32(item.interval / time.Microsecond)}
			block := blocks[key]
			if block == nil {
				b := NewCompactBlock(start, item.interval, time.Second)
				block = &b
				blocks[key] = block
			}
			block.Put(item.sample)
		case now := <-ticker.C:
			r.cfgMu.RLock()
			cfg := r.cfg
			r.cfgMu.RUnlock()
			interval := time.Duration(cfg.Settings.CheckerBatchMS) * time.Millisecond
			if interval <= 0 {
				interval = time.Second
			}
			if now.Sub(lastFlush) < interval {
				continue
			}
			if len(blocks) == 0 && len(cfg.Receivers) == 0 {
				lastFlush = now
				continue
			}
			outBlocks := make([]LiveBlock, 0, len(blocks))
			for key, block := range blocks {
				raw := make([]byte, len(block.Slots)*2)
				for i, value := range block.Slots {
					binary.BigEndian.PutUint16(raw[i*2:i*2+2], value)
				}
				outBlocks = append(outBlocks, LiveBlock{NodeID: key.nodeID, ReceiverID: key.receiverID, StartMS: key.startMS, IntervalUS: key.intervalUS, Slots: base64.StdEncoding.EncodeToString(raw)})
			}
			blocks = make(map[liveBlockKey]*CompactBlock)
			health := r.health.Snapshot(now)
			aggregateEvery := time.Duration(cfg.Settings.AggregationMS) * time.Millisecond
			if aggregateEvery <= 0 {
				aggregateEvery = time.Second
			}
			if lastAggregate.IsZero() || now.Sub(lastAggregate) >= aggregateEvery {
				cachedPaths, cachedNodes = r.runtime.Aggregates(now, health, cfg.Settings.Thresholds())
				cachedNodes = r.states.Apply(cachedNodes, cfg.Settings)
				lastAggregate = now
			}
			r.loadMu.RLock()
			load := r.load
			r.loadMu.RUnlock()
			report := PanelReport{
				Revision: cfg.Revision, GeneratedAtMS: now.UnixMilli(), Blocks: outBlocks,
				Paths: cachedPaths, Nodes: cachedNodes, ReceiverHealth: health, Runtime: r.runtime.Stats(),
				ReporterQueue: len(r.input), ReporterDropped: r.dropped.Load(),
				Load: load,
			}
			if err := r.send(ctx, report); err != nil {
				r.sendErrors.Add(1)
			}
			lastFlush = now
		}
	}
}

func (r *Reporter) send(ctx context.Context, report PanelReport) error {
	var body bytes.Buffer
	gz := gzip.NewWriter(&body)
	if err := json.NewEncoder(gz).Encode(report); err != nil {
		_ = gz.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Panel UDP batch returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func (r *Reporter) Dropped() uint64           { return r.dropped.Load() }
func (r *Reporter) QueueLen() int             { return len(r.input) }
func (r *Reporter) QueueCapacity() int        { return cap(r.input) }
func (r *Reporter) SendErrors() uint64        { return r.sendErrors.Load() }
func (r *Reporter) MarkConfigApplyError()     { r.configApplyErrors.Add(1) }
func (r *Reporter) ConfigApplyErrors() uint64 { return r.configApplyErrors.Load() }

func (r *Reporter) Revision() int64 {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.cfg.Revision
}

func (r *Reporter) LoadSnapshot() LoadStatus {
	r.loadMu.RLock()
	defer r.loadMu.RUnlock()
	return r.load
}
