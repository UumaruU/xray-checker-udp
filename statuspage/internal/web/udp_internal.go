package web

import (
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"xray-status/internal/poller"
	"xray-status/internal/store"
)

func (in *Internal) udpCheckerConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !constantTokenMatch(in.cfg.CheckerInternalToken, bearer(r)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	cfg, err := in.st.UDPCheckerConfig()
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

type udpLiveBlockRequest struct {
	NodeID     string `json:"nodeId"`
	ReceiverID string `json:"receiverId"`
	StartMS    int64  `json:"startMs"`
	IntervalUS uint32 `json:"intervalUs"`
	Slots      string `json:"slots"`
}

type udpPathReport struct {
	NodeID      string  `json:"nodeId"`
	ReceiverID  string  `json:"receiverId"`
	State       string  `json:"state"`
	MedianRTT   int64   `json:"medianRtt"`
	P95RTT      int64   `json:"p95Rtt"`
	Jitter      int64   `json:"jitter"`
	Loss        float64 `json:"loss"`
	Coverage    float64 `json:"coverage"`
	SampleCount int     `json:"sampleCount"`
}

type udpNodeReport struct {
	NodeID string `json:"nodeId"`
	State  string `json:"state"`
}

type udpReceiverHealthReport struct {
	State        string  `json:"state"`
	HeartbeatAge int64   `json:"heartbeatAge"`
	DirectLoss   float64 `json:"directLoss"`
}

type udpReceiverAlertState struct {
	Initialized   bool
	HeartbeatLost bool
	Unreachable   bool
}

type udpBatchRequest struct {
	Revision       int64                              `json:"revision"`
	GeneratedAtMS  int64                              `json:"generatedAtMs"`
	Blocks         []udpLiveBlockRequest              `json:"blocks"`
	Paths          []udpPathReport                    `json:"paths"`
	Nodes          []udpNodeReport                    `json:"nodes"`
	ReceiverHealth map[string]udpReceiverHealthReport `json:"receiverHealth"`
}

func (in *Internal) emitUDPEvent(event poller.Event, nodeSID, receiverID string, now time.Time) {
	_ = in.st.RecordUDPEvent(string(event.Type), nodeSID, receiverID, "{}", now)
	if in.udpOnEvent != nil {
		// Telegram/network delivery must never backpressure the Checker batch path.
		go in.udpOnEvent(event)
	}
}

func (in *Internal) detectUDPTransitions(batch udpBatchRequest, settings store.UDPSettings, now time.Time) {
	if !settings.Enabled {
		return
	}
	receivers, _ := in.st.UDPReceivers()
	receiverNames := make(map[string]string, len(receivers))
	for _, receiver := range receivers {
		receiverNames[receiver.ID] = receiver.Name
	}
	healthy := 0
	for id, health := range batch.ReceiverHealth {
		if health.State == "HEALTHY" {
			healthy++
		}
		name := receiverNames[id]
		if name == "" {
			name = id
		}
		current := udpReceiverAlertState{
			Initialized: true, HeartbeatLost: health.HeartbeatAge > int64(15*time.Second),
			Unreachable: health.State == "UNREACHABLE" || health.DirectLoss >= 0.95,
		}
		previous := in.udpReceiverState[id]
		if previous.Initialized {
			if current.HeartbeatLost != previous.HeartbeatLost {
				typ := poller.EventUDPReceiverHBRestored
				if current.HeartbeatLost {
					typ = poller.EventUDPReceiverHBLost
				}
				in.emitUDPEvent(poller.Event{Type: typ, Name: name}, "", id, now)
			}
			if current.Unreachable != previous.Unreachable {
				typ := poller.EventUDPReceiverRestored
				if current.Unreachable {
					typ = poller.EventUDPReceiverUnreachable
				}
				in.emitUDPEvent(poller.Event{Type: typ, Name: name}, "", id, now)
			}
		}
		in.udpReceiverState[id] = current
	}

	globalUnknown := len(batch.ReceiverHealth) > 0 && healthy == 0
	if !in.udpGlobalKnown {
		in.udpGlobalKnown = true
		in.udpGlobalUnknown = globalUnknown
	} else if globalUnknown != in.udpGlobalUnknown {
		in.udpGlobalUnknown = globalUnknown
		if globalUnknown {
			in.udpGlobalAlerted = true
			in.emitUDPEvent(poller.Event{Type: poller.EventUDPGlobalUnknown}, "", "", now)
		} else if in.udpGlobalAlerted {
			in.udpGlobalAlerted = false
			in.emitUDPEvent(poller.Event{Type: poller.EventUDPGlobalRestored}, "", "", now)
		}
	}

	names := make(map[string]string)
	if rows, err := in.st.CurrentRows(); err == nil {
		for _, row := range rows {
			names[row.SID] = row.Name
		}
	}
	for _, node := range batch.Nodes {
		previous, seen := in.udpNodeState[node.NodeID]
		in.udpNodeState[node.NodeID] = node.State
		if !seen || previous == node.State || globalUnknown {
			continue
		}
		name := names[node.NodeID]
		if name == "" {
			name = node.NodeID
		}
		var typ poller.EventType
		switch node.State {
		case "OFFLINE":
			typ = poller.EventUDPOffline
		case "DEGRADED":
			typ = poller.EventUDPDegraded
		case "ONLINE":
			if previous == "OFFLINE" {
				typ = poller.EventUDPRestored
			} else if previous == "DEGRADED" {
				typ = poller.EventUDPNormal
			}
		}
		if typ != "" {
			in.emitUDPEvent(poller.Event{Type: typ, Name: name}, node.NodeID, "", now)
		}
	}
}

func readUDPBatchBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	var reader io.Reader = http.MaxBytesReader(w, r.Body, 8<<20)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = io.LimitReader(gz, 16<<20)
	}
	return io.ReadAll(reader)
}

func mergePendingMinute(pending map[string]store.UDPLiveBlockRow, block udpLiveBlockRequest) error {
	raw, err := base64.StdEncoding.DecodeString(block.Slots)
	if err != nil || len(raw) == 0 || len(raw)%2 != 0 || block.IntervalUS == 0 {
		return fmt.Errorf("invalid compact slots")
	}
	parentStart := block.StartMS - block.StartMS%60000
	parentCount := int(int64(time.Minute/time.Microsecond) / int64(block.IntervalUS))
	if parentCount < 1 || parentCount > 5000 {
		return fmt.Errorf("invalid compact interval")
	}
	key := fmt.Sprintf("%s\x00%s\x00%d\x00%d", block.NodeID, block.ReceiverID, parentStart, block.IntervalUS)
	row, ok := pending[key]
	if !ok {
		row = store.UDPLiveBlockRow{NodeSID: block.NodeID, ReceiverID: block.ReceiverID, StartMS: parentStart, IntervalUS: block.IntervalUS, Slots: make([]byte, parentCount*2)}
		for i := 0; i < parentCount; i++ {
			binary.BigEndian.PutUint16(row.Slots[i*2:i*2+2], 0xfffd)
		}
	}
	offset := int(((block.StartMS - parentStart) * 1000) / int64(block.IntervalUS))
	for i := 0; i < len(raw)/2; i++ {
		dst := offset + i
		if dst < 0 || dst >= len(row.Slots)/2 {
			continue
		}
		value := binary.BigEndian.Uint16(raw[i*2 : i*2+2])
		if value == 0xfffd {
			continue
		}
		binary.BigEndian.PutUint16(row.Slots[dst*2:dst*2+2], value)
	}
	pending[key] = row
	return nil
}

func (in *Internal) udpBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !constantTokenMatch(in.cfg.CheckerInternalToken, bearer(r)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	data, err := readUDPBatchBody(w, r)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var batch udpBatchRequest
	if err := json.Unmarshal(data, &batch); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if batch.GeneratedAtMS <= 0 {
		http.Error(w, "missing batch timestamp", http.StatusBadRequest)
		return
	}
	var liveEnvelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &liveEnvelope); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	in.udpMu.Lock()
	defer in.udpMu.Unlock()
	for _, block := range batch.Blocks {
		if err := mergePendingMinute(in.udpPending, block); err != nil {
			http.Error(w, "invalid live block", http.StatusBadRequest)
			return
		}
	}
	now := time.Now()
	settings := in.st.UDPSettings()
	in.detectUDPTransitions(batch, settings, now)
	in.udpBrowserBlocks = append(in.udpBrowserBlocks, batch.Blocks...)
	in.udpBrowserLatest = liveEnvelope
	browserEvery := time.Duration(settings.BrowserBatchMS) * time.Millisecond
	if browserEvery <= 0 {
		browserEvery = time.Second
	}
	if in.lastUDPBrowser.IsZero() || now.Sub(in.lastUDPBrowser) >= browserEvery {
		blocksJSON, _ := json.Marshal(in.udpBrowserBlocks)
		in.udpBrowserLatest["blocks"] = blocksJSON
		if live, err := json.Marshal(in.udpBrowserLatest); err == nil {
			globalUDPLiveHub.publish(live)
		}
		in.udpBrowserBlocks = in.udpBrowserBlocks[:0]
		in.lastUDPBrowser = now
	}
	persistEvery := time.Duration(settings.DBPersistMS) * time.Millisecond
	if persistEvery <= 0 {
		persistEvery = 10 * time.Second
	}
	if in.lastUDPPersist.IsZero() || now.Sub(in.lastUDPPersist) >= persistEvery {
		rows := make([]store.UDPLiveBlockRow, 0, len(in.udpPending))
		for _, row := range in.udpPending {
			rows = append(rows, row)
		}
		if len(rows) > 0 {
			if err := in.st.MergeUDPLiveBlocks(rows); err != nil {
				http.Error(w, "UDP history write failed", http.StatusInternalServerError)
				return
			}
		}
		aggs := make([]store.UDPPathAggregateRow, 0, len(batch.Paths))
		for _, path := range batch.Paths {
			aggs = append(aggs, store.UDPPathAggregateRow{
				TS: batch.GeneratedAtMS / 1000, NodeSID: path.NodeID, ReceiverID: path.ReceiverID,
				State: path.State, MedianUS: path.MedianRTT / 1000, P95US: path.P95RTT / 1000,
				JitterUS: path.Jitter / 1000, Loss: path.Loss, Coverage: path.Coverage, Samples: path.SampleCount,
			})
		}
		if err := in.st.SaveUDPPathAggregates(aggs); err != nil {
			http.Error(w, "UDP aggregate write failed", http.StatusInternalServerError)
			return
		}
		in.udpPending = make(map[string]store.UDPLiveBlockRow)
		in.lastUDPPersist = now
	}
	if in.lastUDPCleanup.IsZero() || now.Sub(in.lastUDPCleanup) >= time.Minute {
		if err := in.st.CleanupUDPHistory(now); err == nil {
			in.lastUDPCleanup = now
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
