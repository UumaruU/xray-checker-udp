package web

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type liveEvent struct {
	id   uint64
	data []byte
}

type udpLiveHub struct {
	mu     sync.RWMutex
	subs   map[chan liveEvent]struct{}
	latest liveEvent
	next   atomic.Uint64
}

var globalUDPLiveHub = &udpLiveHub{subs: make(map[chan liveEvent]struct{})}

func (h *udpLiveHub) publish(data []byte) {
	event := liveEvent{id: h.next.Add(1), data: append([]byte(nil), data...)}
	h.mu.Lock()
	h.latest = event
	for ch := range h.subs {
		select {
		case ch <- event:
		default:
		}
	}
	h.mu.Unlock()
}

func (h *udpLiveHub) subscribe() (chan liveEvent, liveEvent, func()) {
	ch := make(chan liveEvent, 4)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	latest := h.latest
	h.mu.Unlock()
	return ch, latest, func() { h.mu.Lock(); delete(h.subs, ch); close(ch); h.mu.Unlock() }
}

func (s *Server) udpLive(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch, latest, unsubscribe := globalUDPLiveHub.subscribe()
	defer unsubscribe()
	if latest.id != 0 {
		fmt.Fprintf(w, "id: %d\nevent: snapshot\ndata: %s\n\n", latest.id, latest.data)
		flusher.Flush()
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			fmt.Fprintf(w, "id: %d\nevent: udp\ndata: %s\n\n", event.id, event.data)
			flusher.Flush()
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) udpSnapshot(w http.ResponseWriter, r *http.Request) {
	globalUDPLiveHub.mu.RLock()
	latest := globalUDPLiveHub.latest
	globalUDPLiveHub.mu.RUnlock()
	if latest.id == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"available": false})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-UDP-Event-ID", fmt.Sprintf("%d", latest.id))
	_, _ = w.Write(latest.data)
}

func (s *Server) udpHistory(w http.ResponseWriter, r *http.Request) {
	node := r.URL.Query().Get("node")
	receiver := r.URL.Query().Get("receiver")
	toMS, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
	fromMS, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	if toMS <= 0 {
		toMS = time.Now().UnixMilli()
	}
	if fromMS <= 0 {
		fromMS = toMS - int64(time.Hour/time.Millisecond)
	}
	if node == "" || toMS <= fromMS || toMS-fromMS > int64(31*24*time.Hour/time.Millisecond) {
		http.Error(w, "invalid history range", http.StatusBadRequest)
		return
	}
	raw, aggregates, err := s.st.UDPHistory(node, receiver, fromMS, toMS)
	if err != nil {
		http.Error(w, "history query failed", http.StatusInternalServerError)
		return
	}
	blocks := make([]map[string]any, 0, len(raw))
	for _, row := range raw {
		blocks = append(blocks, map[string]any{"nodeId": row.NodeSID, "receiverId": row.ReceiverID, "startMs": row.StartMS, "intervalUs": row.IntervalUS, "slots": base64.StdEncoding.EncodeToString(row.Slots)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"blocks": blocks, "aggregates": aggregates})
}
