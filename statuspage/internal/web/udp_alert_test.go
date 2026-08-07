package web

import (
	"path/filepath"
	"testing"
	"time"

	"xray-status/internal/config"
	"xray-status/internal/poller"
	"xray-status/internal/store"
)

func TestUDPTransitionsSuppressNodesWhenReceiversUnknown(t *testing.T) {
	st, err := store.Open("sqlite", filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	settings := store.DefaultUDPSettings()
	settings.Enabled = true
	if err := st.SetUDPSettings(settings); err != nil {
		t.Fatal(err)
	}
	in := NewInternal(config.Config{}, st)
	events := make(chan poller.Event, 8)
	in.OnUDPEvent(func(event poller.Event) { events <- event })
	now := time.Now()
	healthy := map[string]udpReceiverHealthReport{"r1": {State: "HEALTHY", HeartbeatAge: int64(time.Second), DirectLoss: 0}}
	in.detectUDPTransitions(udpBatchRequest{Nodes: []udpNodeReport{{NodeID: "n1", State: "ONLINE"}}, ReceiverHealth: healthy}, settings, now)
	in.detectUDPTransitions(udpBatchRequest{Nodes: []udpNodeReport{{NodeID: "n1", State: "OFFLINE"}}, ReceiverHealth: healthy}, settings, now.Add(time.Second))
	select {
	case event := <-events:
		if event.Type != poller.EventUDPOffline {
			t.Fatalf("want UDP offline, got %s", event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("missing UDP offline event")
	}

	unknown := map[string]udpReceiverHealthReport{"r1": {State: "DEGRADED", HeartbeatAge: int64(time.Second), DirectLoss: 0.2}}
	in.detectUDPTransitions(udpBatchRequest{Nodes: []udpNodeReport{{NodeID: "n1", State: "ONLINE"}}, ReceiverHealth: unknown}, settings, now.Add(2*time.Second))
	select {
	case event := <-events:
		if event.Type != poller.EventUDPGlobalUnknown {
			t.Fatalf("want global UNKNOWN only, got %s", event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("missing global UNKNOWN event")
	}
	select {
	case event := <-events:
		t.Fatalf("node transition should be suppressed, got %s", event.Type)
	case <-time.After(50 * time.Millisecond):
	}
}
