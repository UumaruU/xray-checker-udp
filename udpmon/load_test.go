package udpmon

import (
	"testing"
	"time"
)

func TestLoadPolicyOnlyReducesDuringConfirmedOverload(t *testing.T) {
	cfg := ControlSettings{AutoManageRate: true, AutoManageReceivers: true, MinProbeHz: 1, AutoReceiverLimit: 3, ProbeIntervalMS: 100}
	base := []PathConfig{{NodeID: "n", ReceiverID: "a", ProbeInterval: 100 * time.Millisecond}, {NodeID: "n", ReceiverID: "b", ProbeInterval: 100 * time.Millisecond}}
	paths, _ := ApplyLoadPolicy(base, cfg, LoadStatus{State: LoadHigh, Level: 2}, nil)
	if paths[0].ProbeInterval != 100*time.Millisecond {
		t.Fatal("HIGH_LOAD changed measurement frequency before confirmed overload")
	}
	paths, _ = ApplyLoadPolicy(base, cfg, LoadStatus{State: LoadOverloaded, Level: 2}, nil)
	if paths[0].ProbeInterval != 200*time.Millisecond {
		t.Fatalf("interval=%s want=200ms", paths[0].ProbeInterval)
	}
}

func TestProblemNodesKeepFullRateDuringOverload(t *testing.T) {
	cfg := ControlSettings{AutoManageRate: true, PrioritizeProblemNodes: true, MinProbeHz: 1}
	base := []PathConfig{{NodeID: "bad", ReceiverID: "a", ProbeInterval: 100 * time.Millisecond}, {NodeID: "ok", ReceiverID: "a", ProbeInterval: 100 * time.Millisecond}}
	paths, _ := ApplyLoadPolicy(base, cfg, LoadStatus{State: LoadOverloaded, Level: 3}, map[string]bool{"bad": true})
	for _, path := range paths {
		if path.NodeID == "bad" && path.ProbeInterval != 100*time.Millisecond {
			t.Fatal("problem node lost full-rate probing")
		}
		if path.NodeID == "ok" && path.ProbeInterval <= 100*time.Millisecond {
			t.Fatal("stable node was not reduced")
		}
	}
}
