package udpmon

import "testing"

func TestBuildPathsHonorsReceiverExclusions(t *testing.T) {
	cfg := ControlConfig{
		Settings: ControlSettings{Enabled: true, ProbeIntervalMS: 100, ProbeTimeoutMS: 1000, AutoReceiverLimit: 5},
		Receivers: []ControlReceiver{
			{ID: "local", Host: "127.0.0.1", UDPPort: 9999, Enabled: true, ExcludedNodes: []string{"node-a"}, KeyVersion: 1, Key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
			{ID: "remote", Host: "192.0.2.1", UDPPort: 9999, Enabled: true, KeyVersion: 1, Key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
		},
	}
	paths, err := BuildPaths(cfg, []NodeDescriptor{{StableID: "node-a", SocksAddress: "127.0.0.1:10000", SupportsUDP: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0].ReceiverID != "remote" {
		t.Fatalf("paths=%+v", paths)
	}
}

func TestSelectedReceiverMode(t *testing.T) {
	cfg := ControlConfig{
		Settings: ControlSettings{Enabled: true, ProbeIntervalMS: 100, ProbeTimeoutMS: 1000},
		Receivers: []ControlReceiver{
			{ID: "a", Host: "a.example", UDPPort: 9999, Enabled: true, Key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
			{ID: "b", Host: "b.example", UDPPort: 9999, Enabled: true, Key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
		},
		Nodes: map[string]ControlNode{"n": {NodeSID: "n", Enabled: true, ReceiverMode: "selected", SelectedReceivers: []string{"b"}}},
	}
	paths, err := BuildPaths(cfg, []NodeDescriptor{{StableID: "n", SocksAddress: "127.0.0.1:10000", SupportsUDP: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0].ReceiverID != "b" {
		t.Fatalf("paths=%+v", paths)
	}
}
