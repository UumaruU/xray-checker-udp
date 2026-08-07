package udpmon

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"xray-checker/udpmon/protocol"
	"xray-checker/udpmon/socks5udp"
	"xray-checker/xray"
)

type protocolReceiverHarness struct {
	conn *net.UDPConn
	id   string
	key  []byte
}

func startProtocolReceiverHarness(t *testing.T, id string, key []byte) *protocolReceiverHarness {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	h := &protocolReceiverHarness{conn: conn, id: id, key: key}
	go func() {
		var buf [protocol.MaxPacketSize]byte
		var out [protocol.MaxPacketSize]byte
		for {
			n, remote, err := conn.ReadFromUDP(buf[:])
			if err != nil {
				return
			}
			msg, err := protocol.Decode(buf[:n], key)
			if err != nil || msg.Type != protocol.TypeProbeRequest || msg.ReceiverID != protocol.IDFromString(id) {
				continue
			}
			msg.Type = protocol.TypeProbeResponse
			msg.Payload = nil
			packet, err := protocol.Encode(out[:0], msg, key)
			if err == nil {
				_, _ = conn.WriteToUDP(packet, remote)
			}
		}
	}()
	t.Cleanup(func() { _ = conn.Close() })
	return h
}

func (h *protocolReceiverHarness) address() string { return h.conn.LocalAddr().String() }

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

func startExternalReceiver(t *testing.T, binary, id string, key []byte) string {
	t.Helper()
	udpPort, healthPort := freeUDPPort(t), freeTCPPort(t)
	dataDir := t.TempDir()
	state := fmt.Sprintf(`{"receiverId":%q,"permanentToken":"e2e-token","config":{"revision":1,"peerIntervalMs":2000,"keys":[{"version":1,"key":%q}],"peers":[]}}`, id, base64.StdEncoding.EncodeToString(key))
	if err := os.WriteFile(filepath.Join(dataDir, "receiver-state.json"), []byte(state), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(),
		"PANEL_URL=http://127.0.0.1:1", "RECEIVER_ID="+id, "DATA_DIR="+dataDir,
		"RECEIVER_UDP_PORT="+strconv.Itoa(udpPort), "HEALTH_LISTEN=127.0.0.1:"+strconv.Itoa(healthPort),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start production Receiver: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(healthPort), 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return net.JoinHostPort("127.0.0.1", strconv.Itoa(udpPort))
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("production Receiver health endpoint did not become ready")
	return ""
}

// TestRealXrayUDPAssociationMultipleReceivers uses the actual xray-core Runner,
// not a SOCKS mock. One persistent UDP ASSOCIATE is reused for two independent
// protocol-compatible Receiver endpoints.
func TestRealXrayUDPAssociationMultipleReceivers(t *testing.T) {
	key := bytes.Repeat([]byte{0x51}, 32)
	r1 := startProtocolReceiverHarness(t, "r1", key)
	r2 := startProtocolReceiverHarness(t, "r2", key)
	port := freeTCPPort(t)
	configPath := filepath.Join(t.TempDir(), "xray.json")
	configJSON := fmt.Sprintf(`{
  "log":{"loglevel":"none"},
  "inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks","tag":"test-in","settings":{"auth":"noauth","udp":true}}],
  "outbounds":[{"tag":"direct","protocol":"freedom","settings":{"domainStrategy":"UseIP"}}],
  "routing":{"domainStrategy":"AsIs","rules":[{"type":"field","inboundTag":["test-in"],"outboundTag":"direct"}]}
}`, port)
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := xray.NewRunner(configPath)
	if err := runner.Start(); err != nil {
		t.Fatalf("start real xray-core: %v", err)
	}
	t.Cleanup(func() { _ = runner.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assoc, err := socks5udp.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		t.Fatalf("real Xray SOCKS5 UDP ASSOCIATE: %v", err)
	}
	defer assoc.Close()

	targets := []struct{ id, address string }{{"r1", r1.address()}, {"r2", r2.address()}}
	for i := 0; i < 10; i++ {
		target := targets[i%len(targets)]
		nonce := [12]byte{byte(i + 1)}
		msg := protocol.Message{Type: protocol.TypeProbeRequest, KeyVersion: 1, ReceiverID: protocol.IDFromString(target.id), NodeID: 7, ProbeID: uint64(i + 1), Sequence: uint64(i + 1), SentAtMS: time.Now().UnixMilli(), Nonce: nonce}
		packet, err := protocol.Encode(nil, msg, key)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if err := assoc.WriteTo(packet, target.address); err != nil {
			t.Fatalf("probe %d send: %v", i, err)
		}
		_ = assoc.SetReadDeadline(time.Now().Add(2 * time.Second))
		var buf [512]byte
		_, payload, err := assoc.ReadFrom(buf[:])
		if err != nil {
			t.Fatalf("probe %d receive: %v", i, err)
		}
		response, err := protocol.Decode(payload, key)
		if err != nil {
			t.Fatalf("probe %d decode: %v", i, err)
		}
		if response.Type != protocol.TypeProbeResponse || response.ProbeID != msg.ProbeID || response.ReceiverID != msg.ReceiverID {
			t.Fatalf("probe %d mismatched response: %+v", i, response)
		}
		if time.Since(started) <= 0 {
			t.Fatal("non-positive monotonic RTT")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRealXrayUDPExternalReceiver is a black-box E2E against the production
// xray-checker-udp-receiver binary. CI may skip it when the sibling binary is
// unavailable; release verification can provide XCU_RECEIVER_BIN explicitly.
func TestRealXrayUDPExternalReceiver(t *testing.T) {
	receiverBinary := os.Getenv("XCU_RECEIVER_BIN")
	if receiverBinary == "" {
		t.Skip("XCU_RECEIVER_BIN is not set")
	}
	key := bytes.Repeat([]byte{0x73}, 32)
	target := startExternalReceiver(t, receiverBinary, "production-r1", key)
	port := freeTCPPort(t)
	configPath := filepath.Join(t.TempDir(), "xray.json")
	configJSON := fmt.Sprintf(`{
  "log":{"loglevel":"none"},
  "inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks","tag":"test-in","settings":{"auth":"noauth","udp":true}}],
  "outbounds":[{"tag":"direct","protocol":"freedom","settings":{"domainStrategy":"UseIP"}}],
  "routing":{"domainStrategy":"AsIs","rules":[{"type":"field","inboundTag":["test-in"],"outboundTag":"direct"}]}
}`, port)
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := xray.NewRunner(configPath)
	if err := runner.Start(); err != nil {
		t.Fatalf("start real xray-core: %v", err)
	}
	t.Cleanup(func() { _ = runner.Stop() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assoc, err := socks5udp.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer assoc.Close()
	for i := 0; i < 10; i++ {
		msg := protocol.Message{Type: protocol.TypeProbeRequest, KeyVersion: 1, ReceiverID: protocol.IDFromString("production-r1"), NodeID: protocol.IDFromString("node-e2e"), ProbeID: uint64(i + 1), Sequence: uint64(i + 1), SentAtMS: time.Now().UnixMilli(), Nonce: [12]byte{byte(i + 1)}}
		packet, err := protocol.Encode(nil, msg, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := assoc.WriteTo(packet, target); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		_ = assoc.SetReadDeadline(time.Now().Add(2 * time.Second))
		var buf [512]byte
		_, payload, err := assoc.ReadFrom(buf[:])
		if err != nil {
			t.Fatalf("receive %d: %v", i, err)
		}
		response, err := protocol.Decode(payload, key)
		if err != nil || response.Type != protocol.TypeProbeResponse || response.ProbeID != msg.ProbeID {
			t.Fatalf("invalid production Receiver response %d: response=%+v err=%v", i, response, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
