package socks5udp

import (
	"bytes"
	"net"
	"testing"
)

func TestDatagramRoundTrip(t *testing.T) {
	tests := []string{
		"192.0.2.1:38191",
		"receiver.example:9999",
		"[2001:db8::1]:443",
	}
	for _, target := range tests {
		packet, err := encodeDatagram(nil, target, []byte("hello"))
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		gotTarget, gotPayload, err := decodeDatagram(packet)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		if gotTarget != target || !bytes.Equal(gotPayload, []byte("hello")) {
			t.Fatalf("round trip: got target=%q payload=%q", gotTarget, gotPayload)
		}
	}
}

func TestFragmentRejected(t *testing.T) {
	packet, err := encodeDatagram(nil, net.JoinHostPort("127.0.0.1", "9999"), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	packet[2] = 1
	if _, _, err := decodeDatagram(packet); err != ErrFragmented {
		t.Fatalf("got %v want %v", err, ErrFragmented)
	}
}
