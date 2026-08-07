package protocol

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestEncodeDecode(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	nonce := [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	want := Message{
		Type: TypeProbeRequest, KeyVersion: 3, ReceiverID: 11, NodeID: 22,
		ProbeID: 33, Sequence: 44, SentAtMS: time.Now().UnixMilli(), Nonce: nonce,
	}
	packet, err := Encode(make([]byte, 0, MinPacketSize), want, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != MinPacketSize {
		t.Fatalf("packet size=%d want=%d", len(packet), MinPacketSize)
	}
	got, err := Decode(packet, key)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.ReceiverID != want.ReceiverID || got.NodeID != want.NodeID || got.ProbeID != want.ProbeID || got.Sequence != want.Sequence || got.Nonce != want.Nonce {
		t.Fatalf("decoded message mismatch: %#v", got)
	}
}

func TestTamperFailsMAC(t *testing.T) {
	key := bytes.Repeat([]byte{0x23}, 32)
	packet, err := Encode(nil, Message{Type: TypeProbeRequest, SentAtMS: time.Now().UnixMilli()}, key)
	if err != nil {
		t.Fatal(err)
	}
	packet[24] ^= 1
	if _, err := Decode(packet, key); !errors.Is(err, ErrMAC) {
		t.Fatalf("got %v, want ErrMAC", err)
	}
}

func TestTimestampValidation(t *testing.T) {
	now := time.Unix(1000, 0)
	msg := Message{SentAtMS: now.Add(-31 * time.Second).UnixMilli()}
	if err := ValidateTimestamp(msg, now, 30*time.Second); !errors.Is(err, ErrTimestamp) {
		t.Fatalf("got %v, want ErrTimestamp", err)
	}
}
