package web

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"
	"time"

	"xray-status/internal/store"
)

func TestProbePendingReceiverAuthenticatesRoundTrip(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	token := "test-bootstrap-token"
	key := sha256.Sum256([]byte(token))
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, pairingPacketSize)
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			done <- err
			return
		}
		buf = buf[:n]
		if !pairingMACValid(buf, key[:]) {
			done <- context.Canceled
			return
		}
		buf[5] = 2 // probe response
		pairingSign(buf, key[:])
		_, err = conn.WriteToUDP(buf, remote)
		done <- err
	}()

	port := conn.LocalAddr().(*net.UDPAddr).Port
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := probePendingReceiver(ctx, store.UDPReceiver{ID: "receiver-test", Host: "127.0.0.1", UDPPort: port}, token); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
