package web

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"xray-status/internal/store"
)

const (
	pairingPacketSize = 96
	pairingHeaderSize = 64
	pairingMACSize    = 32
)

var pairingMagic = [4]byte{'X', 'C', 'U', '1'}

// probePendingReceiver verifies the reverse half of pairing. The receiver
// temporarily serves the normal authenticated UDP echo protocol with key
// version 0, using SHA-256(bootstrap token) as its short-lived key. That lets
// the Panel prove that the advertised public UDP endpoint is really reachable
// before promoting a pending pairing into a real receiver.
func probePendingReceiver(ctx context.Context, receiver store.UDPReceiver, bootstrapToken string) error {
	key := sha256.Sum256([]byte(bootstrapToken))
	return probePendingReceiverKey(ctx, receiver, key[:])
}

func probePendingReceiverKey(ctx context.Context, receiver store.UDPReceiver, key []byte) error {
	if receiver.Host == "" || receiver.UDPPort < 1 {
		return errors.New("receiver endpoint is incomplete")
	}
	if len(key) != sha256.Size {
		return errors.New("invalid UDP pairing key")
	}
	target := net.JoinHostPort(receiver.Host, strconv.Itoa(receiver.UDPPort))
	dialer := net.Dialer{Timeout: 800 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "udp", target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", target, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(1200 * time.Millisecond))

	packet := make([]byte, pairingPacketSize)
	copy(packet[:4], pairingMagic[:])
	packet[4] = 1                              // protocol version
	packet[5] = 1                              // probe request
	binary.BigEndian.PutUint16(packet[6:8], 0) // reserved bootstrap key version
	binary.BigEndian.PutUint64(packet[8:16], pairingID(receiver.ID))
	binary.BigEndian.PutUint64(packet[16:24], pairingID("panel-bootstrap"))
	var random [28]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	probeID := binary.BigEndian.Uint64(random[:8])
	binary.BigEndian.PutUint64(packet[24:32], probeID)
	binary.BigEndian.PutUint64(packet[32:40], probeID)
	binary.BigEndian.PutUint64(packet[40:48], uint64(time.Now().UnixMilli()))
	copy(packet[48:60], random[8:20])
	binary.BigEndian.PutUint16(packet[62:64], 0)
	pairingSign(packet, key)

	if _, err := conn.Write(packet); err != nil {
		return fmt.Errorf("send UDP probe: %w", err)
	}
	response := make([]byte, pairingPacketSize)
	n, err := conn.Read(response)
	if err != nil {
		return fmt.Errorf("wait for UDP response: %w", err)
	}
	response = response[:n]
	if len(response) != pairingPacketSize || subtle.ConstantTimeCompare(response[:4], pairingMagic[:]) != 1 || response[4] != 1 || response[5] != 2 {
		return errors.New("invalid UDP pairing response")
	}
	if binary.BigEndian.Uint16(response[6:8]) != 0 || binary.BigEndian.Uint64(response[8:16]) != pairingID(receiver.ID) || binary.BigEndian.Uint64(response[24:32]) != probeID {
		return errors.New("UDP pairing response does not match receiver")
	}
	if subtle.ConstantTimeCompare(response[48:60], packet[48:60]) != 1 || !pairingMACValid(response, key) {
		return errors.New("UDP pairing response authentication failed")
	}
	return nil
}

func pairingID(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(sum[:8])
}

func pairingSign(packet, key []byte) {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(packet[:pairingHeaderSize])
	copy(packet[pairingHeaderSize:], mac.Sum(nil))
}

func pairingMACValid(packet, key []byte) bool {
	if len(packet) != pairingHeaderSize+pairingMACSize {
		return false
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(packet[:pairingHeaderSize])
	return hmac.Equal(mac.Sum(nil), packet[pairingHeaderSize:])
}
