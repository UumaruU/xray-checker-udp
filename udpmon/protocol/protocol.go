package protocol

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	Version      = 1
	HeaderSize   = 64
	MACSize      = sha256.Size
	MinPacketSize = HeaderSize + MACSize
	MaxPacketSize = 256
)

var magic = [4]byte{'X', 'C', 'U', '1'}

type MessageType uint8

const (
	TypeProbeRequest  MessageType = 1
	TypeProbeResponse MessageType = 2
	TypePeerRequest   MessageType = 3
	TypePeerResponse  MessageType = 4
)

var (
	ErrPacketSize = errors.New("invalid UDP monitor packet size")
	ErrMagic      = errors.New("invalid UDP monitor magic")
	ErrVersion    = errors.New("unsupported UDP monitor protocol version")
	ErrMAC        = errors.New("invalid UDP monitor MAC")
	ErrPayload    = errors.New("invalid UDP monitor payload length")
	ErrTimestamp  = errors.New("UDP monitor timestamp outside allowed clock skew")
)

type Message struct {
	Type       MessageType
	KeyVersion uint16
	ReceiverID uint64
	NodeID     uint64
	ProbeID    uint64
	Sequence   uint64
	SentAtMS   int64
	Nonce      [12]byte
	Flags      uint16
	Payload    []byte
}

// IDFromString maps stable string identifiers to compact wire IDs. The full
// stable IDs remain in the control plane; only their collision-resistant prefix
// is used on the UDP hot path.
func IDFromString(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(sum[:8])
}

func NewNonce() ([12]byte, error) {
	var nonce [12]byte
	_, err := rand.Read(nonce[:])
	return nonce, err
}

func Encode(dst []byte, msg Message, key []byte) ([]byte, error) {
	if len(key) < 16 {
		return nil, errors.New("UDP monitor key must be at least 16 bytes")
	}
	packetLen := HeaderSize + len(msg.Payload) + MACSize
	if packetLen < MinPacketSize || packetLen > MaxPacketSize {
		return nil, ErrPacketSize
	}
	if cap(dst) < packetLen {
		dst = make([]byte, packetLen)
	} else {
		dst = dst[:packetLen]
		clear(dst)
	}

	copy(dst[0:4], magic[:])
	dst[4] = Version
	dst[5] = byte(msg.Type)
	binary.BigEndian.PutUint16(dst[6:8], msg.KeyVersion)
	binary.BigEndian.PutUint64(dst[8:16], msg.ReceiverID)
	binary.BigEndian.PutUint64(dst[16:24], msg.NodeID)
	binary.BigEndian.PutUint64(dst[24:32], msg.ProbeID)
	binary.BigEndian.PutUint64(dst[32:40], msg.Sequence)
	binary.BigEndian.PutUint64(dst[40:48], uint64(msg.SentAtMS))
	copy(dst[48:60], msg.Nonce[:])
	binary.BigEndian.PutUint16(dst[60:62], msg.Flags)
	binary.BigEndian.PutUint16(dst[62:64], uint16(len(msg.Payload)))
	copy(dst[HeaderSize:HeaderSize+len(msg.Payload)], msg.Payload)

	macStart := packetLen - MACSize
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(dst[:macStart])
	copy(dst[macStart:], mac.Sum(nil))
	return dst, nil
}

func Decode(packet, key []byte) (Message, error) {
	if len(packet) < MinPacketSize || len(packet) > MaxPacketSize {
		return Message{}, ErrPacketSize
	}
	if subtle.ConstantTimeCompare(packet[:4], magic[:]) != 1 {
		return Message{}, ErrMagic
	}
	if packet[4] != Version {
		return Message{}, ErrVersion
	}
	payloadLen := int(binary.BigEndian.Uint16(packet[62:64]))
	if HeaderSize+payloadLen+MACSize != len(packet) {
		return Message{}, ErrPayload
	}

	macStart := len(packet) - MACSize
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(packet[:macStart])
	expected := mac.Sum(nil)
	if hmac.Equal(expected, packet[macStart:]) == false {
		return Message{}, ErrMAC
	}

	var msg Message
	msg.Type = MessageType(packet[5])
	msg.KeyVersion = binary.BigEndian.Uint16(packet[6:8])
	msg.ReceiverID = binary.BigEndian.Uint64(packet[8:16])
	msg.NodeID = binary.BigEndian.Uint64(packet[16:24])
	msg.ProbeID = binary.BigEndian.Uint64(packet[24:32])
	msg.Sequence = binary.BigEndian.Uint64(packet[32:40])
	msg.SentAtMS = int64(binary.BigEndian.Uint64(packet[40:48]))
	copy(msg.Nonce[:], packet[48:60])
	msg.Flags = binary.BigEndian.Uint16(packet[60:62])
	msg.Payload = packet[HeaderSize : HeaderSize+payloadLen]
	return msg, nil
}

func ValidateTimestamp(msg Message, now time.Time, maxSkew time.Duration) error {
	if maxSkew <= 0 {
		return nil
	}
	delta := now.UnixMilli() - msg.SentAtMS
	if delta < 0 {
		delta = -delta
	}
	if time.Duration(delta)*time.Millisecond > maxSkew {
		return fmt.Errorf("%w: delta=%s", ErrTimestamp, time.Duration(delta)*time.Millisecond)
	}
	return nil
}
