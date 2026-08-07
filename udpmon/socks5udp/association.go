package socks5udp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	socksVersion = 5
	cmdAssociate  = 3
	atypIPv4      = 1
	atypDomain    = 3
	atypIPv6      = 4
)

var (
	ErrFragmented = errors.New("fragmented SOCKS5 UDP datagram is unsupported")
	ErrMalformed  = errors.New("malformed SOCKS5 UDP datagram")
	ErrClosed     = errors.New("SOCKS5 UDP association is closed")
)

// Association owns one persistent SOCKS5 UDP ASSOCIATE control connection and
// its UDP relay socket. The same association can carry datagrams to many
// Receiver targets for one Xray node.
type Association struct {
	control net.Conn
	udp     *net.UDPConn
	relay   *net.UDPAddr
	writeMu sync.Mutex
	closeMu sync.Mutex
	closed  bool
}

func Dial(ctx context.Context, socksAddress string, handshakeTimeout time.Duration) (*Association, error) {
	dialer := net.Dialer{Timeout: handshakeTimeout}
	control, err := dialer.DialContext(ctx, "tcp", socksAddress)
	if err != nil {
		return nil, fmt.Errorf("dial SOCKS5 control: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			_ = control.Close()
		}
	}()

	if handshakeTimeout > 0 {
		_ = control.SetDeadline(time.Now().Add(handshakeTimeout))
	}
	if _, err := control.Write([]byte{socksVersion, 1, 0}); err != nil {
		return nil, fmt.Errorf("SOCKS5 greeting: %w", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(control, greeting); err != nil {
		return nil, fmt.Errorf("SOCKS5 greeting response: %w", err)
	}
	if greeting[0] != socksVersion || greeting[1] != 0 {
		return nil, fmt.Errorf("SOCKS5 no-auth rejected: version=%d method=%d", greeting[0], greeting[1])
	}

	// 0.0.0.0:0 asks the server to bind an appropriate UDP relay endpoint.
	request := []byte{socksVersion, cmdAssociate, 0, atypIPv4, 0, 0, 0, 0, 0, 0}
	if _, err := control.Write(request); err != nil {
		return nil, fmt.Errorf("SOCKS5 UDP ASSOCIATE: %w", err)
	}
	relay, err := readAssociateReply(control)
	if err != nil {
		return nil, err
	}
	if relay.IP == nil || relay.IP.IsUnspecified() {
		if remote, ok := control.RemoteAddr().(*net.TCPAddr); ok {
			relay.IP = remote.IP
		}
	}
	if relay.IP == nil || relay.IP.IsUnspecified() {
		return nil, errors.New("SOCKS5 returned an unspecified UDP relay address")
	}

	udp, err := net.DialUDP("udp", nil, relay)
	if err != nil {
		return nil, fmt.Errorf("dial SOCKS5 UDP relay: %w", err)
	}
	_ = control.SetDeadline(time.Time{})
	failed = false
	return &Association{control: control, udp: udp, relay: relay}, nil
}

func readAssociateReply(r io.Reader) (*net.UDPAddr, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, fmt.Errorf("SOCKS5 associate reply: %w", err)
	}
	if head[0] != socksVersion || head[2] != 0 {
		return nil, ErrMalformed
	}
	if head[1] != 0 {
		return nil, fmt.Errorf("SOCKS5 UDP ASSOCIATE rejected: reply=%d", head[1])
	}
	host, err := readHost(r, head[3])
	if err != nil {
		return nil, err
	}
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, portBuf); err != nil {
		return nil, ErrMalformed
	}
	ip := net.ParseIP(host)
	if ip == nil {
		ips, err := net.DefaultResolver.LookupIP(context.Background(), "ip", host)
		if err != nil || len(ips) == 0 {
			return nil, fmt.Errorf("resolve SOCKS5 UDP relay %q: %w", host, err)
		}
		ip = ips[0]
	}
	return &net.UDPAddr{IP: ip, Port: int(binary.BigEndian.Uint16(portBuf))}, nil
}

func readHost(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case atypIPv4:
		buf := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", ErrMalformed
		}
		return net.IP(buf).String(), nil
	case atypIPv6:
		buf := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", ErrMalformed
		}
		return net.IP(buf).String(), nil
	case atypDomain:
		length := []byte{0}
		if _, err := io.ReadFull(r, length); err != nil || length[0] == 0 {
			return "", ErrMalformed
		}
		buf := make([]byte, int(length[0]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", ErrMalformed
		}
		return string(buf), nil
	default:
		return "", ErrMalformed
	}
}

func appendAddress(dst []byte, target string) ([]byte, error) {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("target %q has invalid port", target)
	}
	ip := net.ParseIP(host)
	switch {
	case ip != nil && ip.To4() != nil:
		dst = append(dst, atypIPv4)
		dst = append(dst, ip.To4()...)
	case ip != nil:
		dst = append(dst, atypIPv6)
		dst = append(dst, ip.To16()...)
	default:
		if len(host) == 0 || len(host) > 255 {
			return nil, fmt.Errorf("target hostname length is invalid")
		}
		dst = append(dst, atypDomain, byte(len(host)))
		dst = append(dst, host...)
	}
	return binary.BigEndian.AppendUint16(dst, uint16(port)), nil
}

func encodeDatagram(dst []byte, target string, payload []byte) ([]byte, error) {
	dst = append(dst[:0], 0, 0, 0)
	var err error
	dst, err = appendAddress(dst, target)
	if err != nil {
		return nil, err
	}
	dst = append(dst, payload...)
	return dst, nil
}

func decodeDatagram(packet []byte) (target string, payload []byte, err error) {
	if len(packet) < 4 || packet[0] != 0 || packet[1] != 0 {
		return "", nil, ErrMalformed
	}
	if packet[2] != 0 {
		return "", nil, ErrFragmented
	}
	offset := 4
	var host string
	switch packet[3] {
	case atypIPv4:
		if len(packet) < offset+net.IPv4len+2 {
			return "", nil, ErrMalformed
		}
		host = net.IP(packet[offset : offset+net.IPv4len]).String()
		offset += net.IPv4len
	case atypIPv6:
		if len(packet) < offset+net.IPv6len+2 {
			return "", nil, ErrMalformed
		}
		host = net.IP(packet[offset : offset+net.IPv6len]).String()
		offset += net.IPv6len
	case atypDomain:
		if len(packet) < offset+1 {
			return "", nil, ErrMalformed
		}
		length := int(packet[offset])
		offset++
		if length == 0 || len(packet) < offset+length+2 {
			return "", nil, ErrMalformed
		}
		host = string(packet[offset : offset+length])
		offset += length
	default:
		return "", nil, ErrMalformed
	}
	port := binary.BigEndian.Uint16(packet[offset : offset+2])
	offset += 2
	return net.JoinHostPort(host, strconv.Itoa(int(port))), packet[offset:], nil
}

func (a *Association) WriteTo(payload []byte, target string) error {
	a.closeMu.Lock()
	closed := a.closed
	a.closeMu.Unlock()
	if closed {
		return ErrClosed
	}
	var scratch [512]byte
	packet, err := encodeDatagram(scratch[:0], target, payload)
	if err != nil {
		return err
	}
	a.writeMu.Lock()
	_, err = a.udp.Write(packet)
	a.writeMu.Unlock()
	return err
}

func (a *Association) ReadFrom(buffer []byte) (target string, payload []byte, err error) {
	n, err := a.udp.Read(buffer)
	if err != nil {
		return "", nil, err
	}
	target, payload, err = decodeDatagram(buffer[:n])
	return target, payload, err
}

func (a *Association) RelayAddress() *net.UDPAddr { return a.relay }

func (a *Association) Close() error {
	a.closeMu.Lock()
	if a.closed {
		a.closeMu.Unlock()
		return nil
	}
	a.closed = true
	a.closeMu.Unlock()
	errUDP := a.udp.Close()
	errTCP := a.control.Close()
	if errUDP != nil {
		return errUDP
	}
	return errTCP
}
