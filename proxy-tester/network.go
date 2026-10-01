package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type StageError struct {
	Stage string
	Code  string
	Err   error
}

func (e *StageError) Error() string { return e.Stage + ": " + e.Code }
func (e *StageError) Unwrap() error { return e.Err }

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	var stage *StageError
	if errors.As(err, &stage) {
		return stage.Code
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "timeout"
	}
	for _, pair := range []struct {
		err  error
		code string
	}{{syscall.EMFILE, "local_file_limit"}, {syscall.ENFILE, "local_file_limit"},
		{syscall.ENOBUFS, "local_no_buffers"}, {syscall.ENOMEM, "local_no_memory"},
		{syscall.EADDRNOTAVAIL, "local_address_unavailable"}, {syscall.EADDRINUSE, "local_address_in_use"},
		{syscall.ECONNREFUSED, "connection_refused"},
		{syscall.ECONNRESET, "connection_reset"}, {syscall.ENETUNREACH, "network_unreachable"},
		{syscall.EHOSTUNREACH, "host_unreachable"}, {io.EOF, "eof"}, {io.ErrUnexpectedEOF, "unexpected_eof"}} {
		if errors.Is(err, pair.err) {
			return pair.code
		}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		if dns.IsNotFound {
			return "dns_not_found"
		}
		return "dns_error"
	}
	return "network_or_protocol_error"
}

type dnsEntry struct {
	addresses []netip.Addr
	err       error
	expires   time.Time
}

var resolverCache sync.Map
var resolverCacheSize atomic.Int64
var publicResolver = net.Resolver{PreferGo: true}

func publicAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var blockedPrefixes = func() []netip.Prefix {
	var result []netip.Prefix
	for _, value := range strings.Fields("0.0.0.0/8 100.64.0.0/10 192.0.0.0/24 192.0.2.0/24 198.18.0.0/15 198.51.100.0/24 203.0.113.0/24 240.0.0.0/4 2001:db8::/32") {
		result = append(result, netip.MustParsePrefix(value))
	}
	return result
}()

func resolvePublic(ctx context.Context, host string, allowPrivate bool) ([]netip.Addr, error) {
	if address, err := netip.ParseAddr(host); err == nil {
		if !allowPrivate && !publicAddress(address) {
			return nil, &StageError{Stage: "resolve", Code: "non_public_endpoint"}
		}
		return []netip.Addr{address}, nil
	}
	cacheKey := fmt.Sprintf("%t:%s", allowPrivate, host)
	if value, found := resolverCache.Load(cacheKey); found {
		entry := value.(dnsEntry)
		if time.Now().Before(entry.expires) {
			return entry.addresses, entry.err
		}
	}
	addresses, err := publicResolver.LookupNetIP(ctx, "ip", host)
	var accepted []netip.Addr
	for _, address := range addresses {
		if allowPrivate || publicAddress(address) {
			accepted = append(accepted, address.Unmap())
		}
	}
	if err == nil && len(accepted) == 0 {
		err = &StageError{Stage: "resolve", Code: "non_public_endpoint"}
	}
	// Cancellation and resolver saturation must not poison subsequent attempts.
	if ctx.Err() == nil && (err == nil || errorCode(err) == "dns_not_found") {
		lifetime := 5 * time.Minute
		if err != nil {
			lifetime = 30 * time.Second
		}
		if _, exists := resolverCache.Load(cacheKey); exists || resolverCacheSize.Load() < 100_000 {
			if _, loaded := resolverCache.Swap(cacheKey, dnsEntry{accepted, err, time.Now().Add(lifetime)}); !loaded {
				resolverCacheSize.Add(1)
			}
		}
	}
	return accepted, err
}

type attemptDialer struct {
	connectTimeout time.Duration
	allowPrivate   bool
	connectMicros  atomic.Int64
	connected      atomic.Bool
}

// An adapter can reuse its dialer across simultaneous bridge requests. Keep
// each request's observation on its context instead of the shared dialer.
type upstreamConnectionKey struct{}

func (d *attemptDialer) DialContext(parent context.Context, network, address string) (net.Conn, error) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, d.connectTimeout)
	defer cancel()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, &StageError{Stage: "connect", Code: "invalid_endpoint", Err: err}
	}
	addresses, err := resolvePublic(ctx, host, d.allowPrivate)
	if err != nil {
		return nil, &StageError{Stage: "resolve", Code: errorCode(err), Err: err}
	}
	var last error
	for _, ip := range addresses {
		if network == "tcp4" && !ip.Is4() || network == "tcp6" && !ip.Is6() {
			continue
		}
		conn, dialErr := (&net.Dialer{KeepAlive: -1}).DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			d.connected.Store(true)
			if observation, ok := parent.Value(upstreamConnectionKey{}).(*atomic.Bool); ok {
				observation.Store(true)
			}
			d.connectMicros.Store(time.Since(started).Microseconds())
			return conn, nil
		}
		last = dialErr
		if ctx.Err() != nil {
			break
		}
	}
	if last == nil {
		last = errors.New("no usable address")
	}
	return nil, &StageError{Stage: "connect", Code: errorCode(last), Err: last}
}

func (d *attemptDialer) ListenPacket(ctx context.Context, network, address string, remote netip.AddrPort) (net.PacketConn, error) {
	if remote.IsValid() && !d.allowPrivate && !publicAddress(remote.Addr()) {
		return nil, &StageError{Stage: "resolve", Code: "non_public_endpoint"}
	}
	if strings.HasPrefix(network, "udp") {
		network = "udp"
	}
	return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func nativeTunnel(ctx context.Context, d *attemptDialer, c Candidate, protocol string, settings map[string]any, target string) (_ net.Conn, err error) {
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(c.Address, strconv.Itoa(c.Port)))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	initialConn := conn
	stopCancellation := context.AfterFunc(ctx, func() { initialConn.Close() })
	defer stopCancellation()
	if deadline, exists := ctx.Deadline(); exists {
		conn.SetDeadline(deadline)
	}
	if protocol == "https" || boolean(settings["tls"]) {
		serverName := firstString(settings, "sni", "servername")
		if serverName == "" {
			serverName = c.Address
		}
		secured := tls.Client(conn, &tls.Config{ServerName: serverName,
			InsecureSkipVerify: boolean(settings["skip-cert-verify"]), MinVersion: tls.VersionTLS12})
		if err := secured.HandshakeContext(ctx); err != nil {
			return nil, &StageError{Stage: "proxy_tls", Code: errorCode(err), Err: err}
		}
		conn = secured
	}
	username, password := firstString(settings, "username"), firstString(settings, "password")
	switch protocol {
	case "http", "https":
		request := &http.Request{Method: "CONNECT", URL: &url.URL{Opaque: target}, Host: target, Header: make(http.Header)}
		request.Header.Set("User-Agent", "Mozilla/5.0")
		if username != "" || password != "" {
			request.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
		}
		for key, value := range object(settings["headers"]) {
			if strings.EqualFold(key, "Host") || strings.EqualFold(key, "Proxy-Authorization") {
				continue
			}
			request.Header.Set(key, stringValue(value))
		}
		if err := request.Write(conn); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		reader := bufio.NewReaderSize(io.LimitReader(conn, 64*1024), 4096)
		response, err := http.ReadResponse(reader, request)
		if err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		if response.StatusCode != 200 {
			return nil, &StageError{Stage: "proxy_handshake", Code: fmt.Sprintf("proxy_http_%d", response.StatusCode)}
		}
		if reader.Buffered() > 0 {
			buffered, _ := reader.Peek(reader.Buffered())
			prefix := bytes.Clone(buffered)
			conn = &bufferedConn{Conn: conn, reader: bufio.NewReader(io.MultiReader(bytes.NewReader(prefix), conn))}
		}
	case "socks5":
		methods := []byte{5, 1, 0}
		if username != "" || password != "" {
			methods = []byte{5, 2, 0, 2}
		}
		if _, err := conn.Write(methods); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		answer := make([]byte, 2)
		if _, err := io.ReadFull(conn, answer); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		if answer[0] != 5 {
			return nil, &StageError{Stage: "proxy_handshake", Code: "not_socks5"}
		}
		if answer[1] == 2 {
			if len(username) > 255 || len(password) > 255 {
				return nil, configError("invalid_socks5_credentials_length")
			}
			auth := append([]byte{1, byte(len(username))}, username...)
			auth = append(auth, byte(len(password)))
			auth = append(auth, password...)
			if _, err := conn.Write(auth); err != nil {
				return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
			}
			if _, err := io.ReadFull(conn, answer); err != nil {
				return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
			}
			if answer[0] != 1 || answer[1] != 0 {
				return nil, &StageError{Stage: "proxy_handshake", Code: "proxy_authentication_failed"}
			}
		} else if answer[1] != 0 {
			return nil, &StageError{Stage: "proxy_handshake", Code: "proxy_authentication_required"}
		}
		host, port, _ := net.SplitHostPort(target)
		portNumber, _ := strconv.Atoi(port)
		if len(host) > 255 {
			return nil, configError("invalid_target")
		}
		request := append([]byte{5, 1, 0, 3, byte(len(host))}, host...)
		request = binary.BigEndian.AppendUint16(request, uint16(portNumber))
		if _, err := conn.Write(request); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		if header[0] != 5 || header[1] != 0 {
			return nil, &StageError{Stage: "proxy_handshake", Code: fmt.Sprintf("socks5_reply_%d", header[1])}
		}
		remaining := 0
		switch header[3] {
		case 1:
			remaining = 6
		case 4:
			remaining = 18
		case 3:
			length := []byte{0}
			if _, err := io.ReadFull(conn, length); err != nil {
				return nil, err
			}
			remaining = int(length[0]) + 2
		default:
			return nil, &StageError{Stage: "proxy_handshake", Code: "invalid_socks5_address"}
		}
		if _, err := io.CopyN(io.Discard, conn, int64(remaining)); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
	case "socks4":
		host, port, _ := net.SplitHostPort(target)
		portNumber, _ := strconv.Atoi(port)
		if strings.ContainsRune(username, 0) {
			return nil, configError("invalid_socks4_username")
		}
		request := binary.BigEndian.AppendUint16([]byte{4, 1}, uint16(portNumber))
		var targetIPv4 netip.Addr
		addresses, _ := resolvePublic(ctx, host, d.allowPrivate)
		for _, address := range addresses {
			if address.Is4() {
				targetIPv4 = address
				break
			}
		}
		if targetIPv4.IsValid() {
			bytes := targetIPv4.As4()
			request = append(request, bytes[:]...)
		} else {
			request = append(request, 0, 0, 0, 1)
		}
		request = append(request, username...)
		request = append(request, 0)
		if !targetIPv4.IsValid() {
			request = append(request, host...)
			request = append(request, 0)
		}
		if _, err := conn.Write(request); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		answer := make([]byte, 8)
		if _, err := io.ReadFull(conn, answer); err != nil {
			return nil, &StageError{Stage: "proxy_handshake", Code: errorCode(err), Err: err}
		}
		if answer[0] != 0 || answer[1] != 90 {
			return nil, &StageError{Stage: "proxy_handshake", Code: fmt.Sprintf("socks4_reply_%d", answer[1])}
		}
	default:
		return nil, configError("unsupported_native_protocol")
	}
	ok = true
	return conn, nil
}
