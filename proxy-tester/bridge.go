package main

// A loopback CONNECT bridge exposes the tester's existing transports to the
// metadata collector. The collector owns target TLS, HTTP, parsing and scoring.
import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/adapter"
	C "github.com/metacubex/mihomo/constant"
)

type bridgeEntry struct {
	candidate Candidate
	once      sync.Once
	protocol  string
	settings  map[string]any
	proxy     C.Proxy
	err       error
}

func (e *bridgeEntry) prepare(dialer *attemptDialer) {
	e.once.Do(func() {
		e.protocol = e.candidate.WorkingProtocol
		if e.protocol == "" {
			e.protocol = e.candidate.Protocol
		}
		e.settings, e.err = normalize(e.candidate, e.protocol)
		if e.err == nil && !nativeProtocol(e.protocol) {
			e.proxy, e.err = adapter.ParseProxy(e.settings, adapter.WithDialerForAPI(dialer))
			if e.err != nil {
				e.err = configError("adapter_configuration_rejected")
			}
		}
	})
}

func nativeProtocol(protocol string) bool {
	return protocol == "http" || protocol == "https" || protocol == "socks4" || protocol == "socks5"
}

type proxyBridge struct {
	entries        map[int64]*bridgeEntry
	token          string
	connectTimeout time.Duration
	target         string
	allowPrivate   bool // Local protocol tests only; never exposed as a CLI flag.
	mu             sync.Mutex
	connections    map[net.Conn]struct{}
	closed         bool
	requests       sync.WaitGroup
}

func (b *proxyBridge) dial(ctx context.Context, entry *bridgeEntry) (conn net.Conn, connected bool, err error) {
	observation := new(atomic.Bool)
	ctx = context.WithValue(ctx, upstreamConnectionKey{}, observation)
	defer func() { connected = observation.Load() || conn != nil }()
	dialer := &attemptDialer{connectTimeout: b.connectTimeout, allowPrivate: b.allowPrivate}
	entry.prepare(dialer)
	if entry.err != nil {
		return nil, false, entry.err
	}
	if nativeProtocol(entry.protocol) {
		conn, err = nativeTunnel(ctx, dialer, entry.candidate, entry.protocol, entry.settings, b.target)
		return
	}
	host, port, _ := net.SplitHostPort(b.target)
	portNumber, _ := strconv.Atoi(port)
	conn, err = entry.proxy.DialContext(ctx, &C.Metadata{NetWork: C.TCP, Type: C.INNER,
		Host: host, DstPort: uint16(portNumber)})
	return
}

func bridgeConnectionError(err error) string {
	stage := "proxy_tunnel"
	var failure *StageError
	if errors.As(err, &failure) {
		stage = failure.Stage
	}
	return stage + ":" + errorCode(err)
}

func bridgeError(w http.ResponseWriter, status int, local bool) {
	// A distinct reason lets the collector exclude helper/configuration errors
	// from upstream reliability. No adapter errors or credentials are returned.
	if local {
		conn, writer, err := w.(http.Hijacker).Hijack()
		if err == nil {
			defer conn.Close()
			fmt.Fprintf(writer, "HTTP/1.1 %d Proxy Bridge Local Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status)
			writer.Flush()
		}
		return
	}
	http.Error(w, "Proxy tunnel failed", status)
}

func (b *proxyBridge) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		bridgeError(w, http.StatusServiceUnavailable, true)
		return
	}
	b.requests.Add(1)
	b.mu.Unlock()
	defer b.requests.Done()
	defer func() {
		if recover() != nil {
			bridgeError(w, http.StatusServiceUnavailable, true)
		}
	}()
	if request.Method != http.MethodConnect || !strings.EqualFold(request.Host, b.target) {
		bridgeError(w, http.StatusForbidden, true)
		return
	}
	authorization := strings.TrimPrefix(request.Header.Get("Proxy-Authorization"), "Basic ")
	credentials, err := base64.StdEncoding.DecodeString(authorization)
	username, password, ok := strings.Cut(string(credentials), ":")
	if err != nil || !ok || subtle.ConstantTimeCompare([]byte(password), []byte(b.token)) != 1 {
		bridgeError(w, http.StatusProxyAuthRequired, true)
		return
	}
	id, err := strconv.ParseInt(username, 10, 64)
	entry := b.entries[id]
	if err != nil || entry == nil {
		bridgeError(w, http.StatusUnprocessableEntity, true)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), b.connectTimeout)
	upstream, connected, err := b.dial(ctx, entry)
	cancel()
	w.Header().Set("X-Proxy-Connected", strconv.FormatBool(connected))
	if err != nil {
		var invalid configError
		local := errors.As(err, &invalid) || strings.HasPrefix(errorCode(err), "local_") || errors.Is(err, context.Canceled)
		if local {
			bridgeError(w, http.StatusServiceUnavailable, true)
		} else {
			w.Header().Set("X-Proxy-Error", bridgeConnectionError(err))
			bridgeError(w, http.StatusBadGateway, false)
		}
		return
	}
	defer upstream.Close()
	// nativeTunnel sets the establishment deadline. The collector now controls
	// each request deadline and can reuse this tunnel for subsequent requests.
	if err := upstream.SetDeadline(time.Time{}); err != nil {
		w.Header().Set("X-Proxy-Error", bridgeConnectionError(err))
		bridgeError(w, http.StatusBadGateway, false)
		return
	}
	client, buffered, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.connections[client], b.connections[upstream] = struct{}{}, struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.connections, client)
		delete(b.connections, upstream)
		b.mu.Unlock()
	}()
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\nX-Proxy-Connected: true\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	finished := make(chan struct{}, 1)
	go func() {
		io.Copy(upstream, buffered)
		upstream.Close()
		finished <- struct{}{}
	}()
	io.Copy(client, upstream)
	client.Close()
	<-finished
}

func (b *proxyBridge) close() {
	b.mu.Lock()
	b.closed = true
	for conn := range b.connections {
		conn.Close()
	}
	b.mu.Unlock()
	b.requests.Wait()
	for _, entry := range b.entries {
		if entry.proxy != nil {
			entry.proxy.Close()
		}
	}
}

func bridge(args []string) (int, error) {
	flags := flag.NewFlagSet("proxy-tester bridge", flag.ContinueOnError)
	input := flags.String("input", "", "Private catalog selection as JSONL")
	authFile := flags.String("auth-file", "", "Private file containing the bridge token")
	listen := flags.String("listen", "127.0.0.1:0", "Loopback listener address")
	connectTimeout := flags.Duration("connect-timeout", 10*time.Second, "Proxy tunnel establishment timeout")
	if err := flags.Parse(args); err != nil {
		return 2, err
	}
	host, _, splitErr := net.SplitHostPort(*listen)
	if *input == "" || *authFile == "" || *connectTimeout <= 0 || splitErr != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return 2, errors.New("input, auth-file and a loopback listener are required")
	}
	token, err := os.ReadFile(*authFile)
	if err != nil || len(strings.TrimSpace(string(token))) < 32 {
		return 1, errors.New("cannot read a valid bridge token")
	}
	b := &proxyBridge{entries: make(map[int64]*bridgeEntry), token: strings.TrimSpace(string(token)),
		connectTimeout: *connectTimeout, target: "www.youtube.com:443", connections: make(map[net.Conn]struct{})}
	if err := walkCandidates(context.Background(), Shard{File: *input}, func(candidate Candidate) error {
		if b.entries[candidate.ID] != nil {
			return errors.New("duplicate catalog identity")
		}
		b.entries[candidate.ID] = &bridgeEntry{candidate: candidate}
		return nil
	}); err != nil {
		return 1, err
	}
	if len(b.entries) == 0 {
		return 1, errors.New("empty catalog selection")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return 1, errors.New("cannot open bridge listener")
	}
	server := &http.Server{Handler: b, ErrorLog: log.New(io.Discard, "", 0)}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"event": "ready", "address": listener.Addr().String(), "configurations": len(b.entries)}); err != nil {
		server.Close()
		b.close()
		return 1, err
	}
	select {
	case <-ctx.Done():
		server.Close()
		b.close()
		<-finished
	case err := <-finished:
		b.close()
		if !errors.Is(err, http.ErrServerClosed) {
			return 1, errors.New("bridge listener failed")
		}
	}
	return 0, nil
}
