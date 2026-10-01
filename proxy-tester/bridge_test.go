package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/listener/sing_vless"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type vlessFixtureHandler struct{}

func (vlessFixtureHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", metadata.Destination.String())
	if err != nil {
		return err
	}
	relay(conn, upstream)
	return nil
}
func (vlessFixtureHandler) NewPacketConnection(context.Context, N.PacketConn, M.Metadata) error {
	return errors.New("TCP fixture only")
}
func (vlessFixtureHandler) NewError(context.Context, error) {}

func vlessProxyFixture(t *testing.T) Candidate {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const userID = "622693d9-5812-4542-8344-a32bbb5bfbcd"
	service := sing_vless.NewService[string](vlessFixtureHandler{})
	service.UpdateUsers([]string{"test"}, []string{userID}, []string{""})
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer conn.Close()
				service.NewConnection(context.Background(), conn, M.Metadata{})
			})
		}
	}()
	t.Cleanup(func() { listener.Close(); <-accepted; workers.Wait() })
	candidate := candidateAt(listener.Addr().String(), "vless")
	candidate.Settings["uuid"] = userID
	return candidate
}

func bridgeFixture(t *testing.T, candidate Candidate, target string) (*proxyBridge, *url.URL) {
	t.Helper()
	b := &proxyBridge{entries: map[int64]*bridgeEntry{candidate.ID: {candidate: candidate}},
		token: strings.Repeat("a", 64), connectTimeout: 150 * time.Millisecond,
		target: target, allowPrivate: true, connections: make(map[net.Conn]struct{})}
	server := httptest.NewServer(b)
	t.Cleanup(func() { server.Close(); b.close() })
	proxyURL, _ := url.Parse(server.URL)
	proxyURL.User = url.UserPassword("1", b.token)
	return b, proxyURL
}

func TestBridgeTransportsKeepTLSAndReuseTheTunnel(t *testing.T) {
	for _, protocol := range []string{"http", "https", "socks4", "socks5", "vless"} {
		t.Run(protocol, func(t *testing.T) {
			var mu sync.Mutex
			connections := map[string]bool{}
			target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				mu.Lock()
				connections[request.RemoteAddr] = true
				mu.Unlock()
				io.WriteString(w, "verified target data")
			}))
			t.Cleanup(target.Close)
			var candidate Candidate
			switch protocol {
			case "http", "https":
				candidate = httpProxyFixture(t, 0, true, protocol == "https")
			case "socks4":
				candidate = socksProxyFixture(t, 4, false)
			case "socks5":
				candidate = socksProxyFixture(t, 5, true)
			case "vless":
				candidate = vlessProxyFixture(t)
			}
			// Catalog labels may differ from the observed protocol.
			candidate.WorkingProtocol, candidate.Protocol = protocol, "unknown"
			_, proxyURL := bridgeFixture(t, candidate, target.Listener.Addr().String())
			roots := x509.NewCertPool()
			roots.AddCert(target.Certificate())
			transport := &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: roots},
				OnProxyConnectResponse: func(_ context.Context, _ *url.URL, _ *http.Request, response *http.Response) error {
					if response.Header.Get("X-Proxy-Connected") != "true" || response.Header.Get("X-Proxy-Error") != "" {
						return errors.New("successful tunnel is missing its connection observation")
					}
					return nil
				}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			for range 2 {
				response, err := client.Get(target.URL)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "verified target data" || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
					t.Fatalf("missing verified target data: %v", err)
				}
				// Reuse after the establishment deadline would have expired.
				time.Sleep(180 * time.Millisecond)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(connections) != 1 {
				t.Fatalf("target connection was not reused: %d", len(connections))
			}
		})
	}
}

func TestBridgeSeparatesLocalFailuresFromUpstreamFailures(t *testing.T) {
	candidate := httpProxyFixture(t, 407, false, false)
	b, proxyURL := bridgeFixture(t, candidate, "www.youtube.com:443")
	request := func(proxy *url.URL, target string) string {
		transport := &http.Transport{Proxy: http.ProxyURL(proxy)}
		defer transport.CloseIdleConnections()
		_, err := (&http.Client{Transport: transport, Timeout: time.Second}).Get(target)
		if err == nil {
			t.Fatal("unexpected successful request")
		}
		return err.Error()
	}
	if err := request(proxyURL, "https://www.youtube.com/"); !strings.Contains(err, "Bad Gateway") || strings.Contains(err, "Local Error") {
		t.Fatalf("upstream failure misclassified: %s", err)
	}
	wrongToken := *proxyURL
	wrongToken.User = url.UserPassword("1", "incorrect")
	if err := request(&wrongToken, "https://www.youtube.com/"); !strings.Contains(err, "Proxy Bridge Local Error") {
		t.Fatalf("authentication failure misclassified: %s", err)
	}
	wrongID := *proxyURL
	wrongID.User = url.UserPassword("2", b.token)
	if err := request(&wrongID, "https://www.youtube.com/"); !strings.Contains(err, "Proxy Bridge Local Error") {
		t.Fatalf("identity failure misclassified: %s", err)
	}
	if err := request(proxyURL, "https://example.org/"); !strings.Contains(err, "Proxy Bridge Local Error") {
		t.Fatalf("unexpected target was not rejected: %s", err)
	}
}

func TestBridgeReportsTheUpstreamConnectionOnHandshakeAndDialFailures(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddress := listener.Addr().String()
	listener.Close()
	for _, test := range []struct {
		name      string
		candidate Candidate
		connected string
		failure   string
	}{
		{"refused", candidateAt(closedAddress, "http"), "false", "connect:connection_refused"},
		{"rejected", httpProxyFixture(t, 407, false, false), "true", "proxy_handshake:proxy_http_407"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, proxyURL := bridgeFixture(t, test.candidate, "www.youtube.com:443")
			var observed *http.Response
			transport := &http.Transport{Proxy: http.ProxyURL(proxyURL),
				OnProxyConnectResponse: func(_ context.Context, _ *url.URL, _ *http.Request, response *http.Response) error {
					observed = response
					return nil
				}}
			defer transport.CloseIdleConnections()
			_, err := (&http.Client{Transport: transport, Timeout: time.Second}).Get("https://www.youtube.com/")
			if err == nil || observed == nil || observed.StatusCode != http.StatusBadGateway {
				t.Fatalf("expected an upstream failure: %v", err)
			}
			if observed.Header.Get("X-Proxy-Connected") != test.connected || observed.Header.Get("X-Proxy-Error") != test.failure {
				t.Fatalf("wrong connection observation: %v", observed.Header)
			}
		})
	}
}

func TestBridgeReusedAdapterDoesNotReuseAnEarlierConnectionObservation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddress := listener.Addr().String()
	listener.Close()
	candidate := candidateAt(closedAddress, "vless")
	candidate.Settings["uuid"] = "622693d9-5812-4542-8344-a32bbb5bfbcd"
	b, _ := bridgeFixture(t, candidate, "www.youtube.com:443")
	entry := b.entries[candidate.ID]
	reusedDialer := &attemptDialer{connectTimeout: time.Second, allowPrivate: true}
	reusedDialer.connected.Store(true)
	entry.prepare(reusedDialer)
	if entry.err != nil {
		t.Fatal(entry.err)
	}
	for range 2 {
		conn, connected, err := b.dial(context.Background(), entry)
		if conn != nil {
			conn.Close()
		}
		if err == nil || connected {
			t.Fatalf("failed request inherited a previous success: connected=%v, error=%v", connected, err)
		}
	}
}

func TestBridgeDoesNotDisableTargetCertificateVerification(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) { w.WriteHeader(200) }))
	t.Cleanup(target.Close)
	_, proxyURL := bridgeFixture(t, httpProxyFixture(t, 0, false, false), target.Listener.Addr().String())
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer transport.CloseIdleConnections()
	_, err := (&http.Client{Transport: transport, Timeout: time.Second}).Get(target.URL)
	var verification *tls.CertificateVerificationError
	if !errors.As(err, &verification) {
		t.Fatalf("expected certificate verification error, got %v", err)
	}
}

func TestBridgeConcurrentVLESSConnectionsAndShutdown(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		io.WriteString(w, "concurrent data")
	}))
	t.Cleanup(target.Close)
	b, proxyURL := bridgeFixture(t, vlessProxyFixture(t), target.Listener.Addr().String())
	b.connectTimeout = 2 * time.Second
	roots := x509.NewCertPool()
	roots.AddCert(target.Certificate())
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: roots},
		MaxIdleConnsPerHost: 16}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	results := make(chan error, 16)
	for range 16 {
		go func() {
			response, err := client.Get(target.URL)
			if err == nil {
				var body []byte
				body, err = io.ReadAll(response.Body)
				response.Body.Close()
				if err == nil && string(body) != "concurrent data" {
					err = errors.New("wrong target data")
				}
			}
			results <- err
		}()
	}
	for range 16 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	closed := make(chan struct{})
	go func() { b.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("bridge shutdown left active tunnels")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.connections) != 0 {
		t.Fatalf("bridge leaked %d connections", len(b.connections))
	}
}

func TestBridgeLocalResourceErrorsHaveDistinctCodes(t *testing.T) {
	for _, errno := range []error{syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM, syscall.EADDRNOTAVAIL, syscall.EADDRINUSE} {
		err := &net.OpError{Op: "dial", Net: "tcp", Err: errno}
		if code := errorCode(err); !strings.HasPrefix(code, "local_") {
			t.Fatalf("local resource failure was classified as %s", code)
		}
	}
	if code := errorCode(syscall.ECONNREFUSED); strings.HasPrefix(code, "local_") {
		t.Fatalf("upstream connection refusal was classified as %s", code)
	}
}
