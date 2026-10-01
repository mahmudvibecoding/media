package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureOptions(server *httptest.Server) Options {
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	return Options{ConnectTimeout: time.Second, TotalTimeout: 2 * time.Second,
		TargetURL: server.URL + "/youtubei/v1/player", RootCAs: roots, AllowPrivate: true,
		VideoID: defaultVideoID, ClientVersion: defaultClientVersion}
}

func candidateAt(address, protocol string) Candidate {
	host, port, _ := net.SplitHostPort(address)
	number, _ := strconv.Atoi(port)
	return Candidate{ID: 1, Key: strings.Repeat("a", 64), Address: host, Port: number,
		Protocol: protocol, Settings: map[string]any{}}
}

func relay(left, right net.Conn) {
	defer left.Close()
	defer right.Close()
	done := make(chan struct{})
	go func() { io.Copy(left, right); left.Close(); close(done) }()
	io.Copy(right, left)
	right.Close()
	<-done
}

func httpProxyFixture(t *testing.T, reply int, authentication bool, secure bool) Candidate {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reply != 0 {
			w.Header().Set("Server", "YouTube")
			w.WriteHeader(reply)
			io.WriteString(w, "Sign in to confirm you are not a bot")
			return
		}
		if r.Method != http.MethodConnect {
			t.Errorf("expected CONNECT, got %s", r.Method)
			w.WriteHeader(405)
			return
		}
		if authentication && r.Header.Get("Proxy-Authorization") != "Basic dXNlcjpwYXNz" {
			w.WriteHeader(407)
			return
		}
		upstream, err := net.DialTimeout("tcp", r.Host, time.Second)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		buffered.WriteString("HTTP/1.1 200 Connection established\r\n\r\n")
		buffered.Flush()
		relay(client, upstream)
	})
	var server *httptest.Server
	protocol := "http"
	if secure {
		server = httptest.NewTLSServer(handler)
		protocol = "https"
	} else {
		server = httptest.NewServer(handler)
	}
	t.Cleanup(server.Close)
	candidate := candidateAt(server.Listener.Addr().String(), protocol)
	if secure {
		candidate.Settings["skip-cert-verify"] = true
	}
	if authentication {
		candidate.Settings["username"], candidate.Settings["password"] = "user", "pass"
	}
	return candidate
}

func socksProxyFixture(t *testing.T, version int, authenticate bool) Candidate {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(3 * time.Second))
				reader := bufio.NewReader(conn)
				var target string
				if version == 5 {
					hello := make([]byte, 2)
					if _, err := io.ReadFull(reader, hello); err != nil || hello[0] != 5 {
						return
					}
					if _, err := io.CopyN(io.Discard, reader, int64(hello[1])); err != nil {
						return
					}
					method := byte(0)
					if authenticate {
						method = 2
					}
					conn.Write([]byte{5, method})
					if authenticate {
						if _, err := io.ReadFull(reader, hello); err != nil || hello[0] != 1 {
							return
						}
						user := make([]byte, hello[1])
						io.ReadFull(reader, user)
						length, err := reader.ReadByte()
						if err != nil {
							return
						}
						password := make([]byte, length)
						io.ReadFull(reader, password)
						if string(user) != "user" || string(password) != "pass" {
							conn.Write([]byte{1, 1})
							return
						}
						conn.Write([]byte{1, 0})
					}
					header := make([]byte, 5)
					if _, err := io.ReadFull(reader, header); err != nil || header[0] != 5 || header[3] != 3 {
						return
					}
					host := make([]byte, header[4])
					if _, err := io.ReadFull(reader, host); err != nil {
						return
					}
					port := make([]byte, 2)
					if _, err := io.ReadFull(reader, port); err != nil {
						return
					}
					target = net.JoinHostPort(string(host), strconv.Itoa(int(binary.BigEndian.Uint16(port))))
				} else {
					header := make([]byte, 8)
					if _, err := io.ReadFull(reader, header); err != nil || header[0] != 4 {
						return
					}
					if _, err := reader.ReadString(0); err != nil {
						return
					}
					host := net.IP(header[4:8]).String()
					if host == "0.0.0.1" {
						value, err := reader.ReadString(0)
						if err != nil {
							return
						}
						host = strings.TrimSuffix(value, "\x00")
					}
					target = net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(header[2:4]))))
				}
				upstream, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					return
				}
				if version == 5 {
					conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1})
				} else {
					conn.Write([]byte{0, 90, 0, 1, 127, 0, 0, 1})
				}
				relay(conn, upstream)
			})
		}
	}()
	t.Cleanup(func() { listener.Close(); workers.Wait() })
	candidate := candidateAt(listener.Addr().String(), fmt.Sprintf("socks%d", version))
	if authenticate {
		candidate.Settings["username"], candidate.Settings["password"] = "user", "pass"
	}
	return candidate
}

func TestAnyVerifiedYouTubeStatusAndCompleteGzipBody(t *testing.T) {
	for _, status := range []int{200, 302, 403, 427, 429, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			body := strings.Repeat("Sign in to confirm you are not a bot. ", 1000)
			var calls atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/youtubei/v1/player" || r.Header.Get("Accept-Encoding") != "gzip" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("metadata request shape mismatch")
				}
				if r.URL.Query().Get("fields") != metadataFields || r.URL.Query().Get("prettyPrint") != "false" {
					t.Error("field selection mismatch")
				}
				var payload map[string]any
				if json.NewDecoder(r.Body).Decode(&payload) != nil || payload["videoId"] != defaultVideoID {
					t.Error("video payload mismatch")
				}
				client := object(object(payload["context"])["client"])
				if client["clientName"] != "WEB" || client["clientVersion"] != defaultClientVersion || client["hl"] != "en" {
					t.Error("client payload mismatch")
				}
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Location", "https://example.com/never-follow")
				w.WriteHeader(status)
				compressed := gzip.NewWriter(w)
				io.WriteString(compressed, body)
				compressed.Close()
			}))
			defer server.Close()
			candidate := httpProxyFixture(t, 0, true, false)
			result := checkCandidate(context.Background(), candidate, fixtureOptions(server))
			if !result.Responds || len(result.Attempts) != 1 {
				t.Fatalf("expected response: %+v", result)
			}
			attempt := result.Attempts[0]
			if attempt.HTTPStatus != status || !attempt.TLSVerified || !attempt.BodyComplete || attempt.BodyBytes != int64(len(body)) || attempt.ResponseBodyBytes >= attempt.BodyBytes || !attempt.RequestSent {
				t.Fatalf("wrong body or status: %+v", attempt)
			}
			if calls.Load() != 1 {
				t.Fatalf("sent %d metadata requests", calls.Load())
			}
		})
	}
}

func TestProxyHTTPErrorIsNotAYouTubeResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("target should not be reached") }))
	defer server.Close()
	for _, status := range []int{403, 407, 502} {
		candidate := httpProxyFixture(t, status, false, false)
		result := checkCandidate(context.Background(), candidate, fixtureOptions(server))
		if result.Responds || result.Attempts[0].HTTPStatus != 0 || result.Attempts[0].ErrorCode != fmt.Sprintf("proxy_http_%d", status) {
			t.Fatalf("proxy error was misclassified: %+v", result)
		}
	}
}

func TestHTTPSProxyAndSOCKS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403); io.WriteString(w, "blocked") }))
	defer server.Close()
	for _, build := range []struct {
		name      string
		candidate func(*testing.T) Candidate
	}{
		{"https", func(t *testing.T) Candidate { return httpProxyFixture(t, 0, true, true) }},
		{"socks5", func(t *testing.T) Candidate { return socksProxyFixture(t, 5, false) }},
		{"socks5_auth", func(t *testing.T) Candidate { return socksProxyFixture(t, 5, true) }},
		{"socks4", func(t *testing.T) Candidate { return socksProxyFixture(t, 4, false) }},
	} {
		t.Run(build.name, func(t *testing.T) {
			result := checkCandidate(context.Background(), build.candidate(t), fixtureOptions(server))
			if !result.Responds {
				t.Fatalf("proxy failed: %+v", result)
			}
		})
	}
}

func TestUnknownProtocolDetectionAndSingleMetadataRequest(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(427) }))
	defer server.Close()
	candidate := socksProxyFixture(t, 5, false)
	candidate.Protocol = "unknown"
	result := checkCandidate(context.Background(), candidate, fixtureOptions(server))
	if !result.Responds || result.DetectedProtocol != "socks5" || len(result.Attempts) != 2 || calls.Load() != 1 {
		t.Fatalf("detection failed: %+v requests=%d", result, calls.Load())
	}
}

func TestTargetCertificateAlwaysVerified(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unverified target should not receive request") }))
	defer server.Close()
	candidate := httpProxyFixture(t, 0, false, true)
	options := fixtureOptions(server)
	options.RootCAs = nil
	result := checkCandidate(context.Background(), candidate, options)
	if result.Responds || result.Attempts[0].TLSVerified || result.Attempts[0].ErrorCode != "youtube_certificate_verification_failed" {
		t.Fatalf("target verification was relaxed: %+v", result)
	}
}

func TestBodyTimeoutPreservesResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Length", "100000")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				io.WriteString(w, "x")
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer server.Close()
	options := fixtureOptions(server)
	options.TotalTimeout = 150 * time.Millisecond
	result := checkCandidate(context.Background(), httpProxyFixture(t, 0, false, false), options)
	if !result.Responds || result.Attempts[0].BodyComplete || result.Attempts[0].BodyError != "timeout" || result.TotalMS > 1000 {
		t.Fatalf("body timeout mismatch: %+v", result)
	}
}

func TestHeaderTimeoutDoesNotRepeatMetadataRequest(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	options := fixtureOptions(server)
	options.TotalTimeout = 150 * time.Millisecond
	candidate := httpProxyFixture(t, 0, false, false)
	candidate.Protocol = "unknown"
	result := checkCandidate(context.Background(), candidate, options)
	if result.Responds || len(result.Attempts) != 1 || !result.Attempts[0].RequestSent || calls.Load() != 1 || result.TotalMS > 1000 {
		t.Fatalf("request was repeated or deadline failed: %+v", result)
	}
}

func TestUnreachableEndpointDoesNotRepeatProtocols(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	candidate := candidateAt(listener.Addr().String(), "unknown")
	listener.Close()
	result := checkCandidate(context.Background(), candidate, Options{ConnectTimeout: 100 * time.Millisecond, TotalTimeout: 200 * time.Millisecond, TargetURL: metadataEndpoint, AllowPrivate: true})
	if result.Responds || len(result.Attempts) != 1 || result.Attempts[0].Stage != "connect" {
		t.Fatalf("unreachable endpoint retried per protocol: %+v", result)
	}
}
