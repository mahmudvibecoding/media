package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/adapter"
	C "github.com/metacubex/mihomo/constant"
	clog "github.com/metacubex/mihomo/log"
)

const testerVersion = "1.0.1"

const metadataEndpoint = "https://www.youtube.com/youtubei/v1/player"
const metadataFields = "videoDetails(videoId,title,shortDescription,lengthSeconds,thumbnail/thumbnails/url),microformat/playerMicroformatRenderer/publishDate,playabilityStatus(status,reason)"
const defaultVideoID = "1hzvCKusdpc"
const defaultClientVersion = "2.20260925.01.00"

func init() { clog.SetLevel(clog.SILENT) }

type Options struct {
	ConnectTimeout time.Duration
	TotalTimeout   time.Duration
	TargetURL      string
	VideoID        string
	ClientVersion  string
	RootCAs        *x509.CertPool
	AllowPrivate   bool
}

type Attempt struct {
	Protocol          string  `json:"protocol"`
	Status            string  `json:"status"`
	Stage             string  `json:"stage,omitempty"`
	ErrorCode         string  `json:"error_code,omitempty"`
	ErrorType         string  `json:"error_type,omitempty"`
	HTTPStatus        int     `json:"http_status,omitempty"`
	HTTPVersion       string  `json:"http_version,omitempty"`
	BodyError         string  `json:"body_error,omitempty"`
	BodyBytes         int64   `json:"body_bytes,omitempty"`
	ResponseBodyBytes int64   `json:"response_body_bytes,omitempty"`
	RequestBodyBytes  int64   `json:"request_body_bytes,omitempty"`
	ContentEncoding   string  `json:"content_encoding,omitempty"`
	RequestSent       bool    `json:"request_sent"`
	BodyComplete      bool    `json:"body_complete"`
	ConnectMS         float64 `json:"connect_ms,omitempty"`
	HeadersMS         float64 `json:"headers_ms,omitempty"`
	TotalMS           float64 `json:"total_ms"`
	Connected         bool    `json:"connected"`
	Attempted         bool    `json:"attempted"`
	TLSVerified       bool    `json:"tls_verified"`
	RetryAfter        string  `json:"retry_after,omitempty"`
}

type countingReader struct {
	io.Reader
	n int64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	r.n += int64(n)
	return n, err
}

func metadataPayload(options Options) (string, []byte) {
	videoID, clientVersion := options.VideoID, options.ClientVersion
	if videoID == "" {
		videoID = defaultVideoID
	}
	if clientVersion == "" {
		clientVersion = defaultClientVersion
	}
	payload, _ := json.Marshal(map[string]any{"context": map[string]any{"client": map[string]any{
		"clientName": "WEB", "clientVersion": clientVersion, "hl": "en"}}, "videoId": videoID})
	target, _ := url.Parse(options.TargetURL)
	query := target.Query()
	query.Set("prettyPrint", "false")
	query.Set("fields", metadataFields)
	target.RawQuery = query.Encode()
	return target.String(), payload
}

type Result struct {
	ID               int64     `json:"id"`
	Key              string    `json:"key"`
	DeclaredProtocol string    `json:"declared_protocol"`
	DetectedProtocol string    `json:"detected_protocol,omitempty"`
	TestedAt         time.Time `json:"tested_at"`
	Status           string    `json:"status"`
	Responds         bool      `json:"responds"`
	Attempted        bool      `json:"attempted"`
	TotalMS          float64   `json:"total_ms"`
	Attempts         []Attempt `json:"attempts"`
}

func probe(parent context.Context, candidate Candidate, protocol string, options Options) (out Attempt) {
	started := time.Now()
	out.Protocol, out.Status = protocol, "not_responding"
	defer func() {
		out.TotalMS = float64(time.Since(started).Microseconds()) / 1000
		if recovered := recover(); recovered != nil {
			out.Status, out.ErrorCode = "internal_error", "adapter_panic"
			out.ErrorType = fmt.Sprintf("%T", recovered)
		}
	}()
	settings, err := normalize(candidate, protocol)
	if err != nil {
		out.Status, out.Stage, out.ErrorCode = "invalid_configuration", "configuration", string(err.(configError))
		if out.ErrorCode == "telegram_only_protocol" || out.ErrorCode == "unsupported_protocol" || strings.HasPrefix(out.ErrorCode, "unsupported_") {
			out.Status = "incompatible_protocol"
		}
		return
	}
	ctx, cancel := context.WithTimeout(parent, options.TotalTimeout)
	defer cancel()
	var verifiedTLS, sentRequest atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err == nil && len(state.VerifiedChains) > 0 {
				verifiedTLS.Store(true)
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				sentRequest.Store(true)
			}
		},
	})
	defer func() {
		out.TLSVerified = verifiedTLS.Load()
		out.RequestSent = sentRequest.Load()
		if !out.RequestSent {
			out.RequestBodyBytes = 0
		}
	}()
	dialer := &attemptDialer{connectTimeout: options.ConnectTimeout, allowPrivate: options.AllowPrivate}
	defer func() {
		out.ConnectMS = float64(dialer.connectMicros.Load()) / 1000
		out.Connected = dialer.connected.Load()
	}()
	native := protocol == "http" || protocol == "https" || protocol == "socks4" || protocol == "socks5"
	var proxy C.Proxy
	if !native {
		proxy, err = adapter.ParseProxy(settings, adapter.WithDialerForAPI(dialer))
		if err != nil {
			out.Status, out.Stage, out.ErrorCode = "invalid_configuration", "configuration", "adapter_configuration_rejected"
			out.ErrorType = fmt.Sprintf("%T", err)
			return
		}
		defer proxy.Close()
	}
	transport := &http.Transport{
		Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: true,
		TLSClientConfig:        &tls.Config{RootCAs: options.RootCAs, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:    options.TotalTimeout,
		ResponseHeaderTimeout:  options.TotalTimeout,
		MaxResponseHeaderBytes: 1 << 20,
		DialContext: func(_ context.Context, network, address string) (net.Conn, error) {
			// This transport serves one request. Use its deadline even while dialing.
			if native {
				return nativeTunnel(ctx, dialer, candidate, protocol, settings, address)
			}
			host, port, splitErr := net.SplitHostPort(address)
			if splitErr != nil {
				return nil, splitErr
			}
			portNumber, _ := strconv.Atoi(port)
			metadata := &C.Metadata{NetWork: C.TCP, Type: C.INNER, Host: host, DstPort: uint16(portNumber)}
			conn, dialErr := proxy.DialContext(ctx, metadata)
			if dialErr != nil {
				return nil, &StageError{Stage: "proxy_tunnel", Code: errorCode(dialErr), Err: dialErr}
			}
			dialer.connected.Store(true)
			return conn, nil
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	target, payload := metadataPayload(options)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		out.Status, out.ErrorCode, out.Stage = "invalid_configuration", "invalid_target", "configuration"
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "gzip")
	out.RequestBodyBytes = int64(len(payload))
	out.Attempted = true
	response, err := client.Do(request)
	if err != nil {
		out.Stage, out.ErrorCode = "youtube_https", errorCode(err)
		var stage *StageError
		if errors.As(err, &stage) {
			out.Stage, out.ErrorCode = stage.Stage, stage.Code
		}
		var verification *tls.CertificateVerificationError
		if errors.As(err, &verification) && out.Stage == "youtube_https" {
			out.ErrorCode = "youtube_certificate_verification_failed"
		}
		var wrapped *url.Error
		if errors.As(err, &wrapped) {
			out.ErrorType = fmt.Sprintf("%T", wrapped.Err)
		} else {
			out.ErrorType = fmt.Sprintf("%T", err)
		}
		return
	}
	defer response.Body.Close()
	out.HeadersMS = float64(time.Since(started).Microseconds()) / 1000
	out.TLSVerified = response.TLS != nil && len(response.TLS.VerifiedChains) > 0
	if !out.TLSVerified {
		out.Stage, out.ErrorCode = "youtube_https", "unverified_target_tls"
		return
	}
	// HTTP status and body contents do not affect reachability success.
	out.Status, out.HTTPStatus, out.HTTPVersion = "responds", response.StatusCode, response.Proto
	out.ContentEncoding = response.Header.Get("Content-Encoding")
	if len(out.ContentEncoding) > 64 {
		out.ContentEncoding = "unknown"
	}
	out.RetryAfter = response.Header.Get("Retry-After")
	if len(out.RetryAfter) > 128 {
		out.RetryAfter = ""
	}
	counted := &countingReader{Reader: response.Body}
	var body io.Reader = counted
	if strings.EqualFold(out.ContentEncoding, "gzip") {
		decoded, decodeErr := gzip.NewReader(counted)
		if decodeErr != nil {
			out.BodyError = "gzip_decode_error"
			out.ResponseBodyBytes = counted.n
			return
		}
		defer decoded.Close()
		body = decoded
	}
	out.BodyBytes, err = io.Copy(io.Discard, body)
	out.ResponseBodyBytes = counted.n
	if err != nil {
		out.BodyError = errorCode(err)
	} else {
		out.BodyComplete = true
	}
	return
}

func checkCandidate(ctx context.Context, candidate Candidate, options Options) Result {
	started := time.Now()
	result := Result{ID: candidate.ID, Key: candidate.Key, DeclaredProtocol: candidate.Protocol,
		TestedAt: time.Now().UTC(), Status: "not_responding", Attempts: make([]Attempt, 0, 1)}
	protocols := []string{candidate.Protocol}
	if candidate.Protocol == "unknown" {
		protocols = []string{"http", "socks5", "socks4", "https"}
	} else if candidate.Protocol == "https" && !boolean(candidate.Settings["tls"]) {
		// Public list publishers also use 'https' to mean HTTP CONNECT support.
		protocols = []string{"https", "http"}
	}
	for _, protocol := range protocols {
		if ctx.Err() != nil {
			break
		}
		attempt := probe(ctx, candidate, protocol, options)
		result.Attempts = append(result.Attempts, attempt)
		result.Attempted = result.Attempted || attempt.Attempted
		result.Status = attempt.Status
		if attempt.Status == "responds" {
			result.Responds, result.DetectedProtocol = true, protocol
			break
		}
		if attempt.RequestSent || attempt.TLSVerified {
			// The transport is already identified; do not send another metadata request.
			break
		}
		if attempt.Status == "invalid_configuration" || attempt.Status == "incompatible_protocol" || attempt.Status == "internal_error" {
			break
		}
		// A failed TCP connection or DNS lookup is independent of proxy protocol.
		if !attempt.Connected && (attempt.Stage == "connect" || attempt.Stage == "resolve") {
			break
		}
		if strings.HasPrefix(attempt.ErrorCode, "local_") {
			break
		}
	}
	result.TotalMS = float64(time.Since(started).Microseconds()) / 1000
	return result
}
