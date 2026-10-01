package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Counters struct {
	Completed      int64            `json:"completed"`
	Attempted      int64            `json:"attempted"`
	Responses      int64            `json:"youtube_responses"`
	Attempts       int64            `json:"protocol_attempts"`
	BodyBytes      int64            `json:"body_bytes"`
	BodyIncomplete int64            `json:"body_incomplete"`
	LocalErrors    int64            `json:"local_errors"`
	Status         map[string]int64 `json:"statuses"`
	Errors         map[string]int64 `json:"errors"`
	HTTPStatus     map[int]int64    `json:"http_statuses"`
	Protocol       map[string]int64 `json:"protocols"`
}

func newCounters() Counters {
	return Counters{Status: map[string]int64{}, Errors: map[string]int64{}, HTTPStatus: map[int]int64{}, Protocol: map[string]int64{}}
}

func (c *Counters) Add(result Result) {
	if result.Status == "local_error" {
		c.LocalErrors++
		return
	}
	c.Completed++
	c.Status[result.Status]++
	c.Protocol[result.DeclaredProtocol]++
	if result.Attempted {
		c.Attempted++
	}
	if result.Responds {
		c.Responses++
	}
	for _, attempt := range result.Attempts {
		if attempt.Attempted {
			c.Attempts++
		}
		c.BodyBytes += attempt.BodyBytes
		if attempt.Status == "responds" {
			c.HTTPStatus[attempt.HTTPStatus]++
			if !attempt.BodyComplete {
				c.BodyIncomplete++
			}
		}
		if attempt.ErrorCode != "" {
			c.Errors[attempt.Stage+":"+attempt.ErrorCode]++
		}
	}
}

type RunMetadata struct {
	Version        string `json:"version"`
	RunID          string `json:"run_id"`
	InputDigest    string `json:"input_sha256"`
	SelectionHash  string `json:"selection_sha256,omitempty"`
	Input          string `json:"input"`
	Target         string `json:"target"`
	Method         string `json:"method"`
	VideoID        string `json:"video_id"`
	ClientVersion  string `json:"client_version"`
	Fields         string `json:"fields"`
	AcceptEncoding string `json:"accept_encoding"`
	ConnectTimeout string `json:"connect_timeout"`
	TotalTimeout   string `json:"total_timeout"`
}

func run(args []string) (int, error) {
	flags := flag.NewFlagSet("proxy-tester", flag.ContinueOnError)
	input := flags.String("input", "", "Manifest or .jsonl[.gz] snapshot")
	output := flags.String("output", "", "Append-only result journal")
	runID := flags.String("run-id", "", "Stable identifier for this test round")
	videoID := flags.String("video-id", defaultVideoID, "Fixed video used for every metadata request")
	clientVersion := flags.String("client-version", defaultClientVersion, "YouTube web client version")
	concurrency := flags.Int("concurrency", 1000, "Simultaneous candidate tests")
	connectTimeout := flags.Duration("connect-timeout", 3*time.Second, "DNS and TCP connection deadline")
	totalTimeout := flags.Duration("total-timeout", 10*time.Second, "Total deadline per protocol attempt")
	duration := flags.Duration("duration", 0, "Optional maximum run duration for benchmarking")
	limit := flags.Int64("limit", 0, "Optional maximum new candidates in this invocation")
	onlyIDs := flags.String("only-ids", "", "Optional newline-separated proxy ID selection")
	progressInterval := flags.Duration("progress-interval", 5*time.Second, "Durable progress update interval")
	if err := flags.Parse(args); err != nil {
		return 2, err
	}
	if *input == "" || *output == "" || *runID == "" || *concurrency < 1 || *connectTimeout <= 0 || *totalTimeout <= 0 || *totalTimeout < *connectTimeout || *duration < 0 || *limit < 0 || *progressInterval <= 0 {
		return 2, errors.New("input, output, run-id and positive limits are required")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`).MatchString(*videoID) || *clientVersion == "" {
		return 2, errors.New("valid video ID and client version are required")
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		return 1, err
	}
	lock, err := os.OpenFile(*output+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 1, err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return 1, errors.New("another tester owns this output journal")
	}
	shards, expected, err := inputShards(*input)
	if err != nil {
		return 1, err
	}
	digest, err := digestFile(*input)
	if err != nil {
		return 1, err
	}
	selection, err := readIDSet(*onlyIDs)
	if err != nil {
		return 1, err
	}
	selectionHash := ""
	if *onlyIDs != "" {
		selectionHash, err = digestFile(*onlyIDs)
		if err != nil {
			return 1, err
		}
		expected = int64(len(selection))
	}
	metadata := RunMetadata{Version: testerVersion, RunID: *runID, InputDigest: digest,
		SelectionHash: selectionHash, Input: *input, Target: metadataEndpoint, Method: "POST",
		VideoID: *videoID, ClientVersion: *clientVersion, Fields: metadataFields, AcceptEncoding: "gzip",
		ConnectTimeout: connectTimeout.String(), TotalTimeout: totalTimeout.String()}
	if data, err := os.ReadFile(*output + ".meta.json"); err == nil {
		var previous RunMetadata
		if json.Unmarshal(data, &previous) != nil || previous != metadata {
			return 1, errors.New("resume metadata mismatch; use a new result journal")
		}
	} else if !os.IsNotExist(err) {
		return 1, err
	} else if err := atomicJSON(*output+".meta.json", metadata); err != nil {
		return 1, err
	}
	counters := newCounters()
	done, err := loadJournal(*output, counters.Add)
	if err != nil {
		return 1, err
	}
	previousCount := counters.Completed
	resultsFile, err := os.OpenFile(*output, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 1, err
	}
	defer resultsFile.Close()
	writer := bufio.NewWriterSize(resultsFile, 1<<20)
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	if *duration > 0 {
		var deadlineCancel context.CancelFunc
		ctx, deadlineCancel = context.WithTimeout(ctx, *duration)
		defer deadlineCancel()
	}
	started := time.Now()
	jobs := make(chan Candidate, min(*concurrency, 4096))
	results := make(chan Result, min(*concurrency, 4096))
	readerDone := make(chan error, 1)
	var active, queued atomic.Int64
	go func() {
		defer close(jobs)
		var readErr error
		for _, shard := range shards {
			readErr = walkCandidates(ctx, shard, func(candidate Candidate) error {
				if _, exists := done[candidate.ID]; exists {
					return nil
				}
				if selection != nil {
					if _, exists := selection[candidate.ID]; !exists {
						return nil
					}
				}
				if *limit > 0 && queued.Load() >= *limit {
					return io.EOF
				}
				select {
				case jobs <- candidate:
					queued.Add(1)
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if readErr != nil {
				break
			}
		}
		readerDone <- readErr
	}()
	options := Options{ConnectTimeout: *connectTimeout, TotalTimeout: *totalTimeout, TargetURL: metadata.Target, VideoID: *videoID, ClientVersion: *clientVersion}
	var workers sync.WaitGroup
	for range *concurrency {
		workers.Go(func() {
			for candidate := range jobs {
				if ctx.Err() != nil {
					return
				}
				active.Add(1)
				result := checkCandidate(ctx, candidate, options)
				active.Add(-1)
				if ctx.Err() != nil && !result.Responds {
					return
				}
				for _, attempt := range result.Attempts {
					if strings.HasPrefix(attempt.ErrorCode, "local_") {
						result.Status = "local_error"
						break
					}
				}
				results <- result
			}
		})
	}
	go func() { workers.Wait(); close(results) }()

	flush := func() error {
		if err := writer.Flush(); err != nil {
			return err
		}
		return resultsFile.Sync()
	}
	state := "running"
	var writeErr error
	writeProgress := func() error {
		if err := flush(); err != nil {
			return err
		}
		seconds := time.Since(started).Seconds()
		progress := map[string]any{"version": testerVersion, "run_id": *runID, "state": state,
			"updated_at": time.Now().UTC(), "elapsed_seconds": seconds, "concurrency": *concurrency,
			"connect_timeout_seconds": connectTimeout.Seconds(), "total_timeout_seconds": totalTimeout.Seconds(),
			"expected": expected, "resumed_completed": previousCount, "queued_this_invocation": queued.Load(),
			"active": active.Load(), "new_completed": counters.Completed - previousCount,
			"completed_per_second": float64(counters.Completed-previousCount) / max(seconds, 0.001),
			"counters":             counters, "goroutines": runtime.NumGoroutine()}
		if err := atomicJSON(*output+".summary.json", progress); err != nil {
			return err
		}
		compact := map[string]any{"run_id": *runID, "state": state, "completed": counters.Completed,
			"responds": counters.Responses, "active": active.Load(), "seconds": int(seconds),
			"per_second": int(float64(counters.Completed-previousCount) / max(seconds, 0.001)), "local_errors": counters.LocalErrors}
		data, _ := json.Marshal(compact)
		fmt.Println(string(data))
		return nil
	}
	ticker := time.NewTicker(*progressInterval)
	defer ticker.Stop()
	for running := true; running; {
		select {
		case result, exists := <-results:
			if !exists {
				running = false
				break
			}
			if writeErr != nil {
				continue
			}
			data, err := json.Marshal(result)
			if err == nil {
				_, err = writer.Write(append(data, '\n'))
			}
			if err != nil {
				writeErr = err
				cancel()
				continue
			}
			counters.Add(result)
			if counters.LocalErrors >= int64(max(32, *concurrency/100)) {
				cancel()
			}
		case <-ticker.C:
			if writeErr == nil {
				writeErr = writeProgress()
				if writeErr != nil {
					cancel()
				}
			}
		}
	}
	readErr := <-readerDone
	if writeErr != nil {
		return 1, writeErr
	}
	if readErr != nil && readErr != io.EOF && !errors.Is(readErr, context.Canceled) && !errors.Is(readErr, context.DeadlineExceeded) {
		state = "input_error"
		_ = writeProgress()
		return 1, readErr
	}
	state = "complete"
	if ctx.Err() != nil {
		state = "interrupted"
	}
	if counters.LocalErrors >= int64(max(32, *concurrency/100)) {
		state = "local_overload"
	}
	if expected > 0 && counters.Completed != expected && *limit == 0 && state == "complete" {
		state = "coverage_mismatch"
	}
	if err := writeProgress(); err != nil {
		return 1, err
	}
	if state == "local_overload" {
		return 75, nil
	}
	if state == "coverage_mismatch" {
		return 1, errors.New("completed results do not match the selected input count")
	}
	return 0, nil
}

func main() {
	var code int
	var err error
	if len(os.Args) > 1 && os.Args[1] == "audit" {
		code, err = audit(os.Args[2:])
	} else if len(os.Args) > 1 && os.Args[1] == "bridge" {
		code, err = bridge(os.Args[2:])
	} else {
		code, err = run(os.Args[1:])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "proxy-tester:", err)
	}
	os.Exit(code)
}
