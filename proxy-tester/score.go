package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type scoredProxy struct {
	ID           int64     `json:"proxy_id"`
	Key          string    `json:"connection_key"`
	Protocol     string    `json:"protocol"`
	Score        int       `json:"score"`
	Responses    int       `json:"youtube_responses"`
	Checks       int       `json:"checks"`
	AverageMS    float64   `json:"average_response_ms"`
	LastResponse time.Time `json:"last_response_at"`
	LastHTTP     int       `json:"last_http_status"`
	TestedAt     time.Time `json:"tested_at"`
	RunID        string    `json:"test_run_id"`
}

type scoreResult struct {
	Row         scoredProxy
	Performed   int
	PortRetries int
	ResponseMS  float64
	Err         error
}

func scoreOne(ctx context.Context, input Candidate, options Options, runID string, check func(context.Context, Candidate, Options) Result) scoreResult {
	out := scoreResult{}
	if input.ID < 1 || len(input.Key) != 64 {
		out.Err = errors.New("invalid scoring input")
		return out
	}
	out.Row = scoredProxy{ID: input.ID, Key: input.Key, Checks: 3, RunID: runID}
	for out.Performed < 3 {
		if ctx.Err() != nil {
			out.Err = ctx.Err()
			return out
		}
		result := check(ctx, input, options)
		if ctx.Err() != nil {
			out.Err = ctx.Err()
			return out
		}
		retry := false
		for _, attempt := range result.Attempts {
			if strings.HasPrefix(attempt.ErrorCode, "local_") {
				if (attempt.ErrorCode == "local_address_in_use" || attempt.ErrorCode == "local_address_unavailable") && out.PortRetries < 240 {
					out.PortRetries++
					select {
					case <-time.After(250 * time.Millisecond):
						retry = true
					case <-ctx.Done():
						out.Err = ctx.Err()
						return out
					}
					break
				}
				out.Err = fmt.Errorf("local resource overload (%s); no final scores published", attempt.ErrorCode)
				return out
			}
		}
		if retry {
			continue
		}
		out.Performed++
		out.Row.TestedAt = result.TestedAt
		if result.Responds {
			out.Row.Score++
			out.ResponseMS += result.TotalMS
			out.Row.Protocol = result.DetectedProtocol
			out.Row.LastResponse = result.TestedAt
			for _, attempt := range result.Attempts {
				if attempt.Status == "responds" {
					out.Row.LastHTTP = attempt.HTTPStatus
					break
				}
			}
		}
	}
	out.Row.Responses = out.Row.Score
	if out.Row.Score > 0 {
		out.Row.AverageMS = out.ResponseMS / float64(out.Row.Score)
	}
	return out
}

// score keeps only responders in RAM. It writes no journal or resume checkpoint.
func score(args []string) (int, error) {
	flags := flag.NewFlagSet("score", flag.ContinueOnError)
	output := flags.String("output", "", "Final responder file")
	runID := flags.String("run-id", "", "Result identifier")
	concurrency := flags.Int("concurrency", 1000, "Concurrent checks")
	expected := flags.Int64("expected", 0, "Required number of input configurations")
	connectTimeout := flags.Duration("connect-timeout", 3*time.Second, "Connection deadline")
	totalTimeout := flags.Duration("total-timeout", 10*time.Second, "Protocol attempt deadline")
	if err := flags.Parse(args); err != nil {
		return 2, err
	}
	if *output == "" || *runID == "" || *concurrency < 1 || *expected < 1 || *connectTimeout <= 0 || *totalTimeout < *connectTimeout {
		return 2, errors.New("output, run-id, expected count, and positive limits are required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	jobs := make(chan Candidate, 4096)
	results := make(chan scoreResult, 4096)
	inputDone := make(chan error, 1)
	go func() {
		defer close(jobs)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
		for scanner.Scan() {
			var input Candidate
			if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
				inputDone <- errors.New("invalid scoring input JSON")
				return
			}
			select {
			case jobs <- input:
			case <-ctx.Done():
				inputDone <- ctx.Err()
				return
			}
		}
		inputDone <- scanner.Err()
	}()
	options := Options{ConnectTimeout: *connectTimeout, TotalTimeout: *totalTimeout, TargetURL: metadataEndpoint}
	var workers sync.WaitGroup
	for range min(*concurrency, int(*expected)) {
		workers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case input, ok := <-jobs:
					if !ok {
						return
					}
					result := scoreOne(ctx, input, options, *runID, checkCandidate)
					select {
					case results <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		})
	}
	go func() { workers.Wait(); close(results) }()
	started := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var completed, performed, responses, portRetries int64
	var responseMS float64
	var rows []scoredProxy
	var runError error
	running := true
	for running {
		select {
		case result, ok := <-results:
			if !ok {
				running = false
				break
			}
			if result.Err != nil {
				if runError == nil {
					runError = result.Err
				}
				cancel()
				continue
			}
			completed++
			performed += int64(result.Performed)
			portRetries += int64(result.PortRetries)
			responses += int64(result.Row.Score)
			responseMS += result.ResponseMS
			if result.Row.Score > 0 {
				rows = append(rows, result.Row)
			}
		case <-ticker.C:
			_ = json.NewEncoder(os.Stderr).Encode(map[string]any{"event": "proxy_score_progress", "worker": *output, "completed": completed, "new_checks": performed, "port_retries": portRetries, "seconds": time.Since(started).Seconds()})
		}
	}
	if runError != nil {
		return 1, runError
	}
	if ctx.Err() != nil {
		return 130, ctx.Err()
	}
	if err := <-inputDone; err != nil {
		return 1, err
	}
	if completed != *expected || performed != 3*completed {
		return 1, errors.New("scoring coverage does not match the catalog")
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Score != rows[j].Score {
			return rows[i].Score > rows[j].Score
		}
		if rows[i].AverageMS != rows[j].AverageMS {
			return rows[i].AverageMS < rows[j].AverageMS
		}
		return rows[i].ID < rows[j].ID
	})
	f, err := os.OpenFile(*output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 1, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	encoder := json.NewEncoder(w)
	for _, row := range rows {
		if err = encoder.Encode(row); err != nil {
			break
		}
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return 1, err
	}
	if closeErr != nil {
		return 1, closeErr
	}
	return 0, json.NewEncoder(os.Stdout).Encode(map[string]any{"configurations": completed, "new_checks": performed, "responses": responses, "response_time_ms": responseMS, "port_retries": portRetries, "working": len(rows), "seconds": time.Since(started).Seconds()})
}
