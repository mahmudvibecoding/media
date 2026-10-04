package main

import (
	"context"
	"testing"
	"time"
)

func TestScoreUsesThreeChecksAndOnlyResponseTimes(t *testing.T) {
	input := Candidate{ID: 1, Key: "0123456789012345678901234567890123456789012345678901234567890123"}
	n := 0
	check := func(context.Context, Candidate, Options) Result {
		n++
		return Result{Responds: n != 2, TotalMS: float64(n * 10), DetectedProtocol: "http", TestedAt: time.Now(),
			Attempts: []Attempt{{Status: "responds", HTTPStatus: 429}}}
	}
	result := scoreOne(context.Background(), input, Options{}, "run", check)
	if result.Err != nil || n != 3 || result.Row.Score != 2 || result.Row.AverageMS != 20 || result.Row.LastHTTP != 429 {
		t.Fatalf("unexpected score: %+v calls=%d", result, n)
	}
}

func TestScoreDoesNotCountLocalOverloadAsProxyFailure(t *testing.T) {
	input := Candidate{ID: 1, Key: "0123456789012345678901234567890123456789012345678901234567890123"}
	result := scoreOne(context.Background(), input, Options{}, "run", func(context.Context, Candidate, Options) Result {
		return Result{Attempts: []Attempt{{ErrorCode: "local_too_many_open_files"}}}
	})
	if result.Err == nil || result.Performed != 0 {
		t.Fatalf("local error counted: %+v", result)
	}
}

func TestScoreRetriesLocalPortPressureWithoutCountingItAsAProxyCheck(t *testing.T) {
	input := Candidate{ID: 1, Key: "0123456789012345678901234567890123456789012345678901234567890123"}
	calls := 0
	result := scoreOne(context.Background(), input, Options{}, "run", func(context.Context, Candidate, Options) Result {
		calls++
		if calls == 1 {
			return Result{Attempts: []Attempt{{ErrorCode: "local_address_in_use"}}}
		}
		return Result{Responds: true, TotalMS: 15, DetectedProtocol: "http", TestedAt: time.Now()}
	})
	if result.Err != nil || calls != 4 || result.Performed != 3 || result.PortRetries != 1 || result.Row.Score != 3 || result.Row.AverageMS != 15 {
		t.Fatalf("port pressure affected the score: %+v calls=%d", result, calls)
	}
}
