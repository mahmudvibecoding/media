package main

import (
	"context"
	"testing"
	"time"
)

func TestScoreUsesThreeChecksAndOnlyResponseTimes(t *testing.T) {
	input := scoreInput{Candidate: Candidate{ID: 1, Key: "0123456789012345678901234567890123456789012345678901234567890123"}}
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

func TestScoreReusesCommittedChecks(t *testing.T) {
	input := scoreInput{Candidate: Candidate{ID: 1, Key: "0123456789012345678901234567890123456789012345678901234567890123", WorkingProtocol: "socks5"}, ChecksDone: 2, Responses: 1, ResponseMS: 15, LastHTTP: 403, LastResponse: time.Now()}
	n := 0
	result := scoreOne(context.Background(), input, Options{}, "run", func(context.Context, Candidate, Options) Result { n++; return Result{TestedAt: time.Now()} })
	if result.Err != nil || n != 1 || result.Restored != 2 || result.Performed != 1 || result.Row.Score != 1 || result.Row.AverageMS != 15 {
		t.Fatalf("unexpected reused score: %+v calls=%d", result, n)
	}
	input.ChecksDone = 3
	result = scoreOne(context.Background(), input, Options{}, "run", func(context.Context, Candidate, Options) Result { t.Fatal("retested completed proxy"); return Result{} })
	if result.Err != nil || result.Performed != 0 || result.Row.Score != 1 {
		t.Fatalf("unexpected completed score: %+v", result)
	}
}

func TestScoreDoesNotCountLocalOverloadAsProxyFailure(t *testing.T) {
	input := scoreInput{Candidate: Candidate{ID: 1, Key: "0123456789012345678901234567890123456789012345678901234567890123"}}
	result := scoreOne(context.Background(), input, Options{}, "run", func(context.Context, Candidate, Options) Result {
		return Result{Attempts: []Attempt{{ErrorCode: "local_too_many_open_files"}}}
	})
	if result.Err == nil || result.Performed != 0 {
		t.Fatalf("local error counted: %+v", result)
	}
}
