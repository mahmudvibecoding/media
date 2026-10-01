package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/metacubex/mihomo/adapter"
)

func validateConfiguration(c Candidate) (code string) {
	defer func() {
		if recover() != nil {
			code = "adapter_panic"
		}
	}()
	protocol := c.Protocol
	if protocol == "unknown" {
		protocol = "http"
	}
	settings, err := normalize(c, protocol)
	if err != nil {
		return string(err.(configError))
	}
	if protocol == "http" || protocol == "https" || protocol == "socks4" || protocol == "socks5" {
		return "valid"
	}
	proxy, err := adapter.ParseProxy(settings)
	if err != nil {
		// Only known diagnostic words may leave the library; errors can contain credentials.
		code = "adapter_rejected"
		for _, word := range strings.Fields("uuid cipher password network transport key plugin congestion tls alpn obfs flow encryption") {
			if strings.Contains(strings.ToLower(err.Error()), word) {
				code += ":" + word
			}
		}
		return code
	}
	proxy.Close()
	return "valid"
}

func audit(args []string) (int, error) {
	flags := flag.NewFlagSet("audit", flag.ContinueOnError)
	input := flags.String("input", "", "Catalog manifest or JSONL file")
	output := flags.String("output", "", "Configuration audit JSON")
	if err := flags.Parse(args); err != nil {
		return 2, err
	}
	if *input == "" || *output == "" {
		return 2, errors.New("input and output are required")
	}
	shards, expected, err := inputShards(*input)
	if err != nil {
		return 1, err
	}
	counts := map[string]int64{}
	examples := map[string][]int64{}
	var total int64
	for _, shard := range shards {
		err = walkCandidates(context.Background(), shard, func(c Candidate) error {
			key := c.Protocol + ":" + validateConfiguration(c)
			counts[key]++
			if len(examples[key]) < 5 && !strings.HasSuffix(key, ":valid") {
				examples[key] = append(examples[key], c.ID)
			}
			total++
			return nil
		})
		if err != nil {
			return 1, err
		}
	}
	if expected > 0 && expected != total {
		return 1, errors.New("audit coverage mismatch")
	}
	if err := atomicJSON(*output, map[string]any{"records": total, "counts": counts, "example_ids": examples}); err != nil {
		return 1, err
	}
	fmt.Printf("Audited %d configurations\n", total)
	return 0, nil
}
