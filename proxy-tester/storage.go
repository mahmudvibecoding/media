package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Shard struct {
	File    string `json:"file"`
	Records int64  `json:"records"`
	SHA256  string `json:"sha256"`
}

type Manifest struct {
	Records int64   `json:"records"`
	Shards  []Shard `json:"shards"`
}

func digestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func inputShards(path string) ([]Shard, int64, error) {
	if strings.HasSuffix(path, "manifest.json") {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, 0, err
		}
		var manifest Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, 0, errors.New("invalid input manifest")
		}
		var total int64
		for i := range manifest.Shards {
			shard := &manifest.Shards[i]
			if filepath.Base(shard.File) != shard.File || shard.Records < 1 || len(shard.SHA256) != 64 {
				return nil, 0, errors.New("invalid shard declaration")
			}
			shard.File = filepath.Join(filepath.Dir(path), shard.File)
			total += shard.Records
		}
		if total != manifest.Records {
			return nil, 0, errors.New("manifest count mismatch")
		}
		return manifest.Shards, total, nil
	}
	return []Shard{{File: path}}, 0, nil
}

func walkCandidates(ctx context.Context, shard Shard, visit func(Candidate) error) error {
	f, err := os.Open(shard.File)
	if err != nil {
		return err
	}
	defer f.Close()
	hash := sha256.New()
	var reader io.Reader = io.TeeReader(f, hash)
	if strings.HasSuffix(shard.File, ".gz") {
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		defer compressed.Close()
		reader = compressed
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var count int64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var candidate Candidate
		if err := json.Unmarshal(scanner.Bytes(), &candidate); err != nil {
			return fmt.Errorf("invalid candidate JSON in %s at record %d", filepath.Base(shard.File), count+1)
		}
		if candidate.ID <= 0 || len(candidate.Key) != 64 {
			return fmt.Errorf("invalid candidate identity in %s at record %d", filepath.Base(shard.File), count+1)
		}
		count++
		if err := visit(candidate); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if shard.Records > 0 && count != shard.Records {
		return errors.New("shard record count mismatch")
	}
	if shard.SHA256 != "" && hex.EncodeToString(hash.Sum(nil)) != shard.SHA256 {
		return errors.New("shard checksum mismatch")
	}
	return nil
}

func atomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".proxy-tester-")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if _, err = temporary.Write(append(data, '\n')); err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(temporary.Name(), path)
}

func loadJournal(path string, add func(Result)) (map[int64]struct{}, error) {
	done := make(map[int64]struct{})
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				if err := f.Truncate(offset); err != nil {
					return nil, err
				}
				if err := f.Sync(); err != nil {
					return nil, err
				}
			}
			break
		}
		if err != nil {
			return nil, err
		}
		var result Result
		if err := json.Unmarshal(line, &result); err != nil || result.ID <= 0 || len(result.Key) != 64 {
			return nil, errors.New("corrupt complete journal record; refusing to discard saved results")
		}
		offset += int64(len(line))
		if result.Status == "local_error" {
			continue
		}
		if _, exists := done[result.ID]; exists {
			return nil, errors.New("duplicate completed candidate in result journal")
		}
		done[result.ID] = struct{}{}
		add(result)
	}
	return done, nil
}

func readIDSet(path string) (map[int64]struct{}, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	set := map[int64]struct{}{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var id int64
		if _, err := fmt.Sscanf(scanner.Text(), "%d", &id); err != nil || id < 1 {
			return nil, errors.New("invalid ID selection")
		}
		set[id] = struct{}{}
	}
	return set, scanner.Err()
}
