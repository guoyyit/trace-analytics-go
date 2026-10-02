package config

import "time"

// Config holds all configurable resource limits and timeouts.
type Config struct {
	// DataDir is the directory used for the write-ahead log and quarantine file.
	DataDir string

	// MaxSpans is the global cap on persisted spans. Ingestion beyond this
	// cap is rejected with LIMIT_EXCEEDED. 0 disables the cap.
	MaxSpans int
	// MaxSpansPerTrace caps how many spans a single trace may contain.
	MaxSpansPerTrace int
	// MaxAttributesPerSpan caps the number of tags stored per span.
	MaxAttributesPerSpan int
	// MaxAttributeValueLen caps the byte length of a tag value.
	MaxAttributeValueLen int
	// MaxQuarantineRecords bounds the in-memory quarantine ring buffer.
	MaxQuarantineRecords int

	// QueryTimeout bounds the wall-clock time of a single query.
	QueryTimeout time.Duration
	// DefaultPageLimit is used when a search request omits the limit.
	DefaultPageLimit int
	// MaxPageLimit caps a single search page size.
	MaxPageLimit int
	// SnapshotHistory is how many committed snapshots are retained for
	// stable pagination after newer writes arrive.
	SnapshotHistory int

	// TDigestCompression controls the t-digest accuracy/memory trade-off
	// for quantile estimation (typical 10..1000).
	TDigestCompression float64

	// SyncWAL forces an fsync per committed batch when true.
	SyncWAL bool
}

// Default returns a configuration safe for local development.
func Default() *Config {
	return &Config{
		DataDir:              "data",
		MaxSpans:             1_000_000,
		MaxSpansPerTrace:     10_000,
		MaxAttributesPerSpan: 64,
		MaxAttributeValueLen: 4096,
		MaxQuarantineRecords: 10_000,
		QueryTimeout:         5 * time.Second,
		DefaultPageLimit:     50,
		MaxPageLimit:         500,
		SnapshotHistory:      32,
		TDigestCompression:   100,
		SyncWAL:              true,
	}
}
