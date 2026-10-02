package config

import "time"

// Config holds all resource limits and runtime parameters.
type Config struct {
	// DataDir is the directory for the WAL and snapshots.
	DataDir string
	// MaxSpans is the hard global limit on retained spans.
	MaxSpans int
	// MaxSpansPerTrace caps the size of a single trace.
	MaxSpansPerTrace int
	// MaxTagIndexKeys caps the number of distinct indexed tag keys.
	MaxTagIndexKeys int
	// MaxRejectedRetained caps retained malformed-record descriptions.
	MaxRejectedRetained int
	// MaxPinnedViews caps old immutable views kept for stable cursors.
	MaxPinnedViews int
	// QueryTimeout bounds any single query.
	QueryTimeout time.Duration
	// SnapshotAfterBatches triggers a snapshot + WAL rewrite.
	SnapshotAfterBatches uint64
	// SyncWAL forces fsync per committed batch.
	SyncWAL bool
}

// Default returns a config with safe bounded defaults.
func Default() Config {
	return Config{
		DataDir:              "data",
		MaxSpans:             2_000_000,
		MaxSpansPerTrace:     10_000,
		MaxTagIndexKeys:      512,
		MaxRejectedRetained:  10_000,
		MaxPinnedViews:       16,
		QueryTimeout:         5 * time.Second,
		SnapshotAfterBatches: 128,
		SyncWAL:              true,
	}
}
