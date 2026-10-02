package model

import "errors"

// Sentinel errors returned by the service layers.
var (
	// ErrMalformedBatch indicates a structurally invalid ingest request.
	ErrMalformedBatch = errors.New("malformed batch")
	// ErrLimitExceeded indicates a configured storage/query limit was hit.
	ErrLimitExceeded = errors.New("limit exceeded")
	// ErrQueryTimeout indicates a query exceeded its configured timeout.
	ErrQueryTimeout = errors.New("query timeout")
	// ErrCursorInvalid indicates an unparseable or corrupt cursor.
	ErrCursorInvalid = errors.New("invalid cursor")
	// ErrCursorExpired indicates the cursor's pinned snapshot is no longer retained.
	ErrCursorExpired = errors.New("cursor expired")
	// ErrNotFound indicates the requested trace/span does not exist.
	ErrNotFound = errors.New("not found")
)
