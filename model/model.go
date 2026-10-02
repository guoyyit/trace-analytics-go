package model

// SpanStatus is the status of a single span.
type SpanStatus string

const (
	StatusUnspecified SpanStatus = "STATUS_UNSPECIFIED"
	StatusOK          SpanStatus = "STATUS_OK"
	StatusError       SpanStatus = "STATUS_ERROR"
)

// Span is the unit of tracing data. All timestamps are Unix nanoseconds.
type Span struct {
	TraceID       string            `json:"traceId"`
	SpanID        string            `json:"spanId"`
	ParentSpanID  string            `json:"parentSpanId,omitempty"`
	Service       string            `json:"service"`
	Operation     string            `json:"operation"`
	StartUnixNano int64             `json:"startUnixNano"`
	DurationNano  int64             `json:"durationNano"`
	Status        SpanStatus        `json:"status"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

// EndUnixNano returns the span end timestamp.
func (s Span) EndUnixNano() int64 {
	return s.StartUnixNano + s.DurationNano
}

// Trace is a group of spans sharing the same TraceID, assembled out of
// potentially out-of-order, duplicated or late-arriving writes.
type Trace struct {
	TraceID        string   `json:"traceId"`
	Spans          []Span   `json:"spans"`
	StartUnixNano  int64    `json:"startUnixNano"`
	EndUnixNano    int64    `json:"endUnixNano"`
	HasError       bool     `json:"hasError"`
	Services       []string `json:"services,omitempty"`
	MissingParents int      `json:"missingParents"`
}

// RejectReason explains why a span was quarantined instead of accepted.
type RejectReason string

const (
	// RejectMalformed: structurally invalid span (bad ids, negative duration, ...).
	RejectMalformed RejectReason = "MALFORMED"
	// RejectDuplicate: identical span for (traceId, spanId) already committed.
	RejectDuplicate RejectReason = "DUPLICATE"
	// RejectConflict: same span id already committed with a different body.
	RejectConflict RejectReason = "CONFLICT"
	// RejectLimitExceeded: configured storage/resource limit would be violated.
	RejectLimitExceeded RejectReason = "LIMIT_EXCEEDED"
)

// RejectedSpan is a span isolated into quarantine instead of being indexed.
type RejectedSpan struct {
	Span   Span         `json:"span"`
	Reason RejectReason `json:"reason"`
	Detail string       `json:"detail,omitempty"`
}

// IngestReport is the per-batch ingestion result. Accepted data and
// quarantined data are always reported separately.
type IngestReport struct {
	BatchID      string         `json:"batchId"`
	Accepted     int            `json:"accepted"`
	Duplicate    int            `json:"duplicate"`
	Rejected     []RejectedSpan `json:"rejected,omitempty"`
	CommittedSeq uint64         `json:"committedSeq"`
}

// ErrorState is the error-status predicate for trace search.
type ErrorState string

const (
	// ErrorAny: do not filter on status.
	ErrorAny ErrorState = "ANY"
	// ErrorsOnly: match spans/traces containing an error span.
	ErrorsOnly ErrorState = "ERRORS_ONLY"
	// OkOnly: match only when no error span is present.
	OkOnly ErrorState = "OK_ONLY"
)

// TraceFilter is the combined filter for trace search.
// All populated fields are ANDed at the span-predicate level; list fields
// use OR semantics within the list. See docs/design.md for full semantics.
type TraceFilter struct {
	Services        []string          `json:"services,omitempty"`
	Operations      []string          `json:"operations,omitempty"`
	Attributes      map[string]string `json:"attributes,omitempty"`
	MinDurationNano int64             `json:"minDurationNano,omitempty"`
	MaxDurationNano int64             `json:"maxDurationNano,omitempty"`
	ErrorState      ErrorState        `json:"errorState,omitempty"`
	StartUnixNano   int64             `json:"startUnixNano,omitempty"`
	EndUnixNano     int64             `json:"endUnixNano,omitempty"`
	Limit           int               `json:"limit,omitempty"`
	Cursor          string            `json:"cursor,omitempty"`
}

// TraceSummary is a lightweight projection of a trace used in list pages.
type TraceSummary struct {
	TraceID       string   `json:"traceId"`
	StartUnixNano int64    `json:"startUnixNano"`
	EndUnixNano   int64    `json:"endUnixNano"`
	DurationNano  int64    `json:"durationNano"`
	SpanCount     int      `json:"spanCount"`
	Services      []string `json:"services"`
	HasError      bool     `json:"hasError"`
}

// TracePage is one stable page of trace search results.
type TracePage struct {
	Traces     []TraceSummary `json:"traces"`
	NextCursor string         `json:"nextCursor,omitempty"`
}

// StatsFilter selects spans for service/operation aggregation.
type StatsFilter struct {
	Services      []string `json:"services,omitempty"`
	Operations    []string `json:"operations,omitempty"`
	StartUnixNano int64    `json:"startUnixNano,omitempty"`
	EndUnixNano   int64    `json:"endUnixNano,omitempty"`
}

// BucketStats is the aggregated statistics for one (service, operation, time bucket).
type BucketStats struct {
	BucketStartUnixNano int64            `json:"bucketStartUnixNano"`
	Count               int64            `json:"count"`
	ErrorCount          int64            `json:"errorCount"`
	ErrorRate           float64          `json:"errorRate"`
	AvgDurationNano     int64            `json:"avgDurationNano"`
	Quantiles           map[string]int64 `json:"quantiles"`
}

// OperationStats is the per (service, operation) series of time buckets.
type OperationStats struct {
	Service   string        `json:"service"`
	Operation string        `json:"operation"`
	Buckets   []BucketStats `json:"buckets"`
}

// DependencyEdge is an aggregated parent-service -> child-service edge.
type DependencyEdge struct {
	FromService     string  `json:"fromService"`
	ToService       string  `json:"toService"`
	Count           int64   `json:"count"`
	ErrorCount      int64   `json:"errorCount"`
	ErrorRate       float64 `json:"errorRate"`
	AvgDurationNano int64   `json:"avgDurationNano"`
}

// CriticalSpan is one node on a trace critical path.
type CriticalSpan struct {
	SpanID        string `json:"spanId"`
	ParentSpanID  string `json:"parentSpanId,omitempty"`
	Service       string `json:"service"`
	Operation     string `json:"operation"`
	StartUnixNano int64  `json:"startUnixNano"`
	DurationNano  int64  `json:"durationNano"`
}

// CriticalPath is the bottleneck span chain deciding a trace's total duration.
type CriticalPath struct {
	TraceID           string         `json:"traceId"`
	TotalDurationNano int64          `json:"totalDurationNano"`
	Spans             []CriticalSpan `json:"spans"`
}
