package model

import (
	"errors"
	"time"
)

// Status is the status of a span.
type Status uint8

const (
	StatusUnspecified Status = 0
	StatusOK          Status = 1
	StatusError       Status = 2
)

// Span is a single annotated operation within a trace.
type Span struct {
	TraceID       string            `json:"traceId"`
	SpanID        string            `json:"spanId"`
	ParentSpanID  string            `json:"parentSpanId,omitempty"`
	ServiceName   string            `json:"serviceName"`
	OperationName string            `json:"operationName"`
	StartTime     time.Time         `json:"startTime"`
	Duration      time.Duration     `json:"duration"`
	Tags          map[string]string `json:"tags,omitempty"`
	Status        Status            `json:"status"`
}

// End returns the end time of the span.
func (s Span) End() time.Time {
	return s.StartTime.Add(s.Duration)
}

// RejectedSpan describes a malformed or conflicting input span and the reason
// it was isolated from accepted data.
type RejectedSpan struct {
	Index   int    `json:"index"`
	TraceID string `json:"traceId,omitempty"`
	SpanID  string `json:"spanId,omitempty"`
	Reason  string `json:"reason"`
}

// Batch is a committed set of spans sharing one commit sequence number.
type Batch struct {
	Seq   uint64 `json:"seq"`
	Spans []Span `json:"spans"`
}

// SpanNode is a node in the assembled span tree.
type SpanNode struct {
	Span     Span        `json:"span"`
	Children []*SpanNode `json:"children"`
}

// Trace is an assembled view of all spans sharing a trace id.
type Trace struct {
	TraceID         string      `json:"traceId"`
	StartTime       time.Time   `json:"startTime"`
	EndTime         time.Time   `json:"endTime"`
	Spans           []Span      `json:"spans"`
	Roots           []*SpanNode `json:"roots"`
	Complete        bool        `json:"complete"`
	MissingParentID []string    `json:"missingParentIds,omitempty"`
}

// BucketStats is the aggregated statistics for one service/operation in one
// time bucket.
type BucketStats struct {
	BucketStart   time.Time                 `json:"bucketStart"`
	ServiceName   string                    `json:"serviceName"`
	OperationName string                    `json:"operationName"`
	Count         int64                     `json:"count"`
	ErrorCount    int64                     `json:"errorCount"`
	ErrorRate     float64                   `json:"errorRate"`
	Quantiles     map[float64]time.Duration `json:"quantiles"`
}

// DependencyEdge is an aggregated caller/callee relationship.
type DependencyEdge struct {
	ParentService string        `json:"parentService"`
	ChildService  string        `json:"childService"`
	CallCount     int64         `json:"callCount"`
	ErrorCount    int64         `json:"errorCount"`
	TotalDuration time.Duration `json:"totalDuration"`
}

// ErrorRate returns the fraction of calls that errored.
func (e DependencyEdge) ErrorRate() float64 {
	if e.CallCount == 0 {
		return 0
	}
	return float64(e.ErrorCount) / float64(e.CallCount)
}

// CriticalPathHop is one span on the critical path of a trace.
type CriticalPathHop struct {
	SpanID        string        `json:"spanId"`
	ParentSpanID  string        `json:"parentSpanId,omitempty"`
	ServiceName   string        `json:"serviceName"`
	OperationName string        `json:"operationName"`
	StartTime     time.Time     `json:"startTime"`
	Duration      time.Duration `json:"duration"`
	Status        Status        `json:"status"`
}

// CriticalPath is the bottleneck span chain deciding the total trace duration.
type CriticalPath struct {
	TraceID       string            `json:"traceId"`
	TotalDuration time.Duration     `json:"totalDuration"`
	Chain         []CriticalPathHop `json:"chain"`
}

// Sentinel errors returned across the service layers.
var (
	ErrNotFound        = errors.New("not found")
	ErrInvalidCursor   = errors.New("invalid or expired cursor")
	ErrStorageLimit    = errors.New("storage limit exceeded")
	ErrTraceTooLarge   = errors.New("trace span limit exceeded")
	ErrQueryTimeout    = errors.New("query timeout exceeded")
	ErrInvalidArgument = errors.New("invalid argument")
)
