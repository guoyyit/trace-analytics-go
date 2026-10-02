// Package query defines trace filter semantics and stable cursor pagination
// over immutable storage views.
package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/storage"
)

// Filter combines all optional search conditions. All conditions are combined
// with logical AND. Service/Operation/Tags/Error are matched if ANY span in
// the trace satisfies them; duration/time conditions apply to the trace.
type Filter struct {
	ServiceName       string
	OperationName     string
	TagKey            string
	TagValue          string
	MinDuration       time.Duration
	MaxDuration       time.Duration
	OnlyErrors        bool
	StartTime         time.Time
	EndTime           time.Time
	IncludeIncomplete bool
}

// Cursor is an opaque stable pagination position bound to one view version.
type Cursor struct {
	ViewSeq uint64
	LastKey string
}

// SearchResult is one page of matching trace summaries.
type SearchResult struct {
	Traces     []*model.Trace `json:"traces"`
	NextCursor string         `json:"nextCursor,omitempty"`
	HasMore    bool           `json:"hasMore"`
}

// Matches reports whether a trace passes the filter.
func Matches(tr *model.Trace, f Filter) bool {
	dur := tr.EndTime.Sub(tr.StartTime)
	if f.MinDuration > 0 && dur < f.MinDuration {
		return false
	}
	if f.MaxDuration > 0 && dur > f.MaxDuration {
		return false
	}
	if !f.StartTime.IsZero() && tr.EndTime.Before(f.StartTime) {
		return false
	}
	if !f.EndTime.IsZero() && tr.StartTime.After(f.EndTime) {
		return false
	}
	if !f.IncludeIncomplete && !tr.Complete {
		return false
	}
	if f.ServiceName == "" && f.OperationName == "" && f.TagKey == "" && !f.OnlyErrors {
		return true
	}
	for _, sp := range tr.Spans {
		if f.ServiceName != "" && sp.ServiceName != f.ServiceName {
			continue
		}
		if f.OperationName != "" && sp.OperationName != f.OperationName {
			continue
		}
		if f.TagKey != "" {
			v, ok := sp.Tags[f.TagKey]
			if !ok || (f.TagValue != "" && v != f.TagValue) {
				continue
			}
		}
		if f.OnlyErrors && sp.Status != model.StatusError {
			continue
		}
		return true
	}
	return false
}

// EncodeCursor serializes a cursor.
func EncodeCursor(c Cursor) string {
	payload, _ := json.Marshal(c)
	return base64.URLEncoding.EncodeToString(payload)
}

// DecodeCursor parses a cursor; returns model.ErrInvalidCursor on bad input.
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	payload, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, model.ErrInvalidCursor
	}
	var c Cursor
	if err := json.Unmarshal(payload, &c); err != nil {
		return Cursor{}, model.ErrInvalidCursor
	}
	return c, nil
}

// Search scans one immutable view and returns a stable page of traces.
func Search(v *storage.View, f Filter, cursor string, limit int, timeout time.Duration) (*SearchResult, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		return nil, errors.Join(model.ErrInvalidArgument, errors.New("limit too large"))
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	c, err := DecodeCursor(cursor)
	if err != nil {
		return nil, err
	}

	ids := v.TraceIDs()
	out := make([]*model.Trace, 0, limit)
	started := c.ViewSeq == 0
	var lastKey string

	for _, id := range ids {
		select {
		case <-ctx.Done():
			return nil, model.ErrQueryTimeout
		default:
		}
		if !started {
			if id == c.LastKey {
				started = true
			}
			continue
		}
		tr, ok := v.Trace(id)
		if !ok || !Matches(tr, f) {
			continue
		}
		out = append(out, tr)
		lastKey = id
		if len(out) == limit {
			break
		}
	}

	// HasMore: another matching trace exists after lastKey in the same view.
	hasMore := false
	if len(out) == limit {
		seenLast := lastKey == ""
		for _, id := range ids {
			if id == lastKey {
				seenLast = true
				continue
			}
			if !seenLast {
				continue
			}
			if tr, ok := v.Trace(id); ok && Matches(tr, f) {
				hasMore = true
				break
			}
		}
	}

	res := &SearchResult{Traces: out, HasMore: hasMore}
	if hasMore {
		res.NextCursor = EncodeCursor(Cursor{ViewSeq: v.Seq(), LastKey: lastKey})
	}
	return res, nil
}
