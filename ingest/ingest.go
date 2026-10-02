// Package ingest validates incoming span batches before they reach the store.
//
// Classification semantics:
//   - MALFORMED spans fail structural validation and are quarantined; they
//     never reach the store.
//   - DUPLICATE / CONFLICT / LIMIT_EXCEEDED spans are structurally valid but
//     cannot be newly indexed; the store isolates them with their own reasons.
//
// Every rejected span is returned in IngestReport.Rejected, kept separate
// from the accepted count.
package ingest

import (
	"fmt"
	"sort"
	"strings"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/store"
)

// Ingester validates raw spans and commits them as atomic batches.
type Ingester struct {
	store *store.Store
	cfg   *config.Config
}

// New creates an Ingester backed by s.
func New(s *store.Store, cfg *config.Config) *Ingester {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Ingester{store: s, cfg: cfg}
}

// Validate performs structural validation of one span. It returns a
// RejectedSpan (reason MALFORMED) when the span cannot be accepted.
func (g *Ingester) Validate(span model.Span) *model.RejectedSpan {
	if strings.TrimSpace(span.TraceID) == "" {
		return malformed(span, "traceId is required")
	}
	if strings.TrimSpace(span.SpanID) == "" {
		return malformed(span, "spanId is required")
	}
	if strings.TrimSpace(span.Service) == "" {
		return malformed(span, "service is required")
	}
	if strings.TrimSpace(span.Operation) == "" {
		return malformed(span, "operation is required")
	}
	if span.StartUnixNano < 0 {
		return malformed(span, "startUnixNano must be >= 0")
	}
	if span.DurationNano < 0 {
		return malformed(span, "durationNano must be >= 0")
	}
	if span.ParentSpanID == span.SpanID {
		return malformed(span, "span cannot be its own parent")
	}
	switch span.Status {
	case model.StatusOK, model.StatusError, model.StatusUnspecified, "":
	default:
		return malformed(span, fmt.Sprintf("unknown status %q", span.Status))
	}
	return nil
}

// sanitize applies bounded-size rules to attributes and normalizes status.
func (g *Ingester) sanitize(span model.Span) model.Span {
	if span.Status == "" {
		span.Status = model.StatusUnspecified
	}
	maxAttrs := g.cfg.MaxAttributesPerSpan
	maxValLen := g.cfg.MaxAttributeValueLen
	if maxAttrs > 0 || maxValLen > 0 {
		keys := make([]string, 0, len(span.Attributes))
		for k := range span.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		cleaned := make(map[string]string, len(span.Attributes))
		for i, k := range keys {
			if maxAttrs > 0 && i >= maxAttrs {
				break
			}
			v := span.Attributes[k]
			if maxValLen > 0 && len(v) > maxValLen {
				v = v[:maxValLen]
			}
			cleaned[k] = v
		}
		span.Attributes = cleaned
	}
	return span
}

// Ingest validates and atomically commits a batch. Malformed spans are
// quarantined up front; all structurally valid spans are committed in one
// store batch, so readers never observe a partially applied request.
func (g *Ingester) Ingest(batchID string, spans []model.Span) (*model.IngestReport, error) {
	report := &model.IngestReport{BatchID: batchID}
	valid := make([]model.Span, 0, len(spans))
	for _, sp := range spans {
		if rejected := g.Validate(sp); rejected != nil {
			report.Rejected = append(report.Rejected, *rejected)
			continue
		}
		valid = append(valid, g.sanitize(sp))
	}

	if len(valid) == 0 {
		// Nothing to commit; still return the quarantine report.
		return report, nil
	}
	accepted, rejected, err := g.store.Commit(&store.Batch{ID: batchID, Spans: valid})
	if err != nil {
		return nil, err
	}
	report.Accepted = accepted
	report.Duplicate = 0
	report.Rejected = append(report.Rejected, rejected...)
	for _, r := range rejected {
		if r.Reason == model.RejectDuplicate {
			report.Duplicate++
		}
	}
	report.CommittedSeq = g.store.Current().Seq()
	return report, nil
}

func malformed(span model.Span, detail string) *model.RejectedSpan {
	return &model.RejectedSpan{Span: span, Reason: model.RejectMalformed, Detail: detail}
}
