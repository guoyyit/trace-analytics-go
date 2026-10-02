package storage

import (
	"sort"
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
)

// View is an immutable, query-consistent snapshot of all committed data.
// Sequential views structurally share unchanged trace objects.
type View struct {
	seq           uint64
	createdAt     time.Time
	traces        map[string]*model.Trace
	traceIDs      []string
	spanCount     int
	spansByKey    map[string]model.Span
	rejected      []model.RejectedSpan
	rejectedTotal int64
	serviceNames  map[string]struct{}
	tagKeys       map[string]struct{}
	tagSaturated  bool
}

func (v *View) Seq() uint64 { return v.seq }

func (v *View) Trace(id string) (*model.Trace, bool) {
	tr, ok := v.traces[id]
	return tr, ok
}

func (v *View) Span(traceID, spanID string) (model.Span, bool) {
	sp, ok := v.spansByKey[spanKey(traceID, spanID)]
	return sp, ok
}

func (v *View) TraceIDs() []string { return append([]string(nil), v.traceIDs...) }

func (v *View) SpanCount() int { return v.spanCount }

func (v *View) Rejected() ([]model.RejectedSpan, int64) {
	return append([]model.RejectedSpan(nil), v.rejected...), v.rejectedTotal
}

// ForEachSpan visits every span in trace-id/start-time order.
func (v *View) ForEachSpan(fn func(model.Span)) {
	for _, id := range v.traceIDs {
		tr := v.traces[id]
		for _, sp := range tr.Spans {
			fn(sp)
		}
	}
}

func (v *View) ServiceNames() []string {
	out := make([]string, 0, len(v.serviceNames))
	for name := range v.serviceNames {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func spanKey(traceID, spanID string) string { return traceID + "\x00" + spanID }
