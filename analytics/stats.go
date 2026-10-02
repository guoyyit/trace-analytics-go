package analytics

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/storage"
)

// StatsOptions configures grouped, bucketed aggregation.
type StatsOptions struct {
	Filter    query.Filter
	Bucket    time.Duration
	Quantiles []float64
	Timeout   time.Duration
	MaxGroups int
}

type groupKey struct {
	bucket    int64
	service   string
	operation string
}

type groupAcc struct {
	stats  model.BucketStats
	digest *TDigest
}

// StatsBucket holds buckets keyed by aligned bucket start.
type StatsBucket struct {
	Groups []model.BucketStats `json:"groups"`
}

var defaultQuantiles = []float64{0.5, 0.9, 0.95, 0.99}

// ServiceStats aggregates span counts, error rate and latency quantiles by
// service (+ optional operation) and aligned time buckets.
func ServiceStats(v *storage.View, byOperation bool, opt StatsOptions) (map[time.Time]StatsBucket, error) {
	bucket := opt.Bucket
	if bucket <= 0 {
		bucket = time.Minute
	}
	quantiles := opt.Quantiles
	if len(quantiles) == 0 {
		quantiles = defaultQuantiles
	}
	maxGroups := opt.MaxGroups
	if maxGroups <= 0 {
		maxGroups = 10_000
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if opt.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opt.Timeout)
	}
	defer cancel()

	groups := make(map[groupKey]*groupAcc)
	v.ForEachSpan(func(sp model.Span) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !spanMatches(sp, opt.Filter) {
			return
		}
		aligned := sp.StartTime.UnixNano() - mod(sp.StartTime.UnixNano(), int64(bucket))
		op := ""
		if byOperation {
			op = sp.OperationName
		}
		key := groupKey{bucket: aligned, service: sp.ServiceName, operation: op}
		acc, ok := groups[key]
		if !ok {
			if len(groups) >= maxGroups {
				return
			}
			acc = &groupAcc{
				digest: NewTDigest(),
				stats: model.BucketStats{
					BucketStart:   time.Unix(0, aligned),
					ServiceName:   sp.ServiceName,
					OperationName: op,
					Quantiles:     make(map[float64]time.Duration, len(quantiles)),
				},
			}
			groups[key] = acc
		}
		acc.stats.Count++
		if sp.Status == model.StatusError {
			acc.stats.ErrorCount++
		}
		acc.digest.Add(sp.Duration)
	})
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, model.ErrQueryTimeout
		}
		return nil, err
	}

	out := make(map[time.Time]StatsBucket)
	for _, acc := range groups {
		if acc.stats.Count > 0 {
			acc.stats.ErrorRate = float64(acc.stats.ErrorCount) / float64(acc.stats.Count)
		}
		for _, q := range quantiles {
			acc.stats.Quantiles[q] = acc.digest.Quantile(q)
		}
		bt := acc.stats.BucketStart
		sb := out[bt]
		sb.Groups = append(sb.Groups, acc.stats)
		out[bt] = sb
	}
	for bt := range out {
		sb := out[bt]
		sort.Slice(sb.Groups, func(i, j int) bool {
			if sb.Groups[i].ServiceName != sb.Groups[j].ServiceName {
				return sb.Groups[i].ServiceName < sb.Groups[j].ServiceName
			}
			return sb.Groups[i].OperationName < sb.Groups[j].OperationName
		})
		out[bt] = sb
	}
	return out, nil
}

func mod(a, b int64) int64 {
	r := a % b
	if r < 0 {
		r += b
	}
	return r
}

// spanMatches checks the span-level subset of the filter. Trace-level
// conditions (duration window, completeness) are intentionally not applied
// per span in aggregations.
func spanMatches(sp model.Span, f query.Filter) bool {
	if f.ServiceName != "" && sp.ServiceName != f.ServiceName {
		return false
	}
	if f.OperationName != "" && sp.OperationName != f.OperationName {
		return false
	}
	if f.TagKey != "" {
		v, ok := sp.Tags[f.TagKey]
		if !ok || (f.TagValue != "" && v != f.TagValue) {
			return false
		}
	}
	if f.OnlyErrors && sp.Status != model.StatusError {
		return false
	}
	if !f.StartTime.IsZero() && sp.End().Before(f.StartTime) {
		return false
	}
	if !f.EndTime.IsZero() && sp.StartTime.After(f.EndTime) {
		return false
	}
	if f.MinDuration > 0 && sp.Duration < f.MinDuration {
		return false
	}
	if f.MaxDuration > 0 && sp.Duration > f.MaxDuration {
		return false
	}
	return true
}
