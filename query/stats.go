package query

import (
	"context"
	"sort"
	"strconv"

	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/tdigest"
)

// statsKey identifies one (service, operation, bucket) aggregator.
type statsKey struct {
	service   string
	operation string
	bucket    int64
}

type statsAcc struct {
	count      int64
	errorCount int64
	sum        int64
	digest     *tdigest.TDigest
}

// Stats returns per (service, operation) time-bucket statistics.
//
// Quantiles are estimated with a mergeable t-digest: memory is bounded to
// O(compression) centroids per group regardless of how many spans match, so
// high-cardinality workloads never degrade into full-sorting. bucketNanos
// must be positive; quantiles defaults to [0.5,0.9,0.95,0.99] when empty.
func (e *Engine) Stats(ctx context.Context, f model.StatsFilter, bucketNanos int64, quantiles []float64) ([]model.OperationStats, error) {
	ctx, cancel := e.runWithTimeout(ctx)
	defer cancel()

	if bucketNanos <= 0 {
		bucketNanos = 60_000_000_000
	}
	if len(quantiles) == 0 {
		quantiles = []float64{0.5, 0.9, 0.95, 0.99}
	}

	snap := e.store.Current()
	ids := e.candidateIDs(snap, model.TraceFilter{Services: f.Services})
	accs := map[statsKey]*statsAcc{}

	for _, id := range ids {
		if ctx.Err() != nil {
			return nil, model.ErrQueryTimeout
		}
		tr, ok := snap.Trace(id)
		if !ok {
			continue
		}
		if f.StartUnixNano > 0 && tr.EndUnixNano < f.StartUnixNano {
			continue
		}
		if f.EndUnixNano > 0 && tr.StartUnixNano > f.EndUnixNano {
			continue
		}
		for _, sp := range tr.Spans {
			if len(f.Operations) > 0 && !contains(f.Operations, sp.Operation) {
				continue
			}
			if f.StartUnixNano > 0 && sp.EndUnixNano() < f.StartUnixNano {
				continue
			}
			if f.EndUnixNano > 0 && sp.StartUnixNano > f.EndUnixNano {
				continue
			}
			bucket := (sp.StartUnixNano / bucketNanos) * bucketNanos
			key := statsKey{sp.Service, sp.Operation, bucket}
			acc := accs[key]
			if acc == nil {
				acc = &statsAcc{digest: tdigest.New(e.cfg.TDigestCompression)}
				accs[key] = acc
			}
			acc.count++
			acc.sum += sp.DurationNano
			if sp.Status == model.StatusError {
				acc.errorCount++
			}
			acc.digest.Add(float64(sp.DurationNano))
		}
	}

	return assembleStats(accs, quantiles), nil
}

func assembleStats(accs map[statsKey]*statsAcc, quantiles []float64) []model.OperationStats {
	grouped := map[string]*model.OperationStats{}
	var order []string
	bucketOf := map[string][]int64{}

	for k, acc := range accs {
		groupID := k.service + "\x00" + k.operation
		g := grouped[groupID]
		if g == nil {
			g = &model.OperationStats{Service: k.service, Operation: k.operation}
			grouped[groupID] = g
			order = append(order, groupID)
		}
		bucketOf[groupID] = append(bucketOf[groupID], k.bucket)

		bucketStats := model.BucketStats{
			BucketStartUnixNano: k.bucket,
			Count:               acc.count,
			ErrorCount:          acc.errorCount,
			ErrorRate:           float64(acc.errorCount) / float64(acc.count),
			AvgDurationNano:     acc.sum / acc.count,
			Quantiles:           map[string]int64{},
		}
		for _, q := range quantiles {
			bucketStats.Quantiles[strconv.FormatFloat(q, 'g', -1, 64)] =
				int64(acc.digest.Quantile(q))
		}
		g.Buckets = append(g.Buckets, bucketStats)
	}

	sort.Strings(order)
	out := make([]model.OperationStats, 0, len(order))
	for _, id := range order {
		g := grouped[id]
		sort.Slice(g.Buckets, func(i, j int) bool {
			return g.Buckets[i].BucketStartUnixNano < g.Buckets[j].BucketStartUnixNano
		})
		out = append(out, *g)
	}
	return out
}
