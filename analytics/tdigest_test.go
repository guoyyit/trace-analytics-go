package analytics

import (
	"math"
	"sort"
	"testing"
	"time"
)

func exactQuantile(values []float64, q float64) float64 {
	sort.Float64s(values)
	idx := int(math.Ceil(q*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func TestTDigestUniformAccuracy(t *testing.T) {
	d := NewTDigest()
	n := 50_000
	values := make([]float64, 0, n)
	for i := 1; i <= n; i++ {
		v := time.Duration(i) * time.Millisecond
		d.Add(v)
		values = append(values, float64(v))
	}
	if d.Count() != int64(n) {
		t.Fatalf("count = %d", d.Count())
	}
	if len(d.centroids) >= n/2 {
		t.Fatalf("digest not bounded: %d centroids for %d points", len(d.centroids), n)
	}
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		got := float64(d.Quantile(q))
		want := exactQuantile(append([]float64(nil), values...), q)
		relErr := math.Abs(got-want) / want
		if relErr > 0.02 {
			t.Fatalf("q=%v got=%v want=%v relErr=%.4f", q, got, want, relErr)
		}
	}
}

func TestTDigestSkewedTailAccuracy(t *testing.T) {
	d := NewTDigest()
	n := 20_000
	values := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		// Log-uniform: small values dominate, occasional huge ones.
		v := time.Duration(math.Pow(10, float64(i%400)/100.0)) * time.Nanosecond
		d.Add(v)
		values = append(values, float64(v))
	}
	for _, q := range []float64{0.5, 0.99} {
		got := float64(d.Quantile(q))
		want := exactQuantile(append([]float64(nil), values...), q)
		relErr := math.Abs(got-want) / want
		if relErr > 0.05 {
			t.Fatalf("skewed q=%v got=%v want=%v relErr=%.4f", q, got, want, relErr)
		}
	}
}

func TestTDigestMergeAndSnapshot(t *testing.T) {
	a, b := NewTDigest(), NewTDigest()
	for i := 1; i <= 1000; i++ {
		a.Add(time.Duration(i) * time.Millisecond)
	}
	for i := 1001; i <= 2000; i++ {
		b.Add(time.Duration(i) * time.Millisecond)
	}
	a.MergeFrom(b)
	if a.Count() != 2000 {
		t.Fatalf("merged count = %d", a.Count())
	}
	median := a.Quantile(0.5)
	if median < 900*time.Millisecond || median > 1100*time.Millisecond {
		t.Fatalf("merged median = %v", median)
	}

	restored := TDigestFromSnapshot(a.Snapshot())
	if restored.Count() != 2000 {
		t.Fatalf("restored count = %d", restored.Count())
	}
	if restored.Quantile(0.9) != a.Quantile(0.9) {
		t.Fatal("snapshot round-trip changed quantiles")
	}
}
