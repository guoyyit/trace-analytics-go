package tdigest

import (
	"math"
	"testing"
)

func TestUniformQuantileAccuracy(t *testing.T) {
	d := New(100)
	const n = 200_000
	for i := 1; i <= n; i++ {
		d.Add(float64(i))
	}
	cases := []struct {
		q, want, relTol float64
	}{
		{0.5, n / 2, 0.01},
		{0.9, 0.9 * n, 0.01},
		{0.95, 0.95 * n, 0.01},
		{0.99, 0.99 * n, 0.01},
	}
	for _, c := range cases {
		got := d.Quantile(c.q)
		if math.Abs(got-c.want)/c.want > c.relTol {
			t.Errorf("q=%.2f got=%.1f want~%.1f", c.q, got, c.want)
		}
	}
	if d.Count() != n {
		t.Errorf("count=%d", d.Count())
	}
	if len(d.centroids) > 500 {
		t.Errorf("centroid count unbounded: %d", len(d.centroids))
	}
}

func TestSkewedHighCardinalityBoundedMemory(t *testing.T) {
	d := New(200)
	for i := 0; i < 1_000_000; i++ {
		// Log-normal-ish skew with high cardinality values.
		d.Add(float64(i*i) + float64(i%7))
	}
	if len(d.centroids) > 2_000 {
		t.Fatalf("centroids grew unbounded: %d", len(d.centroids))
	}
	q := d.Quantile(0.99)
	if q <= 0 || math.IsNaN(q) {
		t.Fatalf("bad p99: %v", q)
	}
}

func TestMergeAndRoundtrip(t *testing.T) {
	a := New(100)
	b := New(100)
	for i := 0; i < 50_000; i++ {
		a.Add(float64(i))
		b.Add(float64(i + 50_000))
	}
	a.MergeFrom(b)
	if a.Count() != 100_000 {
		t.Fatalf("merged count %d", a.Count())
	}
	med := a.Quantile(0.5)
	if math.Abs(med-50_000) > 1_000 {
		t.Fatalf("merged median %.1f", med)
	}
	data, err := a.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	c, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if c.Count() != a.Count() || c.Quantile(0.9) == 0 {
		t.Fatal("roundtrip lost data")
	}
}
