// Package tdigest implements a bounded-memory, mergeable t-digest used for
// accurate high-cardinality quantile estimation without full sorting.
package tdigest

import (
	"bytes"
	"encoding/gob"
	"math"
	"sort"
)

// TDigest is a mergeable streaming quantile summary.
type TDigest struct {
	compression float64
	centroids   []centroid
	buf         []float64
	count       int64
	min         float64
	max         float64
}

type centroid struct {
	Mean   float64
	Weight float64
}

// New creates a t-digest with the given compression (accuracy vs memory).
func New(compression float64) *TDigest {
	if compression < 10 {
		compression = 10
	}
	return &TDigest{
		compression: compression,
		min:         math.NaN(),
		max:         math.NaN(),
	}
}

// Add inserts one sample.
func (t *TDigest) Add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	t.buf = append(t.buf, value)
	t.count++
	if math.IsNaN(t.min) || value < t.min {
		t.min = value
	}
	if math.IsNaN(t.max) || value > t.max {
		t.max = value
	}
	if len(t.buf) >= t.bufferLimit() {
		t.merge()
	}
}

// MergeFrom folds another digest into t.
func (t *TDigest) MergeFrom(other *TDigest) {
	if other == nil || other.count == 0 {
		return
	}
	if len(t.buf) > 0 {
		t.merge()
	}
	t.mergeWith(other.centroids)
	t.mergeWithWeights(other.buf, nil)
	if math.IsNaN(t.min) || other.min < t.min {
		t.min = other.min
	}
	if math.IsNaN(t.max) || other.max > t.max {
		t.max = other.max
	}
	t.count += other.count
}

// Quantile returns the estimated value at quantile q in [0,1].
func (t *TDigest) Quantile(q float64) float64 {
	if len(t.centroids) == 0 {
		if len(t.buf) > 0 {
			t.merge()
		} else {
			return 0
		}
	}
	if q <= 0 {
		return t.min
	}
	if q >= 1 {
		return t.max
	}
	index := q * float64(t.count)

	weightSoFar := 0.0
	for i, c := range t.centroids {
		center := weightSoFar + c.Weight/2
		if index <= center {
			if i == 0 {
				if c.Weight <= 1 {
					return c.Mean
				}
				// First centroid: interpolate toward the observed minimum.
				return t.min + (c.Mean-t.min)*(index+0.5)/(c.Weight/2+0.5)
			}
			prev := t.centroids[i-1]
			prevCenter := weightSoFar - prev.Weight/2
			span := center - prevCenter
			if span == 0 {
				return c.Mean
			}
			return prev.Mean + (c.Mean-prev.Mean)*(index-prevCenter)/span
		}
		weightSoFar += c.Weight
	}
	return t.max
}

// Count returns the total number of samples observed.
func (t *TDigest) Count() int64 { return t.count }

// Marshal serializes the digest for persistence.
func (t *TDigest) Marshal() ([]byte, error) {
	if len(t.buf) > 0 {
		t.merge()
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(gobDigest{
		Compression: t.compression,
		Centroids:   t.centroids,
		Count:       t.count,
		Min:         t.min,
		Max:         t.max,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal restores a digest previously produced by Marshal.
func Unmarshal(data []byte) (*TDigest, error) {
	var g gobDigest
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&g); err != nil {
		return nil, err
	}
	return &TDigest{
		compression: g.Compression,
		centroids:   g.Centroids,
		count:       g.Count,
		min:         g.Min,
		max:         g.Max,
	}, nil
}

type gobDigest struct {
	Compression float64
	Centroids   []centroid
	Count       int64
	Min         float64
	Max         float64
}

func (t *TDigest) bufferLimit() int {
	limit := int(10 * t.compression)
	if limit < 100 {
		return 100
	}
	return limit
}

func (t *TDigest) merge() {
	t.mergeWithWeights(t.buf, nil)
	t.buf = t.buf[:0]
}

func (t *TDigest) mergeWith(weighted []centroid) {
	t.mergeWithWeights(nil, weighted)
}

// mergeWithWeights folds unweighted raw values and/or pre-weighted centroids
// into a new centroid set using the scale function
// k(q) = compression/(2*pi) * asin(2q-1). Each centroid spans at most ~1
// unit of k, which bounds centroid count to O(compression).
func (t *TDigest) mergeWithWeights(raw []float64, weighted []centroid) {
	samples := make([]centroid, 0, len(t.centroids)+len(raw)+len(weighted))
	samples = append(samples, t.centroids...)
	for _, v := range raw {
		samples = append(samples, centroid{Mean: v, Weight: 1})
	}
	samples = append(samples, weighted...)
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].Mean < samples[j].Mean })

	total := 0.0
	for _, s := range samples {
		total += s.Weight
	}

	merged := make([]centroid, 0, len(t.centroids)+len(raw)+len(weighted))
	weightSoFar := 0.0
	cur := samples[0]
	started := false
	k := func(q float64) float64 {
		return t.compression / (2 * math.Pi) * math.Asin(2*q-1)
	}

	flush := func() {
		if cur.Weight > 0 {
			cur.Mean /= cur.Weight
			merged = append(merged, cur)
		}
	}

	for i, s := range samples {
		proposed := cur.Weight + s.Weight
		q1 := weightSoFar / total
		q2 := (weightSoFar + proposed) / total
		if !started || i == len(samples)-1 || k(q2)-k(q1) <= 1 {
			if !started {
				cur = centroid{Mean: s.Mean * s.Weight, Weight: s.Weight}
				started = true
			} else {
				cur.Mean += s.Mean * s.Weight
				cur.Weight = proposed
			}
			continue
		}
		flush()
		weightSoFar += cur.Weight
		cur = centroid{Mean: s.Mean * s.Weight, Weight: s.Weight}
	}
	if started {
		flush()
	}
	t.centroids = merged
}
