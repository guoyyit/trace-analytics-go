// Package analytics provides bounded-memory quantile sketches plus
// service/operation statistics, dependency analysis and critical path.
package analytics

import (
	"math"
	"sort"
	"time"
)

// Compression controls the centroid count vs accuracy tradeoff.
const Compression = 100.0

type centroid struct {
	mean   float64
	weight float64
}

// TDigest is a merging t-digest: bounded memory (O(compression) centroids),
// mergeable, high relative accuracy near the tails. It never stores or sorts
// the full value set.
type TDigest struct {
	centroids   []centroid
	pending     []centroid
	count       int64
	compression float64
}

func NewTDigest() *TDigest {
	return &TDigest{compression: Compression}
}

func (d *TDigest) Add(v time.Duration) {
	d.addWeighted(float64(v), 1)
}

// MergeFrom folds another digest in, preserving centroid weights.
func (d *TDigest) MergeFrom(other *TDigest) {
	if other == nil || other.count == 0 {
		return
	}
	other.merge()
	for _, c := range other.centroids {
		d.addWeighted(c.mean, c.weight)
	}
}

func (d *TDigest) addWeighted(mean, weight float64) {
	if d.compression == 0 {
		d.compression = Compression
	}
	d.pending = append(d.pending, centroid{mean: mean, weight: weight})
	d.count += int64(math.Round(weight))
	if len(d.pending) >= d.bufferLimit() {
		d.merge()
	}
}

// Quantile returns the estimated value at q in [0,1].
func (d *TDigest) Quantile(q float64) time.Duration {
	d.merge()
	if len(d.centroids) == 0 {
		return 0
	}
	if q <= 0 {
		return time.Duration(d.centroids[0].mean)
	}
	if q >= 1 {
		return time.Duration(d.centroids[len(d.centroids)-1].mean)
	}

	total := d.totalWeight()
	target := q * total
	cum := 0.0
	for i := 0; i < len(d.centroids); i++ {
		c := d.centroids[i]
		half := c.weight / 2
		if cum+half >= target {
			if i == 0 {
				return time.Duration(c.mean)
			}
			prev := d.centroids[i-1]
			denom := prev.weight/2 + half
			frac := 0.5
			if denom > 0 {
				frac = (target - (cum - half)) / denom
			}
			frac = math.Max(0, math.Min(1, frac))
			return time.Duration(prev.mean + frac*(c.mean-prev.mean))
		}
		cum += c.weight
	}
	return time.Duration(d.centroids[len(d.centroids)-1].mean)
}

func (d *TDigest) Count() int64 { return d.count }

// Snapshot returns a serializable [mean, weight] centroid representation.
func (d *TDigest) Snapshot() [][2]float64 {
	d.merge()
	out := make([][2]float64, len(d.centroids))
	for i, c := range d.centroids {
		out[i] = [2]float64{c.mean, c.weight}
	}
	return out
}

func TDigestFromSnapshot(snap [][2]float64) *TDigest {
	d := NewTDigest()
	var count int64
	for _, cw := range snap {
		d.centroids = append(d.centroids, centroid{mean: cw[0], weight: cw[1]})
		count += int64(math.Round(cw[1]))
	}
	d.count = count
	return d
}

func (d *TDigest) bufferLimit() int {
	return int(5 * d.compression)
}

func (d *TDigest) totalWeight() float64 {
	n := 0.0
	for _, c := range d.centroids {
		n += c.weight
	}
	return n
}

// merge folds pending centroids into the merged set using the k-scale size
// limit from the t-digest paper.
func (d *TDigest) merge() {
	if len(d.pending) == 0 {
		return
	}
	weighted := make([]centroid, 0, len(d.pending)+len(d.centroids))
	weighted = append(weighted, d.pending...)
	weighted = append(weighted, d.centroids...)
	sort.Slice(weighted, func(i, j int) bool { return weighted[i].mean < weighted[j].mean })

	total := 0.0
	for _, c := range weighted {
		total += c.weight
	}

	merged := make([]centroid, 0, len(weighted))
	cur := centroid{}
	cumBefore := 0.0
	started := false

	for _, c := range weighted {
		if !started {
			cur = c
			started = true
			continue
		}
		proposed := cur.weight + c.weight
		qLeft := cumBefore / total
		qRight := (cumBefore + proposed) / total
		if d.k(qRight)-d.k(qLeft) <= 1 {
			cur.mean = (cur.mean*cur.weight + c.mean*c.weight) / proposed
			cur.weight = proposed
		} else {
			merged = append(merged, cur)
			cumBefore += cur.weight
			cur = c
		}
	}
	if started {
		merged = append(merged, cur)
	}

	d.centroids = merged
	d.pending = d.pending[:0]
}

// k is the standard scale function k(q) = (delta / (2*pi)) * asin(2q - 1).
func (d *TDigest) k(q float64) float64 {
	q = math.Max(-1+1e-12, math.Min(1-1e-12, 2*q-1))
	return (d.compression / (2 * math.Pi)) * math.Asin(q)
}
