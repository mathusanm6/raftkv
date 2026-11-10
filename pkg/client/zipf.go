package client

import (
	"math"
	"math/rand"
	"sync"
)

// ZipfGenerator implements the standard Jim Gray / YCSB Zipfian generator
// supporting theta in (0, 1), such as theta = 0.9 required by benchmark plan §3.1.
type ZipfGenerator struct {
	mu    sync.Mutex
	rng   *rand.Rand
	n     int64   // Number of items (e.g. 1,000,000)
	theta float64 // Skewness parameter (e.g. 0.9)
	alpha float64
	zetan float64
	eta   float64
}

// NewZipfGenerator initializes a Zipfian generator for n items with skew theta.
func NewZipfGenerator(n int64, theta float64, seed int64) *ZipfGenerator {
	rng := rand.New(rand.NewSource(seed))

	// Precompute zeta(n, theta)
	var zetan float64
	for i := int64(1); i <= n; i++ {
		zetan += 1.0 / math.Pow(float64(i), theta)
	}

	zeta2 := 1.0 + 1.0/math.Pow(2.0, theta)
	alpha := 1.0 / (1.0 - theta)
	eta := (1.0 - math.Pow(2.0/float64(n), 1.0-theta)) / (1.0 - zeta2/zetan)

	return &ZipfGenerator{
		rng:   rng,
		n:     n,
		theta: theta,
		alpha: alpha,
		zetan: zetan,
		eta:   eta,
	}
}

// Next returns a random key index in [0, n-1] following the Zipfian distribution.
func (z *ZipfGenerator) Next() int64 {
	z.mu.Lock()
	u := z.rng.Float64()
	z.mu.Unlock()

	uz := u * z.zetan
	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}

	v := float64(z.n) * math.Pow(z.eta*u-z.eta+1.0, z.alpha)
	idx := int64(v)
	if idx >= z.n {
		idx = z.n - 1
	}
	if idx < 0 {
		idx = 0
	}
	return idx
}
