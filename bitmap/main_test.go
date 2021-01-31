package main

import (
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/require"
)

// Each add is 24ns.
func BenchmarkList(b *testing.B) {
	var l []uint64
	max := int64(b.N) * 1000
	for i := 0; i < b.N; i++ {
		l = append(l, uint64(rand.Int63n(max)))
	}
	b.Logf("Size with N %d: %d\n", b.N, len(l)*8)
}

// Each add is 600ns.
func BenchmarkRoaring(b *testing.B) {
	bm := roaring64.New()
	b.Logf("Running with N = %d\n", b.N)

	max := int64(b.N) * 1000
	for i := 0; i < b.N; i++ {
		bm.Add(uint64(rand.Int63n(max)))
	}
	bm.RunOptimize()
	b.Logf("Size per UID: %.2f\n", float64(bm.GetSizeInBytes())/float64(b.N))
}

// This is taking 60 microseconds over 1M entries.
func BenchmarkCardinality(b *testing.B) {
	bm := newBitmap()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bm.GetCardinality()
	}
}

// This is taking 7ms for 1M entries.
func BenchmarkToArray(b *testing.B) {
	bm := newBitmap()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bm.ToArray()
	}
}

// This is taking 22ms for 1M entries.
func BenchmarkFromArray(b *testing.B) {
	bm := newBitmap()
	arr := bm.ToArray()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := roaring64.New()
		r.AddMany(arr)
	}
}

// Marshal is taking 5ms.
func BenchmarkMarshal(b *testing.B) {
	bm := newBitmap()
	b.Logf("Running with N = %d\n", b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bm.ToBytes()
		require.NoError(b, err)
	}
}

// This is taking 3.7ms for 1M entries. Doing Add does not have any significant impact on the
// performance afterwards.
func BenchmarkUnmarshal(b *testing.B) {
	bm := newBitmap()
	b.Logf("Running with N = %d\n", b.N)

	data, err := bm.ToBytes()
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := roaring64.New()
		err := r.UnmarshalBinary(data)
		require.NoError(b, err)
		r.Add(1)
	}
}

// Clone is taking ~5ms.
func BenchmarkClone(b *testing.B) {
	bm := newBitmap()
	b.Logf("Running with N = %d\n", b.N)
	b.Logf("Copy on write: %v\n", bm.GetCopyOnWrite())

	// So, if we set copy on write to true, then clone is cheap. But, any modifications would invoke
	// the copy.
	// If false, then a copy would happen during clone.
	// bm.SetCopyOnWrite(true)
	// b.Logf("Copy on write 2: %v\n", bm.GetCopyOnWrite())

	r := bm.Clone()
	b.Logf("Copy on write for clone: %v\n", r.GetCopyOnWrite())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clone itself is cheap, but any modifications are not.
		r := bm.Clone()
		_ = r
		// r.Add(1)
	}
}

func BenchmarkCopyOnWrite(b *testing.B) {
	bm := newBitmap()
	max := int64(1000000) * 1000
	b.Logf("Running with N = %d\n", b.N)
	b.Logf("Copy on write: %v\n", bm.GetCopyOnWrite())

	// Takes 345 ns/op.
	b.Run("copy-on-write-false", func(b *testing.B) {
		bm.SetCopyOnWrite(false)
		b.Logf("Copy on write: %v\n", bm.GetCopyOnWrite())

		for i := 0; i < b.N; i++ {
			bm.Add(uint64(rand.Int63n(max)))
		}
	})
	// Takes 524ns/op.
	b.Run("copy-on-write-true", func(b *testing.B) {
		// Copy on write does an extra copy which takes time.
		bm.SetCopyOnWrite(true)
		b.Logf("Copy on write: %v\n", bm.GetCopyOnWrite())
		for i := 0; i < b.N; i++ {
			bm.Add(uint64(rand.Int63n(max)))
		}
	})
}

// Runs in 200 ns/op.
func BenchmarkContains(b *testing.B) {
	bm := roaring64.New()
	b.Logf("Running with N = %d\n", b.N)

	N := 1000000
	max := int64(N) * 1000
	for i := 0; i < N; i++ {
		bm.Add(uint64(rand.Int63n(max)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Contains(uint64(rand.Int63n(max)))
	}
}

func newBitmap() *roaring64.Bitmap {
	bm := roaring64.New()

	N := 1000000
	max := int64(N) * 1000
	for i := 0; i < N; i++ {
		bm.Add(uint64(rand.Int63n(max)))
	}
	return bm
}

// This is taking 16ms. Clone itself takes ~4ms. So, ~12ms for AND.
func BenchmarkAnd(b *testing.B) {
	bm1 := newBitmap()
	bm2 := newBitmap()
	bm2.RunOptimize()
	b.Logf("Stats for bm2: %+v\n", bm2.Stats())

	b.ResetTimer()

	var r *roaring64.Bitmap
	for i := 0; i < b.N; i++ {
		r = roaring64.And(bm1, bm2)
		// r = bm1.Clone()
		// r.And(bm2)
	}
	b.StopTimer()
	b.Logf("Stats for r: %+v\n", r.Stats())
}

// This is taking 20ms. Clone itself takes ~4ms. So, ~16ms for OR.
func BenchmarkOr(b *testing.B) {
	bm1 := newBitmap()
	bm2 := newBitmap()
	b.Logf("Stats for bm2: %+v\n", bm2.Stats())

	b.ResetTimer()

	var r *roaring64.Bitmap
	for i := 0; i < b.N; i++ {
		r = roaring64.Or(bm1, bm2)
		// r.RunOptimize()
		// _, err := r.ToBytes()
		// require.NoError(b, err)
	}
	b.StopTimer()
	b.Logf("Stats for r: %+v\n", r.Stats())
}
