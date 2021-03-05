package main

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"
	"testing"

	"github.com/dgraph-io/roaring"
	"github.com/dgraph-io/roaring/roaring64"
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

// Marshal bitmap is taking 3.2ms. pack is taking <1ms.
func BenchmarkMarshal(b *testing.B) {
	bm := newBitmap()
	b.Logf("Running with N = %d\n", b.N)

	pack := Encode(bm.ToArray(), 256)

	b.Run("bitmap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			buf.Grow(int(bm.GetSizeInBytes()))
			_, err := bm.WriteTo(&buf)
			require.NoError(b, err)
		}
	})

	b.Run("pack", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := pack.Marshal()
			require.NoError(b, err)
		}
	})

	b.Run("bitmap-msgpack", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			buf.Grow(int(bm.GetSizeInBytes()))
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(bm)
			require.NoError(b, err)
		}
	})
}

// This is taking 3.7ms for 1M entries. Doing Add does not have any significant impact on the
// performance afterwards.
// 1.6ms by UidPack unmarshal.
func BenchmarkUnmarshal(b *testing.B) {
	bm := newBitmap()
	b.Logf("Running with N = %d\n", b.N)

	data, err := bm.ToBytes()
	require.NoError(b, err)

	b.Run("bitmap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := roaring64.New()
			err := r.UnmarshalBinary(data)
			require.NoError(b, err)
		}
	})

	pack := Encode(bm.ToArray(), 256)
	data, err = pack.Marshal()
	require.NoError(b, err)

	b.Run("pack", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var p UidPack
			err := p.Unmarshal(data)
			require.NoError(b, err)
		}
	})
}

func BenchmarkRoaring32(b *testing.B) {
	bm := roaring.New()

	max := int32(N) * 1000
	for i := 0; i < N; i++ {
		bm.Add(uint32(rand.Int31n(max)))
	}

	data, err := bm.ToBytes()
	require.NoError(b, err)
	b.Logf("Bitmap size: %d\n", bm.GetCardinality())

	b.Run("bitmap32-marshal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := bm.ToBytes()
			require.NoError(b, err)
		}
	})
	b.Run("bitmap32-unmarshal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := roaring.New()
			err := r.UnmarshalBinary(data)
			require.NoError(b, err)
		}
	})
	b.Run("bitmap32-from-buffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := roaring.New()
			_, err := r.FromBuffer(data)
			require.NoError(b, err)
		}
	})
	b.Run("bitmap32-copy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			out := make([]byte, len(data))
			copy(out, data)
			out[1] = 0xFF
		}
	})
}

// Clone is taking ~5ms.
func BenchmarkClone(b *testing.B) {
	bm := newBitmap()
	b.Logf("Running with N = %d\n", b.N)

	// So, if we set copy on write to true, then clone is cheap. But, any modifications would invoke
	// the copy.
	// If false, then a copy would happen during clone.

	bm.SetCopyOnWrite(false)

	b.Run("cow-false", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := bm.Clone()
			_ = r
		}
	})

	bm.SetCopyOnWrite(true)
	b.Logf("Copy on write: %v\n", bm.GetCopyOnWrite())

	entry := bm.Minimum()

	r := bm.Clone()
	b.Logf("Copy on write for clone: %v\n", r.GetCopyOnWrite())

	b.Run("no-mod", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := bm.Clone()
			_ = r
		}
	})
	b.Run("with-mod", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := bm.Clone()
			r.Remove(entry)
		}
	})

	data, err := bm.ToBytes()
	require.NoError(b, err)

	b.Run("unmarshal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := roaring64.New()
			err := r.UnmarshalBinary(data)
			require.NoError(b, err)
		}
	})
}

func TestClone(t *testing.T) {
	bm := newBitmap()
	bm.SetCopyOnWrite(true)
	t.Logf("Copy on write: %v\n", bm.GetCopyOnWrite())

	entry := bm.Minimum()
	r := bm.Clone()
	r.Remove(entry)
	require.False(t, r.Contains(entry))
	require.True(t, bm.Contains(entry))
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

// Bitmap runs in 177 ns/op.
// List search in 252 ns/op.
// UidPack search is 3024 ns/op.
func BenchmarkContains(b *testing.B) {
	bm := roaring64.New()

	N := 1000000
	max := int64(N) * 1000
	for i := 0; i < N; i++ {
		bm.Add(uint64(rand.Int63n(max)))
	}
	b.Logf("Bitmap Stats: %+v\n", bm.Stats())
	bm.Stats()

	b.Logf("Size per Bitmap int: %.2f\n", float64(bm.GetSizeInBytes())/float64(bm.GetCardinality()))
	b.Run("bitmap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Contains(uint64(rand.Int63n(max)))
		}
	})

	l := newList()
	b.Logf("Size of list: %d\n", len(l))
	b.Run("list", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			uid := uint64(rand.Int63n(max))
			_ = sort.Search(len(l), func(j int) bool {
				return l[j] >= uid
			})
		}
	})
	pack := Encode(l, 256)
	b.Logf("Size per UidPack int: %.2f", float64(pack.Size())/float64(len(l)))
	b.Run("UidPack", func(b *testing.B) {
		dec := NewDecoder(pack)
		for i := 0; i < b.N; i++ {
			uid := uint64(rand.Int63n(max))
			dec.Seek(uid, SeekStart)
		}
	})
}

const N int = 1000000

func newBitmap() *roaring64.Bitmap {
	bm := roaring64.New()

	max := int64(N) * 1000
	for i := 0; i < N; i++ {
		bm.Add(uint64(rand.Int63n(max)))
	}
	return bm
}

func newList() []uint64 {
	var l []uint64
	max := int64(N) * 1000
	for i := 0; i < N; i++ {
		l = append(l, uint64(rand.Int63n(max)))
	}
	sort.Slice(l, func(i, j int) bool {
		return l[i] < l[j]
	})
	out := l[:0]
	var last uint64
	for _, x := range l {
		if x == last {
			continue
		}
		last = x
		out = append(out, x)
	}
	return out
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
