package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func main() {
	fmt.Println("vim-go")

	bm := roaring64.New()

	start := time.Now()
	N := 200000
	for i := 0; i < N; i++ {
		a := time.Now()
		bm.Add(uint64(rand.Int63n(20000000)))
		if s := time.Since(a); s > time.Millisecond {
			fmt.Printf("[%d] Took to add one %s\n", i, s)
		}
	}
	bm.RunOptimize()
	fmt.Printf("Took: %s. Stats: %+v\n", time.Since(start), bm.Stats())
}
