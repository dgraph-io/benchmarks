/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"container/heap"
	"log"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/allegro/bigcache"
	"github.com/coocood/freecache"
	"github.com/dgraph-io/ristretto"
	goburrow "github.com/goburrow/cache"
	"github.com/golang/groupcache/lru"
)

type Cache interface {
	Get(string) (interface{}, bool)
	Set(string, interface{})
	Del(string)
	Log() *policyLog
	Close()
}

type BenchOptimal struct {
	capacity uint64
	hits     map[string]uint64
	access   []string
}

func NewBenchOptimal(capacity int, track bool) Cache {
	return &BenchOptimal{
		capacity: uint64(capacity),
		hits:     make(map[string]uint64),
		access:   make([]string, 0),
	}
}

func (c *BenchOptimal) Get(key string) (interface{}, bool) {
	c.hits[key]++
	c.access = append(c.access, key)
	return nil, false
}

func (c *BenchOptimal) Set(key string, value interface{}) {
	c.hits[key]++
	c.access = append(c.access, key)
}

func (c *BenchOptimal) Del(key string) {}

func (c *BenchOptimal) Log() *policyLog {
	hits, misses, evictions := int64(0), int64(0), int64(0)
	look := make(map[string]struct{}, c.capacity)
	data := &optimalHeap{}
	heap.Init(data)
	for _, key := range c.access {
		if _, has := look[key]; has {
			hits++
			continue
		}
		if uint64(data.Len()) >= c.capacity {
			victim := heap.Pop(data)
			delete(look, victim.(*optimalItem).key)
			evictions++
		}
		misses++
		look[key] = struct{}{}
		heap.Push(data, &optimalItem{key, c.hits[key]})
	}
	return &policyLog{
		hits:      hits,
		misses:    misses,
		evictions: evictions,
	}
}

func (c *BenchOptimal) Close() {}

type optimalItem struct {
	key  string
	hits uint64
}

type optimalHeap []*optimalItem

func (h optimalHeap) Len() int           { return len(h) }
func (h optimalHeap) Less(i, j int) bool { return h[i].hits < h[j].hits }
func (h optimalHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *optimalHeap) Push(x interface{}) {
	*h = append(*h, x.(*optimalItem))
}

func (h *optimalHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type BenchRistretto struct {
	cache *ristretto.Cache
	track bool
	log   *policyLog
}

func NewBenchRistretto(capacity int, track bool) Cache {
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(capacity * 10),
		MaxCost:     int64(capacity),
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	return &BenchRistretto{
		cache: c,
		track: track,
		log:   &policyLog{},
	}
}

func (c *BenchRistretto) Get(key string) (interface{}, bool) {
	return c.cache.Get(key)
}

func (c *BenchRistretto) Set(key string, value interface{}) {
	if c.track {
		if _, ok := c.cache.Get(key); ok {
			c.log.Hit()
		} else {
			c.log.Miss()
		}
	}
	c.cache.Set(key, value, 1)
}

func (c *BenchRistretto) Del(key string) {
	c.cache.Del(key)
}

func (c *BenchRistretto) Log() *policyLog {
	return c.log
}

func (c *BenchRistretto) Close() {
	c.cache.Close()
}

type BenchBaseMutex struct {
	sync.Mutex
	cache *lru.Cache
	log   *policyLog
	track bool
}

func NewBenchBaseMutex(capacity int, track bool) Cache {
	return &BenchBaseMutex{
		cache: lru.New(capacity),
		log:   &policyLog{},
		track: track,
	}
}

func (c *BenchBaseMutex) Get(key string) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()
	return c.cache.Get(key)
}

func (c *BenchBaseMutex) Set(key string, value interface{}) {
	c.Lock()
	defer c.Unlock()
	if c.track {
		if value, _ := c.cache.Get(key); value != nil {
			c.log.Hit()
		} else {
			c.log.Miss()
		}
	}
	c.cache.Add(key, value)
}

func (c *BenchBaseMutex) Del(key string) {
	c.cache.Remove(key)
}

func (c *BenchBaseMutex) Log() *policyLog {
	return c.log
}

func (c *BenchBaseMutex) Close() {}

func padString(s string, length int) string {
	if len(s) >= length {
		return s[:length]
	}
	return strings.Repeat("0", length-len(s)) + s
}

type BenchBigCache struct {
	cache *bigcache.BigCache
	log   *policyLog
	track bool
}

func NewBenchBigCache(capacity int, track bool) Cache {
	cache, err := bigcache.NewBigCache(bigcache.Config{
		Shards:             256,
		LifeWindow:         0,
		MaxEntriesInWindow: capacity,
		MaxEntrySize:       1482,
		Verbose:            false,
		HardMaxCacheSize:   capacity * 1482 / 1024 / 1024,
	})
	if err != nil {
		log.Panic(err)
	}
	return &BenchBigCache{
		cache: cache,
		log:   &policyLog{},
		track: track,
	}
}

func (c *BenchBigCache) Get(key string) (interface{}, bool) {
	value, err := c.cache.Get(key)
	if err != nil {
		return nil, false
	}
	return value, true
}

func (c *BenchBigCache) Set(key string, value interface{}) {
	longKey := padString(key, 64)
	longVal := []byte(padString(string(value.([]byte)), 1400))
	if c.track {
		if value, _ := c.cache.Get(longKey); value != nil {
			c.log.Hit()
			return
		} else {
			c.log.Miss()
		}
	}
	if err := c.cache.Set(longKey, longVal); err != nil {
		log.Panic(err)
	}
}

func (c *BenchBigCache) Del(key string) {
	if err := c.cache.Delete(key); err != nil {
		log.Panic(err)
	}
}

func (c *BenchBigCache) Log() *policyLog {
	return c.log
}

func (c *BenchBigCache) Close() {}

type BenchFastCache struct {
	cache *fastcache.Cache
	log   *policyLog
	track bool
}

func NewBenchFastCache(capacity int, track bool) Cache {
	// NOTE: if capacity is less than 32MB, then fastcache sets it to 32MB
	return &BenchFastCache{
		//cache: fastcache.New(capacity * 16),
		//
		// TODO: should we be using this, since the true entry size is 1468?
		//       that's how we're setting the capacity for freecache...
		//
		cache: fastcache.New(capacity * 1468),
		log:   &policyLog{},
		track: track,
	}
}

func (c *BenchFastCache) Get(key string) (interface{}, bool) {
	value := c.cache.Get(nil, []byte(key))
	if len(value) > 0 {
		return value, true
	}
	return value, false
}

func (c *BenchFastCache) Set(key string, value interface{}) {
	longKey := []byte(padString(key, 64))
	longVal := []byte(padString(string(value.([]byte)), 1400))
	if c.track {
		if c.cache.Get(nil, longKey) != nil {
			c.log.Hit()
			return
		} else {
			c.log.Miss()
		}
	}
	c.cache.Set(longKey, longVal)
}

func (c *BenchFastCache) Del(key string) {
	c.cache.Del([]byte(key))
}

func (c *BenchFastCache) Log() *policyLog {
	return c.log
}

func (c *BenchFastCache) Close() {}

type BenchFreeCache struct {
	cache *freecache.Cache
	log   *policyLog
	track bool
}

func NewBenchFreeCache(capacity int, track bool) Cache {
	// NOTE: if capacity is less than 512KB, then freecache sets it to 512KB
	return &BenchFreeCache{
		cache: freecache.NewCache(capacity * 1488),
		log:   &policyLog{},
		track: track,
	}
}

func (c *BenchFreeCache) Get(key string) (interface{}, bool) {
	value, err := c.cache.Get([]byte(key))
	if err != nil {
		return value, false
	}
	return value, true
}

func (c *BenchFreeCache) Set(key string, value interface{}) {
	longKey := []byte(padString(key, 64))
	longVal := []byte(padString(string(value.([]byte)), 1400))
	if c.track {
		if value, _ := c.cache.Get(longKey); value != nil {
			c.log.Hit()
			return
		} else {
			c.log.Miss()
		}
	}
	if err := c.cache.Set(longKey, longVal, 0); err != nil {
		log.Panic(err)
	}
}

func (c *BenchFreeCache) Del(key string) {
	c.cache.Del([]byte(key))
}

func (c *BenchFreeCache) Log() *policyLog {
	return c.log
}

func (c *BenchFreeCache) Close() {}

type BenchGoburrow struct {
	cache goburrow.Cache
	log   *policyLog
	track bool
}

func NewBenchGoburrow(capacity int, track bool) Cache {
	return &BenchGoburrow{
		cache: goburrow.New(
			goburrow.WithMaximumSize(capacity),
		),
		log:   &policyLog{},
		track: track,
	}
}

func (c *BenchGoburrow) Get(key string) (interface{}, bool) {
	return c.cache.GetIfPresent(key)
}

func (c *BenchGoburrow) Set(key string, value interface{}) {
	if c.track {
		if value, _ := c.cache.GetIfPresent(key); value != nil {
			c.log.Hit()
		} else {
			c.log.Miss()
		}
	}
	c.cache.Put(key, value)
}

func (c *BenchGoburrow) Del(key string) {
	c.cache.Invalidate(key)
}

func (c *BenchGoburrow) Log() *policyLog {
	return c.log
}

func (c *BenchGoburrow) Close() {}
