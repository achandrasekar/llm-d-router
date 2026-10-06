/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvblock_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	. "github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func createRadixTrieIndexForTesting(t *testing.T) Index {
	t.Helper()
	cfg := DefaultRadixTrieIndexConfig()
	cfg.PodCacheSize = 500
	index, err := NewRadixTrieIndex(cfg)
	require.NoError(t, err)
	return index
}

func TestRadixTrieIndexBehavior(t *testing.T) {
	testCommonIndexBehavior(t, createRadixTrieIndexForTesting)
}

func TestRadixTrieIndex_Basic(t *testing.T) {
	ctx := context.Background()
	idx, err := NewRadixTrieIndex(nil)
	if err != nil {
		t.Fatalf("failed to create RadixTrieIndex: %v", err)
	}

	requestKeys := []BlockHash{101, 102, 103, 104}
	engineKeys := []BlockHash{1001, 1002, 1003, 1004}
	entries := []PodEntry{
		{PodIdentifier: "pod-1", DeviceTier: "GPU"},
		{PodIdentifier: "pod-2", DeviceTier: "GPU"},
	}

	// 1. Add
	err = idx.Add(ctx, engineKeys, requestKeys, entries)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// 2. Lookup without filter
	res, err := idx.Lookup(ctx, requestKeys, sets.New[string]())
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if len(res) != 4 {
		t.Errorf("expected 4 matched keys, got %d", len(res))
	}
	if len(res[101]) != 2 {
		t.Errorf("expected 2 pods for key 101, got %d", len(res[101]))
	}

	// 3. Lookup with pod filter
	resFiltered, err := idx.Lookup(ctx, requestKeys, sets.New("pod-1"))
	if err != nil {
		t.Fatalf("Lookup with filter failed: %v", err)
	}
	if len(resFiltered[101]) != 1 || resFiltered[101][0].PodIdentifier != "pod-1" {
		t.Errorf("expected pod-1 only, got %v", resFiltered[101])
	}

	// 4. GetRequestKey
	rk, err := idx.GetRequestKey(ctx, 1004)
	if err != nil {
		t.Fatalf("GetRequestKey failed: %v", err)
	}
	if rk != 104 {
		t.Errorf("expected request key 104, got %v", rk)
	}

	// 5. Evict pod-2
	err = idx.Evict(ctx, 101, RequestKey, []PodEntry{{PodIdentifier: "pod-2", DeviceTier: "GPU"}})
	if err != nil {
		t.Fatalf("Evict failed: %v", err)
	}

	resPostEvict, _ := idx.Lookup(ctx, requestKeys, sets.New[string]())
	if len(resPostEvict[101]) != 1 {
		t.Errorf("expected 1 pod after eviction, got %d", len(resPostEvict[101]))
	}

	// 6. Clear pod-1
	err = idx.Clear(ctx, "pod-1")
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	resPostClear, _ := idx.Lookup(ctx, requestKeys, sets.New[string]())
	if len(resPostClear) != 0 {
		t.Errorf("expected 0 matched keys after Clear, got %d", len(resPostClear))
	}
}

func TestRadixTrieIndex_PrefixEarlyStop(t *testing.T) {
	ctx := context.Background()
	idx, _ := NewRadixTrieIndex(nil)

	requestKeys := []BlockHash{201, 202, 203}
	entries := []PodEntry{{PodIdentifier: "pod-1", DeviceTier: "GPU"}}

	_ = idx.Add(ctx, nil, requestKeys, entries)

	// Lookup a prompt where block 0 and 1 match, but block 2 differs (204 instead of 203)
	mismatchKeys := []BlockHash{201, 202, 204, 205}
	res, err := idx.Lookup(ctx, mismatchKeys, sets.New[string]())
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	// Should stop after block 202
	if len(res) != 2 {
		t.Fatalf("expected early stop at 2 keys, got %d", len(res))
	}
	if _, ok := res[201]; !ok {
		t.Errorf("missing key 201")
	}
	if _, ok := res[202]; !ok {
		t.Errorf("missing key 202")
	}
	if _, ok := res[204]; ok {
		t.Errorf("unexpected key 204 in results")
	}
}

func TestRadixTrieIndex_PodCacheSize(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(t.Context())
	idx, err := NewRadixTrieIndex(&RadixTrieIndexConfig{Size: 16, PodCacheSize: 2})
	require.NoError(t, err)

	key := BlockHash(51374550)
	pods := []PodEntry{
		{PodIdentifier: "pod1", DeviceTier: "gpu"},
		{PodIdentifier: "pod2", DeviceTier: "gpu"},
		{PodIdentifier: "pod3", DeviceTier: "cpu"},
	}
	require.NoError(t, idx.Add(ctx, nil, []BlockHash{key}, pods))

	podsPerKey, err := idx.Lookup(ctx, []BlockHash{key}, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []PodEntry{pods[1], pods[2]}, podsPerKey[key])
}

func TestRadixTrieIndex_IncrementalSuffixAdd(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(t.Context())
	idx, err := NewRadixTrieIndex(nil)
	require.NoError(t, err)

	podA := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	podB := PodEntry{PodIdentifier: "pod-b", DeviceTier: "cpu"}

	require.NoError(t, idx.Add(ctx, []BlockHash{1, 2}, []BlockHash{101, 102}, []PodEntry{podA}))
	// Incremental suffix addition anchored at existing key 102
	require.NoError(t, idx.Add(ctx, []BlockHash{2, 3, 4}, []BlockHash{102, 103, 104}, []PodEntry{podA}))
	// Device-tier update on middle key 102
	require.NoError(t, idx.Add(ctx, nil, []BlockHash{102}, []PodEntry{podB}))

	res, err := idx.Lookup(ctx, []BlockHash{101, 102, 103, 104}, nil)
	require.NoError(t, err)
	require.Len(t, res, 4)
	assert.ElementsMatch(t, []PodEntry{podA}, res[101])
	assert.ElementsMatch(t, []PodEntry{podA, podB}, res[102])
	assert.ElementsMatch(t, []PodEntry{podA}, res[103])
	assert.ElementsMatch(t, []PodEntry{podA}, res[104])
}

func TestRadixTrieIndex_WalkKeys(t *testing.T) {
	baseCtx := logging.NewTestLoggerIntoContext(t.Context())
	idx, err := NewRadixTrieIndex(nil)
	require.NoError(t, err)

	gpuA := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	cpuB := PodEntry{PodIdentifier: "pod-b", DeviceTier: "cpu"}
	specA := PodEntry{PodIdentifier: "pod-a", Speculative: true}

	require.NoError(t, idx.Add(baseCtx, nil, []BlockHash{10, 20}, []PodEntry{gpuA, cpuB}))
	require.NoError(t, idx.Add(baseCtx, nil, []BlockHash{20, 30}, []PodEntry{specA}))

	visits := walkAll(t, idx, []BlockHash{10, 20, 40, 30})
	require.Len(t, visits, 4)
	assert.True(t, visits[0].found)
	assert.ElementsMatch(t, []PodEntry{gpuA, cpuB}, podEntries(visits[0].entries))
	assert.True(t, visits[1].found)
	assert.ElementsMatch(t, []PodEntry{gpuA, cpuB, specA}, podEntries(visits[1].entries))
	assert.False(t, visits[2].found)
	assert.Empty(t, visits[2].entries)
	assert.True(t, visits[3].found)
	assert.ElementsMatch(t, []PodEntry{specA}, podEntries(visits[3].entries))

	// Verify stable ordinals across keys
	assert.Equal(t, visits[0].entries[0].PodOrdinal, visits[1].entries[0].PodOrdinal)
	assert.Equal(t, visits[0].entries[0].PodOrdinal, visits[3].entries[0].PodOrdinal)

	// Verify early stop when visit returns false
	visited := 0
	err = idx.WalkKeys(baseCtx, []BlockHash{10, 20, 30}, func(pos int, _ bool, _ []EntryRef) bool {
		visited++
		return pos < 1
	})
	require.NoError(t, err)
	assert.Equal(t, 2, visited)

	// Verify context cancellation
	ctx, cancel := context.WithCancel(baseCtx)
	cancel()
	err = idx.WalkKeys(ctx, []BlockHash{10, 20}, func(int, bool, []EntryRef) bool {
		return true
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRadixTrieIndex_EdgeCompressionAndSplitting(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(t.Context())
	idx, err := NewRadixTrieIndex(nil)
	require.NoError(t, err)

	podA := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	podB := PodEntry{PodIdentifier: "pod-b", DeviceTier: "gpu"}
	podC := PodEntry{PodIdentifier: "pod-c", DeviceTier: "cpu"}

	branchA := []BlockHash{10, 11, 12, 13, 14, 15}
	branchB := []BlockHash{10, 11, 12, 20, 21, 22}

	require.NoError(t, idx.Add(ctx, nil, branchA, []PodEntry{podA}))
	require.NoError(t, idx.Add(ctx, nil, branchB, []PodEntry{podB}))
	require.NoError(t, idx.Add(ctx, nil, []BlockHash{10, 11}, []PodEntry{podC}))

	resA, err := idx.Lookup(ctx, branchA, nil)
	require.NoError(t, err)
	require.Len(t, resA, 6)
	assert.ElementsMatch(t, []PodEntry{podA, podB, podC}, resA[10])
	assert.ElementsMatch(t, []PodEntry{podA, podB, podC}, resA[11])
	assert.ElementsMatch(t, []PodEntry{podA, podB}, resA[12])
	assert.ElementsMatch(t, []PodEntry{podA}, resA[13])
	assert.ElementsMatch(t, []PodEntry{podA}, resA[14])
	assert.ElementsMatch(t, []PodEntry{podA}, resA[15])

	resB, err := idx.Lookup(ctx, branchB, nil)
	require.NoError(t, err)
	require.Len(t, resB, 6)
	assert.ElementsMatch(t, []PodEntry{podB}, resB[20])
	assert.ElementsMatch(t, []PodEntry{podB}, resB[21])
	assert.ElementsMatch(t, []PodEntry{podB}, resB[22])

	// Evict middle key 14 of compressed suffix [13, 14, 15]
	require.NoError(t, idx.Evict(ctx, 14, RequestKey, []PodEntry{podA}))
	resAfterEvict, err := idx.Lookup(ctx, branchA, nil)
	require.NoError(t, err)
	assert.Len(t, resAfterEvict, 4, "chain lookup should stop at evicted key 14")

	// Direct lookup of key 15 should still find podA
	res15, err := idx.Lookup(ctx, []BlockHash{15}, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []PodEntry{podA}, res15[15])
}

func TestRadixTrieIndex_InlineToMapChildFanout(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(t.Context())
	idx, err := NewRadixTrieIndex(nil)
	require.NoError(t, err)

	const fanout = 8
	for branch := 0; branch < fanout; branch++ {
		pod := PodEntry{PodIdentifier: fmt.Sprintf("pod-%d", branch), DeviceTier: "gpu"}
		keys := []BlockHash{100, 101, BlockHash(1000 + branch*10), BlockHash(1001 + branch*10)}
		require.NoError(t, idx.Add(ctx, nil, keys, []PodEntry{pod}))
	}

	for branch := 0; branch < fanout; branch++ {
		pod := PodEntry{PodIdentifier: fmt.Sprintf("pod-%d", branch), DeviceTier: "gpu"}
		keys := []BlockHash{100, 101, BlockHash(1000 + branch*10), BlockHash(1001 + branch*10)}
		res, err := idx.Lookup(ctx, keys, nil)
		require.NoError(t, err)
		require.Len(t, res, 4)
		assert.Equal(t, []PodEntry{pod}, res[keys[2]])
		assert.Equal(t, []PodEntry{pod}, res[keys[3]])

		visits := walkAll(t, idx, keys)
		require.Len(t, visits, 4)
		for _, v := range visits {
			assert.True(t, v.found)
		}
	}

	for branch := 0; branch < fanout; branch++ {
		require.NoError(t, idx.Clear(ctx, fmt.Sprintf("pod-%d", branch)))
	}
	res, err := idx.Lookup(ctx, []BlockHash{100, 101}, nil)
	require.NoError(t, err)
	assert.Empty(t, res)
}

func TestRadixTrieIndex_ConcurrentWalkAndSplit(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(t.Context())
	idx, err := NewRadixTrieIndex(&RadixTrieIndexConfig{Size: 1 << 16, PodCacheSize: 64})
	require.NoError(t, err)

	baseChain := make([]BlockHash, 64)
	for i := range baseChain {
		baseChain[i] = BlockHash(1000 + i)
	}
	seedPod := PodEntry{PodIdentifier: "seed-pod", DeviceTier: "gpu"}
	require.NoError(t, idx.Add(ctx, nil, baseChain, []PodEntry{seedPod}))

	var wg sync.WaitGroup
	// Concurrent writers that split baseChain at every possible offset, add branches, and evict/re-add.
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			pod := PodEntry{PodIdentifier: fmt.Sprintf("worker-%d", worker), DeviceTier: "gpu"}
			for iter := 0; iter < 40; iter++ {
				splitPoint := (worker*3 + iter) % 60
				if splitPoint < 1 {
					splitPoint = 1
				}
				branch := make([]BlockHash, splitPoint+4)
				copy(branch, baseChain[:splitPoint])
				for j := 0; j < 4; j++ {
					branch[splitPoint+j] = BlockHash(uint64(100000 + worker*1000 + iter*10 + j))
				}
				require.NoError(t, idx.Add(ctx, nil, branch, []PodEntry{pod}))
				// Also lookup mid-chain key while other workers split
				midKey := baseChain[splitPoint]
				_, err := idx.Lookup(ctx, []BlockHash{midKey}, nil)
				require.NoError(t, err)
			}
		}(w)
	}

	// Concurrent lock-free WalkKeys and Lookup readers over baseChain and mid-chain sub-slices.
	for r := 0; r < 16; r++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for iter := 0; iter < 80; iter++ {
				start := (reader + iter) % 32
				slice := baseChain[start:]
				err := idx.WalkKeys(ctx, slice, func(_ int, found bool, entries []EntryRef) bool {
					if found {
						for _, e := range entries {
							_ = e.PodIdentifier
						}
					}
					return true
				})
				require.NoError(t, err)
			}
		}(r)
	}

	wg.Wait()

	// Verify seedPod is still present across all 64 keys of baseChain after all splits.
	visits := walkAll(t, idx, baseChain)
	require.Len(t, visits, len(baseChain))
	for i, v := range visits {
		assert.True(t, v.found, "position %d should still be found", i)
		assert.Contains(t, podEntries(v.entries), seedPod)
	}
}

func BenchmarkRadixTrieVsInMemory(b *testing.B) {
	ctx := context.Background()
	numBlocks := 500 // 500 blocks x 16 tokens = 8,000 token prompt

	requestKeys := make([]BlockHash, numBlocks)
	for i := 0; i < numBlocks; i++ {
		requestKeys[i] = BlockHash(1000 + i)
	}
	entries := []PodEntry{
		{PodIdentifier: "pod-1", DeviceTier: "GPU"},
		{PodIdentifier: "pod-2", DeviceTier: "GPU"},
		{PodIdentifier: "pod-3", DeviceTier: "GPU"},
	}

	inMem, _ := NewInMemoryIndex(nil)
	_ = inMem.Add(ctx, nil, requestKeys, entries)

	radix, _ := NewRadixTrieIndex(nil)
	_ = radix.Add(ctx, nil, requestKeys, entries)

	filter := sets.New("pod-1", "pod-2")

	b.Run("InMemoryIndex_Lookup", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = inMem.Lookup(ctx, requestKeys, filter)
		}
	})

	b.Run("RadixTrieIndex_Lookup", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = radix.Lookup(ctx, requestKeys, filter)
		}
	})

	b.Run("InMemoryIndex_WalkKeys", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = inMem.WalkKeys(ctx, requestKeys, func(_ int, found bool, refs []EntryRef) bool {
				return found && len(refs) > 0
			})
		}
	})

	b.Run("RadixTrieIndex_WalkKeys", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = radix.WalkKeys(ctx, requestKeys, func(_ int, found bool, refs []EntryRef) bool {
				return found && len(refs) > 0
			})
		}
	})
}

func BenchmarkScenarioA_500Tokens(b *testing.B) {
	ctx := context.Background()
	numBlocks := 31 // 500 tokens / 16 = ~31 blocks
	requestKeys := make([]BlockHash, numBlocks)
	for i := 0; i < numBlocks; i++ {
		requestKeys[i] = BlockHash(2000 + i)
	}
	entries := []PodEntry{
		{PodIdentifier: "pod-1", DeviceTier: "GPU"},
		{PodIdentifier: "pod-2", DeviceTier: "GPU"},
		{PodIdentifier: "pod-3", DeviceTier: "GPU"},
		{PodIdentifier: "pod-4", DeviceTier: "GPU"},
	}

	inMem, _ := NewInMemoryIndex(nil)
	_ = inMem.Add(ctx, nil, requestKeys, entries)

	radix, _ := NewRadixTrieIndex(nil)
	_ = radix.Add(ctx, nil, requestKeys, entries)

	filter := sets.New("pod-1", "pod-2", "pod-3", "pod-4")

	b.Run("InMemory_500Tokens", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = inMem.Lookup(ctx, requestKeys, filter)
		}
	})

	b.Run("RadixTrie_500Tokens", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = radix.Lookup(ctx, requestKeys, filter)
		}
	})
}

func BenchmarkScenarioB_1000Tokens(b *testing.B) {
	ctx := context.Background()
	numBlocks := 63 // 1000 tokens / 16 = ~63 blocks
	requestKeys := make([]BlockHash, numBlocks)
	for i := 0; i < numBlocks; i++ {
		requestKeys[i] = BlockHash(3000 + i)
	}
	entries := []PodEntry{
		{PodIdentifier: "pod-1", DeviceTier: "GPU"},
		{PodIdentifier: "pod-2", DeviceTier: "GPU"},
		{PodIdentifier: "pod-3", DeviceTier: "GPU"},
		{PodIdentifier: "pod-4", DeviceTier: "GPU"},
	}

	inMem, _ := NewInMemoryIndex(nil)
	_ = inMem.Add(ctx, nil, requestKeys, entries)

	radix, _ := NewRadixTrieIndex(nil)
	_ = radix.Add(ctx, nil, requestKeys, entries)

	filter := sets.New("pod-1", "pod-2", "pod-3", "pod-4")

	b.Run("InMemory_1000Tokens", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = inMem.Lookup(ctx, requestKeys, filter)
		}
	})

	b.Run("RadixTrie_1000Tokens", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = radix.Lookup(ctx, requestKeys, filter)
		}
	})
}

func BenchmarkRadixTrieWalkKeysScale(b *testing.B) {
	const numKeys = 3750
	for _, numPods := range []int{8, 96} {
		b.Run(fmt.Sprintf("keys=3750/pods=%d", numPods), func(b *testing.B) {
			idx, err := NewRadixTrieIndex(&RadixTrieIndexConfig{Size: numKeys * 2, PodCacheSize: numPods})
			require.NoError(b, err)
			keys := make([]BlockHash, numKeys)
			for i := range keys {
				keys[i] = BlockHash(i + 1)
			}
			entries := make([]PodEntry, numPods)
			for p := range entries {
				entries[p] = PodEntry{PodIdentifier: fmt.Sprintf("10.0.0.%d:8000", p), DeviceTier: "gpu"}
			}
			ctx := context.Background()
			require.NoError(b, idx.Add(ctx, nil, keys, entries))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seen := 0
				err := idx.WalkKeys(ctx, keys, func(_ int, _ bool, refs []EntryRef) bool {
					seen += len(refs)
					return true
				})
				if err != nil {
					b.Fatal(err)
				}
				if seen != numKeys*numPods {
					b.Fatalf("visited %d entries, want %d", seen, numKeys*numPods)
				}
			}
		})
	}
}

func populateRadixIndex(b *testing.B, numKeys, numPods int) (*RadixTrieIndex, []BlockHash) {
	b.Helper()
	idx, err := NewRadixTrieIndex(&RadixTrieIndexConfig{
		Size:         1 << 20,
		PodCacheSize: 128,
	})
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]BlockHash, numKeys)
	for i := range keys {
		keys[i] = BlockHash(uint64(i) + 1)
	}
	entries := make([]PodEntry, numPods)
	for p := range entries {
		entries[p] = PodEntry{
			PodIdentifier: fmt.Sprintf("10.0.%d.%d:8000", p/256, p%256),
			DeviceTier:    "gpu",
		}
	}
	if err := idx.Add(context.Background(), nil, keys, entries); err != nil {
		b.Fatal(err)
	}
	return idx, keys
}

func BenchmarkRadixTrieIndexAdd(b *testing.B) {
	idx, _ := populateRadixIndex(b, 1<<16, 4)
	entries := []PodEntry{{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu"}}
	engineKeys := make([]BlockHash, 64)
	requestKeys := make([]BlockHash, 64)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i)*64 + 1
		for j := range engineKeys {
			engineKeys[j] = BlockHash(base + uint64(j))
			requestKeys[j] = BlockHash(base + uint64(j))
		}
		if err := idx.Add(ctx, engineKeys, requestKeys, entries); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRadixTrieIndexMixed(b *testing.B) {
	const numKeys = 3750
	idx, keys := populateRadixIndex(b, numKeys, 16)
	ctx := context.Background()

	stop := make(chan struct{})
	defer close(stop)
	for w := 0; w < 4; w++ {
		go func(w int) {
			entries := []PodEntry{{PodIdentifier: fmt.Sprintf("10.1.0.%d:8000", w), DeviceTier: "gpu"}}
			engineKeys := make([]BlockHash, 64)
			requestKeys := make([]BlockHash, 64)
			for i := uint64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				base := (uint64(w)<<32 | i*64) + 1 //#nosec G115 -- test data, w is a small worker index
				for j := range engineKeys {
					engineKeys[j] = BlockHash(base + uint64(j))
					requestKeys[j] = BlockHash(base + uint64(j))
				}
				_ = idx.Add(ctx, engineKeys, requestKeys, entries)
			}
		}(w)
	}

	podSet := sets.New[string]()
	b.Run("Lookup", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := idx.Lookup(ctx, keys, podSet); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WalkKeys", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := idx.WalkKeys(ctx, keys, func(_ int, found bool, refs []EntryRef) bool {
				return found && len(refs) > 0
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
