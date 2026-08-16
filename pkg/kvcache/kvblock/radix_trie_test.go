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

package kvblock

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
)

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

