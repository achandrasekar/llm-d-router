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
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"k8s.io/apimachinery/pkg/util/sets"
)

// RadixTrieIndexConfig holds the configuration for RadixTrieIndex.
type RadixTrieIndexConfig struct {
	// Size is the maximum number of engine keys to track in LRU mapping.
	Size int `json:"size"`
	// PodCacheSize is the maximum number of pod entries per key node.
	PodCacheSize int `json:"podCacheSize"`
}

// DefaultRadixTrieIndexConfig returns a default configuration for RadixTrieIndex.
func DefaultRadixTrieIndexConfig() *RadixTrieIndexConfig {
	return &RadixTrieIndexConfig{
		Size:         defaultInMemoryIndexSize,
		PodCacheSize: defaultPodsPerKey,
	}
}

// RadixNode represents a node in the KV-cache prefix Radix Trie.
type RadixNode struct {
	key      BlockHash
	entries  []EntryRef
	children map[BlockHash]*RadixNode
	parent   *RadixNode
}

// newRadixNode instantiates a new trie node.
func newRadixNode(key BlockHash, parent *RadixNode) *RadixNode {
	return &RadixNode{
		key:      key,
		children: make(map[BlockHash]*RadixNode),
		parent:   parent,
	}
}

// addEntryRefs inserts records with LRU semantics bounded by capacity.
func addEntryRefs(entries, recs []EntryRef, capacity int) []EntryRef {
	for _, rec := range recs {
		found := false
		for i := range entries {
			if entries[i].PodEntry == rec.PodEntry {
				copy(entries[i:], entries[i+1:])
				entries[len(entries)-1] = rec
				found = true
				break
			}
		}
		if found {
			continue
		}
		if capacity > 0 && len(entries) >= capacity {
			copy(entries, entries[1:])
			entries[len(entries)-1] = rec
			continue
		}
		entries = append(entries, rec)
	}
	return entries
}

// removeEntryRefs deletes matching PodEntry records from entries.
func removeEntryRefs(entries []EntryRef, toRemove []PodEntry) []EntryRef {
	for _, entry := range toRemove {
		for i := range entries {
			if entries[i].PodEntry == entry {
				entries = append(entries[:i], entries[i+1:]...)
				break
			}
		}
	}
	return entries
}

// filterEntryRefs returns the PodEntry slice matching allowed (all entries when allowed is empty).
func filterEntryRefs(entries []EntryRef, allowed sets.Set[string]) []PodEntry {
	if len(entries) == 0 {
		return nil
	}
	if allowed.Len() == 0 {
		matched := make([]PodEntry, len(entries))
		for i := range entries {
			matched[i] = entries[i].PodEntry
		}
		return matched
	}
	var matched []PodEntry
	for i := range entries {
		if allowed.Has(entries[i].PodIdentifier) {
			matched = append(matched, entries[i].PodEntry)
		}
	}
	return matched
}

// RadixTrieIndex is an O(L) Radix-Trie based implementation of the Index interface.
type RadixTrieIndex struct {
	mu                  sync.RWMutex
	root                *RadixNode
	nodeLookup          map[BlockHash]*RadixNode
	engineToRequestKeys *lru.Cache[BlockHash, []BlockHash]
	podCacheSize        int
	pods                *interner
	tiers               *interner
}

var (
	_ Index     = &RadixTrieIndex{}
	_ KeyWalker = &RadixTrieIndex{}
)

// NewRadixTrieIndex creates a new RadixTrieIndex instance.
func NewRadixTrieIndex(cfg *RadixTrieIndexConfig) (*RadixTrieIndex, error) {
	if cfg == nil {
		cfg = DefaultRadixTrieIndexConfig()
	}
	size := cfg.Size
	if size <= 0 {
		size = defaultInMemoryIndexSize
	}
	podCacheSize := cfg.PodCacheSize
	if podCacheSize <= 0 {
		podCacheSize = defaultPodsPerKey
	}

	engineToRequestKeys, err := lru.New[BlockHash, []BlockHash](size)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize radix trie engine key map: %w", err)
	}

	return &RadixTrieIndex{
		root:                newRadixNode(EmptyBlockHash, nil),
		nodeLookup:          make(map[BlockHash]*RadixNode),
		engineToRequestKeys: engineToRequestKeys,
		podCacheSize:        podCacheSize,
		pods:                newInterner(maxInternedPods),
		tiers:               newInterner(maxInternedTiers),
	}, nil
}

// WalkKeys implements KeyWalker, visiting requestKeys in order without materializing
// per-key PodEntry slices.
func (r *RadixTrieIndex) WalkKeys(
	ctx context.Context,
	requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	curr := r.root
	for pos, key := range requestKeys {
		if pos&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		var node *RadixNode
		if curr != nil {
			node = curr.children[key]
		}
		if node == nil {
			node = r.nodeLookup[key]
		}
		if node == nil || len(node.entries) == 0 {
			curr = nil
			if !visit(pos, false, nil) {
				return ctx.Err()
			}
			continue
		}
		curr = node
		if !visit(pos, true, node.entries) {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// Lookup receives a list of requestKeys and a set of pod identifiers,
// and retrieves the filtered pods associated with those keys by traversing the Radix Trie.
func (r *RadixTrieIndex) Lookup(
	ctx context.Context,
	requestKeys []BlockHash,
	podIdentifierSet sets.Set[string],
) (map[BlockHash][]PodEntry, error) {
	if len(requestKeys) == 0 {
		return nil, fmt.Errorf("no requestKeys provided for lookup")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	podsPerKey := make(map[BlockHash][]PodEntry)
	curr := r.root

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}

		child := curr.children[requestKey]
		if child == nil {
			if idx == 0 {
				child = r.nodeLookup[requestKey]
			} else {
				child = r.root.children[requestKey]
			}
		}
		if child == nil || len(child.entries) == 0 {
			break
		}

		matchedPods := filterEntryRefs(child.entries, podIdentifierSet)
		if len(matchedPods) == 0 {
			break
		}

		podsPerKey[requestKey] = matchedPods
		curr = child
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return podsPerKey, nil
}

// Add stores requestKey -> pod entries and (optionally) engineKey -> requestKey mappings in the Radix Trie.
func (r *RadixTrieIndex) Add(
	_ context.Context,
	engineKeys, requestKeys []BlockHash,
	entries []PodEntry,
) error {
	if len(requestKeys) == 0 || len(entries) == 0 {
		return fmt.Errorf("no keys or entries provided for adding to index")
	}

	records, err := internEntryRecords(r.pods, r.tiers, entries)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if engineKeys != nil {
		mappings := engineToRequestMapping(engineKeys, requestKeys)
		for ek, rks := range mappings {
			r.engineToRequestKeys.Add(ek, rks)
		}
	}

	curr := r.root
	for _, requestKey := range requestKeys {
		child, exists := curr.children[requestKey]
		if !exists {
			if existing, ok := r.nodeLookup[requestKey]; ok {
				child = existing
			} else {
				child = newRadixNode(requestKey, curr)
				curr.children[requestKey] = child
				r.nodeLookup[requestKey] = child
			}
		}

		child.entries = addEntryRefs(child.entries, records, r.podCacheSize)
		curr = child
	}

	return nil
}

// Evict removes a key and its associated pod entries from the Radix Trie backend.
func (r *RadixTrieIndex) Evict(
	_ context.Context,
	key BlockHash,
	keyType KeyType,
	entries []PodEntry,
) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries provided for eviction from index")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	switch keyType {
	case EngineKey:
		rks, found := r.engineToRequestKeys.Peek(key)
		if !found {
			return nil
		}

		for _, rk := range rks {
			r.evictPodsFromNode(rk, entries)
		}

		allEmpty := true
		for _, rk := range rks {
			if node, ok := r.nodeLookup[rk]; ok && len(node.entries) > 0 {
				allEmpty = false
				break
			}
		}

		if allEmpty {
			r.engineToRequestKeys.Remove(key)
		}
		return nil

	case RequestKey:
		r.evictPodsFromNode(key, entries)
		return nil

	default:
		return fmt.Errorf("unknown key type: %d", keyType)
	}
}

// evictPodsFromNode removes given entries from node, and prunes unused subtrees.
func (r *RadixTrieIndex) evictPodsFromNode(requestKey BlockHash, entries []PodEntry) {
	node, exists := r.nodeLookup[requestKey]
	if !exists {
		return
	}

	node.entries = removeEntryRefs(node.entries, entries)

	if len(node.entries) == 0 && len(node.children) == 0 {
		r.pruneNode(node)
	}
}

// pruneNode removes a node from its parent and nodeLookup if it has no pods and no children.
func (r *RadixTrieIndex) pruneNode(node *RadixNode) {
	curr := node
	for curr != nil && curr != r.root {
		if len(curr.entries) > 0 || len(curr.children) > 0 {
			break
		}

		parent := curr.parent
		if parent != nil {
			delete(parent.children, curr.key)
		}
		delete(r.nodeLookup, curr.key)
		curr = parent
	}
}

// Clear removes every entry for the pod from the trie, across all nodes.
func (r *RadixTrieIndex) Clear(_ context.Context, podIdentifier string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, node := range r.nodeLookup {
		n := 0
		for _, ref := range node.entries {
			if ref.PodIdentifier != podIdentifier {
				node.entries[n] = ref
				n++
			}
		}
		clear(node.entries[n:])
		node.entries = node.entries[:n]

		if len(node.entries) == 0 && len(node.children) == 0 {
			r.pruneNode(node)
		}
	}
	return nil
}

// GetRequestKey returns the last request key associated with the given engineKey.
func (r *RadixTrieIndex) GetRequestKey(_ context.Context, engineKey BlockHash) (BlockHash, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rks, found := r.engineToRequestKeys.Get(engineKey)
	if !found || len(rks) == 0 {
		return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
	}
	return rks[len(rks)-1], nil
}
