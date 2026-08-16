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

const (
	defaultRadixTrieCapacity = 1e8
	defaultRadixTriePodSize  = 10
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
		Size:         defaultRadixTrieCapacity,
		PodCacheSize: defaultRadixTriePodSize,
	}
}

// RadixNode represents a node in the KV-cache prefix Radix Trie.
type RadixNode struct {
	key      BlockHash
	pods     map[PodEntry]struct{}
	children map[BlockHash]*RadixNode
	parent   *RadixNode
}

// newRadixNode instantiates a new trie node.
func newRadixNode(key BlockHash, parent *RadixNode) *RadixNode {
	return &RadixNode{
		key:      key,
		pods:     make(map[PodEntry]struct{}),
		children: make(map[BlockHash]*RadixNode),
		parent:   parent,
	}
}

// RadixTrieIndex is an O(L) Radix-Trie based implementation of the Index interface.
type RadixTrieIndex struct {
	mu                  sync.RWMutex
	root                *RadixNode
	nodeLookup          map[BlockHash]*RadixNode
	engineToRequestKeys *lru.Cache[BlockHash, []BlockHash]
	podCacheSize        int
}

var _ Index = &RadixTrieIndex{}

// NewRadixTrieIndex creates a new RadixTrieIndex instance.
func NewRadixTrieIndex(cfg *RadixTrieIndexConfig) (*RadixTrieIndex, error) {
	if cfg == nil {
		cfg = DefaultRadixTrieIndexConfig()
	}

	engineToRequestKeys, err := lru.New[BlockHash, []BlockHash](cfg.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize radix trie engine key map: %w", err)
	}

	return &RadixTrieIndex{
		root:                newRadixNode(EmptyBlockHash, nil),
		nodeLookup:          make(map[BlockHash]*RadixNode),
		engineToRequestKeys: engineToRequestKeys,
		podCacheSize:        cfg.PodCacheSize,
	}, nil
}

// Lookup receives a list of requestKeys and a set of pod identifiers,
// and retrieves the filtered pods associated with those keys by traversing the Radix Trie.
func (r *RadixTrieIndex) Lookup(
	_ context.Context,
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

	for _, requestKey := range requestKeys {
		child, exists := curr.children[requestKey]
		if !exists || len(child.pods) == 0 {
			// Early stop: prefix chain breaks here
			break
		}

		var matchedPods []PodEntry
		if podIdentifierSet.Len() == 0 {
			matchedPods = make([]PodEntry, 0, len(child.pods))
			for pod := range child.pods {
				matchedPods = append(matchedPods, pod)
			}
		} else {
			for pod := range child.pods {
				if podIdentifierSet.Has(pod.PodIdentifier) {
					matchedPods = append(matchedPods, pod)
				}
			}
		}

		if len(matchedPods) == 0 {
			// No pods match after filtering: prefix chain breaks here
			break
		}

		podsPerKey[requestKey] = matchedPods
		curr = child
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
			child = newRadixNode(requestKey, curr)
			curr.children[requestKey] = child
			r.nodeLookup[requestKey] = child
		}

		for _, entry := range entries {
			child.pods[entry] = struct{}{}
		}

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
		rks, found := r.engineToRequestKeys.Get(key)
		if !found {
			return nil
		}

		for _, rk := range rks {
			r.evictPodsFromNode(rk, entries)
		}

		allEmpty := true
		for _, rk := range rks {
			if node, ok := r.nodeLookup[rk]; ok && len(node.pods) > 0 {
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

	for _, entry := range entries {
		delete(node.pods, entry)
	}

	// Prune node if empty and has no children
	if len(node.pods) == 0 && len(node.children) == 0 {
		r.pruneNode(node)
	}
}

// pruneNode removes a node from its parent and nodeLookup if it has no pods and no children.
func (r *RadixTrieIndex) pruneNode(node *RadixNode) {
	curr := node
	for curr != nil && curr != r.root {
		if len(curr.pods) > 0 || len(curr.children) > 0 {
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
		for entry := range node.pods {
			if entry.PodIdentifier == podIdentifier {
				delete(node.pods, entry)
			}
		}

		if len(node.pods) == 0 && len(node.children) == 0 {
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
