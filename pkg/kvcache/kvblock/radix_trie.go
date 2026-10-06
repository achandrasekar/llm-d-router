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
	"slices"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"k8s.io/apimachinery/pkg/util/sets"
)

const inlineChildCapacity = 4

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

type inlineChild struct {
	key  BlockHash
	node *RadixNode
}

type nodeRef struct {
	node   *RadixNode
	offset int
}

// RadixNode represents a compressed edge node in the KV-cache prefix Radix Trie.
// Unbranched sequences of request block keys are stored contiguously in edge.
// When all keys along edge share the same pod entries, sharedEntries holds them
// once for the entire edge and perOffsetEntries is nil.
type RadixNode struct {
	edge             []BlockHash
	sharedEntries    []EntryRef
	perOffsetEntries [][]EntryRef
	inlineChildren   [inlineChildCapacity]inlineChild
	numInline        uint8
	children         map[BlockHash]*RadixNode
	parent           *RadixNode
}

func newRootRadixNode() *RadixNode {
	return &RadixNode{}
}

func (n *RadixNode) getChild(key BlockHash) *RadixNode {
	for i := uint8(0); i < n.numInline; i++ {
		if n.inlineChildren[i].key == key {
			return n.inlineChildren[i].node
		}
	}
	if n.children != nil {
		return n.children[key]
	}
	return nil
}

func (n *RadixNode) setChild(key BlockHash, child *RadixNode) {
	for i := uint8(0); i < n.numInline; i++ {
		if n.inlineChildren[i].key == key {
			n.inlineChildren[i].node = child
			return
		}
	}
	if n.children != nil {
		n.children[key] = child
		return
	}
	if int(n.numInline) < inlineChildCapacity {
		n.inlineChildren[n.numInline] = inlineChild{key: key, node: child}
		n.numInline++
		return
	}
	n.children = make(map[BlockHash]*RadixNode, inlineChildCapacity+1)
	for i := uint8(0); i < n.numInline; i++ {
		n.children[n.inlineChildren[i].key] = n.inlineChildren[i].node
		n.inlineChildren[i] = inlineChild{}
	}
	n.numInline = 0
	n.children[key] = child
}

func (n *RadixNode) deleteChild(key BlockHash) {
	for i := uint8(0); i < n.numInline; i++ {
		if n.inlineChildren[i].key == key {
			last := n.numInline - 1
			n.inlineChildren[i] = n.inlineChildren[last]
			n.inlineChildren[last] = inlineChild{}
			n.numInline = last
			return
		}
	}
	if n.children != nil {
		delete(n.children, key)
	}
}

func (n *RadixNode) hasChildren() bool {
	return n.numInline > 0 || len(n.children) > 0
}

func (n *RadixNode) entriesAt(offset int) []EntryRef {
	if n.perOffsetEntries != nil {
		return n.perOffsetEntries[offset]
	}
	return n.sharedEntries
}

func (n *RadixNode) ensurePerOffset() {
	if n.perOffsetEntries != nil {
		return
	}
	n.perOffsetEntries = make([][]EntryRef, len(n.edge))
	for i := range n.edge {
		if len(n.sharedEntries) > 0 {
			n.perOffsetEntries[i] = slices.Clone(n.sharedEntries)
		}
	}
	n.sharedEntries = nil
}

func (n *RadixNode) addRecordsFullEdge(recs []EntryRef, capacity int) {
	if n.perOffsetEntries == nil {
		n.sharedEntries = addEntryRefs(n.sharedEntries, recs, capacity)
		return
	}
	for i := range n.perOffsetEntries {
		n.perOffsetEntries[i] = addEntryRefs(n.perOffsetEntries[i], recs, capacity)
	}
}

func (n *RadixNode) addRecordsRange(start, end int, recs []EntryRef, capacity int) {
	if start == 0 && end == len(n.edge) {
		n.addRecordsFullEdge(recs, capacity)
		return
	}
	n.ensurePerOffset()
	for i := start; i < end; i++ {
		n.perOffsetEntries[i] = addEntryRefs(n.perOffsetEntries[i], recs, capacity)
	}
}

func (n *RadixNode) removeRecordsAt(offset int, toRemove []PodEntry) {
	if n.perOffsetEntries == nil {
		if !hasAnyPodEntry(n.sharedEntries, toRemove) {
			return
		}
		if len(n.edge) == 1 {
			n.sharedEntries = removeEntryRefs(n.sharedEntries, toRemove)
			return
		}
		n.ensurePerOffset()
	}
	n.perOffsetEntries[offset] = removeEntryRefs(n.perOffsetEntries[offset], toRemove)
}

func (n *RadixNode) removePodFromNode(podIdentifier string) {
	if n.perOffsetEntries == nil {
		n.sharedEntries = removePodEntryRefs(n.sharedEntries, podIdentifier)
		return
	}
	for i := range n.perOffsetEntries {
		n.perOffsetEntries[i] = removePodEntryRefs(n.perOffsetEntries[i], podIdentifier)
	}
}

func (n *RadixNode) isEmpty() bool {
	if n.perOffsetEntries == nil {
		return len(n.sharedEntries) == 0
	}
	for _, entries := range n.perOffsetEntries {
		if len(entries) > 0 {
			return false
		}
	}
	return true
}

func hasAnyPodEntry(entries []EntryRef, targets []PodEntry) bool {
	for i := range entries {
		for j := range targets {
			if entries[i].PodEntry == targets[j] {
				return true
			}
		}
	}
	return false
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

// removePodEntryRefs deletes all records belonging to podIdentifier.
func removePodEntryRefs(entries []EntryRef, podIdentifier string) []EntryRef {
	n := 0
	for _, ref := range entries {
		if ref.PodIdentifier != podIdentifier {
			entries[n] = ref
			n++
		}
	}
	clear(entries[n:])
	return entries[:n]
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

// RadixTrieIndex is a compressed Radix-Trie implementation of the Index and KeyWalker interfaces.
type RadixTrieIndex struct {
	mu                  sync.RWMutex
	root                *RadixNode
	nodeLookup          map[BlockHash]nodeRef
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
		root:                newRootRadixNode(),
		nodeLookup:          make(map[BlockHash]nodeRef),
		engineToRequestKeys: engineToRequestKeys,
		podCacheSize:        podCacheSize,
		pods:                newInterner(maxInternedPods),
		tiers:               newInterner(maxInternedTiers),
	}, nil
}

// WalkKeys implements KeyWalker, traversing compressed edge slices in order without
// materializing per-key PodEntry slices.
func (r *RadixTrieIndex) WalkKeys(
	ctx context.Context,
	requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var node *RadixNode
	offset := 0
	parent := r.root

	for pos, key := range requestKeys {
		if pos&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}

		if node != nil && offset < len(node.edge) && node.edge[offset] == key {
			// Fast path: advance along the current compressed edge slice.
		} else {
			node = nil
			if parent != nil {
				if child := parent.getChild(key); child != nil {
					node = child
					offset = 0
				}
			}
			if node == nil {
				if ref, ok := r.nodeLookup[key]; ok {
					node = ref.node
					offset = ref.offset
				}
			}
		}

		if node == nil {
			parent = nil
			if !visit(pos, false, nil) {
				return ctx.Err()
			}
			continue
		}

		entries := node.entriesAt(offset)
		if len(entries) == 0 {
			node = nil
			parent = nil
			if !visit(pos, false, nil) {
				return ctx.Err()
			}
			continue
		}

		if !visit(pos, true, entries) {
			return ctx.Err()
		}

		offset++
		if offset == len(node.edge) {
			parent = node
			node = nil
		}
	}

	return ctx.Err()
}

// Lookup receives a list of requestKeys and a set of pod identifiers,
// and retrieves the filtered pods associated with those keys by traversing the compressed Radix Trie.
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
	var node *RadixNode
	offset := 0
	parent := r.root

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if node != nil && offset < len(node.edge) && node.edge[offset] == requestKey {
			// Fast path: contiguous compressed edge match.
		} else {
			node = nil
			if parent != nil {
				if child := parent.getChild(requestKey); child != nil {
					node = child
					offset = 0
				}
			}
			if node == nil {
				if idx == 0 {
					if ref, ok := r.nodeLookup[requestKey]; ok {
						node = ref.node
						offset = ref.offset
					}
				} else if child := r.root.getChild(requestKey); child != nil {
					node = child
					offset = 0
				}
			}
		}

		if node == nil {
			break
		}

		entries := node.entriesAt(offset)
		if len(entries) == 0 {
			break
		}

		matchedPods := filterEntryRefs(entries, podIdentifierSet)
		if len(matchedPods) == 0 {
			break
		}

		podsPerKey[requestKey] = matchedPods
		offset++
		if offset == len(node.edge) {
			parent = node
			node = nil
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return podsPerKey, nil
}

// splitNode splits node at splitIdx (0 < splitIdx < len(node.edge)) and returns
// the newly created prefix node covering node.edge[:splitIdx].
func (r *RadixTrieIndex) splitNode(node *RadixNode, splitIdx int) *RadixNode {
	parent := node.parent
	prefixEdge := node.edge[:splitIdx:splitIdx]
	suffixEdge := node.edge[splitIdx:]

	prefixNode := &RadixNode{
		edge:   prefixEdge,
		parent: parent,
	}

	if node.perOffsetEntries == nil {
		prefixNode.sharedEntries = slices.Clone(node.sharedEntries)
	} else {
		prefixNode.perOffsetEntries = node.perOffsetEntries[:splitIdx:splitIdx]
		node.perOffsetEntries = node.perOffsetEntries[splitIdx:]
	}

	node.edge = suffixEdge
	node.parent = prefixNode

	if parent != nil {
		parent.setChild(prefixEdge[0], prefixNode)
	}
	prefixNode.setChild(suffixEdge[0], node)

	for i, k := range prefixEdge {
		r.nodeLookup[k] = nodeRef{node: prefixNode, offset: i}
	}
	for i, k := range suffixEdge {
		r.nodeLookup[k] = nodeRef{node: node, offset: i}
	}

	return prefixNode
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
	pos := 0
	for pos < len(requestKeys) {
		key := requestKeys[pos]

		if child := curr.getChild(key); child != nil {
			matchLen := 1
			maxMatch := min(len(child.edge), len(requestKeys)-pos)
			for matchLen < maxMatch && child.edge[matchLen] == requestKeys[pos+matchLen] {
				matchLen++
			}

			if matchLen == len(child.edge) {
				child.addRecordsFullEdge(records, r.podCacheSize)
				curr = child
				pos += matchLen
				continue
			}

			if pos+matchLen == len(requestKeys) {
				child.addRecordsRange(0, matchLen, records, r.podCacheSize)
				return nil
			}

			prefixNode := r.splitNode(child, matchLen)
			prefixNode.addRecordsFullEdge(records, r.podCacheSize)
			curr = prefixNode
			pos += matchLen
			continue
		}

		if ref, exists := r.nodeLookup[key]; exists {
			node := ref.node
			startOff := ref.offset
			matchLen := 1
			maxMatch := min(len(node.edge)-startOff, len(requestKeys)-pos)
			for matchLen < maxMatch && node.edge[startOff+matchLen] == requestKeys[pos+matchLen] {
				matchLen++
			}

			node.addRecordsRange(startOff, startOff+matchLen, records, r.podCacheSize)
			pos += matchLen
			if pos == len(requestKeys) {
				return nil
			}

			if startOff+matchLen == len(node.edge) {
				curr = node
			} else {
				curr = r.splitNode(node, startOff+matchLen)
			}
			continue
		}

		newLen := 1
		for pos+newLen < len(requestKeys) {
			if _, exists := r.nodeLookup[requestKeys[pos+newLen]]; exists {
				break
			}
			newLen++
		}

		newEntries := addEntryRefs(nil, records, r.podCacheSize)
		if curr != r.root && !curr.hasChildren() && curr.perOffsetEntries == nil &&
			slices.Equal(curr.sharedEntries, newEntries) {
			baseOff := len(curr.edge)
			curr.edge = append(curr.edge, requestKeys[pos:pos+newLen]...)
			for i := 0; i < newLen; i++ {
				r.nodeLookup[curr.edge[baseOff+i]] = nodeRef{node: curr, offset: baseOff + i}
			}
			pos += newLen
			continue
		}

		edge := slices.Clone(requestKeys[pos : pos+newLen])
		child := &RadixNode{
			edge:          edge,
			sharedEntries: newEntries,
			parent:        curr,
		}
		curr.setChild(edge[0], child)
		for i, k := range edge {
			r.nodeLookup[k] = nodeRef{node: child, offset: i}
		}
		curr = child
		pos += newLen
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
			if ref, ok := r.nodeLookup[rk]; ok && len(ref.node.entriesAt(ref.offset)) > 0 {
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
	ref, exists := r.nodeLookup[requestKey]
	if !exists {
		return
	}

	ref.node.removeRecordsAt(ref.offset, entries)

	if !ref.node.hasChildren() && ref.node.isEmpty() {
		r.pruneNode(ref.node)
	}
}

// pruneNode removes an empty leaf node and any resulting empty ancestors.
func (r *RadixTrieIndex) pruneNode(node *RadixNode) {
	curr := node
	for curr != nil && curr != r.root {
		if curr.hasChildren() || !curr.isEmpty() {
			break
		}

		parent := curr.parent
		if parent != nil && len(curr.edge) > 0 {
			parent.deleteChild(curr.edge[0])
		}
		for _, k := range curr.edge {
			delete(r.nodeLookup, k)
		}
		curr = parent
	}
}

// Clear removes every entry for the pod from the trie, across all nodes.
func (r *RadixTrieIndex) Clear(_ context.Context, podIdentifier string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.clearSubtree(r.root, podIdentifier)
	return nil
}

func (r *RadixTrieIndex) clearSubtree(node *RadixNode, podIdentifier string) {
	for i := int(node.numInline) - 1; i >= 0; i-- {
		r.clearSubtree(node.inlineChildren[i].node, podIdentifier)
	}
	for _, child := range node.children {
		r.clearSubtree(child, podIdentifier)
	}

	if node == r.root {
		return
	}

	node.removePodFromNode(podIdentifier)
	if !node.hasChildren() && node.isEmpty() {
		if node.parent != nil && len(node.edge) > 0 {
			node.parent.deleteChild(node.edge[0])
		}
		for _, k := range node.edge {
			delete(r.nodeLookup, k)
		}
	}
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
