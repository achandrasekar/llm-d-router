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
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	inlineChildCapacity  = 4
	radixShardCount      = 256
	maxCompressedEdgeLen = 64
)

func radixShardIndex(key BlockHash) uint8 {
	return uint8(key ^ (key >> 32) ^ (key >> 16) ^ (key >> 8))
}

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

// radixNodeSnapshot holds an immutable view of a RadixNode's compressed edge,
// entries, and child links. Readers load the snapshot via atomic.Pointer without
// acquiring any tree lock; writers publish a new snapshot via copy-on-write.
type radixNodeSnapshot struct {
	edge             []BlockHash
	sharedEntries    []EntryRef
	perOffsetEntries [][]EntryRef
	inlineChildren   [inlineChildCapacity]inlineChild
	numInline        uint8
	children         map[BlockHash]*RadixNode
}

func (s *radixNodeSnapshot) getChild(key BlockHash) *RadixNode {
	for i := uint8(0); i < s.numInline; i++ {
		if s.inlineChildren[i].key == key {
			return s.inlineChildren[i].node
		}
	}
	if s.children != nil {
		return s.children[key]
	}
	return nil
}

func (s *radixNodeSnapshot) hasChildren() bool {
	return s.numInline > 0 || len(s.children) > 0
}

func (s *radixNodeSnapshot) entriesAt(offset int) []EntryRef {
	if offset < 0 || offset >= len(s.edge) {
		return nil
	}
	if s.perOffsetEntries != nil {
		return s.perOffsetEntries[offset]
	}
	return s.sharedEntries
}

func (s *radixNodeSnapshot) isEmpty() bool {
	if s.perOffsetEntries == nil {
		return len(s.sharedEntries) == 0
	}
	for _, entries := range s.perOffsetEntries {
		if len(entries) > 0 {
			return false
		}
	}
	return true
}

// RadixNode represents a compressed edge node in the KV-cache prefix Radix Trie.
type RadixNode struct {
	snap    atomic.Pointer[radixNodeSnapshot]
	parent  *RadixNode
	shardID uint8
}

func newRootRadixNode(shardID uint8) *RadixNode {
	n := &RadixNode{shardID: shardID}
	n.snap.Store(&radixNodeSnapshot{})
	return n
}

func (n *RadixNode) setChild(key BlockHash, child *RadixNode) {
	old := n.snap.Load()
	next := *old

	for i := uint8(0); i < next.numInline; i++ {
		if next.inlineChildren[i].key == key {
			next.inlineChildren[i].node = child
			n.snap.Store(&next)
			return
		}
	}
	if next.children != nil {
		newMap := make(map[BlockHash]*RadixNode, len(old.children)+1)
		maps.Copy(newMap, old.children)
		newMap[key] = child
		next.children = newMap
		n.snap.Store(&next)
		return
	}
	if int(next.numInline) < inlineChildCapacity {
		next.inlineChildren[next.numInline] = inlineChild{key: key, node: child}
		next.numInline++
		n.snap.Store(&next)
		return
	}
	newMap := make(map[BlockHash]*RadixNode, inlineChildCapacity+1)
	for i := uint8(0); i < next.numInline; i++ {
		newMap[next.inlineChildren[i].key] = next.inlineChildren[i].node
		next.inlineChildren[i] = inlineChild{}
	}
	next.numInline = 0
	newMap[key] = child
	next.children = newMap
	n.snap.Store(&next)
}

func (n *RadixNode) deleteChild(key BlockHash) {
	old := n.snap.Load()
	next := *old

	for i := uint8(0); i < next.numInline; i++ {
		if next.inlineChildren[i].key == key {
			last := next.numInline - 1
			next.inlineChildren[i] = next.inlineChildren[last]
			next.inlineChildren[last] = inlineChild{}
			next.numInline = last
			n.snap.Store(&next)
			return
		}
	}
	if old.children != nil {
		if _, exists := old.children[key]; !exists {
			return
		}
		if len(old.children) == 1 {
			next.children = nil
		} else {
			newMap := make(map[BlockHash]*RadixNode, len(old.children)-1)
			for k, v := range old.children {
				if k != key {
					newMap[k] = v
				}
			}
			next.children = newMap
		}
		n.snap.Store(&next)
	}
}

func (n *RadixNode) addRecordsFullEdge(recs []EntryRef, capacity int) {
	old := n.snap.Load()
	next := *old
	if old.perOffsetEntries == nil {
		next.sharedEntries = addEntryRefsCOW(old.sharedEntries, recs, capacity)
		n.snap.Store(&next)
		return
	}
	newPerOffset := make([][]EntryRef, len(old.perOffsetEntries))
	for i := range old.perOffsetEntries {
		newPerOffset[i] = addEntryRefsCOW(old.perOffsetEntries[i], recs, capacity)
	}
	next.perOffsetEntries = newPerOffset
	n.snap.Store(&next)
}

func (n *RadixNode) removeRecordsAt(offset int, toRemove []PodEntry) {
	old := n.snap.Load()
	if offset < 0 || offset >= len(old.edge) {
		return
	}
	if old.perOffsetEntries == nil {
		if !hasAnyPodEntry(old.sharedEntries, toRemove) {
			return
		}
		next := *old
		if len(old.edge) == 1 {
			next.sharedEntries = removeEntryRefsCOW(old.sharedEntries, toRemove)
			n.snap.Store(&next)
			return
		}
		newPerOffset := make([][]EntryRef, len(old.edge))
		for i := range old.edge {
			if i == offset {
				newPerOffset[i] = removeEntryRefsCOW(old.sharedEntries, toRemove)
			} else {
				newPerOffset[i] = old.sharedEntries
			}
		}
		next.sharedEntries = nil
		next.perOffsetEntries = newPerOffset
		n.snap.Store(&next)
		return
	}
	if !hasAnyPodEntry(old.perOffsetEntries[offset], toRemove) {
		return
	}
	next := *old
	newPerOffset := slices.Clone(old.perOffsetEntries)
	newPerOffset[offset] = removeEntryRefsCOW(old.perOffsetEntries[offset], toRemove)
	next.perOffsetEntries = newPerOffset
	n.snap.Store(&next)
}

func (n *RadixNode) removePodFromNode(podIdentifier string) {
	old := n.snap.Load()
	if old.perOffsetEntries == nil {
		if !hasPodIdentifier(old.sharedEntries, podIdentifier) {
			return
		}
		next := *old
		next.sharedEntries = removePodEntryRefsCOW(old.sharedEntries, podIdentifier)
		n.snap.Store(&next)
		return
	}
	changed := false
	for i := range old.perOffsetEntries {
		if hasPodIdentifier(old.perOffsetEntries[i], podIdentifier) {
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	next := *old
	newPerOffset := make([][]EntryRef, len(old.perOffsetEntries))
	for i := range old.perOffsetEntries {
		if hasPodIdentifier(old.perOffsetEntries[i], podIdentifier) {
			newPerOffset[i] = removePodEntryRefsCOW(old.perOffsetEntries[i], podIdentifier)
		} else {
			newPerOffset[i] = old.perOffsetEntries[i]
		}
	}
	next.perOffsetEntries = newPerOffset
	n.snap.Store(&next)
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

func hasPodIdentifier(entries []EntryRef, podIdentifier string) bool {
	for i := range entries {
		if entries[i].PodIdentifier == podIdentifier {
			return true
		}
	}
	return false
}

// addEntryRefsCOW returns a new slice with recs inserted under LRU semantics bounded by capacity.
func addEntryRefsCOW(oldEntries, recs []EntryRef, capacity int) []EntryRef {
	entries := make([]EntryRef, len(oldEntries), min(max(len(oldEntries)+len(recs), capacity), max(len(oldEntries)+len(recs), 1)))
	copy(entries, oldEntries)
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

// removeEntryRefsCOW returns a new slice with matching PodEntry records removed.
func removeEntryRefsCOW(oldEntries []EntryRef, toRemove []PodEntry) []EntryRef {
	if len(oldEntries) == 0 {
		return nil
	}
	entries := slices.Clone(oldEntries)
	for _, entry := range toRemove {
		for i := range entries {
			if entries[i].PodEntry == entry {
				entries = append(entries[:i], entries[i+1:]...)
				break
			}
		}
	}
	if len(entries) == 0 {
		return nil
	}
	return entries
}

// removePodEntryRefsCOW returns a new slice with all records belonging to podIdentifier removed.
func removePodEntryRefsCOW(oldEntries []EntryRef, podIdentifier string) []EntryRef {
	if len(oldEntries) == 0 {
		return nil
	}
	var entries []EntryRef
	for _, ref := range oldEntries {
		if ref.PodIdentifier != podIdentifier {
			if entries == nil {
				entries = make([]EntryRef, 0, len(oldEntries)-1)
			}
			entries = append(entries, ref)
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

type treeShard struct {
	mu   sync.Mutex
	root *RadixNode
}

type lookupShard struct {
	mu    sync.RWMutex
	table map[BlockHash]nodeRef
}

type engineKeyShard struct {
	mu  sync.Mutex
	lru *simplelru.LRU[BlockHash, []BlockHash]
}

// RadixTrieIndex is a sharded, lock-free-read compressed Radix-Trie implementation
// of the Index and KeyWalker interfaces.
type RadixTrieIndex struct {
	treeShards   [radixShardCount]treeShard
	lookupShards [radixShardCount]lookupShard
	engineShards [radixShardCount]engineKeyShard
	podCacheSize int
	pods         *interner
	tiers        *interner
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

	idx := &RadixTrieIndex{
		podCacheSize: podCacheSize,
		pods:         newInterner(maxInternedPods),
		tiers:        newInterner(maxInternedTiers),
	}

	shardSize := max((size+radixShardCount-1)/radixShardCount, 16)
	for i := 0; i < radixShardCount; i++ {
		idx.treeShards[i].root = newRootRadixNode(uint8(i))
		idx.lookupShards[i].table = make(map[BlockHash]nodeRef)
		shardLRU, err := simplelru.NewLRU[BlockHash, []BlockHash](shardSize, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize radix trie engine key shard: %w", err)
		}
		idx.engineShards[i].lru = shardLRU
	}

	return idx, nil
}

func (r *RadixTrieIndex) getNodeRef(key BlockHash) (nodeRef, bool) {
	sh := &r.lookupShards[radixShardIndex(key)]
	sh.mu.RLock()
	ref, ok := sh.table[key]
	sh.mu.RUnlock()
	return ref, ok
}

func (r *RadixTrieIndex) hasNodeRef(key BlockHash) bool {
	sh := &r.lookupShards[radixShardIndex(key)]
	sh.mu.RLock()
	_, ok := sh.table[key]
	sh.mu.RUnlock()
	return ok
}

func (r *RadixTrieIndex) trySetNodeRef(key BlockHash, ref nodeRef) bool {
	sh := &r.lookupShards[radixShardIndex(key)]
	sh.mu.Lock()
	if _, exists := sh.table[key]; exists {
		sh.mu.Unlock()
		return false
	}
	sh.table[key] = ref
	sh.mu.Unlock()
	return true
}

func (r *RadixTrieIndex) setNodeRefs(node *RadixNode, keys []BlockHash, baseOffset int) {
	for i, k := range keys {
		sh := &r.lookupShards[radixShardIndex(k)]
		sh.mu.Lock()
		sh.table[k] = nodeRef{node: node, offset: baseOffset + i}
		sh.mu.Unlock()
	}
}

func (r *RadixTrieIndex) deleteNodeRefs(keys []BlockHash) {
	for _, k := range keys {
		sh := &r.lookupShards[radixShardIndex(k)]
		sh.mu.Lock()
		delete(sh.table, k)
		sh.mu.Unlock()
	}
}

func (r *RadixTrieIndex) addEngineKeyMappings(engineKeys, requestKeys []BlockHash) {
	if len(engineKeys) == 0 || len(requestKeys) == 0 {
		return
	}
	if len(engineKeys) == len(requestKeys) {
		reqCopy := slices.Clone(requestKeys)
		for i, ek := range engineKeys {
			sh := &r.engineShards[radixShardIndex(ek)]
			sh.mu.Lock()
			sh.lru.Add(ek, reqCopy[i:i+1:i+1])
			sh.mu.Unlock()
		}
		return
	}
	mappings := engineToRequestMapping(engineKeys, requestKeys)
	for ek, rks := range mappings {
		sh := &r.engineShards[radixShardIndex(ek)]
		sh.mu.Lock()
		sh.lru.Add(ek, rks)
		sh.mu.Unlock()
	}
}

func (r *RadixTrieIndex) peekEngineKey(key BlockHash) ([]BlockHash, bool) {
	sh := &r.engineShards[radixShardIndex(key)]
	sh.mu.Lock()
	rks, found := sh.lru.Peek(key)
	sh.mu.Unlock()
	return rks, found
}

func (r *RadixTrieIndex) getEngineKey(key BlockHash) ([]BlockHash, bool) {
	sh := &r.engineShards[radixShardIndex(key)]
	sh.mu.Lock()
	rks, found := sh.lru.Get(key)
	sh.mu.Unlock()
	return rks, found
}

func (r *RadixTrieIndex) removeEngineKey(key BlockHash) {
	sh := &r.engineShards[radixShardIndex(key)]
	sh.mu.Lock()
	sh.lru.Remove(key)
	sh.mu.Unlock()
}

// WalkKeys implements KeyWalker, traversing compressed edge snapshots lock-free
// in input order without materializing per-key PodEntry slices.
func (r *RadixTrieIndex) WalkKeys(
	ctx context.Context,
	requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	var snap *radixNodeSnapshot
	offset := 0
	var parentSnap *radixNodeSnapshot

	for pos, key := range requestKeys {
		if pos&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}

		if snap != nil && offset < len(snap.edge) && snap.edge[offset] == key {
			// Fast path: advance along the current compressed edge slice.
		} else {
			snap = nil
			if parentSnap != nil {
				if child := parentSnap.getChild(key); child != nil {
					snap = child.snap.Load()
					offset = 0
				}
			} else if pos == 0 {
				rootSnap := r.treeShards[radixShardIndex(key)].root.snap.Load()
				if child := rootSnap.getChild(key); child != nil {
					snap = child.snap.Load()
					offset = 0
				}
			}
			if snap == nil {
				if ref, ok := r.getNodeRef(key); ok {
					s := ref.node.snap.Load()
					if s != nil && ref.offset < len(s.edge) && s.edge[ref.offset] == key {
						snap = s
						offset = ref.offset
					}
				}
			}
		}

		if snap == nil {
			parentSnap = nil
			if !visit(pos, false, nil) {
				return ctx.Err()
			}
			continue
		}

		entries := snap.entriesAt(offset)
		if len(entries) == 0 {
			snap = nil
			parentSnap = nil
			if !visit(pos, false, nil) {
				return ctx.Err()
			}
			continue
		}

		if !visit(pos, true, entries) {
			return ctx.Err()
		}

		offset++
		if offset == len(snap.edge) {
			parentSnap = snap
			snap = nil
		}
	}

	return ctx.Err()
}

// Lookup receives a list of requestKeys and a set of pod identifiers,
// and retrieves the filtered pods associated with those keys by traversing
// compressed edge snapshots lock-free.
func (r *RadixTrieIndex) Lookup(
	ctx context.Context,
	requestKeys []BlockHash,
	podIdentifierSet sets.Set[string],
) (map[BlockHash][]PodEntry, error) {
	if len(requestKeys) == 0 {
		return nil, fmt.Errorf("no requestKeys provided for lookup")
	}

	podsPerKey := make(map[BlockHash][]PodEntry, len(requestKeys))
	var snap *radixNodeSnapshot
	var sharedMatchedPods []PodEntry
	offset := 0
	var parentSnap *radixNodeSnapshot

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if snap != nil && offset < len(snap.edge) && snap.edge[offset] == requestKey {
			// Fast path: contiguous compressed edge match.
		} else {
			snap = nil
			sharedMatchedPods = nil
			if parentSnap != nil {
				if child := parentSnap.getChild(requestKey); child != nil {
					snap = child.snap.Load()
					offset = 0
				}
			}
			if snap == nil {
				rootSnap := r.treeShards[radixShardIndex(requestKey)].root.snap.Load()
				if child := rootSnap.getChild(requestKey); child != nil {
					snap = child.snap.Load()
					offset = 0
				} else if idx == 0 {
					if ref, ok := r.getNodeRef(requestKey); ok {
						s := ref.node.snap.Load()
						if s != nil && ref.offset < len(s.edge) && s.edge[ref.offset] == requestKey {
							snap = s
							offset = ref.offset
						}
					}
				}
			}
		}

		if snap == nil {
			break
		}

		var matchedPods []PodEntry
		if snap.perOffsetEntries == nil {
			if sharedMatchedPods == nil {
				if len(snap.sharedEntries) == 0 {
					break
				}
				sharedMatchedPods = filterEntryRefs(snap.sharedEntries, podIdentifierSet)
				if len(sharedMatchedPods) == 0 {
					break
				}
			}
			matchedPods = sharedMatchedPods
		} else {
			entries := snap.entriesAt(offset)
			if len(entries) == 0 {
				break
			}
			matchedPods = filterEntryRefs(entries, podIdentifierSet)
			if len(matchedPods) == 0 {
				break
			}
		}

		podsPerKey[requestKey] = matchedPods
		offset++
		if offset == len(snap.edge) {
			parentSnap = snap
			snap = nil
			sharedMatchedPods = nil
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return podsPerKey, nil
}

// splitNodePair splits node at splitIdx (0 < splitIdx < len(node.edge)) into newly
// allocated prefixNode and suffixNode instances, leaving the old node's snapshot
// intact for any in-flight lock-free reader. Called with treeShards[node.shardID].mu held.
func (r *RadixTrieIndex) splitNodePair(node *RadixNode, splitIdx int) (*RadixNode, *RadixNode) {
	oldSnap := node.snap.Load()
	parent := node.parent
	prefixEdge := oldSnap.edge[:splitIdx:splitIdx]
	suffixEdge := oldSnap.edge[splitIdx:]

	prefixSnap := &radixNodeSnapshot{
		edge: prefixEdge,
	}
	suffixSnap := &radixNodeSnapshot{
		edge:           suffixEdge,
		inlineChildren: oldSnap.inlineChildren,
		numInline:      oldSnap.numInline,
		children:       oldSnap.children,
	}

	if oldSnap.perOffsetEntries == nil {
		prefixSnap.sharedEntries = oldSnap.sharedEntries
		suffixSnap.sharedEntries = oldSnap.sharedEntries
	} else {
		prefixSnap.perOffsetEntries = oldSnap.perOffsetEntries[:splitIdx:splitIdx]
		suffixSnap.perOffsetEntries = oldSnap.perOffsetEntries[splitIdx:]
	}

	prefixNode := &RadixNode{
		parent:  parent,
		shardID: node.shardID,
	}
	prefixNode.snap.Store(prefixSnap)

	suffixNode := &RadixNode{
		parent:  prefixNode,
		shardID: node.shardID,
	}
	suffixNode.snap.Store(suffixSnap)

	for i := uint8(0); i < suffixSnap.numInline; i++ {
		suffixSnap.inlineChildren[i].node.parent = suffixNode
	}
	for _, child := range suffixSnap.children {
		child.parent = suffixNode
	}

	prefixNode.setChild(suffixEdge[0], suffixNode)
	if parent != nil {
		parent.setChild(prefixEdge[0], prefixNode)
	}

	r.setNodeRefs(prefixNode, prefixEdge, 0)
	r.setNodeRefs(suffixNode, suffixEdge, 0)

	return prefixNode, suffixNode
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

	lockedShard := -1
	lockShard := func(s uint8) {
		if lockedShard == int(s) {
			return
		}
		if lockedShard >= 0 {
			r.treeShards[lockedShard].mu.Unlock()
		}
		r.treeShards[s].mu.Lock()
		lockedShard = int(s)
	}
	defer func() {
		if lockedShard >= 0 {
			r.treeShards[lockedShard].mu.Unlock()
		}
	}()

	initialShard := radixShardIndex(requestKeys[0])
	if ref, exists := r.getNodeRef(requestKeys[0]); exists {
		initialShard = ref.node.shardID
	}
	lockShard(initialShard)

	curr := r.treeShards[lockedShard].root
	pos := 0
	for pos < len(requestKeys) {
		key := requestKeys[pos]
		currSnap := curr.snap.Load()

		if child := currSnap.getChild(key); child != nil {
			childSnap := child.snap.Load()
			matchLen := 1
			maxMatch := min(len(childSnap.edge), len(requestKeys)-pos)
			for matchLen < maxMatch && childSnap.edge[matchLen] == requestKeys[pos+matchLen] {
				matchLen++
			}

			if matchLen == len(childSnap.edge) {
				child.addRecordsFullEdge(records, r.podCacheSize)
				curr = child
				pos += matchLen
				continue
			}

			prefixNode, _ := r.splitNodePair(child, matchLen)
			prefixNode.addRecordsFullEdge(records, r.podCacheSize)
			curr = prefixNode
			pos += matchLen
			continue
		}

		if ref, exists := r.getNodeRef(key); exists {
			if ref.node.shardID != uint8(lockedShard) {
				lockShard(ref.node.shardID)
				ref, exists = r.getNodeRef(key)
				if !exists {
					curr = r.treeShards[lockedShard].root
					continue
				}
			}
			node := ref.node
			nodeSnap := node.snap.Load()
			startOff := ref.offset
			if startOff >= len(nodeSnap.edge) || nodeSnap.edge[startOff] != key {
				curr = r.treeShards[lockedShard].root
				continue
			}

			matchLen := 1
			maxMatch := min(len(nodeSnap.edge)-startOff, len(requestKeys)-pos)
			for matchLen < maxMatch && nodeSnap.edge[startOff+matchLen] == requestKeys[pos+matchLen] {
				matchLen++
			}

			if startOff > 0 {
				_, node = r.splitNodePair(node, startOff)
				nodeSnap = node.snap.Load()
			}
			if matchLen < len(nodeSnap.edge) {
				node, _ = r.splitNodePair(node, matchLen)
			}
			node.addRecordsFullEdge(records, r.podCacheSize)
			curr = node
			pos += matchLen
			continue
		}

		if curr == r.treeShards[lockedShard].root {
			keyShard := radixShardIndex(key)
			if keyShard != uint8(lockedShard) {
				lockShard(keyShard)
				curr = r.treeShards[lockedShard].root
				continue
			}
		}

		currSnap = curr.snap.Load()
		canExtendCurr := curr != r.treeShards[lockedShard].root && !currSnap.hasChildren() &&
			currSnap.perOffsetEntries == nil && len(currSnap.edge) < maxCompressedEdgeLen

		limit := min(len(requestKeys)-pos, maxCompressedEdgeLen)
		if canExtendCurr {
			limit = min(len(requestKeys)-pos, maxCompressedEdgeLen-len(currSnap.edge))
		}

		maxLen := 1
		for maxLen < limit {
			nextKey := requestKeys[pos+maxLen]
			if r.hasNodeRef(nextKey) || slices.Contains(requestKeys[pos:pos+maxLen], nextKey) {
				break
			}
			maxLen++
		}

		newEntries := addEntryRefsCOW(nil, records, r.podCacheSize)
		if canExtendCurr && slices.Equal(currSnap.sharedEntries, newEntries) {
			baseOff := len(currSnap.edge)
			claimed := 0
			for claimed < maxLen {
				if !r.trySetNodeRef(requestKeys[pos+claimed], nodeRef{node: curr, offset: baseOff + claimed}) {
					break
				}
				claimed++
			}
			if claimed == 0 {
				continue
			}
			newEdge := make([]BlockHash, baseOff+claimed)
			copy(newEdge, currSnap.edge)
			copy(newEdge[baseOff:], requestKeys[pos:pos+claimed])
			next := *currSnap
			next.edge = newEdge
			curr.snap.Store(&next)
			pos += claimed
			continue
		}

		edge := slices.Clone(requestKeys[pos : pos+maxLen])
		child := &RadixNode{
			parent:  curr,
			shardID: curr.shardID,
		}
		child.snap.Store(&radixNodeSnapshot{
			edge:          edge,
			sharedEntries: newEntries,
		})
		claimed := 0
		for claimed < maxLen {
			if !r.trySetNodeRef(edge[claimed], nodeRef{node: child, offset: claimed}) {
				break
			}
			claimed++
		}
		if claimed == 0 {
			continue
		}
		if claimed < maxLen {
			edge = edge[:claimed:claimed]
			child.snap.Store(&radixNodeSnapshot{
				edge:          edge,
				sharedEntries: newEntries,
			})
		}
		curr.setChild(edge[0], child)
		curr = child
		pos += claimed
	}

	if engineKeys != nil {
		lockShard(radixShardIndex(requestKeys[0]))
		r.addEngineKeyMappings(engineKeys, requestKeys)
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

	switch keyType {
	case EngineKey:
		rks, found := r.peekEngineKey(key)
		if !found {
			return nil
		}

		for _, rk := range rks {
			r.evictPodsFromNode(rk, entries)
		}

		if len(rks) > 0 {
			sh := &r.treeShards[radixShardIndex(rks[0])]
			sh.mu.Lock()
			allEmpty := true
			for _, rk := range rks {
				if ref, ok := r.getNodeRef(rk); ok {
					s := ref.node.snap.Load()
					if s != nil && len(s.entriesAt(ref.offset)) > 0 {
						allEmpty = false
						break
					}
				}
			}
			if allEmpty {
				r.removeEngineKey(key)
			}
			sh.mu.Unlock()
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
	ref, exists := r.getNodeRef(requestKey)
	if !exists {
		return
	}

	sh := &r.treeShards[ref.node.shardID]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ref, exists = r.getNodeRef(requestKey)
	if !exists {
		return
	}

	ref.node.removeRecordsAt(ref.offset, entries)
	snap := ref.node.snap.Load()
	if !snap.hasChildren() && snap.isEmpty() {
		r.pruneNode(ref.node, sh.root)
	}
}

// pruneNode removes an empty leaf node and any resulting empty ancestors.
// Called with treeShards[node.shardID].mu held.
func (r *RadixTrieIndex) pruneNode(node, root *RadixNode) {
	curr := node
	for curr != nil && curr != root {
		snap := curr.snap.Load()
		if snap.hasChildren() || !snap.isEmpty() {
			break
		}

		parent := curr.parent
		if parent != nil && len(snap.edge) > 0 {
			parent.deleteChild(snap.edge[0])
		}
		r.deleteNodeRefs(snap.edge)
		curr = parent
	}
}

// Clear removes every entry for the pod from the trie, across all shards.
func (r *RadixTrieIndex) Clear(_ context.Context, podIdentifier string) error {
	for i := 0; i < radixShardCount; i++ {
		sh := &r.treeShards[i]
		sh.mu.Lock()
		r.clearSubtree(sh.root, sh.root, podIdentifier)
		sh.mu.Unlock()
	}
	return nil
}

func (r *RadixTrieIndex) clearSubtree(node, root *RadixNode, podIdentifier string) {
	snap := node.snap.Load()
	for i := int(snap.numInline) - 1; i >= 0; i-- {
		r.clearSubtree(snap.inlineChildren[i].node, root, podIdentifier)
	}
	for _, child := range snap.children {
		r.clearSubtree(child, root, podIdentifier)
	}

	if node == root {
		return
	}

	node.removePodFromNode(podIdentifier)
	snap = node.snap.Load()
	if !snap.hasChildren() && snap.isEmpty() {
		if node.parent != nil && len(snap.edge) > 0 {
			node.parent.deleteChild(snap.edge[0])
		}
		r.deleteNodeRefs(snap.edge)
	}
}

// GetRequestKey returns the last request key associated with the given engineKey.
func (r *RadixTrieIndex) GetRequestKey(_ context.Context, engineKey BlockHash) (BlockHash, error) {
	rks, found := r.getEngineKey(engineKey)
	if !found || len(rks) == 0 {
		return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
	}
	return rks[len(rks)-1], nil
}
