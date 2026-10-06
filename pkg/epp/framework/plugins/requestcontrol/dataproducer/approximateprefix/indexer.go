/*
Copyright 2025 The Kubernetes Authors.
Copyright 2026 The llm-d Authors.

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

package approximateprefix

import (
	"context"
	"slices"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

const approxInlineChildren = 4

type approxInlineChild struct {
	key  blockHash
	node *approxRadixNode
}

type approxNodeRef struct {
	node   *approxRadixNode
	offset int
}

type approxRadixNode struct {
	edge           []blockHash
	sharedPods     []ServerID
	perOffsetPods  [][]ServerID
	inlineChildren [approxInlineChildren]approxInlineChild
	numInline      uint8
	children       map[blockHash]*approxRadixNode
	parent         *approxRadixNode
}

func (n *approxRadixNode) getChild(key blockHash) *approxRadixNode {
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

func (n *approxRadixNode) setChild(key blockHash, child *approxRadixNode) {
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
	if int(n.numInline) < approxInlineChildren {
		n.inlineChildren[n.numInline] = approxInlineChild{key: key, node: child}
		n.numInline++
		return
	}
	n.children = make(map[blockHash]*approxRadixNode, approxInlineChildren+1)
	for i := uint8(0); i < n.numInline; i++ {
		n.children[n.inlineChildren[i].key] = n.inlineChildren[i].node
		n.inlineChildren[i] = approxInlineChild{}
	}
	n.numInline = 0
	n.children[key] = child
}

func (n *approxRadixNode) deleteChild(key blockHash) {
	for i := uint8(0); i < n.numInline; i++ {
		if n.inlineChildren[i].key == key {
			last := n.numInline - 1
			n.inlineChildren[i] = n.inlineChildren[last]
			n.inlineChildren[last] = approxInlineChild{}
			n.numInline = last
			return
		}
	}
	if n.children != nil {
		delete(n.children, key)
	}
}

func (n *approxRadixNode) hasChildren() bool {
	return n.numInline > 0 || len(n.children) > 0
}

func (n *approxRadixNode) ensurePerOffset() {
	if n.perOffsetPods != nil {
		return
	}
	n.perOffsetPods = make([][]ServerID, len(n.edge))
	for i := range n.edge {
		if len(n.sharedPods) > 0 {
			n.perOffsetPods[i] = slices.Clone(n.sharedPods)
		}
	}
	n.sharedPods = nil
}

func addServerID(pods []ServerID, pod ServerID) []ServerID {
	if slices.Contains(pods, pod) {
		return pods
	}
	return append(pods, pod)
}

func removeServerID(pods []ServerID, pod ServerID) []ServerID {
	for i, s := range pods {
		if s == pod {
			last := len(pods) - 1
			pods[i] = pods[last]
			pods[last] = ServerID{}
			return pods[:last]
		}
	}
	return pods
}

func (n *approxRadixNode) addPodFullEdge(pod ServerID) {
	if n.perOffsetPods == nil {
		n.sharedPods = addServerID(n.sharedPods, pod)
		return
	}
	for i := range n.perOffsetPods {
		n.perOffsetPods[i] = addServerID(n.perOffsetPods[i], pod)
	}
}

func (n *approxRadixNode) addPodRange(start, end int, pod ServerID) {
	if start == 0 && end == len(n.edge) {
		n.addPodFullEdge(pod)
		return
	}
	n.ensurePerOffset()
	for i := start; i < end; i++ {
		n.perOffsetPods[i] = addServerID(n.perOffsetPods[i], pod)
	}
}

func (n *approxRadixNode) removePodAt(offset int, pod ServerID) {
	if n.perOffsetPods == nil {
		if !slices.Contains(n.sharedPods, pod) {
			return
		}
		if len(n.edge) == 1 {
			n.sharedPods = removeServerID(n.sharedPods, pod)
			return
		}
		n.ensurePerOffset()
	}
	n.perOffsetPods[offset] = removeServerID(n.perOffsetPods[offset], pod)
}

func (n *approxRadixNode) isEmpty() bool {
	if n.perOffsetPods == nil {
		return len(n.sharedPods) == 0
	}
	for _, pods := range n.perOffsetPods {
		if len(pods) > 0 {
			return false
		}
	}
	return true
}

// indexer implements the indexerInterface interface.
type indexer struct {
	mu             sync.RWMutex
	root           *approxRadixNode
	nodeLookup     map[blockHash]approxNodeRef
	hashToPods     map[blockHash]podSet                         // the lookup data structure to find pods that have the blockHash cached
	podToLRU       map[ServerID]*lru.Cache[blockHash, struct{}] // key is pod namespacedName, value is an LRU cache
	defaultLRUSize int
	pluginName     string
	pluginType     string
}

// newIndexer initializes an indexer with size limits and starts cache size reporting.
func newIndexer(ctx context.Context, defaultLRUSize int, pluginName, pluginType string) indexerInterface {
	i := &indexer{
		root:           &approxRadixNode{},
		nodeLookup:     make(map[blockHash]approxNodeRef),
		hashToPods:     make(map[blockHash]podSet),
		podToLRU:       make(map[ServerID]*lru.Cache[blockHash, struct{}]),
		defaultLRUSize: defaultLRUSize,
		pluginName:     pluginName,
		pluginType:     pluginType,
	}

	go i.reportLRUSize(ctx, time.Second)
	return i
}

// Add adds a list of prefix hashes to the cache, tied to the server.
func (i *indexer) Add(hashes []blockHash, pod server) {
	if len(hashes) == 0 {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Check if the LRU pod exist
	lruForPod, exists := i.podToLRU[pod.ServerID]
	if !exists {
		lruSize := pod.NumOfGPUBlocks
		if lruSize <= 0 {
			lruSize = i.defaultLRUSize
		}
		// We ignore the error since the only possible error is if size <= 0.
		newLRU, _ := lru.NewWithEvict(lruSize, i.makeEvictionFn(pod.ServerID))
		i.podToLRU[pod.ServerID] = newLRU
		lruForPod = newLRU
	}

	// Insert hashes tail-first: matching is anchored at the first block, so
	// the head is the most valuable entry and must stay cached longest; an
	// oversized batch naturally keeps exactly its head. hashToPods is updated
	// in the same iteration because the eviction callback can fire mid-batch.
	for idx := len(hashes) - 1; idx >= 0; idx-- {
		hash := hashes[idx]
		lruForPod.Add(hash, struct{}{})
		podIDs := i.hashToPods[hash]
		if podIDs == nil {
			podIDs = make(podSet)
		}
		podIDs[pod.ServerID] = struct{}{}
		i.hashToPods[hash] = podIDs
	}

	// Determine the prefix of hashes that survived in hashToPods for this pod.
	surviving := len(hashes)
	for k := len(hashes) - 1; k >= 0; k-- {
		if pods, ok := i.hashToPods[hashes[k]]; ok {
			if _, has := pods[pod.ServerID]; has {
				break
			}
		}
		surviving = k
	}
	if surviving > 0 {
		i.addPrefixToRadix(hashes[:surviving], pod.ServerID)
	}
}

func (i *indexer) splitRadixNode(node *approxRadixNode, splitIdx int) *approxRadixNode {
	parent := node.parent
	prefixEdge := node.edge[:splitIdx:splitIdx]
	suffixEdge := node.edge[splitIdx:]

	prefixNode := &approxRadixNode{
		edge:   prefixEdge,
		parent: parent,
	}

	if node.perOffsetPods == nil {
		prefixNode.sharedPods = slices.Clone(node.sharedPods)
	} else {
		prefixNode.perOffsetPods = node.perOffsetPods[:splitIdx:splitIdx]
		node.perOffsetPods = node.perOffsetPods[splitIdx:]
	}

	node.edge = suffixEdge
	node.parent = prefixNode

	if parent != nil {
		parent.setChild(prefixEdge[0], prefixNode)
	}
	prefixNode.setChild(suffixEdge[0], node)

	for off, k := range prefixEdge {
		i.nodeLookup[k] = approxNodeRef{node: prefixNode, offset: off}
	}
	for off, k := range suffixEdge {
		i.nodeLookup[k] = approxNodeRef{node: node, offset: off}
	}

	return prefixNode
}

func (i *indexer) addPrefixToRadix(hashes []blockHash, pod ServerID) {
	curr := i.root
	pos := 0
	for pos < len(hashes) {
		key := hashes[pos]
		if podSet, ok := i.hashToPods[key]; !ok {
			pos++
			continue
		} else if _, has := podSet[pod]; !has {
			pos++
			continue
		}

		if child := curr.getChild(key); child != nil {
			matchLen := 1
			maxMatch := min(len(child.edge), len(hashes)-pos)
			for matchLen < maxMatch && child.edge[matchLen] == hashes[pos+matchLen] {
				matchLen++
			}

			if matchLen == len(child.edge) {
				child.addPodFullEdge(pod)
				curr = child
				pos += matchLen
				continue
			}

			if pos+matchLen == len(hashes) {
				child.addPodRange(0, matchLen, pod)
				return
			}

			prefixNode := i.splitRadixNode(child, matchLen)
			prefixNode.addPodFullEdge(pod)
			curr = prefixNode
			pos += matchLen
			continue
		}

		if ref, exists := i.nodeLookup[key]; exists {
			node := ref.node
			startOff := ref.offset
			matchLen := 1
			maxMatch := min(len(node.edge)-startOff, len(hashes)-pos)
			for matchLen < maxMatch && node.edge[startOff+matchLen] == hashes[pos+matchLen] {
				matchLen++
			}

			node.addPodRange(startOff, startOff+matchLen, pod)
			pos += matchLen
			if pos == len(hashes) {
				return
			}

			if startOff+matchLen == len(node.edge) {
				curr = node
			} else {
				curr = i.splitRadixNode(node, startOff+matchLen)
			}
			continue
		}

		newLen := 1
		for pos+newLen < len(hashes) {
			nextKey := hashes[pos+newLen]
			if _, exists := i.nodeLookup[nextKey]; exists || slices.Contains(hashes[pos:pos+newLen], nextKey) {
				break
			}
			newLen++
		}

		if curr != i.root && !curr.hasChildren() && curr.perOffsetPods == nil &&
			len(curr.sharedPods) == 1 && curr.sharedPods[0] == pod {
			baseOff := len(curr.edge)
			curr.edge = append(curr.edge, hashes[pos:pos+newLen]...)
			for off := 0; off < newLen; off++ {
				i.nodeLookup[curr.edge[baseOff+off]] = approxNodeRef{node: curr, offset: baseOff + off}
			}
			pos += newLen
			continue
		}

		edge := slices.Clone(hashes[pos : pos+newLen])
		child := &approxRadixNode{
			edge:       edge,
			sharedPods: []ServerID{pod},
			parent:     curr,
		}
		curr.setChild(edge[0], child)
		for off, k := range edge {
			i.nodeLookup[k] = approxNodeRef{node: child, offset: off}
		}
		curr = child
		pos += newLen
	}
}

func (i *indexer) evictPodFromRadix(hash blockHash, pod ServerID) {
	ref, exists := i.nodeLookup[hash]
	if !exists {
		return
	}
	ref.node.removePodAt(ref.offset, pod)
	if !ref.node.hasChildren() && ref.node.isEmpty() {
		curr := ref.node
		for curr != nil && curr != i.root {
			if curr.hasChildren() || !curr.isEmpty() {
				break
			}
			parent := curr.parent
			if parent != nil && len(curr.edge) > 0 {
				parent.deleteChild(curr.edge[0])
			}
			for _, k := range curr.edge {
				delete(i.nodeLookup, k)
			}
			curr = parent
		}
	}
}

// Get returns a set of servers that have the given prefix hash cached.
func (i *indexer) Get(hash blockHash) podSet {
	i.mu.RLock()
	defer i.mu.RUnlock()

	pods := i.hashToPods[hash]
	if pods == nil {
		return nil
	}

	res := make(podSet, len(pods))
	for pod := range pods {
		// Deep copy to avoid race condition.
		res[pod] = struct{}{}
	}

	return res
}

// MatchLongestPrefix traverses the compressed radix tree in a single lock acquisition
// without allocating per-block podSet maps, aggregating full edge matches in O(pods).
func (i *indexer) MatchLongestPrefix(hashes []blockHash) map[ServerID]int {
	if len(hashes) == 0 {
		return make(map[ServerID]int)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	var res map[ServerID]int
	var node *approxRadixNode
	offset := 0
	parent := i.root
	pos := 0

	for pos < len(hashes) {
		hash := hashes[pos]

		if node != nil && offset < len(node.edge) && node.edge[offset] == hash {
			// Contiguous compressed edge match.
		} else {
			node = nil
			if parent != nil {
				if child := parent.getChild(hash); child != nil {
					node = child
					offset = 0
				}
			}
			if node == nil {
				if ref, ok := i.nodeLookup[hash]; ok {
					node = ref.node
					offset = ref.offset
				}
			}
		}

		if node == nil {
			break
		}

		if node.perOffsetPods == nil {
			pods := node.sharedPods
			if len(pods) == 0 {
				break
			}
			runLen := 1
			maxRun := min(len(node.edge)-offset, len(hashes)-pos)
			for runLen < maxRun && node.edge[offset+runLen] == hashes[pos+runLen] {
				runLen++
			}
			if res == nil {
				res = make(map[ServerID]int, len(pods))
			}
			for _, server := range pods {
				res[server] += runLen
			}
			pos += runLen
			offset += runLen
			if offset == len(node.edge) {
				parent = node
				node = nil
			}
			continue
		}

		pods := node.perOffsetPods[offset]
		if len(pods) == 0 {
			break
		}
		if res == nil {
			res = make(map[ServerID]int, len(pods))
		}
		for _, server := range pods {
			res[server]++
		}
		pos++
		offset++
		if offset == len(node.edge) {
			parent = node
			node = nil
		}
	}

	if res == nil {
		return make(map[ServerID]int)
	}
	return res
}

// makeEvictionFn returns a per-pod LRU eviction callback that removes the pod from hashToPods and the radix tree on eviction.
func (i *indexer) makeEvictionFn(pod ServerID) func(blockHash, struct{}) {
	return func(hash blockHash, _ struct{}) {
		// Remove the pod from the hash->pods map
		if podSet, ok := i.hashToPods[hash]; ok {
			delete(podSet, pod)
			if len(podSet) == 0 {
				delete(i.hashToPods, hash)
			}
		}
		i.evictPodFromRadix(hash, pod)
	}
}

// reportLRUSize starts a goroutine that periodically reports the LRU cache size metric.
func (i *indexer) reportLRUSize(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.reportOnce(ctx)
		}
	}
}

func (i *indexer) reportOnce(ctx context.Context) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	totalEntries := 0
	maxPodEntries := 0
	var maxPodName ServerID

	for pod, lruCache := range i.podToLRU {
		size := lruCache.Len()
		totalEntries += size
		if size > maxPodEntries {
			maxPodEntries = size
			maxPodName = pod
		}
	}

	numPods := len(i.podToLRU)
	avg := 0.0
	if numPods > 0 {
		avg = float64(totalEntries) / float64(numPods)
	}

	recordPrefixCacheSize(i.pluginName, i.pluginType, int64(totalEntries))

	log.FromContext(ctx).V(logutil.TRACE).Info("Prefix cache state",
		"total entries", totalEntries,
		"# pods", numPods,
		"avg entries per pod", avg,
		"pod with max cache", maxPodName,
		"max pod size", maxPodEntries,
		"global max LRU cache capacity per pod", i.defaultLRUSize,
	)
}

// RemovePod removes a pod and its associated entries from the indexer.
func (i *indexer) RemovePod(pod ServerID) {
	i.mu.Lock()
	defer i.mu.Unlock()

	lruCache, exists := i.podToLRU[pod]
	if !exists {
		return
	}

	// Remove all hashes associated with the pod from hashToPods and the radix tree (triggers eviction callbacks).
	for _, hash := range lruCache.Keys() {
		lruCache.Remove(hash)
	}

	delete(i.podToLRU, pod)
}

// Pods returns the list of all pods currently tracked in the indexer.
func (i *indexer) Pods() []ServerID {
	i.mu.RLock()
	defer i.mu.RUnlock()

	pods := make([]ServerID, 0, len(i.podToLRU))
	for pod := range i.podToLRU {
		pods = append(pods, pod)
	}
	return pods
}

// PodBlockCounts returns the number of cached blocks currently tracked per pod.
func (i *indexer) PodBlockCounts() map[ServerID]int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	counts := make(map[ServerID]int, len(i.podToLRU))
	for pod, lruCache := range i.podToLRU {
		counts[pod] = lruCache.Len()
	}
	return counts
}
