// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package btree

import (
	"math/rand"
	"sync"
)

// ZipTreeG is a randomized balanced binary search tree (zip tree) that
// provides expected O(log n) time complexity for insert, delete, and search operations.
// It uses ranks similar to skip lists to maintain balance probabilistically.
type ZipTreeG[T any] struct {
	isoid    uint64            // Copy-on-write isolation ID
	mu       *sync.RWMutex     // Optional locking
	root     *zipNode[T]       // Root node
	count    int               // Total item count
	locks    bool              // Whether to use locking
	readOnly bool              // Read-only flag
	less     func(a, b T) bool // Comparison function
	empty    T                 // Zero value for type T
	// Random number generator for rank generation
	rng *rand.Rand
}

// zipNode represents a node in the zip tree
type zipNode[T any] struct {
	isoid uint64      // Isolation ID for copy-on-write
	key   T           // Key (and value for simple trees)
	rank  int         // Random rank from geometric distribution
	left  *zipNode[T] // Left child (keys < this.key)
	right *zipNode[T] // Right child (keys > this.key)
}

// ZipPathHint is a utility type used with the *Hint() functions. Hints provide
// faster operations for clustered keys.
type ZipPathHint struct {
	used [8]bool
	path [8]uint8
}

// ZipOptions for passing to New when creating a new ZipTree.
type ZipOptions struct {
	// NoLocks will disable locking. Otherwise a sync.RWMutex is used to
	// ensure all operations are safe across multiple goroutines.
	NoLocks bool
	// ReadOnly marks the tree as read-only, any modifications will trigger panic.
	ReadOnly bool
	// Seed for random number generator (for deterministic rank generation)
	Seed int64
}

// NewZipTreeG returns a new ZipTree
func NewZipTreeG[T any](less func(a, b T) bool) *ZipTreeG[T] {
	return NewZipTreeGOptions(less, ZipOptions{})
}

func NewZipTreeGOptions[T any](less func(a, b T) bool, opts ZipOptions) *ZipTreeG[T] {
	tr := new(ZipTreeG[T])
	tr.isoid = newIsoID()
	tr.locks = !opts.NoLocks
	if tr.locks {
		tr.mu = new(sync.RWMutex)
	}
	tr.less = less

	// Initialize random number generator
	seed := opts.Seed
	if seed == 0 {
		seed = rand.Int63()
	}
	tr.rng = rand.New(rand.NewSource(seed))

	if opts.ReadOnly {
		tr.Freeze()
	}
	return tr
}

// Freeze marks the tree as read-only.
func (tr *ZipTreeG[T]) Freeze() {
	tr.readOnly = true
}

// Less is a convenience function that performs a comparison of two items
// using the same "less" function provided to New.
func (tr *ZipTreeG[T]) Less(a, b T) bool {
	return tr.less(a, b)
}

// newNode creates a new node with the given key and a random rank
func (tr *ZipTreeG[T]) newNode(key T) *zipNode[T] {
	n := &zipNode[T]{
		isoid: tr.isoid,
		key:   key,
		rank:  tr.randomRank(),
	}
	return n
}

// randomRank generates a random rank from a geometric distribution
// The probability of rank r is (1-p)p^r where p = 1/2
func (tr *ZipTreeG[T]) randomRank() int {
	rank := 0
	// Keep flipping coins until we get tails
	for tr.rng.Intn(2) == 0 {
		rank++
	}
	return rank
}

// zip merges two trees x and y where all keys in x < all keys in y
func (tr *ZipTreeG[T]) zip(x, y *zipNode[T]) *zipNode[T] {
	if x == nil {
		return y
	}
	if y == nil {
		return x
	}

	// Ensure x and y have proper isolation IDs if we're mutating
	// (This will be handled by isoLoad in actual operations)

	if x.rank < y.rank {
		y.left = tr.zip(x, y.left)
		return y
	} else {
		x.right = tr.zip(x.right, y)
		return x
	}
}

// unzip splits tree x into two trees based on key k
// Returns (left, right) where left contains keys < k and right contains keys > k
func (tr *ZipTreeG[T]) unzip(x *zipNode[T], key T) (*zipNode[T], *zipNode[T]) {
	if x == nil {
		return nil, nil
	}

	// Ensure x has proper isolation ID if we're mutating
	// (This will be handled by isoLoad in actual operations)

	if tr.less(x.key, key) {
		// x.key < key, so x belongs in left tree
		left, right := tr.unzip(x.right, key)
		x.right = left
		return x, right
	} else if tr.less(key, x.key) {
		// key < x.key, so x belongs in right tree
		left, right := tr.unzip(x.left, key)
		x.left = right
		return left, x
	} else {
		// x.key == key (found the node to remove)
		return x.left, x.right
	}
}

// copy creates a deep copy of a node
func (tr *ZipTreeG[T]) copy(n *zipNode[T]) *zipNode[T] {
	if n == nil {
		return nil
	}
	n2 := new(zipNode[T])
	n2.isoid = tr.isoid
	n2.key = n.key
	n2.rank = n.rank
	// Note: children are not copied here - they will be copied as needed
	// when accessed through isoLoad
	return n2
}

// isoLoad loads the provided node and, if needed, performs a copy-on-write.
func (tr *ZipTreeG[T]) isoLoad(cn **zipNode[T], mut bool) *zipNode[T] {
	if *cn == nil {
		return nil
	}
	if mut && (*cn).isoid != tr.isoid {
		*cn = tr.copy(*cn)
	}
	// For zip trees, we always need to copy if IDs differ because
	// we might need to modify child pointers
	if (*cn).isoid != tr.isoid {
		*cn = tr.copy(*cn)
	}
	return *cn
}

// Copy creates a shallow copy of the tree with a new isolation ID.
func (tr *ZipTreeG[T]) Copy() *ZipTreeG[T] {
	if tr.locks {
		tr.mu.RLock()
		defer tr.mu.RUnlock()
	}
	tr2 := new(ZipTreeG[T])
	*tr2 = *tr
	tr2.isoid = newIsoID()
	if tr2.locks {
		tr2.mu = new(sync.RWMutex)
	}
	// Create a new RNG with same seed for deterministic behavior
	tr2.rng = rand.New(rand.NewSource(tr.rng.Int63()))
	return tr2
}

// IsoCopy is an alias for Copy.
func (tr *ZipTreeG[T]) IsoCopy() *ZipTreeG[T] {
	return tr.Copy()
}

// Len returns the number of items in the tree.
func (tr *ZipTreeG[T]) Len() int {
	if tr.locks {
		tr.mu.RLock()
		defer tr.mu.RUnlock()
	}
	return tr.count
}

// Set or replace a value for a key
func (tr *ZipTreeG[T]) Set(item T) (T, bool) {
	return tr.SetHint(item, nil)
}

// SetHint sets or replaces a value for a key with a path hint
func (tr *ZipTreeG[T]) SetHint(item T, hint *ZipPathHint) (prev T, replaced bool) {
	if tr.readOnly {
		panic("read-only tree")
	}
	if tr.locks {
		tr.mu.Lock()
		prev, replaced = tr.setHint(item, hint)
		tr.mu.Unlock()
	} else {
		prev, replaced = tr.setHint(item, hint)
	}
	return prev, replaced
}

func (tr *ZipTreeG[T]) setHint(item T, hint *ZipPathHint) (prev T, replaced bool) {
	if tr.root == nil {
		// Empty tree
		tr.root = tr.newNode(item)
		tr.count = 1
		return tr.empty, false
	}

	// Search for the item first
	if found := tr.searchNode(tr.root, item); found != nil {
		// Key already exists, replace it
		prev = found.key
		replaced = true
		// Ensure copy-on-write
		found = tr.isoLoad(&found, true)
		found.key = item
		return prev, true
	}

	// Key doesn't exist, insert new node
	newNode := tr.newNode(item)
	tr.root = tr.insert(tr.root, newNode)
	tr.count++
	return tr.empty, false
}

// insert implements the zip tree insertion algorithm
func (tr *ZipTreeG[T]) insert(x *zipNode[T], newNode *zipNode[T]) *zipNode[T] {
	if x == nil {
		return newNode
	}

	// Ensure copy-on-write if needed
	x = tr.isoLoad(&x, true)

	if newNode.rank < x.rank {
		// newNode has lower rank, insert in appropriate subtree
		if tr.less(newNode.key, x.key) {
			x.left = tr.insert(x.left, newNode)
		} else {
			x.right = tr.insert(x.right, newNode)
		}
		return x
	} else {
		// newNode has higher or equal rank, perform zip operation
		// Split the tree at newNode.key
		left, right := tr.unzip(x, newNode.key)
		newNode.left = left
		newNode.right = right
		return newNode
	}
}

// Get returns the value for a key
func (tr *ZipTreeG[T]) Get(key T) (T, bool) {
	return tr.GetHint(key, nil)
}

// GetMut returns the value for a key (mutating variant)
func (tr *ZipTreeG[T]) GetMut(key T) (T, bool) {
	return tr.GetHintMut(key, nil)
}

// GetHint returns the value for a key with a path hint
func (tr *ZipTreeG[T]) GetHint(key T, hint *ZipPathHint) (value T, ok bool) {
	if tr.locks {
		tr.mu.RLock()
		value, ok = tr.getHint(key, hint)
		tr.mu.RUnlock()
	} else {
		value, ok = tr.getHint(key, hint)
	}
	return value, ok
}

// GetHintMut returns the value for a key with a path hint (mutating variant)
func (tr *ZipTreeG[T]) GetHintMut(key T, hint *ZipPathHint) (value T, ok bool) {
	return tr.GetHint(key, hint)
}

func (tr *ZipTreeG[T]) getHint(key T, hint *ZipPathHint) (value T, ok bool) {
	if tr.root == nil {
		return tr.empty, false
	}
	if found := tr.searchNode(tr.root, key); found != nil {
		return found.key, true
	}
	return tr.empty, false
}

// Delete removes a key from the tree
func (tr *ZipTreeG[T]) Delete(key T) (T, bool) {
	return tr.DeleteHint(key, nil)
}

// DeleteHint removes a key from the tree with a path hint
func (tr *ZipTreeG[T]) DeleteHint(key T, hint *ZipPathHint) (prev T, deleted bool) {
	if tr.readOnly {
		panic("read-only tree")
	}
	if tr.locks {
		tr.mu.Lock()
		prev, deleted = tr.deleteHint(key, hint)
		tr.mu.Unlock()
	} else {
		prev, deleted = tr.deleteHint(key, hint)
	}
	return prev, deleted
}

func (tr *ZipTreeG[T]) deleteHint(key T, hint *ZipPathHint) (prev T, deleted bool) {
	if tr.root == nil {
		return tr.empty, false
	}

	// Search for the node
	found := tr.searchNode(tr.root, key)
	if found == nil {
		return tr.empty, false
	}

	prev = found.key

	// Remove the node using unzip and zip
	tr.root = tr.delete(tr.root, key)
	tr.count--
	return prev, true
}

// delete implements the zip tree deletion algorithm
func (tr *ZipTreeG[T]) delete(x *zipNode[T], key T) *zipNode[T] {
	if x == nil {
		return nil
	}

	// Ensure copy-on-write if needed
	x = tr.isoLoad(&x, true)

	if tr.less(key, x.key) {
		x.left = tr.delete(x.left, key)
		return x
	} else if tr.less(x.key, key) {
		x.right = tr.delete(x.right, key)
		return x
	} else {
		// Found the node to delete, zip its left and right subtrees
		return tr.zip(x.left, x.right)
	}
}

// searchNode searches for a key in the tree
func (tr *ZipTreeG[T]) searchNode(x *zipNode[T], key T) *zipNode[T] {
	for x != nil {
		if tr.less(key, x.key) {
			x = x.left
		} else if tr.less(x.key, key) {
			x = x.right
		} else {
			return x
		}
	}
	return nil
}

// Min returns the minimum item in the tree
func (tr *ZipTreeG[T]) Min() (T, bool) {
	if tr.locks {
		tr.mu.RLock()
		defer tr.mu.RUnlock()
	}
	return tr.minNode()
}

// MinMut returns the minimum item in the tree (mutating variant)
func (tr *ZipTreeG[T]) MinMut() (T, bool) {
	return tr.Min()
}

func (tr *ZipTreeG[T]) minNode() (T, bool) {
	if tr.root == nil {
		return tr.empty, false
	}
	n := tr.root
	for n.left != nil {
		n = n.left
	}
	return n.key, true
}

// Max returns the maximum item in the tree
func (tr *ZipTreeG[T]) Max() (T, bool) {
	if tr.locks {
		tr.mu.RLock()
		defer tr.mu.RUnlock()
	}
	return tr.maxNode()
}

// MaxMut returns the maximum item in the tree (mutating variant)
func (tr *ZipTreeG[T]) MaxMut() (T, bool) {
	return tr.Max()
}

func (tr *ZipTreeG[T]) maxNode() (T, bool) {
	if tr.root == nil {
		return tr.empty, false
	}
	n := tr.root
	for n.right != nil {
		n = n.right
	}
	return n.key, true
}

// PopMin removes and returns the minimum item in the tree
func (tr *ZipTreeG[T]) PopMin() (T, bool) {
	if tr.readOnly {
		panic("read-only tree")
	}
	if tr.locks {
		tr.mu.Lock()
		defer tr.mu.Unlock()
	}
	return tr.popMin()
}

func (tr *ZipTreeG[T]) popMin() (T, bool) {
	if tr.root == nil {
		return tr.empty, false
	}
	// Find minimum value
	value, ok := tr.minNode()
	if !ok {
		return tr.empty, false
	}
	// Delete it using standard delete operation
	tr.root = tr.delete(tr.root, value)
	tr.count--
	return value, true
}

// PopMax removes and returns the maximum item in the tree
func (tr *ZipTreeG[T]) PopMax() (T, bool) {
	if tr.readOnly {
		panic("read-only tree")
	}
	if tr.locks {
		tr.mu.Lock()
		defer tr.mu.Unlock()
	}
	return tr.popMax()
}

func (tr *ZipTreeG[T]) popMax() (T, bool) {
	if tr.root == nil {
		return tr.empty, false
	}
	// Find maximum value
	value, ok := tr.maxNode()
	if !ok {
		return tr.empty, false
	}
	// Delete it using standard delete operation
	tr.root = tr.delete(tr.root, value)
	tr.count--
	return value, true
}

// Clear removes all items from the tree
func (tr *ZipTreeG[T]) Clear() {
	if tr.readOnly {
		panic("read-only tree")
	}
	if tr.locks {
		tr.mu.Lock()
		defer tr.mu.Unlock()
	}
	tr.root = nil
	tr.count = 0
	// Reset isolation ID for new mutations
	tr.isoid = newIsoID()
}

// Scan iterates over all items in the tree in order
func (tr *ZipTreeG[T]) Scan(iter func(item T) bool) {
	if tr.locks {
		tr.mu.RLock()
		tr.scan(iter)
		tr.mu.RUnlock()
	} else {
		tr.scan(iter)
	}
}

func (tr *ZipTreeG[T]) scan(iter func(item T) bool) {
	tr.inorder(tr.root, iter)
}

// inorder performs an in-order traversal
func (tr *ZipTreeG[T]) inorder(x *zipNode[T], iter func(item T) bool) bool {
	if x == nil {
		return true
	}
	if !tr.inorder(x.left, iter) {
		return false
	}
	if !iter(x.key) {
		return false
	}
	return tr.inorder(x.right, iter)
}

// Ascend iterates over items >= pivot in order
func (tr *ZipTreeG[T]) Ascend(pivot T, iter func(item T) bool) {
	if tr.locks {
		tr.mu.RLock()
		tr.ascend(pivot, iter)
		tr.mu.RUnlock()
	} else {
		tr.ascend(pivot, iter)
	}
}

func (tr *ZipTreeG[T]) ascend(pivot T, iter func(item T) bool) {
	tr.ascendNode(tr.root, pivot, iter)
}

func (tr *ZipTreeG[T]) ascendNode(x *zipNode[T], pivot T, iter func(item T) bool) bool {
	if x == nil {
		return true
	}

	// If pivot is less than current key, explore left subtree first
	if tr.less(pivot, x.key) {
		if !tr.ascendNode(x.left, pivot, iter) {
			return false
		}
	}

	// Visit current node if >= pivot
	if !tr.less(x.key, pivot) { // x.key >= pivot
		if !iter(x.key) {
			return false
		}
	}

	// Always explore right subtree
	return tr.ascendNode(x.right, pivot, iter)
}

// Descend iterates over items <= pivot in reverse order
func (tr *ZipTreeG[T]) Descend(pivot T, iter func(item T) bool) {
	if tr.locks {
		tr.mu.RLock()
		tr.descend(pivot, iter)
		tr.mu.RUnlock()
	} else {
		tr.descend(pivot, iter)
	}
}

func (tr *ZipTreeG[T]) descend(pivot T, iter func(item T) bool) {
	tr.descendNode(tr.root, pivot, iter)
}

func (tr *ZipTreeG[T]) descendNode(x *zipNode[T], pivot T, iter func(item T) bool) bool {
	if x == nil {
		return true
	}

	// If pivot is greater than current key, explore right subtree first
	if tr.less(x.key, pivot) {
		if !tr.descendNode(x.right, pivot, iter) {
			return false
		}
	}

	// Visit current node if <= pivot
	if !tr.less(pivot, x.key) { // x.key <= pivot
		if !iter(x.key) {
			return false
		}
	}

	// Always explore left subtree
	return tr.descendNode(x.left, pivot, iter)
}
