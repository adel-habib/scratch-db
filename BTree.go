package main

import "encoding/binary"

const (
	BNODE_NODE         = 1    // internal nodes without values
	BNODE_LEAF         = 2    // leaf nodes with values
	HEADER             = 4    // Header Size
	BTREE_PAGE_SIZE    = 4096 // Page Size
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

// BNode represents a single Node in the B tree
type BNode struct {
	// | type | nkeys | pointers   | offsets    | key-values
	// | 2B   | 2B    | nkeys * 8B | nkeys * 2B | ...
	// KV Paris
	// | klen | vlen | key | val |
	// | 2B   | 2B   | ... | ... |
	data []byte
}

// Type of the node (internal or leaf)
func (node BNode) btype() uint16 {
	// read first 2 bytes ( 16bit = 2 * 8 = 2 bytes) as uint
	return binary.LittleEndian.Uint16(node.data)
}

// number of keys in the node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// set the type of the node and the number of keys
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// retrieves the pointer at the provided index. the pointer represents a link to child nodes in the B-tree
func (node BNode) getPtr(idx uint16) uint64 {
	// make sure that the index is less than the number of keys in the node
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

// sets the pointer at the provided index to a given value
func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
// The idea behind using offsets is to optimize the space used by the node and make the access to the key-value pairs more efficient.
// Offsets allow you to directly jump to a key-value pair in the byte slice, instead of sequentially scanning through it.
// In the context of your B-tree nodes, a node stores a number of key-value pairs.
// These are stored sequentially (one after another) in the byte slice, and the byte slice also stores an offset for each key-value pair.
// Each offset is the distance from the start of the key-value pairs section to the start of a key-value pair.

// offsetPos calculates the position within the node's data byte slice where a specific offset is stored.
// It doesn't return the offset value itself, but rather its location within the byte slice.
// It takes an index idx (1-based), then calculates the byte position by skipping past the header and pointers
// and moving to the appropriate position in the offsets section.
// This position can be used with functions like getOffset and setOffset to read or write a specific offset.
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// getOffset returns the offset value at the given index within the node's data.
// An offset represents the start position of a key-value pair in the byte slice, relative to the start of the key-value pairs section.
// If idx is 0, which represents the position before the first key-value pair, the function returns 0.
// Otherwise, it calculates the byte position of the offset in the data byte slice using the offsetPos function,
// then reads two bytes from that position and interprets them as a little-endian 16-bit integer, which is the offset value.
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

// kvPos returns the position of the KV pair at index idx inside the node slice
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	// Size Of Headers + Size Of Pointers + Size of Offsets + Offset to KV-Pair
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// getKey retrieves the key at the given index within the BNode's data byte slice.
// It calculates the byte position of the key using the kvPos function and the length of the key,
// then returns the key as a byte slice.
func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	// KV-Pairs always start with 2-bytes key length, 2 bytes value length and then the key and the value
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	// another way would be node.data[pos+4:pos+4+klen] to directly get the right slice
	return node.data[pos+4:][:klen]
}

// getVal retrieves the value at the given index within the BNode's data byte slice.
// It calculates the byte position and length of the value using the kvPos function,
// then returns the value as a byte slice.
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// nbytes returns the total size of the node in bytes. It uses the kvPos function with the number of keys
// to calculate the start position of the next key-value pair in the data byte slice. Since the key-value pairs
// are packed sequentially in the slice, this position is also the total size of the node up to this point.
// Thus, the function effectively returns the total size of the BNode as it currently stands.
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}
