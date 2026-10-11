package packet

import "sync"

// bufferPool provides reusable byte slices for packet and property encoding,
// reducing allocations on the hot encode path (mempool).
var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// getBuffer returns a zero-length buffer from the pool.
func getBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

// putBuffer resets and returns a buffer to the pool. Oversized buffers are
// discarded so a single large packet cannot bloat the pool.
func putBuffer(b *[]byte) {
	if cap(*b) > 64*1024 {
		*b = make([]byte, 0, 512)
	} else {
		*b = (*b)[:0]
	}
	bufferPool.Put(b)
}
