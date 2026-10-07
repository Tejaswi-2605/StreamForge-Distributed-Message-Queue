package replay

import (
	"encoding/binary"
	"hash"
)

// AppendDigest fingerprints ordered offsets, keys and payloads with explicit
// lengths. One SHA-256 state per partition verifies replay without an O(N) map.
func AppendDigest(h hash.Hash, offset int64, key, payload []byte) {
	var field [8]byte
	binary.LittleEndian.PutUint64(field[:], uint64(offset))
	h.Write(field[:])
	binary.LittleEndian.PutUint64(field[:], uint64(len(key)))
	h.Write(field[:])
	h.Write(key)
	binary.LittleEndian.PutUint64(field[:], uint64(len(payload)))
	h.Write(field[:])
	h.Write(payload)
}
