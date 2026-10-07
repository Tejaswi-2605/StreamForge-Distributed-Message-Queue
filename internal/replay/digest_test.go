package replay

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestDigestDetectsChangedPayloadKeyOffsetAndOrder(t *testing.T) {
	baseline := sha256.New()
	AppendDigest(baseline, 0, []byte("a"), []byte("bc"))
	AppendDigest(baseline, 1, []byte("d"), []byte("ef"))
	for _, variant := range []int{0, 1, 2, 3} {
		h := sha256.New()
		switch variant {
		case 0:
			AppendDigest(h, 0, []byte("a"), []byte("changed"))
			AppendDigest(h, 1, []byte("d"), []byte("ef"))
		case 1:
			AppendDigest(h, 0, []byte("ab"), []byte("c"))
			AppendDigest(h, 1, []byte("d"), []byte("ef"))
		case 2:
			AppendDigest(h, 2, []byte("a"), []byte("bc"))
			AppendDigest(h, 1, []byte("d"), []byte("ef"))
		case 3:
			AppendDigest(h, 1, []byte("d"), []byte("ef"))
			AppendDigest(h, 0, []byte("a"), []byte("bc"))
		}
		if bytes.Equal(h.Sum(nil), baseline.Sum(nil)) {
			t.Fatal("digest failed to detect variant", variant)
		}
	}
}
