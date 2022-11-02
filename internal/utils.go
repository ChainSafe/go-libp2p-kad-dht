package internal

import (
	"crypto/sha256"

	"github.com/multiformats/go-multihash"
)

type Hash [32]byte

func Sha256Multihash(mh multihash.Multihash) Hash {
	return sha256.Sum256(mh)
}

// PrefixByBits returns prefix of the key with the given length (in bits).
// if bits == 0, it just returns the whole key.
func PrefixByBits(key []byte, bits int) []byte {
	if bits == 0 {
		return key
	}

	if bits >= len(key)*8 {
		return key
	}

	res := make([]byte, (bits/8)+1)
	copy(res[:bits/8], key[:bits/8])

	bitsToKeep := bits % 8
	bitmask := ^byte(0) >> byte(8-bitsToKeep)
	res[bits/8] = key[bits/8] & bitmask
	return res
}
