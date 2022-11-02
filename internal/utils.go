package internal

import (
	"github.com/multiformats/go-multihash"
)

const keysize = 32

func Sha256Multihash(mh multihash.Multihash) multihash.Multihash {
	prefix := []byte("CR_DOUBLEHASH")
	mh, err := multihash.Sum(append(prefix, mh...), multihash.DBL_SHA2_256, keysize)
	if err != nil {
		// this shouldn't ever happen
		panic(err)
	}
	return mh[len(mh)-keysize:]
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
