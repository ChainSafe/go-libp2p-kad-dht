package internal

import (
	"fmt"

	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

const keysize = 32

func Sha256Multihash(mh multihash.Multihash) (multihash.Multihash, int) {
	prefix := []byte("CR_DOUBLEHASH")
	mh, err := multihash.Sum(append(prefix, mh...), multihash.DBL_SHA2_256, keysize)
	if err != nil {
		// this shouldn't ever happen
		panic(err)
	}
	return mh, len(mh) - keysize
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
	if bitsToKeep == 0 {
		return res[:bits/8]
	}

	bitmask := ^byte(0) >> byte(8-bitsToKeep)
	res[bits/8] = key[bits/8] & bitmask
	return res
}

// DecodePrefixedKey accepts a key that's a multihash that's been prefix-sliced,
// so it can't be decoded by multihash.Encode anymore.
// this function returns the (sliced) multihash digest stored.
// loosely based off the code here: https://github.com/multiformats/go-multihash/blob/608669da49b636a646de3472101d0183889ae6c4/multihash.go#L148
func DecodePrefixedKey(key []byte) ([]byte, error) {
	// read multihash code
	code, c, err := varint.FromUvarint(key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode sliced multihash code: %s", err)
	}

	if c == 0 {
		return nil, fmt.Errorf("sliced multihash key was empty")
	}

	if code != multihash.DBL_SHA2_256 {
		return nil, fmt.Errorf("expected code to be DBL_SHA2_256")
	}

	// read encoded digest length
	key = key[c:]
	_, c, err = varint.FromUvarint(key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode sliced multihash digest length: %s", err)
	}

	if c >= len(key) {
		return nil, fmt.Errorf("somehow read more bytes than what's in the buffer when decoding sliced multihash")
	}

	// key should now be the start of the digest
	key = key[c:]
	return key, nil
}
