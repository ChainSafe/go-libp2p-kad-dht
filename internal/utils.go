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
	return mh
}
