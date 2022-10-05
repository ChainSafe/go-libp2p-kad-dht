package internal

import (
	"github.com/multiformats/go-multihash"
)

func Sha256Multihash(mh multihash.Multihash) multihash.Multihash {
	prefix := []byte("CR_DOUBLEHASH")
	//dh := sha256.Sum256(append(prefix, mh...))
	mh, err := multihash.Sum(append(prefix, mh...), multihash.DBL_SHA2_256, 30)
	if err != nil {
		panic(err)
	}
	return mh
}
