package internal

import (
	"crypto/sha256"

	"github.com/multiformats/go-multihash"
)

type Hash [32]byte

func Sha256Multihash(mh multihash.Multihash) Hash {
	return sha256.Sum256(mh)
}
