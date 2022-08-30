package dht

import (
	"crypto/sha256"

	"github.com/multiformats/go-multihash"
)

type Hash [32]byte

func sha256Multihash(mh multihash.Multihash) Hash {
	return sha256.Sum256(mh)
}
