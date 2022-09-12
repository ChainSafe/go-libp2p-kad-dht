package providers

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// A providerSet has the list of providers and the time that they were added
// It is used as an intermediary data struct between what is stored in the datastore
// and the list of providers that get passed to the consumer of a .GetProviders call
type providerSet struct {
	providers      []peer.ID
	set            map[peer.ID]time.Time
	providerToKeys map[peer.ID][][]byte // TODO: maybe there's a more efficient way to do this
}

func newProviderSet() *providerSet {
	return &providerSet{
		providerToKeys: make(map[peer.ID][][]byte),
		set:            make(map[peer.ID]time.Time),
	}
}

// func (ps *providerSet) Add(p peer.ID, key []byte) {
// 	ps.setVal(p, key, time.Now())
// }

func (ps *providerSet) setVal(p peer.ID, key []byte, t time.Time) {
	_, found := ps.set[p]
	if !found {
		ps.providers = append(ps.providers, p)
	}

	keys, has := ps.providerToKeys[p]
	if !has {
		ps.providerToKeys[p] = [][]byte{key}
	} else {
		ps.providerToKeys[p] = append(keys, key)
	}

	ps.set[p] = t
}
