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
	keyToProviders map[string][]peer.ID
}

func newProviderSet() *providerSet {
	return &providerSet{
		keyToProviders: make(map[string][]peer.ID),
		set:            make(map[peer.ID]time.Time),
	}
}

func (ps *providerSet) setVal(p peer.ID, key []byte, t time.Time) {
	_, found := ps.set[p]
	if !found {
		ps.providers = append(ps.providers, p)
	}

	provs, has := ps.keyToProviders[string(key)]
	if !has {
		ps.keyToProviders[string(key)] = []peer.ID{p}
	} else {
		ps.keyToProviders[string(key)] = append(provs, p)
	}

	ps.set[p] = t
}
