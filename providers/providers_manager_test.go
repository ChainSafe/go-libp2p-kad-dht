package providers

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	mh "github.com/multiformats/go-multihash"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	u "github.com/ipfs/go-ipfs-util"
	//
	// used by TestLargeProvidersSet: do not remove
	// lds "github.com/ipfs/go-ds-leveldb"
)

func TestProviderManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mid := peer.ID("testing")

	p, err := NewProviderManager(ctx, mid, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}
	a := u.Hash([]byte("test"))
	err = p.AddProvider(ctx, a, peer.ID("testingprovider"))
	if err != nil {
		t.Fatal(err)
	}

	// Not cached
	// TODO verify that cache is empty
	resp, _ := p.GetProviders(ctx, a)
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	// Cached
	// TODO verify that cache is populated
	resp, _ = p.GetProviders(ctx, a)
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	err = p.AddProvider(ctx, a, peer.ID("testingprovider2"))
	if err != nil {
		t.Fatal(err)
	}
	err = p.AddProvider(ctx, a, peer.ID("testingprovider3"))
	if err != nil {
		t.Fatal(err)
	}

	// TODO verify that cache is already up to date
	resp, _ = p.GetProviders(ctx, a)
	if len(resp) != 3 {
		t.Fatalf("Should have got 3 providers, got %d", len(resp))
	}

	p.proc.Close()
}

func TestProvidersDatastore(t *testing.T) {
	old := lruCacheSize
	lruCacheSize = 10
	defer func() { lruCacheSize = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mid := peer.ID("testing")

	p, err := NewProviderManager(ctx, mid, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.proc.Close()

	friend := peer.ID("friend")
	var mhs []mh.Multihash
	for i := 0; i < 100; i++ {
		h := u.Hash([]byte(fmt.Sprint(i)))
		mhs = append(mhs, h)
		err = p.AddProvider(ctx, h, friend)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range mhs {
		resp, _ := p.GetProviders(ctx, c)
		if len(resp) != 1 {
			t.Fatal("Could not retrieve provider.")
		}
		if resp[0] != friend {
			t.Fatal("expected provider to be 'friend'")
		}
	}
}

func TestProvidersSerialization(t *testing.T) {
	dstore := dssync.MutexWrap(ds.NewMapDatastore())

	k := u.Hash(([]byte("my key!")))
	p1 := peer.ID("peer one")
	p2 := peer.ID("peer two")
	pt1 := time.Now()
	pt2 := pt1.Add(time.Hour)

	err := writeProviderEntry(context.Background(), dstore, k, p1, pt1)
	if err != nil {
		t.Fatal(err)
	}

	err = writeProviderEntry(context.Background(), dstore, k, p2, pt2)
	if err != nil {
		t.Fatal(err)
	}

	pset, err := loadProviderSet(context.Background(), dstore, k)
	if err != nil {
		t.Fatal(err)
	}

	lt1, ok := pset.set[p1]
	if !ok {
		t.Fatal("failed to load set correctly")
	}

	if !pt1.Equal(lt1) {
		t.Fatalf("time wasnt serialized correctly, %v != %v", pt1, lt1)
	}

	lt2, ok := pset.set[p2]
	if !ok {
		t.Fatal("failed to load set correctly")
	}

	if !pt2.Equal(lt2) {
		t.Fatalf("time wasnt serialized correctly, %v != %v", pt1, lt1)
	}
}

func TestProvidesExpire(t *testing.T) {
	t.Skip("This test is flaky, see https://github.com/libp2p/go-libp2p-kad-dht/issues/725.")

	pval := ProvideValidity
	cleanup := defaultCleanupInterval
	ProvideValidity = time.Second / 2
	defaultCleanupInterval = time.Second / 2
	defer func() {
		ProvideValidity = pval
		defaultCleanupInterval = cleanup
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds := dssync.MutexWrap(ds.NewMapDatastore())
	mid := peer.ID("testing")

	p, err := NewProviderManager(ctx, mid, ds)
	if err != nil {
		t.Fatal(err)
	}

	peers := []peer.ID{"a", "b"}
	var mhs []mh.Multihash
	for i := 0; i < 10; i++ {
		h := u.Hash([]byte(fmt.Sprint(i)))
		mhs = append(mhs, h)
	}

	for _, h := range mhs[:5] {
		err = p.AddProvider(ctx, h, peers[0])
		if err != nil {
			t.Fatal(err)
		}
		err = p.AddProvider(ctx, h, peers[1])
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second / 4)

	for _, h := range mhs[5:] {
		err = p.AddProvider(ctx, h, peers[0])
		if err != nil {
			t.Fatal(err)
		}
		err = p.AddProvider(ctx, h, peers[1])
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range mhs {
		out, _ := p.GetProviders(ctx, h)
		if len(out) != 2 {
			t.Fatal("expected providers to still be there")
		}
	}

	time.Sleep(3 * time.Second / 8)

	for _, h := range mhs[:5] {
		out, _ := p.GetProviders(ctx, h)
		if len(out) > 0 {
			t.Fatal("expected providers to be cleaned up, got: ", out)
		}
	}

	for _, h := range mhs[5:] {
		out, _ := p.GetProviders(ctx, h)
		if len(out) != 2 {
			t.Fatal("expected providers to still be there")
		}
	}

	time.Sleep(time.Second / 2)

	// Stop to prevent data races
	p.Process().Close()

	if p.cache.Len() != 0 {
		t.Fatal("providers map not cleaned up")
	}

	res, err := ds.Query(context.Background(), dsq.Query{Prefix: ProvidersKeyPrefix})
	if err != nil {
		t.Fatal(err)
	}
	rest, err := res.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) > 0 {
		t.Fatal("expected everything to be cleaned out of the datastore")
	}
}

var _ = io.NopCloser
var _ = os.DevNull

// TestLargeProvidersSet can be used for profiling.
// The datastore can be switched to levelDB by uncommenting the section below and the import above
func TestLargeProvidersSet(t *testing.T) {
	t.Skip("This can be used for profiling. Skipping it for now to avoid incurring extra CI time")
	old := lruCacheSize
	lruCacheSize = 10
	defer func() { lruCacheSize = old }()

	dstore := ds.NewMapDatastore()

	//dirn, err := os.MkdirTemp("", "provtest")
	//	t.Fatal(err)
	// }
	//
	// opts := &lds.Options{
	//	NoSync:      true,
	//	Compression: 1,
	// }
	// lds, err := lds.NewDatastore(dirn, opts)
	// if err != nil {
	//	t.Fatal(err)
	// }
	// dstore = lds
	//
	// defer func() {
	//	os.RemoveAll(dirn)
	// }()

	ctx := context.Background()
	var peers []peer.ID
	for i := 0; i < 3000; i++ {
		peers = append(peers, peer.ID(fmt.Sprint(i)))
	}

	mid := peer.ID("myself")

	p, err := NewProviderManager(ctx, mid, dstore)
	if err != nil {
		t.Fatal(err)
	}
	defer p.proc.Close()

	var mhs []mh.Multihash
	for i := 0; i < 1000; i++ {
		h := u.Hash([]byte(fmt.Sprint(i)))
		mhs = append(mhs, h)
		for _, pid := range peers {
			err = p.AddProvider(ctx, h, pid)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 0; i < 5; i++ {
		start := time.Now()
		for _, h := range mhs {
			_, _ = p.GetProviders(ctx, h)
		}
		elapsed := time.Since(start)
		fmt.Printf("query %f ms\n", elapsed.Seconds()*1000)
	}
}

func TestUponCacheMissProvidersAreReadFromDatastore(t *testing.T) {
	old := lruCacheSize
	lruCacheSize = 1
	defer func() { lruCacheSize = old }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1, p2 := peer.ID("a"), peer.ID("b")
	h1 := u.Hash([]byte("1"))
	h2 := u.Hash([]byte("2"))

	pm, err := NewProviderManager(ctx, p1, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}

	// add provider
	err = pm.AddProvider(ctx, h1, p1)
	if err != nil {
		t.Fatal(err)
	}
	// make the cached provider for h1 go to datastore
	err = pm.AddProvider(ctx, h2, p1)
	if err != nil {
		t.Fatal(err)
	}
	// now just offloaded record should be brought back and joined with p2
	err = pm.AddProvider(ctx, h1, p2)
	if err != nil {
		t.Fatal(err)
	}

	h1Provs, _ := pm.GetProviders(ctx, h1)
	if len(h1Provs) != 2 {
		t.Fatalf("expected h1 to be provided by 2 peers, is by %d", len(h1Provs))
	}
}

func TestWriteUpdatesCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1, p2 := peer.ID("a"), peer.ID("b")
	h1 := u.Hash([]byte("1"))

	pm, err := NewProviderManager(ctx, p1, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}

	// add provider
	err = pm.AddProvider(ctx, h1, p1)
	if err != nil {
		t.Fatal(err)
	}
	// force into the cache
	_, err = pm.GetProviders(ctx, h1)
	if err != nil {
		t.Fatal(err)
	}
	// add a second provider
	err = pm.AddProvider(ctx, h1, p2)
	if err != nil {
		t.Fatal(err)
	}

	c1Provs, _ := pm.GetProviders(ctx, h1)
	if len(c1Provs) != 2 {
		t.Fatalf("expected h1 to be provided by 2 peers, is by %d", len(c1Provs))
	}
}

func TestPrefixesMatch(t *testing.T) {
	testCases := []struct {
		a, b     []byte
		bits     int
		expected bool
	}{
		{
			a:        []byte{},
			b:        []byte{},
			bits:     0,
			expected: true,
		},
		{
			a:        []byte{},
			b:        []byte{},
			bits:     1,
			expected: false,
		},
		{
			a:        []byte{0x1},
			b:        []byte{},
			bits:     1,
			expected: false,
		},
		{
			a:        []byte{0x1},
			b:        []byte{0x3},
			bits:     1,
			expected: true,
		},
		{
			a:        []byte{0x1},
			b:        []byte{0xff, 0xff},
			bits:     1,
			expected: true,
		},
		{
			a:        []byte{0x1},
			b:        []byte{0b11111110, 0xff},
			bits:     1,
			expected: false,
		},
		{
			a:        []byte{0x3},
			b:        []byte{0xff},
			bits:     2,
			expected: true,
		},
		{
			a:        []byte{0xff, 0x1},
			b:        []byte{0xff, 0xff},
			bits:     9,
			expected: true,
		},
		{
			a:        []byte{0xff, 0x0},
			b:        []byte{0xff, 0xff},
			bits:     9,
			expected: false,
		},
		{
			a:        []byte{0xff},
			b:        []byte{0xff},
			bits:     9,
			expected: false,
		},
		{
			a:        []byte{0xff, 0b01111111},
			b:        []byte{0xff, 0xff},
			bits:     15,
			expected: true,
		},
	}

	for i, tc := range testCases {
		res := prefixesMatch(tc.a, tc.b, tc.bits)
		require.Equal(t, tc.expected, res, fmt.Sprintf("case %d failed", i))
	}

}
