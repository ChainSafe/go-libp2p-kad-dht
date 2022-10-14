package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-base32"
)

// dhthandler specifies the signature of functions that handle DHT messages.
type dhtHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (dht *IpfsDHT) handlerForMsgType(t pb.Message_MessageType) dhtHandler {
	switch t {
	case pb.Message_FIND_NODE:
		return dht.handleFindPeer
	case pb.Message_PING:
		return dht.handlePing
	}

	if dht.enableValues {
		switch t {
		case pb.Message_GET_VALUE:
			return dht.handleGetValue
		case pb.Message_PUT_VALUE:
			return dht.handlePutValue
		}
	}

	if dht.enableProviders {
		switch t {
		case pb.Message_ADD_PROVIDER:
			return dht.handleAddProvider
		case pb.Message_GET_PROVIDERS:
			return dht.handleGetProviders
		}
	}

	return nil
}

func (dht *IpfsDHT) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	// first, is there even a key?
	k := pmes.GetKey()
	if len(k) == 0 {
		return nil, errors.New("handleGetValue but no key was provided")
	}

	// setup response
	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	rec, err := dht.checkLocalDatastore(ctx, k)
	if err != nil {
		return nil, err
	}
	resp.Record = rec

	// Find closest peer on given cluster to desired key and reply with that info
	closer := dht.betterPeersToQuery(pmes, p, dht.bucketSize)
	if len(closer) > 0 {
		resp.CloserPeers = pb.PeerIDsToPBPeers(dht.host.Network(), dht.peerstore, closer)
	}

	return resp, nil
}

func (dht *IpfsDHT) checkLocalDatastore(ctx context.Context, k []byte) (*recpb.Record, error) {
	logger.Debugf("%s handleGetValue looking into ds", dht.self)
	dskey := convertToDsKey(k)
	buf, err := dht.datastore.Get(ctx, dskey)
	logger.Debugf("%s handleGetValue looking into ds GOT %v", dht.self, buf)

	if err == ds.ErrNotFound {
		return nil, nil
	}

	// if we got an unexpected error, bail.
	if err != nil {
		return nil, err
	}

	// if we have the value, send it back
	logger.Debugf("%s handleGetValue success!", dht.self)

	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		logger.Debug("failed to unmarshal DHT record from datastore")
		return nil, err
	}

	var recordIsBad bool
	recvtime, err := u.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		logger.Info("either no receive time set on record, or it was invalid: ", err)
		recordIsBad = true
	}

	if time.Since(recvtime) > dht.maxRecordAge {
		logger.Debug("old record found, tossing.")
		recordIsBad = true
	}

	// NOTE: We do not verify the record here beyond checking these timestamps.
	// we put the burden of checking the records on the requester as checking a record
	// may be computationally expensive

	if recordIsBad {
		err := dht.datastore.Delete(ctx, dskey)
		if err != nil {
			logger.Error("Failed to delete bad record from datastore: ", err)
		}

		return nil, nil // can treat this as not having the record at all
	}

	return rec, nil
}

// Cleans the record (to avoid storing arbitrary data).
func cleanRecord(rec *recpb.Record) {
	rec.TimeReceived = ""
}

// Store a value in this peer local storage
func (dht *IpfsDHT) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	if len(pmes.GetKey()) == 0 {
		return nil, errors.New("handleGetValue but no key was provided")
	}

	rec := pmes.GetRecord()
	if rec == nil {
		logger.Debugw("got nil record from", "from", p)
		return nil, errors.New("nil record")
	}

	if !bytes.Equal(pmes.GetKey(), rec.GetKey()) {
		return nil, errors.New("put key doesn't match record key")
	}

	cleanRecord(rec)

	// Make sure the record is valid (not expired, valid signature etc)
	if err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		logger.Infow("bad dht record in PUT", "from", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()), "error", err)
		return nil, err
	}

	dskey := convertToDsKey(rec.GetKey())

	// fetch the striped lock for this key
	var indexForLock byte
	if len(rec.GetKey()) == 0 {
		indexForLock = 0
	} else {
		indexForLock = rec.GetKey()[len(rec.GetKey())-1]
	}
	lk := &dht.stripedPutLocks[indexForLock]
	lk.Lock()
	defer lk.Unlock()

	// Make sure the new record is "better" than the record we have locally.
	// This prevents a record with for example a lower sequence number from
	// overwriting a record with a higher sequence number.
	existing, err := dht.getRecordFromDatastore(ctx, dskey)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		recs := [][]byte{rec.GetValue(), existing.GetValue()}
		i, err := dht.Validator.Select(string(rec.GetKey()), recs)
		if err != nil {
			logger.Warnw("dht record passed validation but failed select", "from", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()), "error", err)
			return nil, err
		}
		if i != 0 {
			logger.Infow("DHT record in PUT older than existing record (ignoring)", "peer", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()))
			return nil, errors.New("old record")
		}
	}

	// record the time we receive every record
	rec.TimeReceived = u.FormatRFC3339(time.Now())

	data, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	err = dht.datastore.Put(ctx, dskey, data)
	return pmes, err
}

// returns nil, nil when either nothing is found or the value found doesn't properly validate.
// returns nil, some_error when there's a *datastore* error (i.e., something goes very wrong)
func (dht *IpfsDHT) getRecordFromDatastore(ctx context.Context, dskey ds.Key) (*recpb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		logger.Errorw("error retrieving record from datastore", "key", dskey, "error", err)
		return nil, err
	}
	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// Bad data in datastore, log it but don't return an error, we'll just overwrite it
		logger.Errorw("failed to unmarshal record from datastore", "key", dskey, "error", err)
		return nil, nil
	}

	err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// Invalid record in datastore, probably expired but don't return an error,
		// we'll just overwrite it
		logger.Debugw("local record verify failed", "key", rec.GetKey(), "error", err)
		return nil, nil
	}

	return rec, nil
}

func (dht *IpfsDHT) handlePing(_ context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	logger.Debugf("%s Responding to ping from %s!\n", dht.self, p)
	return pmes, nil
}

func (dht *IpfsDHT) handleFindPeer(ctx context.Context, from peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())
	var closest []peer.ID

	if len(pmes.GetKey()) == 0 {
		return nil, fmt.Errorf("handleFindPeer with empty key")
	}

	// if looking for self... special case where we send it on CloserPeers.
	targetPid := peer.ID(pmes.GetKey())
	if targetPid == dht.self {
		closest = []peer.ID{dht.self}
	} else {
		closest = dht.betterPeersToQuery(pmes, from, dht.bucketSize)

		// Never tell a peer about itself.
		if targetPid != from {
			// Add the target peer to the set of closest peers if
			// not already present in our routing table.
			//
			// Later, when we lookup known addresses for all peers
			// in this set, we'll prune this peer if we don't
			// _actually_ know where it is.
			found := false
			for _, p := range closest {
				if targetPid == p {
					found = true
					break
				}
			}
			if !found {
				closest = append(closest, targetPid)
			}
		}
	}

	if closest == nil {
		return resp, nil
	}

	withAddresses := make([]peer.AddrInfo, 0, len(closest))
	for _, p := range closest {
		addrInfo := dht.peerstore.PeerInfo(p)
		if len(addrInfo.Addrs) > 0 {
			withAddresses = append(withAddresses, addrInfo)
		}
	}
	resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), withAddresses)
	return resp, nil
}

func (dht *IpfsDHT) handleGetProviders(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, fmt.Errorf("handleGetProviders key size too large")
	} else if len(key) == 0 {
		return nil, fmt.Errorf("handleGetProviders key is empty")
	}

	resp := pb.NewMessage(pmes.GetType(), key, pmes.GetClusterLevel())

	if len(key) < 32 {
		// since the datastore lookup is already by prefix, the only difference is that this call
		// also returns the keys that the peers provide.
		provsToKeys, err := dht.providerStore.GetProvidersForPrefix(ctx, key)
		if err != nil {
			return nil, err
		}

		resp.ProviderPeers = pb.PeerIDsToPBPeersWithKeys(dht.host.Network(), dht.peerstore, provsToKeys)
	} else {
		// setup providers
		providers, err := dht.providerStore.GetProviders(ctx, key)
		if err != nil {
			return nil, err
		}
		resp.ProviderPeers = pb.PeerIDsToPBPeers(dht.host.Network(), dht.peerstore, providers)
	}

	if len(resp.ProviderPeers) > 0 {
		logger.Infof("%s got providers for lookup request for key %s",
			dht.self,
			b58.Encode(key))

		for _, prov := range resp.ProviderPeers {
			logger.Infof("provider peer ID (encrypted): %s", b58.Encode([]byte(prov.Id)))
		}
	}

	// Also send closer peers.
	closer := dht.betterPeersToQuery(pmes, p, dht.bucketSize)
	if closer != nil {
		resp.CloserPeers = pb.PeerIDsToPBPeers(dht.host.Network(), dht.peerstore, closer)
	}

	if len(resp.ProviderPeers) == 0 {
		logger.Infof("%s got no providers for key %s, sending %d closer peers",
			dht.self,
			b58.Encode(key),
			len(closer),
		)
	}

	return resp, nil
}

func (dht *IpfsDHT) handleAddProvider(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, fmt.Errorf("handleAddProvider key size too large")
	} else if len(key) == 0 {
		return nil, fmt.Errorf("handleAddProvider key is empty")
	}

	logger.Debugw("adding provider", "from", p, "key", internal.LoggableProviderRecordBytes(key))

	// add provider should use the address given in the message
	provs := pmes.GetProviderPeers()
	pinfos := pb.PBPeersToPeerInfos(provs)
	for i, pi := range pinfos {
		sig := provs[i].Signature
		pub, err := crypto.PublicKeyFromProto(provs[i].PublicKey)
		if err != nil {
			logger.Debugw("failed to unmarshal public key", "from", p, "peer", pi.ID, "error", err)
			continue
		}

		// verify that public key corresponds to sender peer ID
		id, err := peer.IDFromPublicKey(pub)
		if err != nil {
			logger.Debugw("failed to derive peer ID from public key", "from", p, "peer", pi.ID, "error", err)
			continue
		}

		if id != p {
			logger.Debugw("remote peer ID does not match public key's peer ID", "from", p, "peer", pi.ID, "error", err)
			continue
		}

		ok, err := pub.Verify(append(key, pi.ID...), sig)
		if err != nil {
			logger.Debugw("failed to verify signature", "from", p, "peer", pi.ID, "error", err)
			continue
		}

		if !ok {
			// we should ignore this provider record! not from originator.
			// (we should sign them and check signature later...)
			logger.Debugw("received provider from wrong peer", "from", p, "peer", pi.ID)
			continue
		}

		if len(pi.Addrs) < 1 {
			logger.Debugw("no valid addresses for provider", "from", p)
			continue
		}

		logger.Infof("%s adding provider %s for key %s",
			dht.self,
			pi.ID,
			b58.Encode(key),
		)

		err = dht.providerStore.AddProvider(ctx, key, pi.ID)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func convertToDsKey(s []byte) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString(s))
}
