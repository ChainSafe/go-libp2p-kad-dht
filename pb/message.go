package dht_pb

import (
	"bytes"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	logging "github.com/ipfs/go-log"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("dht.pb")

type PeerRoutingInfo struct {
	peer.AddrInfo
	network.Connectedness
}

// NewMessage constructs a new dht message with given type, key, and level
func NewMessage(typ Message_MessageType, key []byte, level int) *Message {
	m := &Message{
		Type: typ,
		Key: &Message_Key{
			Key: key,
		},
	}
	m.SetClusterLevel(level)
	return m
}

// NewGetProvidersMessage constructs a new dht message with given type, key, prefixBitLength and level
func NewGetProvidersMessage(typ Message_MessageType, key []byte, prefixBitLength int, level int) *Message {
	m := &Message{
		Type: typ,
		Key: &Message_Key{
			Key:             key,
			PrefixBitLength: uint32(prefixBitLength),
		},
	}
	m.SetClusterLevel(level)
	return m
}

func peerRoutingInfoToPBPeer(p PeerRoutingInfo) Message_Peer {
	var pbp Message_Peer

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	pbp.Id = byteString(p.ID)
	pbp.Connection = ConnectionType(p.Connectedness)
	return pbp
}

func peerInfoToPBPeer(p peer.AddrInfo) Message_Peer {
	var pbp Message_Peer

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	pbp.Id = byteString(p.ID)
	return pbp
}

// PBPeerToPeer turns a *Message_Peer into its peer.AddrInfo counterpart
func PBPeerToPeerInfo(pbp Message_Peer) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    peer.ID(pbp.Id),
		Addrs: pbp.Addresses(),
	}
}

func PeersToPeersWithKey(in []Message_Peer) []Message_PeerWithKey {
	pbpeers := make([]Message_PeerWithKey, len(in))
	for i, p := range in {
		pbpeers[i] = Message_PeerWithKey{
			Id:         p.Id,
			Addrs:      p.Addrs,
			Connection: p.Connection,
		}
	}
	return pbpeers
}

// RawPeerInfosToPBPeers converts a slice of Peers into a slice of *Message_Peers,
// ready to go out on the wire.
func RawPeerInfosToPBPeers(peers []peer.AddrInfo) []Message_Peer {
	pbpeers := make([]Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerInfoToPBPeer(p)
	}
	return pbpeers
}

// PeersToPBPeers converts given []peer.Peer into a set of []*Message_Peer,
// which can be written to a message and sent out. the key thing this function
// does (in addition to PeersToPBPeers) is set the ConnectionType with
// information from the given network.Network.
func PeerInfosToPBPeers(n network.Network, peers []peer.AddrInfo) []Message_Peer {
	pbps := RawPeerInfosToPBPeers(peers)
	for i, pbp := range pbps {
		c := ConnectionType(n.Connectedness(peers[i].ID))
		pbp.Connection = c
	}
	return pbps
}

// PeerIDsToPBPeers converts given []peer.Peer into a set of []Message_Peer,
// which can be written to a message and sent out. the key thing this function
// does (in addition to PeersToPBPeers) is set the ConnectionType with
// information from the given network.Network.
func PeerIDsToPBPeers(n network.Network, ps peerstore.Peerstore, provs []peer.ID) []Message_Peer {
	pbps := make([]Message_Peer, 0, len(provs))
	for _, p := range provs {
		if len(p) == 0 {
			continue
		}
		addrInfo := ps.PeerInfo(p)
		pbp := peerInfoToPBPeer(addrInfo)
		c := ConnectionType(n.Connectedness(p))
		pbp.Connection = c
		pbps = append(pbps, pbp)
	}
	return pbps
}

func peerInfoToPBPeerWithKey(p peer.AddrInfo, key []byte) Message_PeerWithKey {
	var pbp Message_PeerWithKey

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	pbp.Id = byteString(p.ID)
	pbp.Key = key
	return pbp
}

// KeyToProvsToPB converts a map of keys to list of peer IDs to a slice of Message_PeerWithKey.
func KeyToProvsToPB(n network.Network, ps peerstore.Peerstore, keyToProvs map[string][]peer.ID) []Message_PeerWithKey {
	res := []Message_PeerWithKey{}

	for key, peers := range keyToProvs {
		for _, p := range peers {
			addrInfo := ps.PeerInfo(p)
			pbp := peerInfoToPBPeerWithKey(addrInfo, []byte(key))
			c := ConnectionType(n.Connectedness(p))
			pbp.Connection = c
			res = append(res, pbp)
		}
	}

	return res
}

// PBPeersToAddrInfos converts list of Message_PeerWithKey to a list of peer.AddrInfo.
func PBPeersToAddrInfos(pbm []Message_PeerWithKey) []*peer.AddrInfo {
	res := []*peer.AddrInfo{}

	for _, mp := range pbm {
		addrInfo := PBPeerToPeerInfo(Message_Peer{
			Id:         mp.Id,
			Addrs:      mp.Addrs,
			Connection: mp.Connection,
		})
		res = append(res, &addrInfo)
	}

	return res
}

// PBPeersToAddrInfos converts list of Message_PeerWithKey to a list of peer.AddrInfo that have the given key.
func PBPeersWithKeyToAddrInfos(pbm []Message_PeerWithKey, key []byte) []*peer.AddrInfo {
	res := []*peer.AddrInfo{}

	for _, mp := range pbm {
		if !bytes.Equal(mp.Key, key) {
			continue
		}

		addrInfo := PBPeerToPeerInfo(Message_Peer{
			Id:         mp.Id,
			Addrs:      mp.Addrs,
			Connection: mp.Connection,
		})
		res = append(res, &addrInfo)
	}

	return res
}

func PeerRoutingInfosToPBPeers(peers []PeerRoutingInfo) []Message_Peer {
	pbpeers := make([]Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerRoutingInfoToPBPeer(p)
	}
	return pbpeers
}

// PBPeersToPeerInfos converts given []*Message_Peer into []*peer.AddrInfo
// Invalid addresses will be silently omitted.
func PBPeersToPeerInfos(pbps []Message_Peer) []*peer.AddrInfo {
	peers := make([]*peer.AddrInfo, 0, len(pbps))
	for _, pbp := range pbps {
		ai := PBPeerToPeerInfo(pbp)
		peers = append(peers, &ai)
	}
	return peers
}

// Addresses returns a multiaddr associated with the Message_Peer entry
func (m *Message_Peer) Addresses() []ma.Multiaddr {
	if m == nil {
		return nil
	}

	maddrs := make([]ma.Multiaddr, 0, len(m.Addrs))
	for _, addr := range m.Addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			log.Debugw("error decoding multiaddr for peer", "peer", peer.ID(m.Id), "error", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}
	return maddrs
}

// GetClusterLevel gets and adjusts the cluster level on the message.
// a +/- 1 adjustment is needed to distinguish a valid first level (1) and
// default "no value" protobuf behavior (0)
func (m *Message) GetClusterLevel() int {
	level := m.GetClusterLevelRaw() - 1
	if level < 0 {
		return 0
	}
	return int(level)
}

// SetClusterLevel adjusts and sets the cluster level on the message.
// a +/- 1 adjustment is needed to distinguish a valid first level (1) and
// default "no value" protobuf behavior (0)
func (m *Message) SetClusterLevel(level int) {
	lvl := int32(level)
	m.ClusterLevelRaw = lvl + 1
}

// ConnectionType returns a Message_ConnectionType associated with the
// network.Connectedness.
func ConnectionType(c network.Connectedness) Message_ConnectionType {
	switch c {
	default:
		return Message_NOT_CONNECTED
	case network.NotConnected:
		return Message_NOT_CONNECTED
	case network.Connected:
		return Message_CONNECTED
	case network.CanConnect:
		return Message_CAN_CONNECT
	case network.CannotConnect:
		return Message_CANNOT_CONNECT
	}
}

// Connectedness returns an network.Connectedness associated with the
// Message_ConnectionType.
func Connectedness(c Message_ConnectionType) network.Connectedness {
	switch c {
	default:
		return network.NotConnected
	case Message_NOT_CONNECTED:
		return network.NotConnected
	case Message_CONNECTED:
		return network.Connected
	case Message_CAN_CONNECT:
		return network.CanConnect
	case Message_CANNOT_CONNECT:
		return network.CannotConnect
	}
}
