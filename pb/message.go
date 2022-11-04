package dht_pb

import (
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

// KeyToProvsToPB converts a map of keys to list of peer IDs to an equivalent PB map.
func KeyToProvsToPB(n network.Network, ps peerstore.Peerstore, keyToProvs map[string][]peer.ID) map[string]*Message_Providers {
	res := make(map[string]*Message_Providers)

	for key, peers := range keyToProvs {
		mps := &Message_Providers{
			Peers: []*Message_Peer{},
		}

		for _, p := range peers {
			addrInfo := ps.PeerInfo(p)
			pbp := peerInfoToPBPeer(addrInfo)
			c := ConnectionType(n.Connectedness(p))
			pbp.Connection = c
			mps.Peers = append(mps.Peers, &pbp)
		}

		res[key] = mps
	}
	return res
}

// PBToKeyToProvs converts a map of keys to Message_Providers to an equivalent map of keys to a list of AddrInfo.
func PBToKeyToProvs(pbm map[string]*Message_Providers) map[string][]*peer.AddrInfo {
	res := make(map[string][]*peer.AddrInfo)

	for key, mps := range pbm {
		addrInfos := []*peer.AddrInfo{}

		for _, p := range mps.Peers {
			addrInfo := PBPeerToPeerInfo(*p)
			addrInfos = append(addrInfos, &addrInfo)
		}

		res[key] = addrInfos
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
