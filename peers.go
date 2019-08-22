// peers
// author: baoqiang
// time: 2019-08-22 20:35
package groupcache

import (
	pb "github.com/githubao/xiao-groupcache/groupcachepb"
)

type Context interface{}

type ProtoGetter interface {
	Get(context Context, in *pb.GetRequest, out *pb.GetResponse) error
}

type PeerPicker interface {
	PickPeer(key string) (peer ProtoGetter, ok bool)
}

// no peer impl PeerPicker
type NoPeers struct{}

func (NoPeers) PickPeer(key string) (peer ProtoGetter, ok bool) {
	return
}

// a func type for get PeerPicker
var (
	portPicker func(groupName string) PeerPicker
)

func RegisterPeerPicker(fn func() PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = func(_ string) PeerPicker {
		return fn()
	}
}

func getPeers(groupName string) PeerPicker {
	if portPicker == nil {
		return NoPeers{}
	}

	pk := portPicker(groupName)
	if pk == nil {
		pk = NoPeers{}
	}

	return pk
}
