package cluster

import (
	"github.com/hashicorp/memberlist"
)

type delegate struct {
	bcast   *memberlist.TransmitLimitedQueue
	peer	*Peer
}

func newDelegate(p *Peer,retransmit int)*delegate{
	bcast:=&memberlist.TransmitLimitedQueue{
		NumNodes:p.ClusterSize,
		RetransmitMult:retransmit,
	}
	d:=&delegate{
		bcast: bcast,
		peer:p,
	}
	return d
}


//NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message.
func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (d *delegate)NotifyMsg(b []byte){
	if d.peer.event.NotifyMsg!=nil{
		d.peer.event.NotifyMsg(b)
	}
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (d *delegate)GetBroadcasts(overhead, limit int) [][]byte{
	return d.bcast.GetBroadcasts(overhead,limit)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate)LocalState(join bool) []byte{
	if d.peer.event.LocalState!=nil{
		return d.peer.event.LocalState(join)
	}
  return []byte{}
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate)MergeRemoteState(buf []byte, join bool){
	if d.peer.event.MergeRemoteState!=nil{
		d.peer.event.MergeRemoteState(buf,join)
	}
}

 // NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (d *delegate)NotifyJoin(n *memberlist.Node){
	if d.peer.event.NotifyJoin!=nil{
		d.peer.event.NotifyJoin(n.Name)
	}
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (d *delegate)NotifyLeave(n *memberlist.Node){
	if d.peer.event.NotifyLeave!=nil{
		d.peer.event.NotifyLeave(n.Name)
	}
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (d *delegate)NotifyUpdate(n *memberlist.Node){
	if d.peer.event.NotifyUpdate!=nil{
		d.peer.event.NotifyUpdate(n.Name)
	}
}