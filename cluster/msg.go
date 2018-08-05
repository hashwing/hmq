package cluster

import (
	"sync"

	"github.com/hashwing/log"
	"github.com/hashicorp/memberlist"
)

// MsgChan msg channel
type MsgChan struct{
	key   string
	peer *Peer
	msgc  chan *Msg
	stopc chan bool
}

// Msg msg to send
type Msg struct{
	nodes []*memberlist.Node
	data []byte
}


// a simple broadcast
type simpleBroadcast []byte

func (b simpleBroadcast) Message() []byte { 
	return []byte(b)
 }
func (b simpleBroadcast) Invalidates(memberlist.Broadcast) bool {
	 return false
 }
func (b simpleBroadcast) Finished(){}  

// NewMsgChan new msg chan
func NewMsgChan(key string,peer *Peer)*MsgChan{
	mc:=&MsgChan{
		key:key,
		peer:peer,
		stopc:make(chan bool),
		msgc:make(chan *Msg),
	}
	go mc.handelMessages()
	return mc
}

// handelMessages handel send msg to nodes
func (mc *MsgChan) handelMessages() {
	var wg sync.WaitGroup
	for {
		select {
		case msg := <-mc.msgc:
			for _, n := range msg.nodes {
				wg.Add(1)
				go func(n *memberlist.Node) {
					defer wg.Done()
					if err :=mc.peer.mlist.SendReliable(n, msg.data); err != nil {
						log.Debug("failed to send reliable key %s node %s error: %v", mc.key, n.Name, err)
						return
					}
				}(n)
			}

			wg.Wait()
		case <-mc.stopc:
			return
		case <-mc.peer.stopc:
			return
		}
	}
}

// Broadcast broascast to every node
func (mc *MsgChan)Broadcast(b []byte){
	if len(b)>mc.peer.cfg.MaxGossipPacketSize/2{
		msg :=&Msg{
			nodes:mc.allPeers(),
			data:b,
		}
		select {
		case mc.msgc <- msg:
		default:
			log.Debug("oversized gossip channel full")
		}
	} else {
		log.Debug("start bcast")
		mc.peer.delegate.bcast.QueueBroadcast(simpleBroadcast(b))
	}
}

// SendTo send msg to nodes
func (mc *MsgChan)SendTo(msg *Msg){
	mc.msgc<-msg
}

func (mc *MsgChan)Close(){
	close(mc.stopc)
}

func (mc *MsgChan)CreateMsg(names []string,data []byte)*Msg{
	nodes:=make([]*memberlist.Node,0)
	for _,name:=range names{
		for _,n:=range mc.peer.mlist.Members(){
			if n.Name==name{
				nodes=append(nodes,n)
			}
		}
	}
	return &Msg{nodes:nodes,data:data}
}

func (mc *MsgChan)allPeers()[]*memberlist.Node{
	nodes:=mc.peer.mlist.Members()
	for i, n := range nodes {
		if n.Name == mc.peer.Self().Name {
			nodes = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}
	return nodes
}

