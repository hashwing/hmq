package cluster_test

import (
	"testing"

	"github.com/hashwing/log"
	"github.com/hashwing/hmq/cluster"
)


func Test_Create(t *testing.T){
	event:=new(cluster.PeerEvent)
	event.NotifyLeave=func(name string){
		log.Info("node %s leave",name)
	}
	event.NotifyJoin=func(name string){
		log.Info("node %s join",name)
	}
	event.NotifyMsg =func(b []byte){
		log.Info("peer receive msg: ",string(b))
	}
	cfg:=cluster.DefaultPeerConfig()
	cfg.BindAddr="0.0.0.0:0"
	peer,err:=cluster.NewPeer(cfg,event)
	if err!=nil{
		t.Error(err)
		return
	}
	peer.Join()
	log.Info(peer.ClusterSize())
	//peer.WaitReady()

	event2:=new(cluster.PeerEvent)
	event2.NotifyMsg =func(b []byte){
		log.Info("peer2 receive msg: ",string(b))
	}
	cfg2:=cluster.DefaultPeerConfig()
	cfg2.BindAddr="0.0.0.0:0"
	cfg2.KnownPeers=[]string{peer.Self().Address()}
	peer2,err:=cluster.NewPeer(cfg2,event2)
	if err!=nil{
		t.Error(err)
		return
	}

	peer2.Join()
	log.Info(peer.ClusterSize())
	//peer.Leave(0)
	mc:=cluster.NewMsgChan("test",peer)
	mc.Broadcast([]byte("bcast test msg"))
	for _,node:=range peer.Members(){
		log.Debug(node.Name,len(peer.Members()))
		mc.SendTo(mc.CreateMsg([]string{node.Name},[]byte("peer: test msg")))
	}
	
	
	select{}
}