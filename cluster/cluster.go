package cluster

import (
	"strings"
	"fmt"
	"net"
	"time"
	"sync"
	"context"
	"strconv"

	"github.com/hashwing/log"
	
	"github.com/hashicorp/memberlist"
)

// Peer a peer in gossip cluster
type Peer struct{
	mlist *memberlist.Memberlist
	resolvedPeers 	[]string

	stopc  			chan bool
	readyc 			chan bool

	peerLock    	sync.RWMutex
	failedNodes 	[]Node
	nodes			map[string]Node
	delegate		*delegate

	reconnCounter   int
	cfg 			*PeerConfig
	event			*PeerEvent
}

type PeerEvent struct {
	NotifyMsg 			func([]byte)
	LocalState			func(join bool) []byte
	MergeRemoteState 	func(buf []byte, join bool)
	NotifyJoin 			func(string)
	NotifyLeave 		func(string)
	NotifyUpdate 		func(string)
}

// PeerConfig 
type PeerConfig struct {
	BindAddr   			string
	AdvertiseAddr		string
	KnownPeers			[]string
	WaitIfEmpty			bool
	GossipInv  			time.Duration
	PushPullInv 		time.Duration
	TcpTimeout			time.Duration
	ProbeTimeout		time.Duration
	ProbeInv			time.Duration
	ReconnectInterval 	time.Duration
	ReconnectTimeout  	time.Duration
	MaxGossipPacketSize	int
}

// DefaultPeerConfig get default PeerConfig
func DefaultPeerConfig()*PeerConfig{
	return &PeerConfig{
		BindAddr:"0.0.0.0:8888",
		AdvertiseAddr:"",
		KnownPeers:[]string{},
		WaitIfEmpty: true,
		GossipInv:200 * time.Millisecond,
		PushPullInv:60 * time.Second,
		TcpTimeout:10 * time.Second,
		ProbeTimeout:500 * time.Millisecond,
		ProbeInv:1 * time.Second,
		ReconnectInterval:10 * time.Second,
		ReconnectTimeout:6 * time.Hour,
		MaxGossipPacketSize:1400,
	}
}
// NewPeer new a peer
func NewPeer(pcfg *PeerConfig,event *PeerEvent)(*Peer,error){
	// 监听地址和端口
	bindHost,portStr,err:=net.SplitHostPort(pcfg.BindAddr)
	if err!=nil{
		return nil,err
	}
	bindPort,err:=strconv.Atoi(portStr)
	if err!=nil{
		return nil,err
	}

	// 广播地址和端口
	var advertiseHost string
	var advertisePort int
	if pcfg.AdvertiseAddr != "" {
		var advertisePortStr string
		advertiseHost, advertisePortStr, err = net.SplitHostPort( pcfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid advertise address %v", err)
		}
		advertisePort, err = strconv.Atoi(advertisePortStr)
		if err != nil {
			return nil,  fmt.Errorf("invalid advertise address, wrong port %v",err)
		}
	}

	// 解析同伴（peer）地址
	resolvedPeers, err := resolvePeers(context.Background(), pcfg.KnownPeers, pcfg.AdvertiseAddr, net.Resolver{}, pcfg.WaitIfEmpty)
	if err != nil {
		return nil, fmt.Errorf("resolve peers %v",err)
	}

	log.Debug("resolved peers to following addresses %s",strings.Join(pcfg.KnownPeers,","))

	// 初始化用户的指定的广播地址
	addr, err := calculateAdvertiseAddress(bindHost, advertiseHost)
	if err != nil {
		log.Warn("couldn't deduce an advertise address: %v",err)
	} else if hasNonlocal(resolvedPeers) && isUnroutable(addr.String()) {
		log.Warn("this node advertises itself on an unroutable address,this node will be unreachable in the cluster")
	} else if isAny(pcfg.BindAddr) && advertiseHost == "" {
		// memberlist doesn't advertise properly when the bind address is empty or unspecified.
		log.Info("setting advertise address explicitly addr %s:%v", addr.String(), bindPort)
		advertiseHost = addr.String()
		advertisePort = bindPort
	}
	p := &Peer{
		stopc:         make(chan bool),
		readyc:        make(chan bool),
		nodes:         map[string]Node{},    
		resolvedPeers: resolvedPeers,
		event: event,
		cfg:pcfg,
	}
	

	// 设置重传 
	retransmit:= len(pcfg.KnownPeers)/2
	if retransmit<3{
		retransmit = 3
	}

	p.delegate=newDelegate(p,retransmit)

	// 配置 memberlist
	cfg := memberlist.DefaultLANConfig()
	cfg.Delegate=p.delegate
	cfg.Events=p.delegate
	cfg.Name=newPeerName()
	cfg.BindAddr=bindHost
	cfg.BindPort=bindPort
	cfg.GossipInterval=pcfg.GossipInv
	cfg.PushPullInterval=pcfg.PushPullInv
	cfg.TCPTimeout=pcfg.TcpTimeout
	cfg.ProbeTimeout = pcfg.ProbeTimeout
	cfg.ProbeInterval = pcfg.ProbeInv
	cfg.GossipNodes = retransmit
	cfg.LogOutput = &logWriter{} 
	cfg.UDPBufferSize = pcfg.MaxGossipPacketSize
	if advertiseHost!=""{
		cfg.AdvertiseAddr = advertiseHost
		cfg.AdvertisePort = advertisePort
		p.setInitialFailed(resolvedPeers, fmt.Sprintf("%s:%d", advertiseHost, advertisePort))
	}else{
		p.setInitialFailed(resolvedPeers,pcfg.BindAddr)
	}
	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("create memberlist error: %v",err)
	}
	p.mlist = ml
	return p, nil
}


// Join  peer join cluster
func (p *Peer) Join(){
	n,err:=p.mlist.Join(p.resolvedPeers)
	if err!=nil{
		log.Warn("fail to jion cluster:",err)
		if p.cfg.ReconnectInterval!=0{
			log.Info("retry joining cluster every %s",p.cfg.ReconnectInterval.String())
		}
	}else{
		log.Debug("success to jion cluster, peers:",n)
	}

	if p.cfg.ReconnectInterval!=0{
		go p.handleReconnect( p.cfg.ReconnectInterval)
	}

	if p.cfg.ReconnectTimeout!=0{
		go p.handleReconnectTimeout( p.cfg.ReconnectInterval, p.cfg.ReconnectTimeout)
	}

}

// Leave the cluster, waiting up to timeout.
func (p *Peer) Leave(timeout time.Duration) error {
	close(p.stopc)
	log.Info("leaving cluster")
	return p.mlist.Leave(timeout)
}

// Wait until Settle() has finished.
func (p *Peer) WaitReady() {
	<-p.readyc
}

func (p *Peer)ClusterSize()int{
	return len(p.mlist.Members())
}

func (p *Peer)Self()*Node{
	n:=p.mlist.LocalNode()
	return &Node{
		Name:n.Name,
		Addr:n.Addr.String(),
		Port:int(n.Port),
	}
}

func (p *Peer)Members()map[string]*Node{
	nodes:=make(map[string]*Node)
	for _,n:=range p.mlist.Members(){
		nodes[n.Name]=&Node{
			Name:n.Name,
			Addr:n.Addr.String(),
			Port:int(n.Port),
		}
	}
	return nodes
}


// handleReconnect peer reconnect when failto  join cluser 
func (p *Peer) handleReconnect(d time.Duration) {
	tick := time.NewTicker(d)
	defer tick.Stop()

	for {
		select {
		case <-p.stopc:
			return
		case <-tick.C:
			p.reconnect()
		}
	}
}

// handleReconnectTimeout peer reconnect when failto  join cluser in timeout period
func (p *Peer) handleReconnectTimeout(d time.Duration, timeout time.Duration) {
	tick := time.NewTicker(d)
	defer tick.Stop()

	for {
		select {
		case <-p.stopc:
			return
		case <-tick.C:
			p.removeFailedPeers(timeout)
		}
	}
}

func (p *Peer) removeFailedPeers(timeout time.Duration) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()

	now := time.Now()

	keep := make([]Node, 0, len(p.failedNodes))
	for _, pr := range p.failedNodes {
		if pr.LeaveTime.Add(timeout).After(now) {
			keep = append(keep, pr)
		} else {
			log.Debug("failed peer had timed out, addr: %s", pr.Address())
			delete(p.nodes, pr.Name)
		}
	}

	p.failedNodes = keep
}

// reconnect when fail to join cluster 
func (p *Peer) reconnect() {
	p.peerLock.RLock()
	failedNodes := p.failedNodes
	p.peerLock.RUnlock()

	for _, pr := range failedNodes {
		if _, err := p.mlist.Join([]string{pr.Address()}); err != nil {
			log.Info("reconnect failure, addr %s", pr.Address())
		} else {
			log.Info("reconnect success, addr %s", pr.Address())
		}
	}
}



// All peers are initially added to the failed list. They will be removed from
// this list in peerJoin when making their initial connection.
func (p *Peer) setInitialFailed(peers []string, myAddr string) {
	if len(peers) == 0 {
		return
	}

	p.peerLock.RLock()
	defer p.peerLock.RUnlock()

	now := time.Now()
	for _, peerAddr := range peers {
		if peerAddr == myAddr {
			// Don't add ourselves to the initially failing list,
			// we don't connect to ourselves.
			continue
		}
		host, port, err := net.SplitHostPort(peerAddr)
		if err != nil {
			continue
		}
		ip := net.ParseIP(host)
		if ip == nil {
			// Don't add textual addresses since memberlist only advertises
			// dotted decimal or IPv6 addresses.
			continue
		}
		portInt, err := strconv.ParseInt(port, 10, 16)
		if err != nil {
			continue
		}

		pr := Node{
			Status:    StatusFailed,
			LeaveTime: now,
			Addr: host,
			Port: int(portInt),
		}
		p.failedNodes = append(p.failedNodes, pr)
		p.nodes[peerAddr] = pr
	}
}