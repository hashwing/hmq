package cluster


import (
	"net"
	"fmt"
	"time"
	"strings"
	"context"

	"github.com/hashwing/log"
	"github.com/satori/go.uuid"
)

// NodeStatus is the state of a peer
type NodeStatus int

const (
	StatusNone NodeStatus = iota
	StatusAlive
	StatusFailed
)


// Node a peer detail
type Node struct{
	Name		string
	Addr 		string
	Port		int
	Status 		NodeStatus
	LeaveTime 	time.Time
}

func (n *Node)Address()string{
	return fmt.Sprintf("%s:%v",n.Addr,n.Port)
}





func newPeerName()string{
	name:=uuid.NewV4().String()
	name=strings.Replace(name,"-","",-1)
	return name
}


// log
type logWriter struct{
}

func (l *logWriter)Write(b []byte)(int,error){
	log.Debug(string(b))
	return len(b),nil
}

func resolvePeers(ctx context.Context, peers []string, myAddress string, res net.Resolver, waitIfEmpty bool) ([]string, error) {
	var resolvedPeers []string

	for _, peer := range peers {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return nil, fmt.Errorf("split host/port for peer %s,error:%v", peer,err)
		}

		retryCtx, cancel := context.WithCancel(ctx)

		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			// Assume direct address.
			resolvedPeers = append(resolvedPeers, peer)
			continue
		}

		if len(ips) == 0 {
			var lookupErrSpotted bool

			err := retry(2*time.Second, retryCtx.Done(), func() error {
				if lookupErrSpotted {
					// We need to invoke cancel in next run of retry when lookupErrSpotted to preserve LookupIPAddr error.
					cancel()
				}

				ips, err = res.LookupIPAddr(retryCtx, host)
				if err != nil {
					lookupErrSpotted = true
					return fmt.Errorf("IP Addr lookup for peer %s, error:%v", peer,err)
				}

				ips = removeMyAddr(ips, port, myAddress)
				if len(ips) == 0 {
					if !waitIfEmpty {
						return nil
					}
					return fmt.Errorf("empty IPAddr result. Retrying")
				}

				return nil
			})
			if err != nil {
				return nil, err
			}
		}

		for _, ip := range ips {
			resolvedPeers = append(resolvedPeers, net.JoinHostPort(ip.String(), port))
		}
	}

	return resolvedPeers, nil
}

// retry executes f every interval seconds until timeout or no error is returned from f.
func retry(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	var err error
	for {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-stopc:
			return err
		case <-tick.C:
		}
	}
}
