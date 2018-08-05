package cluster

import (
	"net"
	"fmt"
	"strings"

	"github.com/hashicorp/go-sockaddr"
)


func removeMyAddr(ips []net.IPAddr, targetPort string, myAddr string) []net.IPAddr {
	var result []net.IPAddr

	for _, ip := range ips {
		if net.JoinHostPort(ip.String(), targetPort) == myAddr {
			continue
		}
		result = append(result, ip)
	}

	return result
}

func hasNonlocal(clusterPeers []string) bool {
	for _, peer := range clusterPeers {
		if host, _, err := net.SplitHostPort(peer); err == nil {
			peer = host
		}
		if ip := net.ParseIP(peer); ip != nil && !ip.IsLoopback() {
			return true
		} else if ip == nil && strings.ToLower(peer) != "localhost" {
			return true
		}
	}
	return false
}

func isUnroutable(addr string) bool {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	if ip := net.ParseIP(addr); ip != nil && (ip.IsUnspecified() || ip.IsLoopback()) {
		return true // typically 0.0.0.0 or localhost
	} else if ip == nil && strings.ToLower(addr) == "localhost" {
		return true
	}
	return false
}

func isAny(addr string) bool {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	return addr == "" || net.ParseIP(addr).IsUnspecified()
}

type getPrivateIPFunc func() (string, error)

// This is overriden in unit tests to mock the sockaddr.GetPrivateIP function.
var getPrivateAddress getPrivateIPFunc = sockaddr.GetPrivateIP

func calculateAdvertiseAddress(bindAddr, advertiseAddr string) (net.IP, error) {
	if advertiseAddr != "" {
		ip := net.ParseIP(advertiseAddr)
		if ip == nil {
			return nil, fmt.Errorf("failed to parse advertise addr '%s'", advertiseAddr)
		}
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
		}
		return ip, nil
	}

	if isAny(bindAddr) {
		privateIP, err := getPrivateAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to get private IP %v",err)
		}
		if privateIP == "" {
			return nil, fmt.Errorf("no private IP found, explicit advertise addr not provided")
		}
		ip := net.ParseIP(privateIP)
		if ip == nil {
			return nil,fmt.Errorf("failed to parse private IP '%s'", privateIP)
		}
		return ip, nil
	}

	ip := net.ParseIP(bindAddr)
	if ip == nil {
		return nil, fmt.Errorf("failed to parse bind addr '%s'", bindAddr)
	}
	return ip, nil
}