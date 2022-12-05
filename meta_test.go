package nakamacluster

import (
	"net"
	"testing"

	sockaddr "github.com/hashicorp/go-sockaddr"
)

func TestMeta(t *testing.T) {
	cAddr := "0.0.0.0"
	addr := ""
	ip, err := net.ResolveIPAddr("ip", cAddr)
	if err == nil && cAddr != "" && cAddr != "0.0.0.0" {
		addr = ip.String()
	} else {
		addr, err = sockaddr.GetPrivateIP()
		if err != nil {
			panic(err)
		}
	}

	t.Log(addr)
}
