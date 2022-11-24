package utils

import (
	"net"
	"testing"
)

func TestIp(t *testing.T) {
	ip, err := net.ResolveIPAddr("ip", "192.168.0.256")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ip.String())
}
