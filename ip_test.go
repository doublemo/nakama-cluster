package nakamacluster

import (
	"fmt"
	"testing"

	addr "github.com/hashicorp/go-sockaddr"
)

func TestIp(t *testing.T) {
	fmt.Println(addr.GetPrivateIP())
	fmt.Println(addr.GetPublicIP())

	v, err := addr.NewIPAddr("189.2.3.56:7895")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(v.NetIP(), v.IPPort())
	fmt.Println(PublicIP())
	fmt.Println(LocalIP())
}
