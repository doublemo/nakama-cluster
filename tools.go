package nakamacluster

import (
	"time"

	"github.com/hashicorp/memberlist"
)

func buildMemberListConfig(c Config) *memberlist.Config {
	memberlistConfig := memberlist.DefaultLocalConfig()
	memberlistConfig.BindAddr = c.Addr
	memberlistConfig.BindPort = c.Port
	memberlistConfig.PushPullInterval = time.Duration(c.ProbeInterval) * time.Second
	memberlistConfig.GossipInterval = time.Duration(c.GossipInterval) * time.Millisecond
	memberlistConfig.ProbeInterval = time.Duration(c.ProbeInterval) * time.Second
	memberlistConfig.ProbeTimeout = time.Duration(c.ProbeTimeout) * time.Millisecond
	memberlistConfig.UDPBufferSize = c.MaxGossipPacketSize
	memberlistConfig.TCPTimeout = time.Duration(c.TCPTimeout) * time.Second
	return memberlistConfig
}
