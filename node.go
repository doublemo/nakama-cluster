package nakamacluster

import (
	"encoding/json"

	"github.com/doublemo/nakama-cluster/sd"
)

// 节点类型
type NodeType int

// 节点状态
type NodeStatus int

const (
	NODE_TYPE_NAKAMA        NodeType = iota + 1 // nakama主服务
	NODE_TYPE_MICROSERVICES                     // 微服务
)

const (
	NODE_STATUS_STARTING NodeStatus = iota
	NODE_STATUS_SYNC                // 节点正在同步数据
	NODE_STATUS_READYED             // 节点准备就绪
)

type Node struct {
	Id         string     `json:"id"`
	Name       string     `json:"name"`
	Rpc        string     `json:"rpc"`
	Weight     int        `json:"weight"`
	Region     string     `json:"region"`
	NodeType   NodeType   `json:"node_type"`
	NodeStatus NodeStatus `json:"node_status"`
	Addr       string     `json:"addr"`
}

func (n *Node) FromString(s string) error {
	return n.FromBytes([]byte(s))
}

func (n *Node) FromBytes(s []byte) error {
	return json.Unmarshal(s, n)
}

func (n *Node) ToBytes() []byte {
	bytes, _ := json.Marshal(n)
	return bytes
}

func (n *Node) ToService(prefix string) sd.Service {
	data, _ := json.Marshal(n)
	return sd.Service{
		Key:   prefix + "/" + n.Id,
		Value: string(data),
	}
}

func (n *Node) Clone() *Node {
	return &Node{
		Id:         n.Id,
		Name:       n.Name,
		Rpc:        n.Rpc,
		Weight:     n.Weight,
		Region:     n.Region,
		NodeType:   n.NodeType,
		NodeStatus: n.NodeStatus,
		Addr:       n.Addr,
	}
}
