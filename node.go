package nakamacluster

import (
	"encoding/json"

	"github.com/doublemo/nakama-cluster/sd"
)

type NodeType int

const (
	NODE_TYPE_NAKAMA        NodeType = iota + 1 // nakama主服务
	NODE_TYPE_MICROSERVICES                     // 微服务
)

type Node struct {
	Id       string   `json:"id"`
	Name     string   `json:"name"`
	Rpc      string   `json:"rpc"`
	Weight   int      `json:"weight"`
	Region   string   `json:"region"`
	NodeType NodeType `json:"node_type"`
	Addr     string   `json:"addr"`
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
