package nakamacluster

import "encoding/json"

type Node struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	Rpc      string `json:"rpc"`
	Weight   int    `json:"weight"`
	Region   string `json:"region"`
	NodeType int    `json:"node_type"`
}

func (n *Node) FromString(s string) error {
	return n.FromBytes([]byte(s))
}

func (n *Node) FromBytes(s []byte) error {
	return json.Unmarshal(s, n)
}
