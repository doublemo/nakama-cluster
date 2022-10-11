package nakamacluster

import (
	"sync"

	"github.com/serialx/hashring"
)

type Peer struct {
	ring  *hashring.HashRing
	nodes map[string]*Node
	sync.RWMutex
}

func (peer *Peer) Get(id string) (*Node, bool) {
	peer.Lock()
	defer peer.Unlock()
	node, ok := peer.nodes[id]
	if !ok {
		return nil, false
	}
	return node.Clone(), true
}

func (peer *Peer) All() []*Node {
	len := peer.Len()
	nodes := make([]*Node, len)

	i := 0
	peer.RLock()
	for _, v := range peer.nodes {
		peer.RUnlock()
		nodes[i] = v.Clone()
		i++
		peer.RLock()
	}
	peer.RUnlock()
	return nodes
}

func (peer *Peer) AllToMap() map[string]*Node {
	nodes := make(map[string]*Node)
	peer.RLock()
	for k, v := range peer.nodes {
		peer.RUnlock()
		nodes[k] = v.Clone()
		peer.RLock()
	}
	peer.RUnlock()
	return nodes
}

func (peer *Peer) Add(node Node) {
	peer.Lock()
	defer peer.Unlock()
	if _, ok := peer.nodes[node.Id]; !ok {
		peer.ring.AddNode(node.Id)
	}
	peer.nodes[node.Id] = &node
}

func (peer *Peer) Delete(id string) {
	peer.Lock()
	peer.ring.RemoveNode(id)
	delete(peer.nodes, id)
	peer.Unlock()
}

func (peer *Peer) Len() int {
	peer.RLock()
	defer peer.RUnlock()
	return len(peer.nodes)
}

func (peer *Peer) GetNodeWithHashRing(k string) (*Node, bool) {
	peer.RLock()
	id, ok := peer.ring.GetNode(k)
	peer.RUnlock()

	if !ok {
		return nil, false
	}

	peer.RLock()
	node, ok := peer.nodes[id]
	peer.RUnlock()

	if !ok {
		return nil, false
	}
	return node, true
}

func NewPeer() *Peer {
	return &Peer{
		ring:  hashring.New(make([]string, 0)),
		nodes: make(map[string]*Node, 0),
	}
}
