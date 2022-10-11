package nakamacluster

import (
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (s *Delegate) NotifyJoin(node *memberlist.Node) {
	meta := &Node{}
	if err := meta.FromBytes(node.Meta); err != nil {
		s.logger.Error("NotifyJoin meta error:", zap.Error(err))
		return
	}

	s.server.nakamaPeers.add(meta)
	s.logger.Debug("NotifyJoin", zap.String("name", node.Name), zap.Int("node_num", s.server.nakamaPeers.Size()))
	if s.server.metrics != nil {
		s.server.metrics.NodeJoin()
	}
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (s *Delegate) NotifyLeave(node *memberlist.Node) {
	s.logger.Debug("NotifyLeave", zap.Any("node", node.Name), zap.Int("node_num", s.server.nakamaPeers.Size()))
	s.server.nakamaPeers.delete(node.Name)
	if s.server.metrics != nil {
		s.server.metrics.NodeLeave()
	}
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (s *Delegate) NotifyUpdate(node *memberlist.Node) {
	meta := &Node{}
	if err := meta.FromBytes(node.Meta); err != nil {
		s.logger.Error("NotifyJoin meta error:", zap.Error(err))
		return
	}

	s.server.nakamaPeers.add(meta)
	s.logger.Debug("NotifyUpdate", zap.Any("node", node))
}

// NotifyAlive implements the memberlist.AliveDelegate interface.
func (s *Delegate) NotifyAlive(peer *memberlist.Node) error {
	//s.logger.Debug("NotifyAlive", zap.Any("node", peer))
	return nil
}
