package nakamacluster

import "github.com/hashicorp/memberlist"

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (s *Delegate) NotifyJoin(node *memberlist.Node) {}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (s *Delegate) NotifyLeave(node *memberlist.Node) {}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (s *Delegate) NotifyUpdate(node *memberlist.Node) {}

// NotifyAlive implements the memberlist.AliveDelegate interface.
func (s *Delegate) NotifyAlive(peer *memberlist.Node) error {
	return nil
}
