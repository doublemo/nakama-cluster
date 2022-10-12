package nakamacluster

import (
	"sync"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type NotifyMsgHandle func(*api.Envelope)

type Delegate struct {
	logger          *zap.Logger
	server          *Server
	messageCur      map[string][]byte
	maxMessageBytes int
	messageLooped   bool
	mutex           sync.RWMutex
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (s *Delegate) NodeMeta(limit int) []byte {
	node := s.server.Node()
	return node.ToBytes()
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (s *Delegate) NotifyMsg(msg []byte) {
	if s.server.metrics != nil {
		s.server.metrics.RecvBroadcast(int64(len(msg)))
	}

	handler, ok := s.server.onNotifyMsg.Load().(NotifyMsgHandle)
	if !ok || handler == nil {
		return
	}

	var notify api.Envelope
	if err := proto.Unmarshal(msg, &notify); err != nil {
		s.logger.Warn("NotifyMsg parse failed", zap.Error(err))
		return
	}

	mod := int(notify.Id % uint64(s.maxMessageBytes))
	s.mutex.RLock()
	if _, ok := s.messageCur[notify.Node]; !ok {
		s.messageCur[notify.Node] = make([]byte, s.maxMessageBytes)
	}

	cur := s.messageCur[notify.Node][mod]
	looped := s.messageLooped
	s.mutex.RUnlock()

	if mod == 0 && !looped {
		s.mutex.Lock()
		s.messageCur[notify.Node] = make([]byte, s.maxMessageBytes)
		s.messageLooped = true
		s.mutex.Unlock()
	}

	if cur == 0x1 {
		return
	}

	s.mutex.Lock()
	s.messageCur[notify.Node][mod] = 0x1
	if mod != 0 && looped {
		s.messageLooped = false
	}
	s.mutex.Unlock()
	handler(&notify)
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (s *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return s.server.msgQueue.GetBroadcasts(overhead, limit)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. ALogger
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (s *Delegate) LocalState(join bool) []byte {
	node := s.server.Node()
	bytes, _ := proto.Marshal(&api.NodeStatus{
		Node:   node.Id,
		Status: int32(node.NodeStatus),
		Join:   join,
	})
	return bytes
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (s *Delegate) MergeRemoteState(buf []byte, join bool) {
	var nodeStatus api.NodeStatus
	if err := proto.Unmarshal(buf, &nodeStatus); err != nil {
		s.logger.Error("MergeRemoteState handle failed", zap.Error(err))
		return
	}
	s.server.nakamaPeers.update(nodeStatus.Node, NodeStatus(nodeStatus.Status))
}

func newDelegate(logger *zap.Logger, s *Server, maxMessageBytes int) *Delegate {
	return &Delegate{
		logger:          logger,
		server:          s,
		messageCur:      make(map[string][]byte),
		maxMessageBytes: maxMessageBytes,
	}
}
