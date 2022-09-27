package nakamacluster

import (
	"sync"

	"github.com/doublemo/nakama-cluster/pb"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type NotifyMsgHandle func(*pb.Notify)

type Delegate struct {
	logger     *zap.Logger
	server     *Server
	messageCur map[string]uint64
	mutex      sync.RWMutex
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (s *Delegate) NodeMeta(limit int) []byte {
	return s.server.localNode.ToBytes()
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

	var notify pb.Notify
	if err := proto.Unmarshal(msg, &notify); err != nil {
		s.logger.Warn("NotifyMsg parse failed", zap.Error(err))
		return
	}

	s.mutex.RLock()
	lastId := s.messageCur[notify.Node]
	s.mutex.RUnlock()

	if notify.Id <= lastId {
		return
	}

	s.mutex.Lock()
	s.messageCur[notify.Node] = notify.Id
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
	return nil
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (s *Delegate) MergeRemoteState(buf []byte, join bool) {
}

func newDelegate(logger *zap.Logger, s *Server) *Delegate {
	return &Delegate{
		logger:     logger,
		server:     s,
		messageCur: make(map[string]uint64),
	}
}
