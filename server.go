package nakamacluster

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublemo/nakama-cluster/sd"
	sockaddr "github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/memberlist"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type Server struct {
	ctx         context.Context
	cancelFn    context.CancelFunc
	sdClient    sd.Client
	memberlist  *memberlist.Memberlist
	localNode   *Node
	nakamaPeers *LocalPeer
	microPeers  *LocalPeer
	msgQueue    *memberlist.TransmitLimitedQueue
	msgChan     chan Broadcast
	onNotifyMsg atomic.Value
	metrics     *Metrics
	config      Config
	grpcServer  *grpc.Server

	logger   *zap.Logger
	once     sync.Once
	enabled  int32
	nodeType int32
	sync.RWMutex
}

func (s *Server) Enabled() bool {
	return atomic.LoadInt32(&s.enabled) > 0
}

func (s *Server) NodeType() NodeType {
	return NodeType(atomic.LoadInt32(&s.nodeType))
}

func (s *Server) Node() *Node {
	s.RLock()
	defer s.RUnlock()
	return s.localNode.Clone()
}

func (s *Server) OnNotifyMsg(f NotifyMsgHandle) {
	s.onNotifyMsg.Store(f)
}

func (s *Server) Metrics() *Metrics {
	return s.metrics
}

func (s *Server) Broadcast(msg Broadcast) bool {
	nt := s.NodeType()
	if nt != NODE_TYPE_NAKAMA {
		return false
	}

	select {
	case s.msgChan <- msg:
		return true
	case <-s.ctx.Done():
	default:
	}
	return false
}

func (s *Server) StartApiServer(handler GrpcHandler, stream GrpcStreamHandler) (err error) {
	s.Lock()
	s.grpcServer, err = newGrpcServer(s.ctx, s.logger, handler, stream, *s.config.Rpc)
	s.Unlock()
	return
}

func (s *Server) NakamaPeer() Peer {
	return s.nakamaPeers
}

func (s *Server) MicroPeer() Peer {
	return s.microPeers
}

func (s *Server) Stop() {
	s.once.Do(func() {
		if s.grpcServer != nil {
			s.grpcServer.Stop()
		}

		if s.cancelFn != nil {
			s.cancelFn()
		}
	})
}

func (s *Server) nakamaServe() {
	watchCh := make(chan struct{}, 1)
	s.sdClient.Register(s.localNode.ToService(s.config.Prefix))
	defer func() {
		s.sdClient.Deregister(s.localNode.ToService(s.config.Prefix))
		s.memberlist.Leave(time.Second * 5)
	}()

	go s.sdClient.WatchPrefix(s.config.Prefix, watchCh)
	var seqid uint64
	for {
		select {
		case msg, ok := <-s.msgChan:
			if !ok {
				return
			}

			if s.metrics != nil {
				s.metrics.SentBroadcast(int64(len(msg.Message())))
			}

			seqid++
			msg.SetId(uint64(seqid))
			msg.SetNode(s.localNode.Id)
			if len(msg.to) < 1 {
				s.msgQueue.QueueBroadcast(&msg)
			} else {
				for _, to := range msg.to {
					ipaddr, err := sockaddr.NewIPAddr(to.Addr)
					if err != nil {
						s.logger.Error("Parse failed, node ip is valid", zap.Error(err), zap.String("addr", to.Addr))
						continue
					}

					node := &memberlist.Node{Name: to.Id, Addr: *ipaddr.NetIP(), Port: uint16(ipaddr.IPPort())}
					err = s.memberlist.SendReliable(node, msg.Message())
					if err != nil {
						s.logger.Error("Send message failed", zap.Error(err))
						continue
					}
				}
			}

		case <-watchCh:
			s.updatePeers()

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) microServe() {
	watchCh := make(chan struct{}, 1)
	s.sdClient.Register(s.localNode.ToService(s.config.Prefix))
	defer func() {
		s.sdClient.Deregister(s.localNode.ToService(s.config.Prefix))
	}()

	go s.sdClient.WatchPrefix(s.config.Prefix, watchCh)
	for {
		select {
		case <-watchCh:
			s.updatePeers()

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) updatePeers() {
	nodes, err := s.nodes()
	if err != nil {
		s.logger.Fatal("Load nodes failed", zap.Error(err))
		return
	}

	isNakama := s.localNode.NodeType == NODE_TYPE_NAKAMA
	for _, node := range nodes {
		if node.NodeType == NODE_TYPE_NAKAMA {
			if isNakama {
				continue
			}
			s.nakamaPeers.add(node)
		} else {
			s.microPeers.add(node)
		}
	}
}

func (s *Server) up(list *memberlist.Memberlist) {
	nodes, err := s.nodes()
	if err != nil {
		s.logger.Fatal("join failed", zap.Error(err))
		return
	}

	joins := make([]string, 0)
	for _, node := range nodes {
		if node == nil || node.Id == s.localNode.Id || node.NodeType != NODE_TYPE_NAKAMA {
			continue
		}

		joins = append(joins, node.Addr)
	}

	if _, err := list.Join(joins); err != nil {
		s.logger.Error("Join failed", zap.Error(err))
		return
	}

	s.logger.Info("Node up", zap.Any("nodes", joins))
}

func (s *Server) nodes() ([]*Node, error) {
	entries, err := s.sdClient.GetEntries(s.config.Prefix)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, len(entries))
	for k, v := range entries {
		node := Node{}
		if err := node.FromString(v); err != nil {
			s.logger.Warn("invalid node", zap.Error(err))
			continue
		}
		nodes[k] = &node
	}
	return nodes, nil
}

func NewServer(ctx context.Context, logger *zap.Logger, client sd.Client, node Node, metrics tally.Scope, c Config) *Server {
	ctx, cancel := context.WithCancel(ctx)
	peerOptions := PeerOptions{
		MaxIdle:              c.GrpcPoolMaxIdle,
		MaxActive:            c.GrpcPoolMaxActive,
		MaxConcurrentStreams: c.GrpcPoolMaxConcurrentStreams,
		Reuse:                c.GrpcPoolReuse,
		MessageQueueSize:     c.MaxGossipPacketSize,
	}

	s := &Server{
		ctx:         ctx,
		cancelFn:    cancel,
		sdClient:    client,
		logger:      logger,
		config:      c,
		nakamaPeers: NewPeer(ctx, logger, peerOptions),
		microPeers:  NewPeer(ctx, logger, peerOptions),
		localNode:   &node,
		msgChan:     make(chan Broadcast, c.BroadcastQueueSize),
	}

	// metrics
	if metrics != nil {
		s.metrics = newMetrics(metrics)
	}

	if node.NodeType == NODE_TYPE_MICROSERVICES {
		microServer(s)
	} else {
		nakamaServer(s)
	}

	atomic.StoreInt32(&s.enabled, 1)
	atomic.StoreInt32(&s.nodeType, int32(node.NodeType))
	return s
}

func microServer(s *Server) {
	go s.microServe()
}

func nakamaServer(s *Server) {
	c := s.config
	s.msgQueue = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return s.nakamaPeers.Size()
		},
		RetransmitMult: c.RetransmitMult,
	}

	delegate := newDelegate(s.logger, s)
	memberlistConfig := buildMemberListConfig(c)
	memberlistConfig.Name = s.localNode.Id
	memberlistConfig.Ping = delegate
	memberlistConfig.Delegate = delegate
	memberlistConfig.Events = delegate
	memberlistConfig.Alive = delegate
	if !s.logger.Core().Enabled(zapcore.DebugLevel) {
		memberlistConfig.Logger.SetOutput(io.Discard)
	}

	list, err := memberlist.Create(memberlistConfig)
	if err != nil {
		s.logger.Fatal("Create memberlist failed", zap.Error(err))
	}
	s.memberlist = list
	s.up(list)
	go s.nakamaServe()
}
