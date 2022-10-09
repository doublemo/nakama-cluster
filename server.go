package nakamacluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublemo/nakama-cluster/sd"
	"github.com/hashicorp/memberlist"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// 集群配置
type Config struct {
	Addr                string      `yaml:"gossip_bindaddr" json:"gossip_bindaddr" usage:"Interface address to bind Nakama to for discovery. By default listening on all interfaces."`
	Port                int         `yaml:"gossip_bindport" json:"gossip_bindport" usage:"Port number to bind Nakama to for discovery. Default value is 7352."`
	Prefix              string      `yaml:"prefix" json:"prefix" usage:"service prefix"`
	Weight              int         `yaml:"weight" json:"weight" usage:"Peer weight"`
	PushPullInterval    int         `yaml:"push_pull_interval" json:"push_pull_interval" usage:"push_pull_interval is the interval between complete state syncs, Default value is 60 Second"`
	GossipInterval      int         `yaml:"gossip_interval" json:"gossip_interval" usage:"gossip_interval is the interval after which a node has died that, Default value is 200 Millisecond"`
	TCPTimeout          int         `yaml:"tcp_timeout" json:"tcp_timeout" usage:"tcp_timeout is the timeout for establishing a stream connection with a remote node for a full state sync, and for stream read and writeoperations, Default value is 10 Second"`
	ProbeTimeout        int         `yaml:"probe_timeout" json:"probe_timeout" usage:"probe_timeout is the timeout to wait for an ack from a probed node before assuming it is unhealthy. This should be set to 99-percentile of RTT (round-trip time) on your network, Default value is 500 Millisecond"`
	ProbeInterval       int         `yaml:"probe_interval" json:"probe_interval" usage:"probe_interval is the interval between random node probes. Setting this lower (more frequent) will cause the memberlist cluster to detect failed nodes more quickly at the expense of increased bandwidth usage., Default value is 1 Second"`
	RetransmitMult      int         `yaml:"retransmit_mult" json:"retransmit_mult" usage:"retransmit_mult is the multiplier used to determine the maximum number of retransmissions attempted, Default value is 2"`
	MaxGossipPacketSize int         `yaml:"max_gossip_packet_size" json:"max_gossip_packet_size" usage:"max_gossip_packet_size Maximum number of bytes that memberlist will put in a packet (this will be for UDP packets by default with a NetTransport), Default value is 1400"`
	Rpc                 *GrpcConfig `yaml:"rpc" json:"rpc" usage:"rpc setting"`
}

type Server struct {
	ctx         context.Context
	cancelFn    context.CancelFunc
	sdClient    sd.Client
	memberlist  *memberlist.Memberlist
	localNode   *Node
	nakamaPeers *Peer
	snowflake   *snowflake
	microPeers  atomic.Value
	msgQueue    *memberlist.TransmitLimitedQueue
	msgChan     chan Broadcast
	onNotifyMsg atomic.Value
	metrics     *Metrics
	config      Config
	grpcServer  *grpc.Server

	logger *zap.Logger
	once   sync.Once
	sync.RWMutex
}

func (s *Server) Enabled() bool {
	return s.ctx != nil && s.cancelFn != nil
}

func (s *Server) NextMessageId() (uint64, error) {
	return s.snowflake.NextId()
}

func (s *Server) Node() *Node {
	return s.localNode.Clone()
}

func (s *Server) OnNotifyMsg(f NotifyMsgHandle) {
	s.onNotifyMsg.Store(f)
}

func (s *Server) Metrics() *Metrics {
	return s.metrics
}

func (s *Server) Broadcast(msg Broadcast) bool {
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

func (s *Server) serve() {
	watchCh := make(chan struct{}, 1)
	s.sdClient.Register(s.localNode.ToService(s.config.Prefix))
	defer func() {
		s.sdClient.Deregister(s.localNode.ToService(s.config.Prefix))
		s.memberlist.Leave(time.Second * 5)
	}()

	go s.sdClient.WatchPrefix(s.config.Prefix, watchCh)
	for {
		select {
		case msg, ok := <-s.msgChan:
			if !ok {
				return
			}

			if s.metrics != nil {
				s.metrics.SentBroadcast(int64(len(msg.Message())))
			}
			s.msgQueue.QueueBroadcast(&msg)

		case <-watchCh:
			s.watchNodes()

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) watchNodes() {
	nodes, err := s.nodes()
	if err != nil {
		s.logger.Warn("Load nodes failed", zap.Error(err))
		return
	}

	microPeers := make(map[string]*Peer)
	for _, node := range nodes {
		if node.NodeType == NODE_TYPE_NAKAMA {
			continue
		}

		if _, ok := microPeers[node.Name]; !ok {
			microPeers[node.Name] = NewPeer()
		}
		microPeers[node.Name].Add(*node)
	}
	s.microPeers.Store(microPeers)
}

func (s *Server) up(list *memberlist.Memberlist) {
	nodes, err := s.nodes()
	if err != nil {
		s.logger.Warn("join failed", zap.Error(err))
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
	s := &Server{
		ctx:         ctx,
		cancelFn:    cancel,
		sdClient:    client,
		logger:      logger,
		config:      c,
		nakamaPeers: NewPeer(),
		localNode:   &node,
		msgChan:     make(chan Broadcast, 128),
		snowflake:   newSnowflake(node.Id),
	}

	// metrics
	if metrics != nil {
		s.metrics = newMetrics(metrics)
	}

	s.microPeers.Store(make(map[string]*Peer))
	s.msgQueue = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return s.nakamaPeers.Len()
		},
		RetransmitMult: c.RetransmitMult,
	}

	delegate := newDelegate(logger, s)
	memberlistConfig := buildMemberListConfig(c)
	memberlistConfig.Name = node.Id
	memberlistConfig.Ping = delegate
	memberlistConfig.Delegate = delegate
	memberlistConfig.Events = delegate
	memberlistConfig.Alive = delegate
	list, err := memberlist.Create(memberlistConfig)
	if err != nil {
		logger.Fatal("Create memberlist failed", zap.Error(err))
	}
	s.memberlist = list
	s.up(list)
	go s.serve()
	return s
}

func NewConfig() *Config {
	c := &Config{
		Addr:                "0.0.0.0",
		Port:                7355,
		Prefix:              "/nakama-cluster/services/",
		Weight:              1,
		PushPullInterval:    10,
		GossipInterval:      200,
		TCPTimeout:          10,
		ProbeTimeout:        500,
		ProbeInterval:       1,
		RetransmitMult:      2,
		MaxGossipPacketSize: 1400,
		Rpc: &GrpcConfig{
			Addr:    "",
			Port:    7353,
			X509Pem: "",
			X509Key: "",
			Token:   "",
		},
	}
	return c
}
