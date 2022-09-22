package nakamacluster

import (
	"context"
	"sync"
	"time"

	"github.com/doublemo/nakama-cluster/sd"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// 集群配置
type Config struct {
	Addr                string `yaml:"gossip_bindaddr" json:"gossip_bindaddr" usage:"Interface address to bind Nakama to for discovery. By default listening on all interfaces."`
	Port                int    `yaml:"gossip_bindport" json:"gossip_bindport" usage:"Port number to bind Nakama to for discovery. Default value is 7352."`
	Prefix              string `yaml:"prefix" json:"prefix" usage:"service prefix"`
	Weight              int    `yaml:"weight" json:"weight" usage:"Peer weight"`
	PushPullInterval    int    `yaml:"push_pull_interval" json:"push_pull_interval" usage:"push_pull_interval is the interval between complete state syncs, Default value is 60 Second"`
	GossipInterval      int    `yaml:"gossip_interval" json:"gossip_interval" usage:"gossip_interval is the interval after which a node has died that, Default value is 200 Millisecond"`
	TCPTimeout          int    `yaml:"tcp_timeout" json:"tcp_timeout" usage:"tcp_timeout is the timeout for establishing a stream connection with a remote node for a full state sync, and for stream read and writeoperations, Default value is 10 Second"`
	ProbeTimeout        int    `yaml:"probe_timeout" json:"probe_timeout" usage:"probe_timeout is the timeout to wait for an ack from a probed node before assuming it is unhealthy. This should be set to 99-percentile of RTT (round-trip time) on your network, Default value is 500 Millisecond"`
	ProbeInterval       int    `yaml:"probe_interval" json:"probe_interval" usage:"probe_interval is the interval between random node probes. Setting this lower (more frequent) will cause the memberlist cluster to detect failed nodes more quickly at the expense of increased bandwidth usage., Default value is 1 Second"`
	ReconnectInterval   int    `yaml:"reconnect_interval" json:"reconnect_interval" usage:"reconnect_interval is reconnect, Default value is 10 Second"`
	ReconnectTimeout    int    `yaml:"reconnect_timeout" json:"reconnect_timeout" usage:"reconnect_timeout is reconnect time out, Default value is 6 Hour"`
	RefreshInterval     int    `yaml:"refresh_interval" json:"refresh_interval" usage:", Default value is 15 Second"`
	MaxGossipPacketSize int    `yaml:"max_gossip_packet_size" json:"max_gossip_packet_size" usage:"max_gossip_packet_size Maximum number of bytes that memberlist will put in a packet (this will be for UDP packets by default with a NetTransport), Default value is 1400"`
}

type Server struct {
	ctx        context.Context
	cancelFn   context.CancelFunc
	sdClient   sd.Client
	memberlist *memberlist.Memberlist
	peers      map[string]*Peer
	config     Config

	logger *zap.Logger
	once   sync.Once
	sync.RWMutex
}

func (s *Server) Stop() {
	s.once.Do(func() {
		if s.cancelFn != nil {
			s.cancelFn()
		}
	})
}

func (s *Server) serve() {
	watchCh := make(chan struct{}, 1)
	go s.sdClient.WatchPrefix(s.config.Prefix, watchCh)
	for {
		select {
		case <-watchCh:
			s.handelWatchNodes()

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) handelWatchNodes() {
	entries, err := s.sdClient.GetEntries(s.config.Prefix)
	if err != nil {
		s.logger.Warn("GetEntries failed", zap.Error(err))
		return
	}

	nodes := make([]string, len(entries))
	for _, item := range entries {
		node := Node{}
		if err := node.FromString(item); err != nil {
			s.logger.Warn("invalid node", zap.Error(err))
			continue
		}

		nodes = append(nodes, node.Id)
		s.RLock()
		peer, ok := s.peers[node.Name]
		s.RUnlock()
		if !ok {
			peer = NewPeer()
		}

		peer.Add(node)
		s.Lock()
		s.peers[node.Id] = peer
		s.Unlock()
	}

	s.memberlist.Join(nodes)
	s.memberlist.UpdateNode(time.Second)
}

func NewServer(ctx context.Context, logger *zap.Logger, client sd.Client, c Config) *Server {
	ctx, cancel := context.WithCancel(ctx)
	s := &Server{
		ctx:      ctx,
		cancelFn: cancel,
		sdClient: client,
		logger:   logger,
		config:   c,
		peers:    make(map[string]*Peer),
	}

	delegate := newDelegate(logger, s)
	memberlistConfig := buildMemberListConfig(c)
	memberlistConfig.Ping = delegate
	memberlistConfig.Delegate = delegate
	memberlistConfig.Events = delegate
	memberlistConfig.Alive = delegate
	list, err := memberlist.Create(memberlistConfig)
	if err != nil {
		logger.Fatal("Create memberlist failed", zap.Error(err))
	}
	s.memberlist = list

	go s.serve()
	return s
}

func NewConfig() *Config {
	return &Config{
		Addr:                "0.0.0.0",
		Port:                7352,
		Prefix:              "/nakama-cluster/services/",
		Weight:              1,
		PushPullInterval:    10,
		GossipInterval:      200,
		TCPTimeout:          10,
		ProbeTimeout:        500,
		ProbeInterval:       1,
		ReconnectInterval:   10,
		ReconnectTimeout:    6,
		RefreshInterval:     15,
		MaxGossipPacketSize: 1400,
	}
}
