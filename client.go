package nakamacluster

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const NAKAMA = "nakama"

var (
	ErrMessageQueueFull    = errors.New("message incoming queue full")
	ErrMessageNotWaitReply = errors.New("invalid message")
)

type Client struct {
	ctx              context.Context
	cancelFn         context.CancelFunc
	config           *Config
	incomingCh       chan *Message
	peers            Peer
	memberlist       *memberlist.Memberlist
	messageQueue     *memberlist.TransmitLimitedQueue
	messageWaitQueue sync.Map
	wathcer          *Watcher
	delegate         atomic.Value
	logger           *zap.Logger
	once             sync.Once
}

func (s *Client) OnDelegate(delegate Delegate) {
	s.delegate.Store(delegate)
}

func (s *Client) GetNodesByNakama() []string {
	metas := s.peers.GetByName(NAKAMA)
	nodes := make([]string, len(metas))
	for _, meta := range metas {
		nodes = append(nodes, meta.Addr)
	}
	return nodes
}

func (s *Client) Broadcast(msg *Message) error {
	select {
	case s.incomingCh <- msg:
	default:
		return ErrMessageQueueFull
	}
	return nil
}

func (s *Client) Send(msg *Message, to ...string) ([]*api.Envelope, error) {
	select {
	case s.incomingCh <- msg:
	default:
		return nil, ErrMessageQueueFull
	}

	if !msg.IsWaitReply() {
		return nil, nil
	}

	s.messageWaitQueue.Store(msg.ID().String(), msg)
	defer func() {
		s.messageWaitQueue.Delete(msg.ID().String())
	}()

	return msg.Wait()
}

func (s *Client) Stop() {
	s.once.Do(func() {
		if s.cancelFn != nil {
			s.wathcer.Stop()
			s.cancelFn()
		}
	})
}

func (s *Client) onUpdate(metas []*Meta) {
	for _, meta := range metas {
		if meta.Type == NODE_TYPE_MICROSERVICES && meta.Name == NAKAMA {
			s.logger.Warn("Invalid node name", zap.String("ID", meta.Id))
			continue
		}
		s.peers.Add(meta)
	}
}

func (s *Client) processIncoming() {
	for {
		select {
		case frame, ok := <-s.incomingCh:
			if !ok {
				return
			}

			_ = frame

		case <-s.ctx.Done():
			return
		}
	}
}

func NewClient(ctx context.Context, logger *zap.Logger, sdclient sd.Client, id string, vars map[string]string, config Config) *Client {
	var err error
	ctx, cancel := context.WithCancel(ctx)
	meta := NewNodeMetaFromConfig(id, NAKAMA, NODE_TYPE_NAKAMA, vars, config)
	addr := "0.0.0.0"
	if config.Addr != "" {
		addr = config.Addr
	}

	s := &Client{
		ctx:        ctx,
		cancelFn:   cancel,
		logger:     logger,
		config:     &config,
		incomingCh: make(chan *Message, config.BroadcastQueueSize),
		peers: NewPeer(ctx, logger, PeerOptions{
			MaxIdle:              config.GrpcPoolMaxIdle,
			MaxActive:            config.GrpcPoolMaxActive,
			MaxConcurrentStreams: config.GrpcPoolMaxConcurrentStreams,
			Reuse:                config.GrpcPoolReuse,
			MessageQueueSize:     config.MaxGossipPacketSize,
		}),
	}

	memberlistConfig := memberlist.DefaultLocalConfig()
	memberlistConfig.BindAddr = addr
	memberlistConfig.BindPort = config.Port
	memberlistConfig.PushPullInterval = time.Duration(config.PushPullInterval) * time.Second
	memberlistConfig.GossipInterval = time.Duration(config.GossipInterval) * time.Millisecond
	memberlistConfig.ProbeInterval = time.Duration(config.ProbeInterval) * time.Second
	memberlistConfig.ProbeTimeout = time.Duration(config.ProbeTimeout) * time.Millisecond
	memberlistConfig.UDPBufferSize = config.MaxGossipPacketSize
	memberlistConfig.TCPTimeout = time.Duration(config.TCPTimeout) * time.Second
	memberlistConfig.RetransmitMult = config.RetransmitMult
	memberlistConfig.Name = id
	// memberlistConfig.Ping = s
	// memberlistConfig.Delegate = s
	// memberlistConfig.Events = s
	// memberlistConfig.Alive = s
	memberlistConfig.Logger = log.New(os.Stdout, "nakama-cluster", 0)

	if !logger.Core().Enabled(zapcore.DebugLevel) {
		memberlistConfig.Logger.SetOutput(io.Discard)
	}

	s.messageQueue = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return s.memberlist.NumMembers()
		},

		RetransmitMult: config.RetransmitMult,
	}
	s.memberlist, err = memberlist.Create(memberlistConfig)
	if err != nil {
		logger.Fatal("Failed to create memberlist", zap.Error(err))
	}

	s.wathcer = NewWatcher(ctx, logger, sdclient, config.Prefix, meta)
	metas, err := s.wathcer.GetEntries()
	if err != nil {
		logger.Fatal(err.Error())
	}
	s.onUpdate(metas)
	s.wathcer.OnUpdate(s.onUpdate)

	if _, err := s.memberlist.Join(s.GetNodesByNakama()); err != nil {
		logger.Warn("Failed to join cluster", zap.Error(err))
	}

	go s.processIncoming()
	return s
}
