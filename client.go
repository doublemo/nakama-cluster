package nakamacluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/doublemo/nakama-cluster/sd/etcdv3"
	sockaddr "github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

const NAKAMA = "nakama"

var (
	ErrMessageQueueFull    = errors.New("message incoming queue full")
	ErrMessageNotWaitReply = errors.New("invalid message")
	ErrMessageSendFailed   = errors.New("failed message send to node")
	ErrNodeNotFound        = errors.New("not found")
)

type Client struct {
	ctx              context.Context
	cancelFn         context.CancelFunc
	config           *Config
	registrar        sd.Registrar
	instancer        sd.Instancer
	endpointer       *sd.DefaultEndpointer
	incomingCh       chan *Message
	peers            Peer
	nodes            map[string]*memberlist.Node
	memberlist       *memberlist.Memberlist
	messageQueue     *memberlist.TransmitLimitedQueue
	messageWaitQueue sync.Map
	messageSeq       *MessageSeq
	messageCursor    *MessageCursor
	endpoint         endpoint.Endpoint
	delegate         atomic.Value
	logger           *zap.Logger
	once             sync.Once
	sync.Mutex
}

func (s *Client) OnDelegate(delegate Delegate) {
	s.delegate.Store(delegate)
}

func (s *Client) Endpoint() endpoint.Endpoint {
	return s.endpoint
}

func (s *Client) GetLocalNode() *memberlist.Node {
	return s.memberlist.LocalNode()
}

func (s *Client) UpdateMeta(vars map[string]string) error {
	for k, v := range vars {
		s.endpoint.SetVar(k, v)
	}
	return s.memberlist.UpdateNode(time.Second * 30)
}

func (s *Client) Broadcast(msg *Message) error {
	select {
	case s.incomingCh <- msg:
	default:
		return ErrMessageQueueFull
	}
	return nil
}

func (s *Client) Send(msg *Message, to ...string) ([][]byte, error) {
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

func (s *Client) Rpc(ctx context.Context, serviceName string, in *api.Envelope) (*api.Envelope, error) {
	return s.peers.Do(ctx, serviceName, in)
}

func (s *Client) Stop() {
	s.once.Do(func() {
		if s.cancelFn != nil {
			s.endpointer.Close()
			s.instancer.Stop()
			s.registrar.Deregister()
			s.peers.Close()
			s.cancelFn()
		}
	})
}

func (s *Client) processIncoming() {
	for {
		select {
		case message, ok := <-s.incomingCh:
			if !ok {
				return
			}

			toSize := len(message.To())
			frame := api.Frame{
				Id:     message.ID().String(),
				Node:   s.GetLocalNode().Name,
				Bytes:  message.Payload(),
				Direct: api.Frame_Send,
			}

			if toSize < 1 {
				frame.Direct = api.Frame_Broadcast
				frame.SeqID = s.messageSeq.NextBroadcastID()
			}

			switch frame.Direct {
			case api.Frame_Broadcast:
				broadcast := NewBroadcast(&frame)
				// to udp
				s.messageQueue.QueueBroadcast(broadcast)

				// stat

			case api.Frame_Send:
				for _, node := range message.To() {
					s.Lock()
					memberlistNode, ok := s.nodes[node]
					s.Unlock()

					if !ok || memberlistNode == nil {
						message.SendErr(fmt.Errorf("could not send message to %s", node))
						continue
					}

					frame.SeqID = s.messageSeq.NextID(node)
					messageBytes, err := proto.Marshal(&frame)
					if err != nil {
						message.SendErr(err)
						continue
					}

					if err := s.memberlist.SendReliable(memberlistNode, messageBytes); err != nil {
						message.SendErr(err)
						continue
					}
				}

			default:
				continue
			}

		case <-s.ctx.Done():
			return
		}
	}
}

func NewClient(ctx context.Context, logger *zap.Logger, sdclient etcdv3.Client, id string, vars map[string]string, config Config) *Client {
	var err error
	ctx, cancel := context.WithCancel(ctx)
	local, err := NewNakaMaEndpoint(id, vars, config)
	if err != nil {
		logger.Panic("Failed to NewNakaMaEndpoint", zap.Error(err))
	}

	addr := "0.0.0.0"
	if config.Addr != "" {
		addr = config.Addr
	}

	s := &Client{
		ctx:           ctx,
		cancelFn:      cancel,
		logger:        logger,
		config:        &config,
		incomingCh:    make(chan *Message, config.BroadcastQueueSize),
		peers:         NewLocalPeer(ctx, logger, sdclient, &config),
		messageSeq:    NewMessageSeq(),
		messageCursor: NewMessageCursor(64),
		nodes:         make(map[string]*memberlist.Node),
		registrar: etcdv3.NewRegistrar(sdclient, etcdv3.Service{
			Key:   config.Prefix + local.Name() + "/" + local.ID(),
			Value: local.String(),
		}, logger),
		endpoint: local,
	}

	s.registrar.Register()
	instancer, err := etcdv3.NewInstancer(sdclient, config.Prefix+local.Name(), logger)
	if err != nil {
		s.registrar.Deregister()
		logger.Panic("Failed to etcdv3.NewInstancer", zap.Error(err))
	}
	s.instancer = instancer
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
	memberlistConfig.Ping = s
	memberlistConfig.Delegate = s
	memberlistConfig.Events = s
	memberlistConfig.Alive = s
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
		logger.Panic("Failed to create memberlist", zap.Error(err))
	}

	s.endpointer = sd.NewEndpointer(instancer, func(instance string) (endpoint.Endpoint, io.Closer, error) {
		node := endpoint.New("", "", "", nil, func(ctx context.Context, request interface{}) (response interface{}, err error) {
			return nil, nil
		})
		node.FromString(instance)
		return node, nil, nil
	}, logger)

	endpoints, err := s.endpointer.Endpoints()
	if err != nil {
		logger.Panic("endpointer.Endpoints", zap.Error(err))
	}

	members := make([]string, len(endpoints))
	for k, v := range endpoints {
		members[k] = v.Address()
	}

	n, err := s.memberlist.Join(members)
	if err != nil {
		logger.Panic("Failed to join cluster", zap.Error(err))
	}

	logger.Info("Successed to join cluster", zap.Int("number", n))
	go s.processIncoming()
	return s
}

func NewNakaMaEndpoint(id string, vars map[string]string, config Config) (endpoint.Endpoint, error) {
	addr := ""
	ip, err := net.ResolveIPAddr("ip", config.Addr)
	if err == nil && config.Addr != "" && config.Addr != "0.0.0.0" {
		addr = ip.String()
	} else {
		addr, err = sockaddr.GetPrivateIP()
		if err != nil {
			return nil, err
		}
	}

	vars["domian"] = config.Domain
	return endpoint.New(id, NAKAMA, fmt.Sprintf("%s:%d", addr, config.Port), vars, nil), nil
}
