package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	nakamacluster "github.com/doublemo/nakama-cluster"
	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd/etcdv3"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/metadata"
)

type Delegate struct {
	logger *zap.Logger
	conn   *nakamacluster.Client
}

// LocalState 发送本地状态信息
func (s *Delegate) LocalState(join bool) []byte {
	s.logger.Info("Call LocalState", zap.Bool("join", join))
	return []byte("dddddd-" + s.conn.GetLocalNode().Name)
}

// MergeRemoteState 发送本地状态信息
func (s *Delegate) MergeRemoteState(buf []byte, join bool) {
	s.logger.Info("Call MergeRemoteState", zap.Bool("join", join))
	fmt.Println("--dd--->", string(buf))
}

// NotifyJoin 接收节点加入通知
func (s *Delegate) NotifyJoin(node endpoint.Endpoint) {
	s.logger.Info("Call NotifyJoin", zap.String("meta", node.String()))
}

// NotifyLeave 接收节点离线通知
func (s *Delegate) NotifyLeave(node endpoint.Endpoint) {
	s.logger.Info("Call NotifyLeave", zap.String("meta", node.String()))
}

// NotifyUpdate 接收节点更新通知
func (s *Delegate) NotifyUpdate(node endpoint.Endpoint) {
	s.logger.Info("Call NotifyUpdate", zap.String("meta", node.String()))
}

// NotifyAlive 接收节点活动通知
func (s *Delegate) NotifyAlive(node endpoint.Endpoint) error {
	s.logger.Info("Call NotifyAlive", zap.String("meta", node.String()))
	return nil
}

// NotifyMsg 接收节来至其它节点的信息
func (s *Delegate) NotifyMsg(node string, msg []byte) []byte {
	s.logger.Info("Call NotifyMsg", zap.String("msg", string(msg)))
	return nil
}

// Call rpc call
func (s *Delegate) Call(ctx context.Context, in *api.Envelope) (*api.Envelope, error) {
	s.logger.Info("Call", zap.String("CID", in.Cid.String()))
	return &api.Envelope{Cid: api.Message_BROADCAST, Payload: &api.Envelope_Error{Error: &api.Error{Code: 500, Message: s.conn.GetLocalNode().Name}}}, nil
}

// Stream rpc stream
func (s *Delegate) Stream(ctx context.Context, client func(out *api.Envelope) bool, in *api.Envelope) error {
	s.logger.Info("stream", zap.String("CID", in.Cid.String()))
	client(&api.Envelope{
		Cid: api.Message_SESSIONSTART,
		Payload: &api.Envelope_Error{
			Error: &api.Error{Code: 501, Message: s.conn.GetLocalNode().Name}}})

	return nil
}

// OnStreamClose rpc stream close
func (s *Delegate) OnStreamClose(ctx context.Context) {

}

func main() {
	rand.Seed(time.Now().UnixNano())
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}), zapcore.Lock(os.Stdout), zapcore.DebugLevel)

	options := []zap.Option{zap.AddCaller()}
	log := zap.New(core, options...)

	ctx, cancel := context.WithCancel(context.Background())
	client, err := etcdv3.NewClient(ctx, []string{"192.168.0.71:12379", "192.168.0.71:22379", "192.168.0.71:32379"}, etcdv3.ClientOptions{})
	if err != nil {
		log.Fatal("连接etcd失败", zap.Error(err))
	}

	c := nakamacluster.NewConfig()
	c.Port = 10000 + rand.Intn(10000)
	c.RetransmitMult = 5
	c.Prefix = "/nk/samples/"
	serverId := fmt.Sprintf("node-%d", rand.Intn(10000))
	vars := map[string]string{"weight": "1", "nakama-rpc": strconv.Itoa(c.Port)}
	node, _ := nakamacluster.NewNakaMaEndpoint(serverId, vars, *c)
	// Create Prometheus reporter and root scope.
	reporter := prometheus.NewReporter(prometheus.Options{
		OnRegisterError: func(err error) {
			log.Error("Error registering Prometheus metric", zap.Error(err))
		},
	})
	tags := map[string]string{"node_name": node.ID()}
	scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
		Prefix:          "/testv",
		Tags:            tags,
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &prometheus.DefaultSanitizerOpts,
	}, time.Duration(5)*time.Second)

	_ = scope
	s := nakamacluster.NewClient(ctx, log, client, serverId, make(map[string]string), *c)
	s.OnDelegate(&Delegate{logger: log, conn: s})

	c2 := nakamacluster.NewConfig()
	c2.Port = 10000 + rand.Intn(10000)
	c2.RetransmitMult = 5
	c2.Prefix = "/nk/samples/"
	serverId2 := fmt.Sprintf("node-server-%d", rand.Intn(10000))
	client2, err := etcdv3.NewClient(ctx, []string{"192.168.0.71:12379", "192.168.0.71:22379", "192.168.0.71:32379"}, etcdv3.ClientOptions{})
	if err != nil {
		log.Fatal("连接etcd失败", zap.Error(err))
	}
	ss := nakamacluster.NewServer(ctx, log, client2, serverId2, "CC", vars, *c2)
	ss.OnDelegate(&Delegate{logger: log, conn: s})
	log.Info("服务启动成功", zap.String("addr", c.Addr), zap.Int("port", c.Port))
	go func() {
		t := time.NewTicker(time.Second * 10)
		defer t.Stop()

		ch := make(chan *api.Envelope, 10)
		stream, err := ss.GetPeers().Connect(ctx, "CC", ch, metadata.Pairs("sss", "dd"))
		if err != nil {
			panic(err)
		}

		for {
			select {
			case <-t.C:
				data := make([]byte, 32)
				binary.BigEndian.PutUint32(data, rand.Uint32())
				s.Send(nakamacluster.NewMessage([]byte("1919")))

				res, err := ss.Rpc(context.Background(), "CCccc", &api.Envelope{Cid: api.Message_BROADCAST})
				fmt.Println("---send zero---", res, err)

				fmt.Println("sream send:", stream.Send(&api.Envelope{Cid: api.Message_SESSIONEND}))

			case m := <-ch:
				fmt.Println("---sream recv---", m.Cid, m.Payload)

			case <-ctx.Done():
				return
			}
		}
	}()

	hs := &http.Server{
		Addr:         fmt.Sprintf(":%d", 30000+rand.Intn(10000)),
		ReadTimeout:  time.Millisecond * 10000,
		WriteTimeout: time.Millisecond * 10000,
		IdleTimeout:  time.Millisecond * 60000,
		Handler:      reporter.HTTPHandler(),
	}

	go func() {
		log.Info("Starting Prometheus server for metrics requests", zap.String("Addr", hs.Addr))
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Prometheus listener failed", zap.Error(err))
		}
	}()

	//s.UpdateMeta(nakamacluster.META_STATUS_READYED, vars)

	sign := make(chan os.Signal, 1)
	signal.Notify(sign, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-sign:
		s.Stop()

	case <-ctx.Done():
	}

	log.Info("服务已经关闭")
	scopeCloser.Close()
	cancel()
	s.Stop()
	ss.Stop()
}
