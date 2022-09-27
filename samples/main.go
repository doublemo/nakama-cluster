package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	nakamacluster "github.com/doublemo/nakama-cluster"
	"github.com/doublemo/nakama-cluster/pb"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
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
	client, err := sd.NewEtcdV3Client(ctx, []string{"127.0.0.1:12379", "127.0.0.1:22379", "127.0.0.1:32379"}, sd.EtcdClientOptions{})
	if err != nil {
		log.Fatal("连接etcd失败", zap.Error(err))
	}

	c := nakamacluster.NewConfig()
	c.Port = 7355
	node := nakamacluster.Node{
		Id:       "node-1",
		Name:     "nakama",
		Rpc:      "127.0.0.1:7953",
		Weight:   1,
		Region:   "",
		NodeType: 1,
		Addr:     fmt.Sprintf("%s:%d", "127.0.0.1", c.Port),
	}

	// Create Prometheus reporter and root scope.
	reporter := prometheus.NewReporter(prometheus.Options{
		OnRegisterError: func(err error) {
			log.Error("Error registering Prometheus metric", zap.Error(err))
		},
	})
	tags := map[string]string{"node_name": node.Id}
	scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
		Prefix:          "/testv",
		Tags:            tags,
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &prometheus.DefaultSanitizerOpts,
	}, time.Duration(5)*time.Second)

	s := nakamacluster.NewServer(ctx, log, client, node, scope, *c)
	log.Info("服务启动成功", zap.String("addr", c.Addr), zap.Int("port", c.Port))
	go func() {
		t := time.NewTicker(time.Second * 10)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				data := make([]byte, 32)
				binary.BigEndian.PutUint32(data, rand.Uint32())
				id, err := s.NextMessageId()
				if err != nil {
					log.Error("获取ID失败", zap.Error(err))
					return
				}

				msg := nakamacluster.NewBroadcast(&pb.Notify{Id: id, Node: s.Node().Id, Payload: &pb.Notify_Message{Message: &pb.Nakama_Message{Body: []byte{0x1}}}})
				s.Broadcast(*msg)
			case <-ctx.Done():
			}
		}
	}()

	hs := &http.Server{
		Addr:         ":1745",
		ReadTimeout:  time.Millisecond * 10000,
		WriteTimeout: time.Millisecond * 10000,
		IdleTimeout:  time.Millisecond * 60000,
		Handler:      reporter.HTTPHandler(),
	}

	go func() {
		log.Info("Starting Prometheus server for metrics requests", zap.Int("port", 1745))
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Prometheus listener failed", zap.Error(err))
		}
	}()

	s.OnNotifyMsg(func(msg *pb.Notify) {
		log.Debug("收到广播信息", zap.Uint64("id", msg.Id), zap.String("node", msg.Node))
	})

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
}
