package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	nakamacluster "github.com/doublemo/nakama-cluster"
	"github.com/doublemo/nakama-cluster/sd"
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
	c.Port = 7356
	node := nakamacluster.Node{
		Id:       "node-2",
		Name:     "nakama",
		Rpc:      "127.0.0.1:7953",
		Weight:   1,
		Region:   "",
		NodeType: 1,
		Addr:     fmt.Sprintf("%s:%d", "127.0.0.1", c.Port),
	}

	s := nakamacluster.NewServer(ctx, log, client, node, *c)
	log.Info("服务启动成功", zap.String("addr", c.Addr), zap.Int("port", c.Port))
	go func() {
		t := time.NewTicker(time.Second * 10)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				data := make([]byte, 32)
				binary.BigEndian.PutUint32(data, rand.Uint32())
				msg := nakamacluster.NewBroadcast("test", data)
				s.Broadcast(*msg)
			case <-ctx.Done():
			}
		}
	}()
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-sign:
		s.Stop()

	case <-ctx.Done():
	}

	log.Info("服务已经关闭")
	cancel()
}
