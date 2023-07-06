package nakamacluster

import (
	"context"
	"sync"

	"github.com/doublemo/nakama-cluster/api"
)

type PeerStream struct {
	ctx            context.Context
	ctxCancelFn    context.CancelFunc
	messageQueueCh chan *api.Envelope
	stream         api.ApiServer_StreamClient
	once           sync.Once
}

func NewPeerStream(ctx context.Context, messageQueueCh chan *api.Envelope, stream api.ApiServer_StreamClient) *PeerStream {
	ctx, cancel := context.WithCancel(ctx)
	s := &PeerStream{
		ctx:            ctx,
		ctxCancelFn:    cancel,
		stream:         stream,
		messageQueueCh: messageQueueCh,
	}

	go s.recv()
	return s
}

func (s *PeerStream) Send(msg *api.Envelope) error {
	return s.stream.Send(msg)
}

func (s *PeerStream) recv() error {
	defer func() {
		s.stream.CloseSend()
	}()

	for {
		out, err := s.stream.Recv()
		if err != nil {
			return err
		}

		select {
		case s.messageQueueCh <- out:
		case <-s.ctx.Done():
			return nil
		}
	}
}

func (s *PeerStream) Close() {
	s.once.Do(func() {
		if s.ctxCancelFn != nil {
			s.ctxCancelFn()
		}
	})
}
