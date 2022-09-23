package nakamacluster

import (
	"context"
	"fmt"
	"time"

	"github.com/doublemo/nakama-cluster/pb"
	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"
)

type Broadcast struct {
	id       uint64
	name     string
	payload  []byte
	finished chan struct{}
}

// Invalidates checks if enqueuing the current broadcast
// invalidates a previous broadcast
func (b *Broadcast) Invalidates(other memberlist.Broadcast) bool {
	nb, ok := other.(memberlist.NamedBroadcast)
	if !ok {
		return false
	}
	return b.Name() == nb.Name()
}

// Returns a byte form of the message
func (b *Broadcast) Message() []byte {
	return b.payload
}

// Finished is invoked when the message will no longer
// be broadcast, either due to invalidation or to the
// transmit limit being reached
func (b *Broadcast) Finished() {
	select {
	case b.finished <- struct{}{}:
	default:
	}
}

func (b *Broadcast) Sended() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-b.finished:
		return true
	case <-ctx.Done():
	}
	return false
}

// NamedBroadcast is an optional extension of the Broadcast interface that
// gives each message a unique string name, and that is used to optimize
//
// You shoud ensure that Invalidates() checks the same uniqueness as tmemberlist
// example below:
//
// func (b *foo) Invalidates(other Broadcast) bool {
// 	nb, ok := other.(NamedBroadcast)
// 	if !ok {
// 		return false
// 	}
// 	return b.Name() == nb.Name()
// }
//
// Invalidates() isn't currently used for NamedBroadcasts, but that may change
// in the future.

// The unique identity of this broadcast message.
func (b *Broadcast) Name() string {
	return b.name
}

// NewBroadcast 创建广播
func NewBroadcast(payload *pb.Notify) *Broadcast {
	bytes, _ := proto.Marshal(payload)
	return &Broadcast{
		id:       payload.Id,
		name:     fmt.Sprint(payload.Id),
		payload:  bytes,
		finished: make(chan struct{}),
	}
}

func NewBroadcastWithError(payload *pb.Notify) (*Broadcast, error) {
	bytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Broadcast{
		id:       payload.Id,
		name:     fmt.Sprint(payload.Id),
		payload:  bytes,
		finished: make(chan struct{}),
	}, nil
}
