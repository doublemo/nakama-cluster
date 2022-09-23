package nakamacluster

import (
	"github.com/hashicorp/memberlist"
)

type Broadcast struct {
	name    string
	message []byte
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
	return b.message
}

// Finished is invoked when the message will no longer
// be broadcast, either due to invalidation or to the
// transmit limit being reached
func (b *Broadcast) Finished() {
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
func NewBroadcast(name string, message []byte) *Broadcast {
	return &Broadcast{
		name:    name,
		message: message,
	}
}
