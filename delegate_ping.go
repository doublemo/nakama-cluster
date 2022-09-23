package nakamacluster

import (
	"time"

	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// AckPayload is invoked when an ack is being sent; the returned bytes will be appended to the ack
func (s *Delegate) AckPayload() []byte {
	return []byte{}
}

// NotifyPing is invoked when an ack for a ping is received
func (s *Delegate) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	s.logger.Debug("NotifyPingComplete", zap.Any("node", other.Name), zap.String("rtt", rtt.String()), zap.Binary("payload", payload))
}
