package nakamacluster

import (
	"github.com/doublemo/nakama-cluster/api"
)

type Delegate interface {
	// LocalState 发送本地状态信息
	LocalState(join bool) []byte

	// MergeRemoteState 发送本地状态信息
	MergeRemoteState(buf []byte, join bool)

	// NotifyJoin 接收节点加入通知
	NotifyJoin(node *Meta)

	// NotifyLeave 接收节点离线通知
	NotifyLeave(node *Meta)

	// NotifyUpdate 接收节点更新通知
	NotifyUpdate(node *Meta)

	// NotifyAlive 接收节点活动通知
	NotifyAlive(node *Meta) error

	// NotifyMsg 接收节来至其它节点的信息
	NotifyMsg(msg *api.Envelope) (*api.Envelope, error)
}
