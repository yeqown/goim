package comet

import (
	"sync"

	"github.com/yeqown/goim/api/comet/grpc"
	"github.com/yeqown/goim/pkg/bufio"
)

// Channel used by message pusher send msg to write goroutine.
type Channel struct {
	Room     *Room            // 指向Channel所在房间
	CliProto Ring             // RingBuffer
	signal   chan *grpc.Proto // mssage channel with buffer
	Writer   bufio.Writer     // 写缓冲区
	Reader   bufio.Reader     // 读缓冲区
	Next     *Channel
	Prev     *Channel

	Mid      int64              // 用户ID
	Key      string             // 链接的唯一标识
	IP       string             // IP地址
	watchOps map[int32]struct{} // 监听的消息类型
	mutex    sync.RWMutex
}

// NewChannel new a channel.
func NewChannel(cli, svr int) *Channel {
	c := new(Channel)
	c.CliProto.Init(cli)
	c.signal = make(chan *grpc.Proto, svr)
	c.watchOps = make(map[int32]struct{})
	return c
}

// Watch watch a operation.
func (c *Channel) Watch(accepts ...int32) {
	c.mutex.Lock()
	for _, op := range accepts {
		c.watchOps[op] = struct{}{}
	}
	c.mutex.Unlock()
}

// UnWatch unwatch an operation
func (c *Channel) UnWatch(accepts ...int32) {
	c.mutex.Lock()
	for _, op := range accepts {
		delete(c.watchOps, op)
	}
	c.mutex.Unlock()
}

// NeedPush verify if in watch.
func (c *Channel) NeedPush(op int32) bool {
	c.mutex.RLock()
	if _, ok := c.watchOps[op]; ok {
		c.mutex.RUnlock()
		return true
	}
	c.mutex.RUnlock()
	return false
}

// Push server push message.
func (c *Channel) Push(p *grpc.Proto) (err error) {
	select {
	case c.signal <- p:
	default:
	}
	return
}

// Ready check the channel ready or close?
func (c *Channel) Ready() *grpc.Proto {
	return <-c.signal
}

// Signal send signal to the channel, protocol ready.
func (c *Channel) Signal() {
	c.signal <- grpc.ProtoReady
}

// Close close the channel.
func (c *Channel) Close() {
	c.signal <- grpc.ProtoFinish
}
