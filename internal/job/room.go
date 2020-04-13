package job

import (
	"errors"
	"time"

	comet "github.com/yeqown/goim/api/comet/grpc"
	"github.com/yeqown/goim/internal/job/conf"
	"github.com/yeqown/goim/pkg/bytes"

	log "github.com/golang/glog"
)

var (
	// ErrComet commet error.
	ErrComet = errors.New("comet rpc is not available")
	// ErrCometFull comet chan full.
	ErrCometFull = errors.New("comet proto chan full")
	// ErrRoomFull room chan full.
	ErrRoomFull = errors.New("room proto chan full")

	roomReadyProto = new(comet.Proto)
)

// Room room.
type Room struct {
	c     *conf.Room        // 关于房间的配置
	job   *Job              // 绑定job，为了追溯Room所属的Job
	id    string            // 房间ID
	proto chan *comet.Proto // 房间发送消息，缓冲区
}

// NewRoom new a room struct, store channel room info.
func NewRoom(job *Job, id string, c *conf.Room) (r *Room) {
	r = &Room{
		c:     c,
		id:    id,
		job:   job,
		proto: make(chan *comet.Proto, c.Batch*2),
	}
	go r.pushproc(c.Batch, time.Duration(c.Signal))
	return
}

// Push push msg to the room, if chan full discard it.
func (r *Room) Push(op int32, msg []byte) (err error) {
	var p = &comet.Proto{
		Ver:  1,
		Op:   op,
		Body: msg,
	}
	select {
	case r.proto <- p:
	default:
		err = ErrRoomFull
	}
	return
}

// pushproc merge proto and push msgs in batch.
func (r *Room) pushproc(batch int, sigTime time.Duration) {
	var (
		n    int
		last time.Time
		p    *comet.Proto
		buf  = bytes.NewWriterSize(int(comet.MaxBodySize)) // 4096B = 4KB
	)
	log.Infof("start room:%s goroutine", r.id)
	td := time.AfterFunc(sigTime, func() {
		select {
		case r.proto <- roomReadyProto:
		default:
		}
	})
	defer td.Stop()
	for {
		if p = <-r.proto; p == nil {
			// 异常数据
			break
		} else if p != roomReadyProto {
			// buf 会自动扩容
			p.WriteTo(buf)
			if n++; n == 1 {
				// 在超时期内，第一个数据包重置定时器为sigTime, 默认配置1s
				last = time.Now()
				td.Reset(sigTime)
				continue
			} else if n < batch {
				if sigTime > time.Since(last) {
					// 容量内，且超时时间未到，则继续
					continue
				}
			}
			// 超出容量
		} else {
			// 定时器时间到
			if n == 0 {
				// 消息已经发送了，且超出了最大闲置时间
				break
			}
		}
		_ = r.job.broadcastRoomRawBytes(r.id, buf.Buffer())
		// TODO use reset buffer
		// after push to room channel, renew a buffer, let old buffer gc
		buf = bytes.NewWriterSize(buf.Size())
		n = 0

		// 设定“房间”的最大闲置时间
		if r.c.Idle != 0 {
			td.Reset(time.Duration(r.c.Idle))
		} else {
			td.Reset(time.Minute)
		}
	}
	r.job.delRoom(r.id)
	log.Infof("room:%s goroutine exit", r.id)
}

// 删除一个房间
// 清理工作时候调用
func (j *Job) delRoom(roomID string) {
	j.roomsMutex.Lock()
	delete(j.rooms, roomID)
	j.roomsMutex.Unlock()
}

// 从Job.romms中取得，如果不存在则新建一个
// 并发安全(mutex)
func (j *Job) getRoom(roomID string) *Room {
	j.roomsMutex.RLock()
	room, ok := j.rooms[roomID]
	j.roomsMutex.RUnlock()
	if !ok {
		j.roomsMutex.Lock()
		if room, ok = j.rooms[roomID]; !ok {
			room = NewRoom(j, roomID, j.c.Room)
			j.rooms[roomID] = room
		}
		j.roomsMutex.Unlock()
		log.Infof("new a room:%s active:%d", roomID, len(j.rooms))
	}
	return room
}
