package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yeqown/goim/internal/infras/mq"
	"github.com/yeqown/goim/internal/job/conf"

	"github.com/bilibili/discovery/naming"
	log "github.com/golang/glog"
)

// Job is push job.
type Job struct {
	c            *conf.Config      // Job 配置
	imq          mq.IMQConsumer    // MQ consumer
	cometServers map[string]*Comet // Comet 实例
	rooms        map[string]*Room  // 房间管理，用于合并房间消息的数据结构
	roomsMutex   sync.RWMutex      // rooms 读写锁
	// consumer     *cluster.Consumer
	// consumer     *nsqConsumer
}

// New new a push job.
func New(c *conf.Config) *Job {
	imq, err := mq.NewNSQ(c.Nsq, true, false)
	if err != nil {
		panic(err)
	}
	j := &Job{
		c:     c,
		imq:   imq,
		rooms: make(map[string]*Room),
		// consumer: NewNSQ(c.Nsq, nil),
		// consumer: newKafka(c.Kafka),
	}

	j.Consume()               // start an goroutine to consume message from mq
	j.watchComet(c.Discovery) // watch comet address changing
	return j
}

// func newKafkaSub(c *conf.Kafka) *cluster.Consumer {
// 	config := cluster.NewConfig()
// 	config.Consumer.Return.Errors = true
// 	config.Group.Return.Notifications = true
// 	consumer, err := cluster.NewConsumer(c.Brokers, c.Group, []string{c.Topic}, config)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return consumer
// }

// Close close resounces.
func (j *Job) Close() error {
	// if j.imq != nil {
	// 	return j.consumer.Close()
	// }
	return nil
}

// Consume messages, watch signals
// func (j *Job) Consume() {
// 	for {
// 		select {
// 		case err := <-j.consumer.Errors():
// 			log.Errorf("consumer error(%v)", err)
// 		case n := <-j.consumer.Notifications():
// 			log.Infof("consumer rebalanced(%v)", n)
// 		case msg, ok := <-j.consumer.Messages():
// 			if !ok {
// 				return
// 			}
// 			j.consumer.MarkOffset(msg, "")
// 			// process push message
// 			pushMsg := new(pb.PushMsg)
// 			if err := proto.Unmarshal(msg.Value, pushMsg); err != nil {
// 				log.Errorf("proto.Unmarshal(%v) error(%v)", msg, err)
// 				continue
// 			}
// 			if err := j.push(context.Background(), pushMsg); err != nil {
// 				log.Errorf("j.push(%v) error(%v)", pushMsg, err)
// 			}
// 			log.Infof("consume: %s/%d/%d\t%s\t%+v", msg.Topic, msg.Partition, msg.Offset, msg.Key, pushMsg)
// 		}
// 	}
// }

// Consume .
func (j *Job) Consume() {
	go func() {
		for msg := range j.imq.Consume() {
			err := j.push(context.Background(), msg)
			log.Errorf("j.push(%+v) error(%+v)", msg, err)
		}
	}()
}

// TODO: 调整这个函数逻辑
// 维持Comet服务地址
func (j *Job) watchComet(c *naming.Config) {
	dis := naming.New(c)
	resolver := dis.Build("goim.comet")
	event := resolver.Watch()
	select {
	case _, ok := <-event:
		if !ok {
			panic("watchComet init failed")
		}
		if ins, ok := resolver.Fetch(); ok {
			if err := j.newAddress(ins.Instances); err != nil {
				panic(err)
			}
			log.Infof("watchComet init newAddress:%+v", ins)
		}
	case <-time.After(10 * time.Second):
		log.Error("watchComet init instances timeout")
	}
	go func() {
		for {
			if _, ok := <-event; !ok {
				log.Info("watchComet exit")
				return
			}
			ins, ok := resolver.Fetch()
			if ok {
				if err := j.newAddress(ins.Instances); err != nil {
					log.Errorf("watchComet newAddress(%+v) error(%+v)", ins, err)
					continue
				}
				log.Infof("watchComet change newAddress:%+v", ins)
			}
		}
	}()
}

func (j *Job) newAddress(insMap map[string][]*naming.Instance) error {
	ins := insMap[j.c.Env.Zone]
	if len(ins) == 0 {
		return fmt.Errorf("watchComet instance is empty")
	}
	comets := map[string]*Comet{}
	for _, in := range ins {
		if old, ok := j.cometServers[in.Hostname]; ok {
			comets[in.Hostname] = old
			continue
		}
		c, err := NewComet(in, j.c.Comet)
		if err != nil {
			log.Errorf("watchComet NewComet(%+v) error(%v)", in, err)
			return err
		}
		comets[in.Hostname] = c
		log.Infof("watchComet AddComet grpc:%+v", in)
	}
	for key, old := range j.cometServers {
		if _, ok := comets[key]; !ok {
			old.cancel()
			log.Infof("watchComet DelComet:%s", key)
		}
	}
	j.cometServers = comets
	return nil
}
