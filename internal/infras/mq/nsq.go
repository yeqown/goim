package mq

import (
	"fmt"

	pb "github.com/yeqown/goim/api/logic/grpc"

	"github.com/gogo/protobuf/proto"
	nsqlib "github.com/nsqio/go-nsq"
)

type nsq struct {
	p *nsqlib.Producer
	c *nsqlib.Consumer

	chPushMsg                  chan *pb.PushMsg // MQ消费消息队列
	openConsumer, openProducer bool             // 生产者消费者开启标志
}

// NSQConf .
type NSQConf struct {
	Topic      string
	Channel    string
	LookupAddr string
}

// NewNSQ .
func NewNSQ(c *NSQConf, openConsumer bool, openProducer bool) (v IMQProduceComsumer, err error) {
	imq := nsq{
		chPushMsg:    make(chan *pb.PushMsg, 128),
		openConsumer: openConsumer,
		openProducer: openProducer,
	}
	cfg := nsqlib.NewConfig()

	if openProducer {
		imq.p, err = nsqlib.NewProducer(c.LookupAddr, cfg)
		if err != nil {
			return
		}
	}

	if openConsumer {
		imq.c, err = nsqlib.NewConsumer(c.Topic, c.Channel, cfg)
		if err != nil {
			return
		}

		imq.c.AddConcurrentHandlers(&imq, 4)
		if err = imq.c.ConnectToNSQLookupd(c.LookupAddr); err != nil {
			return
		}
	}

	v = &imq
	return

}

func (nsq *nsq) Produce(topic string, body []byte) error {
	return nsq.p.Publish(topic, body)
}

func (nsq *nsq) Consume() <-chan *pb.PushMsg {
	return nsq.chPushMsg
}

// impl nsq.Handler
func (nsq *nsq) HandleMessage(msg *nsqlib.Message) (err error) {
	// ctx := context.Background()
	// process push message
	pushMsg := new(pb.PushMsg)

	if err = proto.Unmarshal(msg.Body, pushMsg); err != nil {
		err = fmt.Errorf("proto.Unmarshal(%v) error(%v)", msg, err)
		return
	}

	nsq.chPushMsg <- pushMsg
	// if err = c.job.push(context.Background(), pushMsg); err != nil {
	// 	err = fmt.Errorf("push(%v) error(%v)", pushMsg, err)
	// 	return
	// }

	// fmt.Printf("consume:%s \t%+v", msg.Body, pushMsg)
	// <-ctx.Done()
	return nil
}
