package mq

import pb "github.com/yeqown/goim/api/logic/grpc"

// IMQProducer .
type IMQProducer interface {
	Produce(topic string, body []byte) error
}

// IMQConsumer .
type IMQConsumer interface {
	Consume() <-chan *pb.PushMsg
}

// IMQProduceComsumer .
type IMQProduceComsumer interface {
	IMQProducer
	IMQConsumer
}
