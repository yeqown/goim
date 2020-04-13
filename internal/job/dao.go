package job

// import (
// 	"context"
// 	"fmt"
// 	"log"

// 	pb "github.com/yeqown/goim/api/logic/grpc"
// 	"github.com/yeqown/goim/internal/job/conf"
// 	"github.com/gogo/protobuf/proto"

// 	// "github.com/youzan/go-nsq"
// 	"github.com/nsqio/go-nsq"
// )

// var (
// 	_ JobConsumer = &nsqConsumer{}
// 	_ nsq.Handler = &nsqConsumer{}
// )

// // JobConsumer interface .
// type JobConsumer interface {
// 	Consume(j *Job)
// 	Close() error
// }

// type kafkaConsumer struct {
// 	consumer *cluster.Consumer
// }

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

// func NewKafka(c *conf.Config) *kafkaConsumer {
// 	return &kafkaConsumer{
// 		consumer: newKafkaSub(c.Kafka),
// 	}
// }

// func (c *kafkaConsumer) Close() error {
// 	return c.consumer.Close()
// }

// // Consume messages, watch signals
// func (c *kafkaConsumer) Consume(j *Job) {
// 	for {
// 		select {
// 		case err := <-c.consumer.Errors():
// 			log.Errorf("consumer error(%v)", err)
// 		case n := <-c.consumer.Notifications():
// 			log.Infof("consumer rebalanced(%v)", n)
// 		case msg, ok := <-c.consumer.Messages():
// 			if !ok {
// 				return
// 			}
// 			c.consumer.MarkOffset(msg, "")
// 			// process push message
// 			pushMsg := new(pb.PushMsg)
// 			if err := proto.Unmarshal(msg.Value, pushMsg); err != nil {
// 				log.Errorf("proto.Unmarshal(%v) error(%v)", msg, err)
// 				continue
// 			}
// 			if err := j.push(context.Background(), pushMsg); err != nil {
// 				log.Errorf("c.push(%v) error(%v)", pushMsg, err)
// 			}
// 			log.Infof("consume: %s/%d/%d\t%s\t%+v", msg.Topic, msg.Partition, msg.Offset, msg.Key, pushMsg)
// 		}
// 	}
// }

// // type nsqConsumer struct {
// // 	consumer *nsq.Consumer
// // 	job      *Job
// // }

// // // NewNSQ .
// // func NewNSQ(c *conf.NSQ, j *Job) *nsqConsumer {
// // 	cfg := nsq.NewConfig()
// // 	consumer, err := nsq.NewConsumer(c.Topic, c.Channel, cfg)
// // 	if err != nil {
// // 		log.Fatal(err)
// // 	}
// // 	jobc := &nsqConsumer{
// // 		consumer: consumer,
// // 		job:      j,
// // 	}

// // 	jobc.consumer.AddConcurrentHandlers(jobc, 1)
// // 	if err := jobc.consumer.ConnectToNSQLookupd(c.LookupAddr); err != nil {
// // 		log.Fatal(err)
// // 	}

// // 	return jobc
// // }

// // impl nsq.Handler
// func (c *nsqConsumer) HandleMessage(msg *nsq.Message) (err error) {
// 	// ctx := context.Background()
// 	// process push message
// 	pushMsg := new(pb.PushMsg)

// 	if err = proto.Unmarshal(msg.Body, pushMsg); err != nil {
// 		err = fmt.Errorf("proto.Unmarshal(%v) error(%v)", msg, err)
// 		return
// 	}

// 	if err = c.job.push(context.Background(), pushMsg); err != nil {
// 		err = fmt.Errorf("push(%v) error(%v)", pushMsg, err)
// 		return
// 	}

// 	fmt.Printf("consume:%s \t%+v", msg.Body, pushMsg)
// 	// <-ctx.Done()
// 	return nil
// }

// // Close .
// func (c *nsqConsumer) Close() error {
// 	c.consumer.Stop()
// 	return nil
// }
