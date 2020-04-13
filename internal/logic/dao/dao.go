package dao

import (
	"context"
	"time"

	"github.com/yeqown/goim/internal/infras/mq"
	"github.com/yeqown/goim/internal/logic/conf"

	log "github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
)

// Dao dao.
type Dao struct {
	c           *conf.Config
	imq         mq.IMQProducer
	redis       *redis.Pool
	redisExpire int32
}

// New new a dao and return.
func New(c *conf.Config) *Dao {
	imq, err := mq.NewNSQ(c.Nsq, false, true)
	if err != nil {
		log.Fatal(err)
	}

	d := &Dao{
		c:           c,
		imq:         imq,
		redis:       newRedis(c.Redis),
		redisExpire: int32(time.Duration(c.Redis.Expire) / time.Second),
	}
	return d
}

// func newNsqProducer(c *conf.NSQ) *nsq.Producer {
// 	cfg := nsq.NewConfig()
// 	np, err := nsq.NewProducer(c.Addr, cfg)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	return np
// }

// func newKafkaPub(c *conf.Kafka) kafka.SyncProducer {
// 	kc := kafka.NewConfig()
// 	kc.Producer.RequiredAcks = kafka.WaitForAll // Wait for all in-sync replicas to ack the message
// 	kc.Producer.Retry.Max = 10                  // Retry up to 10 times to produce the message
// 	kc.Producer.Return.Successes = true
// 	pub, err := kafka.NewSyncProducer(c.Brokers, kc)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return pub
// }

func newRedis(c *conf.Redis) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     c.Idle,
		MaxActive:   c.Active,
		IdleTimeout: time.Duration(c.IdleTimeout),
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(c.Network, c.Addr,
				redis.DialConnectTimeout(time.Duration(c.DialTimeout)),
				redis.DialReadTimeout(time.Duration(c.ReadTimeout)),
				redis.DialWriteTimeout(time.Duration(c.WriteTimeout)),
				redis.DialPassword(c.Auth),
			)
			if err != nil {
				log.Errorf("newRedis failed, err=%v, auth=%s", err, c.Auth)
				return nil, err
			}
			return conn, nil
		},
	}
}

// Close close the resource.
func (d *Dao) Close() error {
	return d.redis.Close()
}

// Ping dao ping.
func (d *Dao) Ping(c context.Context) error {
	return d.pingRedis(c)
}
