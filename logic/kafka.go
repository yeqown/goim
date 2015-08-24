package main

import (
	"encoding/json"

	log "code.google.com/p/log4go"
	"github.com/Shopify/sarama"
	"github.com/Terry-Mao/goim/define"
)

const (
	// TODO config
	KafkaPushsTopic = "KafkaPushsTopic"
)

var (
	producer sarama.SyncProducer
)

func InitKafka(kafkaAddrs []string) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err = sarama.NewSyncProducer(kafkaAddrs, config)
	return
}

func multiPushTokafka(cometIds []int32, subkeys [][]string, msg []byte) (err error) {
	var (
		vBytes []byte
		v      = &define.KafkaPushsMsg{CometIds: cometIds, Subkeys: subkeys, Msg: msg}
	)
	// TODO PB
	if vBytes, err = json.Marshal(v); err != nil {
		return
	}
	message := &sarama.ProducerMessage{Topic: KafkaPushsTopic, Key: sarama.StringEncoder(define.KAFKA_MESSAGE_MULTI), Value: sarama.ByteEncoder(vBytes)}
	if _, _, err = producer.SendMessage(message); err != nil {
		return
	}
	log.Debug("produce msg ok, msg:%s", msg)
	return
}

func broadcastTokafka(msg []byte) (err error) {
	message := &sarama.ProducerMessage{Topic: KafkaPushsTopic, Key: sarama.StringEncoder(define.KAFKA_MESSAGE_BROADCAST), Value: sarama.ByteEncoder(msg)}
	if _, _, err = producer.SendMessage(message); err != nil {
		return
	}
	log.Debug("produce msg ok, broadcast msg:%s", msg)
	return
}