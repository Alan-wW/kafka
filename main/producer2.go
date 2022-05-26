package main

import (
	"github.com/vmihailenco/msgpack"
	"log"

	"github.com/Shopify/sarama"
	"github.com/demdxx/gocast"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("42.193.99.39:9092").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("xingbin_test").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

type Student struct {
	Id int
	Name string
}
func main() {
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = *maxRetry
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(*brokerList, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()
	//st := Student{1,"xingbin"
	st := "abc"
	marshal, _ := msgpack.Marshal(st)
	toString := gocast.ToString(marshal)
	msg := &sarama.ProducerMessage{
		Topic: *topic,
		Value: sarama.StringEncoder(toString),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)
}