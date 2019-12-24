package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/linkedin/goavro"

	"alibaba-dts-go/avro"
)

var (
	r io.Reader

	// 仅需改动以下配置即可
	// ***********************************************************
	kafkaUser       = "username"
	kafkaPassWord   = "password"
	kafkaTopic      = "topic"
	kafkaGroupId    = "groupid"
	kafkaBrokerList = []string{"localhost:9020"}
	// ***********************************************************
)

func main() {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Net.MaxOpenRequests = 100
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Net.SASL.Enable = true
	config.Net.SASL.User = kafkaUser + "-" + kafkaGroupId
	config.Net.SASL.Password = kafkaPassWord
	config.Version = sarama.V0_11_0_0

	consumer, err := cluster.NewConsumer(kafkaBrokerList, kafkaGroupId, []string{kafkaTopic}, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			panic(err)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Println("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				r = bytes.NewReader(msg.Value)

				_, err := avro.DeserializeRecord(r)
				if err != nil {
					log.Fatal(err)
				}

				t := avro.NewRecord()
				codec, err := goavro.NewCodec(t.Schema())
				if err != nil {
					log.Fatal(err)
				}
				native, _, err := codec.NativeFromBinary(msg.Value)
				if err != nil {
					log.Fatal("error:", err)
				}
				fmt.Println("native:", native)
				texual, err := codec.TextualFromNative(nil, native)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("texual:", string(texual))
			}
		case <-signals:
			return
		}
	}
}
