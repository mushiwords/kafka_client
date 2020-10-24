package main

import (
	"encoding/json"
	"fmt"
	"kafka_client/kafka"
)

func main() {

	addr := []string{"kafka-server.yycaptain.com:9092"}
	producer, err := kafka.NewKafkaProducer(addr,
		"my_result_topic",
		"my-producer")
	if err != nil {
		fmt.Printf("err:%v", err)
	}
	kafkaProduce := &[]ProduceMessage{{
		Id:     "i-666666",
		Name:   "lili",
		Age:    16,
		Gender: "male",
	}}

	jsonStr, _ := json.Marshal(kafkaProduce)

	if err := producer.ProduceMessage(jsonStr); err != nil {
		fmt.Printf("Produce Message Failed. Error: %v\n", err)
		return
	}
	fmt.Printf("Produce Message Success.\n")

}

type ProduceMessage struct {
	Id     string `json:"id"`
	Name   string `json:"name"`
	Age    int    `json:"age"`
	Gender string `json:"gender"`
}
