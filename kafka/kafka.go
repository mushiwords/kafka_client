package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"time"
)

/**
 * 初始化
 **/
func (k *KafkaProducer) init() error {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = k.clientId

	saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	saramaConfig.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	saramaConfig.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	saramaConfig.Producer.Return.Successes = true

	saramaConfig.Net.DialTimeout = time.Duration(3) * time.Second
	saramaConfig.Net.ReadTimeout = time.Duration(3) * time.Second
	saramaConfig.Net.WriteTimeout = time.Duration(3) * time.Second

	producer, err := sarama.NewAsyncProducer(k.addr, saramaConfig)
	if err != nil {
		return err
	}

	k.producer = producer
	return nil
}

func (k *KafkaConsumer) init() error {
	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = k.clientId
	clusterConfig.Consumer.Offsets.CommitInterval = 1 * time.Second
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerTopics := []string{k.topic}
	clusterConsumer, err := cluster.NewConsumer(k.addr, k.groupId, consumerTopics, clusterConfig)
	if err != nil {
		return err
	}

	k.clusterConsumer = clusterConsumer
	return nil
}

/**
 * 新建一个生产者实例
 **/
func NewKafkaProducer(addr []string, topic, clientId string) (*KafkaProducer, error) {
	kafkaProducer := &KafkaProducer{
		addr:     addr,
		clientId: clientId,
		topic:    topic,
	}

	if err := kafkaProducer.init(); err != nil {
		return nil, err
	}

	return kafkaProducer, nil
}

/**
 * 新建一个消费者实例
 **/
func NewKafkaConsumer(addr []string, topic, clientId, groupId string) (*KafkaConsumer, error) {
	kafkaComsumer := &KafkaConsumer{
		addr:     addr,
		groupId:  groupId,
		clientId: clientId,
		topic:    topic,
	}

	if err := kafkaComsumer.init(); err != nil {
		return nil, err
	}
	return kafkaComsumer, nil
}

type ConsumerFunc func(*sarama.ConsumerMessage) error

/**
 * Kafka 获取消息
 **/
func (k *KafkaConsumer) ConsumerMessage(f ConsumerFunc) {
	for {
		select {
		case msg, ok := <-k.clusterConsumer.Messages():
			if ok {
				if err := f(msg); err == nil { // 操作成功，mark
					k.clusterConsumer.MarkOffset(msg, "")
				}
			}
		case err := <-k.clusterConsumer.Errors():
			if k.logger != nil {
				k.logger.LogError("Consumer Recieve message error: %v", err)
			}
		}
	}
}

func (k *KafkaConsumer) SetLogger(l Logger) {
	k.logger = l
}

/**
 * Kafka 发送消息
 **/
func (k *KafkaProducer) ProduceMessage(result []byte) error {
	var byteEncoder ByteEncoder = result

	k.producer.Input() <- &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   nil,
		Value: &byteEncoder,
	}

	select {
	case <-k.producer.Successes():
		return nil
	case err := <-k.producer.Errors():
		return err
	case <-time.After(time.Second * 2):
		return fmt.Errorf("Producer send message timeout.")
	}
	return nil
}

func (k *KafkaProducer) SetLogger(l Logger) {
	k.logger = l
}
type Logger interface {
	LogDebug(format string, outpara ...interface{})
	LogError(format string, outpara ...interface{})
}

/**
 * Kafka 生产者
 **/
type KafkaProducer struct {
	addr     []string // kafka地址
	clientId string   // 客户端ID，用作日志，可任意自定义
	topic    string   // 订阅Topic

	logger   Logger
	producer sarama.AsyncProducer
}
type ByteEncoder []byte

func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}

func (b ByteEncoder) Length() int {
	return len(b)
}

/**
 * Kafka 订阅者
 **/
type KafkaConsumer struct {
	addr     []string // kafka地址
	clientId string   // 客户端ID，用作日志，可任意自定义
	groupId  string   // 订阅者组ID，相同组内只有一个实例可以接收消息
	topic    string   // 订阅Topic

	logger          Logger
	clusterConsumer *cluster.Consumer
}


