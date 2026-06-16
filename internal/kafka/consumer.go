package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps kafka-go readers for multiple topics.
type Consumer struct {
	readers []*kafka.Reader
}

// NewConsumer creates readers for the given topics under one consumer group.
func NewConsumer(brokers []string, groupID string, topics []string) *Consumer {
	c := &Consumer{}
	for _, topic := range topics {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10e6,
		})
		c.readers = append(c.readers, r)
	}
	return c
}

// MessageHandler processes a single Kafka message.
type MessageHandler func(ctx context.Context, topic string, key, value []byte) error

// Run starts consuming from all topics. Blocks until context is cancelled.
func (c *Consumer) Run(ctx context.Context, handler MessageHandler) {
	for _, r := range c.readers {
		go func(reader *kafka.Reader) {
			topic := reader.Config().Topic
			for {
				m, err := reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("error fetching from %s: %v", topic, err)
					continue
				}
				if err := handler(ctx, topic, m.Key, m.Value); err != nil {
					log.Printf("error handling message from %s: %v", topic, err)
					continue
				}
				if err := reader.CommitMessages(ctx, m); err != nil {
					log.Printf("error committing offset for %s: %v", topic, err)
				}
			}
		}(r)
	}
	<-ctx.Done()
}

// Close closes all readers.
func (c *Consumer) Close() {
	for _, r := range c.readers {
		if err := r.Close(); err != nil {
			log.Printf("error closing kafka reader: %v", err)
		}
	}
}
