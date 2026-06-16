package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer wraps a kafka-go writer.
type Producer struct {
	writers map[string]*kafka.Writer
	brokers []string
}

// NewProducer creates a producer that writes to multiple topics.
func NewProducer(brokers []string, topics []string) *Producer {
	p := &Producer{
		writers: make(map[string]*kafka.Writer),
		brokers: brokers,
	}
	for _, topic := range topics {
		p.writers[topic] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 10 * time.Millisecond,
			RequiredAcks: kafka.RequireOne,
		}
	}
	return p
}

// Publish sends a message to the specified topic, keyed by chargerID for partition ordering.
func (p *Producer) Publish(ctx context.Context, topic, chargerID string, value []byte) error {
	w, ok := p.writers[topic]
	if !ok {
		log.Printf("WARN: unknown topic %s, creating writer on the fly", topic)
		w = &kafka.Writer{
			Addr:         kafka.TCP(p.brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 10 * time.Millisecond,
			RequiredAcks: kafka.RequireOne,
		}
		p.writers[topic] = w
	}
	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(chargerID),
		Value: value,
	})
}

// Close closes all writers.
func (p *Producer) Close() {
	for _, w := range p.writers {
		if err := w.Close(); err != nil {
			log.Printf("error closing kafka writer: %v", err)
		}
	}
}
