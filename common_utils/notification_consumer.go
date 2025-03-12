package common_utils

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
)

// NotificationConsumer provides utilities for receiving messages using notification channel.
// NewNotificationConsumer must be called for a new consumer.
// Close must be called when finished.
type NotificationConsumer struct {
	rc       *redis.Client
	ps       *redis.PubSub
	ch       <-chan *redis.Message
	callback func(*redis.Message)
}

// NewNotificationConsumer returns a new NotificationConsumer.
func NewNotificationConsumer(ch string, callback func(*redis.Message)) (*NotificationConsumer, error) {
	if callback == nil {
		return nil, fmt.Errorf("Failed to create NotificationConsumer: callback is nil")
	}
	n := &NotificationConsumer{}

	log.V(lvl.DEBUG).Infof("NewNotificationConsumer channel: %v", ch)

	// Create redis client.
	var err error
	if n.rc, err = getRedisDBClient(); err != nil {
		return nil, err
	}

	// Subscribe to channel
	n.ps = n.rc.Subscribe(context.Background(), ch)
	// Wait for the subscription to be active
	if _, err := n.ps.Receive(context.Background()); err != nil {
		return nil, err
	}

	n.ch = n.ps.Channel()
	n.callback = callback
	go n.consume()

	return n, nil
}

// Close performs cleanup work.
// Close must be called when finished.
func (n *NotificationConsumer) Close() {
	if n.ps != nil {
		n.ps.Close()
	}
	if n.rc != nil {
		db.CloseRedisClient(n.rc)
	}
}

// consume will read messages on the channel and call the callback function
// every time there is a new msg. It returns when the channel is closed.
func (n *NotificationConsumer) consume() {
	for msg := range n.ch {
		n.callback(msg)
		time.Sleep(1 * time.Second)
	}
}
