package common_utils

import (
	"context"
	"encoding/json"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"

	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
)

// NotificationProducer provides utilities for sending messages using notification channel.
// NewNotificationProducer must be called for a new producer.
// Close must be called when finished.
type NotificationProducer struct {
	ch string
	rc *redis.Client
}

// NewNotificationProducer returns a new NotificationProducer.
func NewNotificationProducer(ch string) (*NotificationProducer, error) {
	n := new(NotificationProducer)
	n.ch = ch

	// Create redis client.
	var err error
	n.rc, err = getRedisDBClient()
	if err != nil {
		return nil, err
	}

	return n, nil
}

// Close performs cleanup works.
// Close must be called when finished.
func (n *NotificationProducer) Close() {
	if n.rc != nil {
		db.CloseRedisClient(n.rc)
	}
}

func (n *NotificationProducer) Send(op, data string, kvs map[string]string) error {
	fvs := []string{op, data}
	for k, v := range kvs {
		fvs = append(fvs, k)
		fvs = append(fvs, v)
	}

	val, err := json.Marshal(fvs)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return err
	}
	return n.SendRaw(string(val))
}

func (n *NotificationProducer) SendRaw(data string) error {
	log.V(lvl.DEBUG).Infof("Publishing to channel (raw) %s: %s.", n.ch, data)
	return n.rc.Publish(context.Background(), n.ch, data).Err()
}
