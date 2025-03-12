package common_utils

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var received = make(chan bool)

func receive(msg *redis.Message) {
	if msg == nil {
		received <- false
		return
	}
	payload := []string{}
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		received <- false
		return
	}
	if payload[0] != "testOp" || payload[1] != "testData" {
		received <- false
		return
	}
	received <- true
}

func TestNotificationConsumerCreateSucceeds(t *testing.T) {
	channel := "CREATE_SUCCEED"
	n, err := NewNotificationConsumer(channel, func(*redis.Message) {})
	if err != nil {
		t.Fatalf("Unexpected error during creation: %v", err)
	}
	n.Close()
}

func TestNotificationConsumerCreateFails(t *testing.T) {
	channel := "CREATE_FAIL"
	_, err := NewNotificationConsumer(channel, nil)
	if err == nil {
		t.Fatalf("Expected error during creation, got %v", err)
	}
}

func TestNotificationConsumerSucceedsWithChannelPublish(t *testing.T) {
	channel := "PUBLISH_SUCCEED"
	c, err := NewNotificationConsumer(channel, receive)
	if err != nil {
		t.Fatalf("Unexpected error during creation: %v", err)
	}
	defer c.Close()
	p, err := NewNotificationProducer(channel)
	if err != nil {
		t.Fatalf("Unexpected error during creation: %v", err)
	}
	defer p.Close()

	p.Send("testOp", "testData", map[string]string{})
	select {
	case valid := <-received:
		if !valid {
			t.Fatal("Invalid payload received")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for response")
	}
}

func TestNotificationConsumerWithChannelClose(t *testing.T) {
	channel := "CHANNEL_CLOSE"
	c, err := NewNotificationConsumer(channel, func(*redis.Message) {})
	if err != nil {
		t.Fatalf("Unexpected error during creation: %v", err)
	}

	p, err := NewNotificationProducer(channel)
	if err != nil {
		t.Fatalf("Unexpected error during creation: %v", err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			p.Send("testOp", "testData", map[string]string{})
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(50 * time.Millisecond)

	// Close the consumer to check for crash/panic
	t.Log("Closing the NotificationConsumer")
	c.Close()
	wg.Wait()
}
