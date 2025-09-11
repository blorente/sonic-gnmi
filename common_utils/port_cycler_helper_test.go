package common_utils

import (
	"testing"

	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

func TestPortCyclerDisableAckd(t *testing.T) {
	stateDb := getRedisClient(t, "STATE_DB")
	defer stateDb.Close()

	t.Run("Valid status, expect ack", func(t *testing.T) {
		stateDb.HSet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS_INFO|port_cycler", "status", "exited").Result()
		ack, err := portCyclerDisableAckd(stateDb)
		if err != nil {
			t.Fatalf("Unexpected error with exit status: %v", err)
		}
		if !ack {
			t.Fatalf("Exit status resulted in negative acknowledgement")
		}
	})
	t.Run("Invalid status, expect no ack", func(t *testing.T) {
		stateDb.HSet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS_INFO|port_cycler", "status", "fail").Result()
		ack, err := portCyclerDisableAckd(stateDb)
		if err != nil {
			t.Fatalf("Unexpected error with non-exit status: %v", err)
		}
		if ack {
			t.Fatalf("Non exit status resulted in positive acknowledgement")
		}
	})
	t.Run("No status, expect ack", func(t *testing.T) {
		stateDb.Del(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS_INFO|port_cycler").Result()
		ack, err := portCyclerDisableAckd(stateDb)
		if err != nil {
			t.Fatalf("Unexpected error other than redis.Nil: %v", err)
		}
		if !ack {
			t.Fatalf("Exit status resulted in negative acknowledgement")
		}
	})
}

func getRedisClient(t *testing.T, db string) *redis.Client {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	dbNum, _ := sdcfg.GetDbId(db, ns)
	dbTcpAddr, _ := sdcfg.GetDbTcpAddr(db, ns)
	rclient := redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        dbTcpAddr,
		Password:    "", // no password set
		DB:          dbNum,
		DialTimeout: 0,
	})
	_, err := rclient.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("failed to connect to redis server/%v error: %v", db, err)
	}
	return rclient
}
