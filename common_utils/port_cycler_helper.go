package common_utils

import (
	"errors"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"

	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	portCyclingTimeout = 15000 * time.Millisecond
	portCyclingPullInv = 500 * time.Millisecond
)

func IsPortCyclerRunning(stateDbClient *redis.Client, cfgDbClient *redis.Client) bool {
	portCyclerStatus, err := stateDbClient.HGet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS_INFO|port_cycler", "status").Result()
	if err == nil && portCyclerStatus == "exited" {
		// On startup, check for PC exited, if found, skip handshake
		// Update DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local in config_db to avoid PC restart
		if _, e := cfgDbClient.HSet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local", "enable", "false").Result(); e != nil {
			log.V(lvl.ERROR).Infof("Setting DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local in config db failed.")
			return true
		}
		log.V(lvl.INFO).Infof("Port Cycler exited on its own, writing disable to prevent it starting on subsequent reboots.")
		return false
	} else {
		// Otherwise, check for the presence of DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local
		// If the table is found, do handshake. Otherwise, skip handshake.
		portCyclerTableKey, err := cfgDbClient.Keys(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local").Result()
		if err != nil {
			log.V(lvl.ERROR).Infof("Failed to find DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local. Error: %w", err)
			return false
		}
		if len(portCyclerTableKey) == 0 {
			return false
		}
		return true
	}
}

// Requests the Port Cycler feature to exit, then polls for the exit
// acknowledgement. An error is returned if the acknowledgement wasn't
// received.
func DisablePortCycler(cfgDb *redis.Client) error {
	if cfgDb == nil {
		return errors.New("Redis Client is unexpectedly nil.")
	}

	// Request Port Cycler exit
	if _, e := cfgDb.HSet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local", "enable", "false").Result(); e != nil {
		return errors.New("Setting DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local in config db failed.")
	}
	log.V(lvl.INFO).Infof("UMF requests Port Cycler to exit.")

	stateDb, stateDbErr := NewStateDBClient()
	if stateDbErr != nil {
		return status.Errorf(codes.Aborted, "Failed to start a new StateDB client with error %w. Cannot get status of Port-Cycling.", stateDbErr)
	}
	defer db.CloseRedisClient(stateDb)

	// UMF will poll for Port Cycler's acknowledgemnt for up to 10s
	start := time.Now()
	log.V(lvl.INFO).Infof("Start pulling port cycler status.")
	for i := 0; i < int(portCyclingTimeout)/int(portCyclingPullInv); i++ {
		ackd, err := portCyclerDisableAckd(stateDb)
		if err != nil {
			log.V(lvl.ERROR).Infof("An error occurred while polling for Port Cycler ack: %w", err)
			return err
		}
		if ackd {
			log.V(lvl.INFO).Infof("Port cycler exited successfully after %v.", time.Since(start))
			return nil
		}
		time.Sleep(portCyclingPullInv)
	}

	return errors.New("Port Cycling did not exit before timeout. Time elapsed: " + time.Since(start).String())
}
func portCyclerDisableAckd(stateDb *redis.Client) (bool, error) {
	value, err := stateDb.HGet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS_INFO|port_cycler", "status").Result()
	if err != nil {
		if err != redis.Nil {
			return false, err
		}
		log.V(lvl.DEBUG).Infof("DYNAMIC_BOOTSTRAP_CYCLE_PORTS_INFO|port_cycler status missed in STATE_DB.")
		return true, nil
	}
	return (value == "exited"), nil
}
