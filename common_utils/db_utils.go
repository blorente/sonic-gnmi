package common_utils

import (
	"fmt"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

func NewConfigDBClient() (*redis.Client, error) {
	return newDbClient("CONFIG_DB")
}

func NewStateDBClient() (*redis.Client, error) {
	return newDbClient("STATE_DB")
}

func newDbClient(db_name string) (*redis.Client, error) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	dbNum, _ := sdcfg.GetDbId(db_name, ns)
	client := db.RedisClient(db.DBNum(dbNum))

	if client == nil {
		return nil, fmt.Errorf("Cannot create Redis %v client.", db_name)
	}
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return client, nil
}
