package gnmi

import (
	"context"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	spb "github.com/sonic-net/sonic-gnmi/proto/gnmi_sonic"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

const table = "TELEMETRY_CONNECTIONS"

type ConnectionManager struct {
	// Maps a connection string to it's streaming or unary status.
	// True indicates the connection is a streaming connection.
	connections map[string]bool

	streamingThreshold  int    // The limit for active streaming RPCs.
	activeStreamingRPCs uint32 // The number of active streaming RPCs.
	streamingRejected   uint32 // The total number of rejected Streaming RPCs.

	unaryThreshold  int    // The limit of active unary RPCs.
	activeUnaryRPCs uint32 // The number of active unary RPCs.
	unaryRejected   uint32 // The total number of rejected Unary RPCs.

	rclient *redis.Client
	mu      sync.Mutex
}

// CreateConnectionManager creates and returns a new ConnectionManager with the given
// streaming and unary thresholds.
func CreateConnectionManager(streamingThreshold, unaryThreshold int) *ConnectionManager {
	cm := &ConnectionManager{
		connections:         make(map[string]bool),
		streamingThreshold:  streamingThreshold,
		activeStreamingRPCs: 0,
		streamingRejected:   0,
		unaryThreshold:      unaryThreshold,
		activeUnaryRPCs:     0,
		unaryRejected:       0,
		mu:                  sync.Mutex{},
	}
	cm.prepareRedis()
	return cm
}

// prepareRedis creates a Redis connection and deletes any existing connections in the STATE_DB.
func (cm *ConnectionManager) prepareRedis() {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	addr, err := sdcfg.GetDbTcpAddr("STATE_DB", ns)
	if err != nil {
		log.Errorf("Addr err: %v", err)
		return
	}
	dbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		log.Errorf("DB err: %v", err)
		return
	}
	cm.rclient = db.TransactionalRedisClientWithOpts(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "",
		DB:          dbId,
		DialTimeout: 0,
	})

	res, _ := cm.rclient.HGetAll(context.Background(), table).Result()

	if res == nil {
		return
	}

	for key, _ := range res {
		cm.rclient.HDel(context.Background(), table, key)
	}
}

// Add adds a new connection to the ConnectionManager. The streaming argument indicates wether or not
// this connection is a streaming or unary RPC. If the server has reached the limit for the given RPC type,
// the connection is rejected and false is returned. This function should always be called when a new RPC is
// received by the server.
func (cm *ConnectionManager) Add(addr net.Addr, query string, streaming bool) (string, bool) {
	if cm == nil || cm.rclient == nil {
		log.V(1).Infof("Cannot add another connection, ConnectionManager is closed!")
		return "", false
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	switch streaming {
	case true:
		if int(cm.activeStreamingRPCs) >= cm.streamingThreshold && cm.streamingThreshold != 0 {
			log.V(1).Infof("Cannot add another streaming client connection as threshold is already at limit")
			cm.streamingRejected++
			return "", false
		}
		cm.activeStreamingRPCs++
	case false:
		if int(cm.activeUnaryRPCs) >= cm.unaryThreshold && cm.unaryThreshold != 0 {
			log.V(1).Infof("Cannot add another unary client connection as threshold is already at limit")
			cm.unaryRejected++
			return "", false
		}
		cm.activeUnaryRPCs++
	}
	key := createKey(addr, query)
	log.V(lvl.DEBUG).Infof("Adding client connection: %s", key)
	cm.connections[key] = streaming
	cm.storeKeyRedis(key)
	return key, true
}

// Remove removes a connection from the ConnectionManager. This function should always be called when an
// RPC is closing.
func (cm *ConnectionManager) Remove(key string) bool {
	if cm == nil || cm.rclient == nil {
		log.V(1).Infof("Cannot remove connection, ConnectionManager is closed!")
		return false
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	streaming, exists := cm.connections[key]
	if exists {
		log.V(lvl.DEBUG).Infof("Closing connection: %s", key)
		if streaming {
			cm.activeStreamingRPCs--
		} else {
			cm.activeUnaryRPCs--
		}
		delete(cm.connections, key)
	}
	cm.deleteKeyRedis(key)
	return exists
}

// Close closes the given ConnectionManager.
func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for key, _ := range cm.connections {
		cm.deleteKeyRedis(key)
	}
	cm.connections = map[string]bool{}
	db.CloseRedisClient(cm.rclient)
}

// createKey creates a unique key for each connection based on the address and query from the client.
func createKey(addr net.Addr, query string) string {
	regexStr := "(?:target|element):\"([a-zA-Z0-9-_*]*)\""
	regex := regexp.MustCompile(regexStr)
	matches := regex.FindAllStringSubmatch(query, -1)
	// connectionKeyString will look like "10.0.0.1|OTHERS|proc|uptime|2017-07-04 00:47:20
	connectionKey := addr.String() + "|"
	for i := 0; i < len(matches); i++ {
		if len(matches[i]) < 2 {
			continue
		}
		connectionKey += matches[i][1] // index 1 contains the value we need
		connectionKey += "|"
	}
	connectionKey += time.Now().UTC().Format(time.RFC3339Nano)
	return connectionKey
}

// storeKeyRedis writes the given key to the STATE_DB.
func (cm *ConnectionManager) storeKeyRedis(key string) {
	if cm.rclient == nil {
		log.V(1).Infof("Redis client is nil, cannot store connection key")
		return
	}
	if _, err := cm.rclient.HSet(context.Background(), table, key, "active").Result(); err != nil {
		log.V(1).Infof("Subscribe client failed to update telemetry connection key:%s err:%v", key, err)
	}
}

// deleteKeyRedis removes the given key from the STATE_DB.
func (cm *ConnectionManager) deleteKeyRedis(key string) {
	if cm.rclient == nil {
		log.V(1).Infof("Redis client is nil, cannot delete connection key")
		return
	}

	ret, err := cm.rclient.HDel(context.Background(), table, key).Result()
	if ret == 0 {
		log.V(1).Infof("Subscribe client failed to delete telemetry connection key:%s err:%v", key, err)
	}
}

func (cm *ConnectionManager) Stats() *spb.ConnectionManagerStats {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return &spb.ConnectionManagerStats{
		StreamingRpcThreshold: uint32(cm.streamingThreshold),
		ActiveStreamingRpc:    cm.activeStreamingRPCs,
		RejectedStreamingRpc:  cm.streamingRejected,
		UnaryRpcThreshold:     uint32(cm.unaryThreshold),
		ActiveUnaryRpc:        cm.activeUnaryRPCs,
		RejectedUnaryRpc:      cm.unaryRejected,
	}
}
