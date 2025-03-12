package gnmi

import (
	"sync"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	"golang.org/x/net/context"
)

type GetResponseCache struct {
	mu                       *sync.Mutex
	response                 *gnmipb.GetResponse
	valid                    bool
	listeningToNotifications bool // Holds the status of the goroutine monitoring the ConfigDB
	done                     chan bool
}

// NewGetResponseCache returns a new instance of a GetResponseCache. The
// cache will not be valid when it is created. The cache will already
// be monitoring the database for changes when it is returned.
func NewGetResponseCache() *GetResponseCache {
	rc := &GetResponseCache{
		mu:                       &sync.Mutex{},
		response:                 nil,
		valid:                    false,
		listeningToNotifications: false,
		done:                     make(chan bool, 1),
	}
	go rc.monitor()
	return rc
}

// Cache will store a new response in the cache and validate it.
func (rc *GetResponseCache) Cache(resp *gnmipb.GetResponse) {
	if resp == nil || rc == nil {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.response = resp
	rc.valid = true
	log.V(lvl.INFO).Info("Cached GetResponse -- GetResponseCache is valid")
}

// GetResponse will return the cached response if valid or nil otherwise.
func (rc *GetResponseCache) GetResponse() *gnmipb.GetResponse {
	if rc == nil {
		return nil
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if !rc.valid || !rc.listeningToNotifications {
		return nil
	}
	return rc.response
}

// invalidate will invalidate the cache by setting the valid bit to false.
func (rc *GetResponseCache) invalidate() {
	if rc == nil {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.valid == true {
		rc.valid = false
		rc.response = nil
		log.V(lvl.INFO).Info("GetResponseCache invalidated -- ConfigDB change detected")
	}
}

// setListening will set the value of listeningToNotifications to the value passed in.
func (rc *GetResponseCache) setListening(listeningToNotifications bool) {
	if rc == nil {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.listeningToNotifications = listeningToNotifications
}

// monitor will subscribe to the CONFIG_DB and invalidate the cache when
// a notification is received from that subscription.
func (rc *GetResponseCache) monitor() {
	if rc == nil {
		return
	}

	client := db.TransactionalRedisClient(db.ConfigDB)
	defer db.CloseRedisClient(client)

	ps := client.PSubscribe(context.Background(), "__keyspace@4__:*")
	if _, err := ps.Receive(context.Background()); err != nil {
		log.V(lvl.INFO).Infof("GetResponseCache: failed to subscribe to redis notifications - %v", err)
		return
	}
	defer ps.Close()

	notifications := ps.Channel()

	rc.setListening(true)
	defer rc.setListening(false)

	for {
		select {
		case _, ok := <-notifications:
			if !ok {
				log.V(lvl.INFO).Info("GetResponseCache closing -- DB notification channel closed")
				return
			}
			rc.invalidate()
		case <-rc.done:
			log.V(lvl.DEBUG).Info("Closing GetResponseCache")
			return
		}
	}

}

// Close will close the response cache and stop monitoring the DB.
func (rc *GetResponseCache) Close() {
	if rc == nil {
		return
	}
	rc.done <- true
}

// IsGetConfigRequest will return true if the request is GetConfig and false otherwise.
func IsGetConfigRequest(req *gnmipb.GetRequest) bool {
	if req == nil {
		return false
	}

	// A GetConfig request only requests data of type CONFIG.
	reqType := req.GetType()
	if reqType != gnmipb.GetRequest_CONFIG {
		return false
	}

	// A GetConfig request is scoped at root, so no paths are included in the request.
	paths := req.GetPath()
	if len(paths) != 0 {
		return false
	}

	// If the encoding is not JSON_IETF, disable the cache.
	if req.GetEncoding() != gnmipb.Encoding_JSON_IETF {
		return false
	}

	return true
}
