package common_utils

import (
	"context"
	"sync"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
)

const (
	switchCapabilityTable = "SWITCH_CAPABILITY|switch"
	bertLqKey             = "prbs_linkqual_capable"
	pktLqKey              = "packet_based_linkqual_capable"
)

type LinkQualificationHelperInterface interface {
	Close()
	SupportsBert() bool
	SetBertCapability(val bool)
	SupportsPktLq() bool
	SetPktLqCapability(val bool)
}

// LinkQualificationHelper provides utilities for checking link qualification
// capabilities of the platform.
// NewLinkQualificationHelper must be called for a new helper.
type LinkQualificationHelper struct {
	db            *redis.Client
	mux           sync.Mutex
	supportsBert  bool
	supportsPktLq bool
}

// NewLinkQualificationHelper returns a new LinkQualificationHelper.
func NewLinkQualificationHelper() (*LinkQualificationHelper, error) {
	h := new(LinkQualificationHelper)

	// Create redis client.
	var err error
	if h.db, err = getRedisDBClient(); err != nil {
		return nil, err
	}

	// Read PRBS bert capability.
	bertLq, err := h.db.HGet(context.Background(), switchCapabilityTable, bertLqKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	bertLqCapable := true
	if bertLq == "false" {
		bertLqCapable = false
		log.V(lvl.INFO).Infof("PRBS BERT linkqual is disabled.")
	}
	h.SetBertCapability(bertLqCapable)

	// Read packet based linkqual capability.
	pktLq, err := h.db.HGet(context.Background(), switchCapabilityTable, pktLqKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	pktLqCapable := true
	if pktLq == "false" {
		pktLqCapable = false
		log.V(lvl.INFO).Infof("Packet based linkqual is disabled.")
	}
	h.SetPktLqCapability(pktLqCapable)

	return h, nil
}

// Close performs cleanup works.
// Close must be called when finished.
func (h *LinkQualificationHelper) Close() {
	if h == nil {
		return
	}
	if err := db.CloseRedisClient(h.db); err != nil {
		log.V(lvl.ERROR).Infof("Fail to close Redis client: %v", err)
	}
}

// SupportsBert returns the capability information.
// Returns false by default.
func (h *LinkQualificationHelper) SupportsBert() bool {
	if h == nil {
		return false
	}
	h.mux.Lock()
	defer h.mux.Unlock()
	return h.supportsBert
}

// SetBertCapability sets the capability information.
func (h *LinkQualificationHelper) SetBertCapability(val bool) {
	if h == nil {
		log.V(lvl.ERROR).Infof("Cannot set BERT capability!")
		return
	}
	h.mux.Lock()
	defer h.mux.Unlock()
	h.supportsBert = val
}

// SupportsPktLq returns the capability information.
// Returns false by default.
func (h *LinkQualificationHelper) SupportsPktLq() bool {
	if h == nil {
		return false
	}
	h.mux.Lock()
	defer h.mux.Unlock()
	return h.supportsPktLq
}

// SetPktLqCapability sets the capability information.
func (h *LinkQualificationHelper) SetPktLqCapability(val bool) {
	if h == nil {
		log.V(lvl.ERROR).Infof("Cannot set packet linkqual capability!")
		return
	}
	h.mux.Lock()
	defer h.mux.Unlock()
	h.supportsPktLq = val
}
