package utils

import (
	"errors"
	"runtime"
	"sync"
	"unsafe"

	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/golang/glog"
	"github.com/golang/groupcache/lru"
	"github.com/openconfig/ygot/ygot"
)

var (
	ygrCache *lru.Cache
	ygrMutex sync.Mutex // mutext to control ygrCache access.
)

func init() {
	ygrMutex.Lock()
	ygrCache = lru.New(100)
	ygrMutex.Unlock()
}

// YGRootMeta is the metadata of a root ygot.GoStruct object
type YGRootMeta struct {
	ygsMeta ygot.YGStructMetaMap
}

// YGSMeta returns the associated YGStructMeta for ygs.
func (m *YGRootMeta) YGSMeta(ygs ygot.GoStruct) *ygot.YGStructMeta {
	if gsm, ok := m.ygsMeta[ygs]; ok {
		return gsm
	}

	gsm := &ygot.YGStructMeta{LeafMeta: ygot.YGLeafMetaMap{}}
	m.ygsMeta[ygs] = gsm
	return gsm
}

// YGSMetaMap returns the YGStructMetaMap of YGRootMeta.
func (m *YGRootMeta) YGSMetaMap() ygot.YGStructMetaMap {
	return m.ygsMeta
}

// CreateReference create a metadata reference to src for ref (i.e.,
// they will share the same ygStructMeta).
// No reference is created if there is no metadata for src.
func (m *YGRootMeta) CreateReference(ref ygot.GoStruct, src ygot.GoStruct) bool {
	if gsm, ok := m.ygsMeta[src]; ok {
		m.ygsMeta[ref] = gsm
		return true
	}
	return false
}

func newYGRootMeta() *YGRootMeta {
	return &YGRootMeta{ygsMeta: map[ygot.GoStruct]*ygot.YGStructMeta{}}
}

// ygRootToKey convert root Device to a map key.
// Use unsafe.Pointer to avoid referencing root.
func ygRootToKey(root ygot.GoStruct) (interface{}, error) {
	d, ok := root.(*ocbinds.Device)
	if !ok {
		return nil, errors.New("invalid Device")
	}
	return uintptr(unsafe.Pointer(d)), nil
}

func freeYGRootMeta(root ygot.GoStruct) {
	ygrMutex.Lock()
	defer ygrMutex.Unlock()

	if key, err := ygRootToKey(root); err == nil {
		ygrCache.Remove(key)
	}
}

func getYGRootMeta(root ygot.GoStruct) (*YGRootMeta, error) {
	ygrMutex.Lock()
	defer ygrMutex.Unlock()

	if root == nil {
		return nil, errors.New("invalid root")
	}

	key, err := ygRootToKey(root)
	if err != nil {
		return nil, err
	}

	if m, ok := ygrCache.Get(key); ok {
		return m.(*YGRootMeta), nil
	}

	nm := newYGRootMeta()
	ygrCache.Add(key, nm)
	// Free YGRootMeta when root is freed.
	// Clean the finalizer.
	// Prevent the failure of 'double finalizer'.
	// The entry is removed from LRU even it is still in usage.
	runtime.SetFinalizer(root, nil)
	runtime.SetFinalizer(root, freeYGRootMeta)
	return nm, nil
}

// FindYGRootMeta returns the associated YGRootMeta for root, or nil if not found.
func FindYGRootMeta(root ygot.GoStruct) *YGRootMeta {
	ygrMutex.Lock()
	defer ygrMutex.Unlock()

	key, err := ygRootToKey(root)
	if err != nil {
		glog.V(lvl.WARNING).Info(err)
		return nil
	}

	if m, ok := ygrCache.Get(key); ok {
		return m.(*YGRootMeta)
	}
	return nil
}

// UpdateYGSTimestamp updates the timestamp (nanoseconds) for ygs in ygRoot tree.
func UpdateYGSTimestamp(ygRoot ygot.GoStruct, ygs ygot.GoStruct, ns int64) error {
	m, err := getYGRootMeta(ygRoot)
	if err != nil {
		return err
	}
	m.YGSMeta(ygs).TimestampNano = ns
	return nil
}
