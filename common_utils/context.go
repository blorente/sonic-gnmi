package common_utils

import (
	"context"
	"fmt"
	"sync/atomic"
)

// AuthInfo holds data about the authenticated user
type AuthInfo struct {
	// Username
	User        string
	AuthEnabled bool
	// Roles
	Roles []string
}

type ParamKey string

// RequestContext holds metadata about REST request.
type RequestContext struct {

	// Unique reqiest id
	ID string

	// Auth contains the authorized user information
	Auth AuthInfo

	//Bundle Version is the release yang models version.
	BundleVersion *string

	// Map for request parameters
	Params map[ParamKey]interface{}
}

type contextkey int

const requestContextKey contextkey = 0

// Request Id generator
var requestCounter uint64

// Keys used to store/retrieve request parameters in RequestContext.Params
const (
	// Stores the gNMI Get Request type (All, Config-Data, State-Data, etc.)
	KeyGetDataType ParamKey = "get:data_type"
)

type CounterType int

const (
	GNMI_GET CounterType = iota
	GNMI_GET_FAIL
	GNMI_SET
	GNMI_SET_FAIL
	GNOI_BURNIN_RESULTS
	GNOI_BURNIN_START
	GNOI_BURNIN_STOP
	GNOI_DEBUG_TUNNEL
	GNOI_HEALTHZ_ACK
	GNOI_HEALTHZ_CHECK
	GNOI_HEALTHZ_COLLECT
	GNOI_REBOOT
	GNOI_FILE_REMOVE
	GNOI_OS_ACTIVATE
	GNOI_OS_INSTALL
	GNOI_OS_VERIFY
	GNOI_SYSTEM_OPTICS
	GNOI_WHITEBOX_SET
	GNOI_FACTORY_RESET
	GNSI_CREDZ_SET
	GNSI_CREDZ_CHECKPOINT
	DBUS
	DBUS_FAIL
	DBUS_APPLY_PATCH_DB
	DBUS_APPLY_PATCH_YANG
	DBUS_CREATE_CHECKPOINT
	DBUS_DELETE_CHECKPOINT
	DBUS_CONFIG_SAVE
	DBUS_CONFIG_RELOAD
	DBUS_STOP_SERVICE
	DBUS_RESTART_SERVICE
	COUNTER_SIZE
)

func (c CounterType) String() string {
	switch c {
	case GNMI_GET:
		return "GNMI get"
	case GNMI_GET_FAIL:
		return "GNMI get fail"
	case GNMI_SET:
		return "GNMI set"
	case GNMI_SET_FAIL:
		return "GNMI set fail"
	case GNOI_BURNIN_RESULTS:
		return "GNOI Burnin Results"
	case GNOI_BURNIN_START:
		return "GNOI Burnin Start"
	case GNOI_BURNIN_STOP:
		return "GNOI Burnin Stop"
	case GNOI_DEBUG_TUNNEL:
		return "GNOI Debug Tunnel"
	case GNOI_HEALTHZ_ACK:
		return "GNOI Healthz Ack"
	case GNOI_HEALTHZ_CHECK:
		return "GNOI Healthz Check"
	case GNOI_HEALTHZ_COLLECT:
		return "GNOI Healthz COLLECT"
	case GNOI_REBOOT:
		return "GNOI reboot"
	case GNOI_FILE_REMOVE:
		return "GNOI File Remove"
	case GNOI_OS_ACTIVATE:
		return "GNOI OS Activate"
	case GNOI_OS_INSTALL:
		return "GNOI OS Install"
	case GNOI_OS_VERIFY:
		return "GNOI OS VERIFY"
	case GNOI_SYSTEM_OPTICS:
		return "GNOI System optics reset"
	case GNOI_WHITEBOX_SET:
		return "GNOI Whitebox Set Controller Connection State"
	case GNOI_FACTORY_RESET:
		return "GNOI Factory Reset"
	case GNSI_CREDZ_SET:
		return "GNSI Credz Set"
	case GNSI_CREDZ_CHECKPOINT:
		return "GNSI Credz Checkpoint"
	case DBUS:
		return "DBUS"
	case DBUS_FAIL:
		return "DBUS fail"
	case DBUS_APPLY_PATCH_DB:
		return "DBUS apply patch db"
	case DBUS_APPLY_PATCH_YANG:
		return "DBUS apply patch yang"
	case DBUS_CREATE_CHECKPOINT:
		return "DBUS create checkpoint"
	case DBUS_DELETE_CHECKPOINT:
		return "DBUS delete checkpoint"
	case DBUS_CONFIG_SAVE:
		return "DBUS config save"
	case DBUS_CONFIG_RELOAD:
		return "DBUS config reload"
	case DBUS_STOP_SERVICE:
		return "DBUS stop service"
	case DBUS_RESTART_SERVICE:
		return "DBUS restart service"
	default:
		return ""
	}
}

var globalCounters [COUNTER_SIZE]uint64

// GetContext function returns the RequestContext object for a
// gRPC request. RequestContext is maintained as a context value of
// the request. Creates a new RequestContext object is not already
// available.
func GetContext(ctx context.Context) (*RequestContext, context.Context) {
	cv := ctx.Value(requestContextKey)
	if cv != nil {
		return cv.(*RequestContext), ctx
	}

	rc := new(RequestContext)
	rc.ID = fmt.Sprintf("TELEMETRY-%v", atomic.AddUint64(&requestCounter, 1))
	rc.Params = make(map[ParamKey]interface{})

	ctx = context.WithValue(ctx, requestContextKey, rc)
	return rc, ctx
}

func GetUsername(ctx context.Context, username *string) {
	rc, _ := GetContext(ctx)
	if rc != nil {
		*username = rc.Auth.User
	}
}

func InitCounters() {
	for i := 0; i < int(COUNTER_SIZE); i++ {
		globalCounters[i] = 0
	}
	SetMemCounters(&globalCounters)
}

func IncCounter(cnt CounterType) {
	atomic.AddUint64(&globalCounters[cnt], 1)
	SetMemCounters(&globalCounters)
}

func SetReqParam(ctx context.Context, key ParamKey, val interface{}) {
	rc, _ := GetContext(ctx)
	rc.Params[key] = val
}

func ReqParam(ctx context.Context, key ParamKey) (interface{}, bool) {
	rc, _ := GetContext(ctx)

	if val, ok := rc.Params[key]; ok {
		return val, true
	}
	return nil, false
}
