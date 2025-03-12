package common_utils

import (
	"context"
	"testing"
	"time"

	log "github.com/golang/glog"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
)

func TestCounter(t *testing.T) {
	log.V(lvl.WARNING).Info("TestCounter")
	// InitCounters should set them all to zero.
	for i := 0; i < len(globalCounters); i++ {
		globalCounters[i] = 7
	}
	InitCounters()
	for i, v := range globalCounters {
		if v != 0 {
			t.Fatalf("Counter %d has non-zero value %v", i, v)
		}
	}

	refCounts := []struct {
		ctr  CounterType
		name string
		val  uint64
	}{
		{
			ctr:  GNMI_GET,
			name: "GNMI get",
			val:  19,
		},
		{
			ctr:  GNMI_GET_FAIL,
			name: "GNMI get fail",
			val:  18,
		},
		{
			ctr:  GNMI_SET,
			name: "GNMI set",
			val:  17,
		},
		{
			ctr:  GNMI_SET_FAIL,
			name: "GNMI set fail",
			val:  16,
		},
		{
			ctr:  GNOI_BURNIN_RESULTS,
			name: "GNOI Burnin Results",
			val:  176,
		},
		{
			ctr:  GNOI_BURNIN_START,
			name: "GNOI Burnin Start",
			val:  175,
		},
		{
			ctr:  GNOI_BURNIN_STOP,
			name: "GNOI Burnin Stop",
			val:  174,
		},
		{
			ctr:  GNOI_DEBUG_TUNNEL,
			name: "GNOI Debug Tunnel",
			val:  177,
		},
		{
			ctr:  GNOI_HEALTHZ_ACK,
			name: "GNOI Healthz Ack",
			val:  173,
		},
		{
			ctr:  GNOI_HEALTHZ_CHECK,
			name: "GNOI Healthz Check",
			val:  172,
		},
		{
			ctr:  GNOI_HEALTHZ_COLLECT,
			name: "GNOI Healthz COLLECT",
			val:  171,
		},
		{
			ctr:  GNOI_REBOOT,
			name: "GNOI reboot",
			val:  15,
		},
		{
			ctr:  GNOI_FILE_REMOVE,
			name: "GNOI File Remove",
			val:  151,
		},
		{
			ctr:  GNOI_OS_ACTIVATE,
			name: "GNOI OS Activate",
			val:  14,
		},
		{
			ctr:  GNOI_OS_INSTALL,
			name: "GNOI OS Install",
			val:  13,
		},
		{
			ctr:  GNOI_OS_VERIFY,
			name: "GNOI OS VERIFY",
			val:  12,
		},
		{
			ctr:  GNOI_SYSTEM_OPTICS,
			name: "GNOI System optics reset",
			val:  11,
		},
		{
			ctr:  GNOI_WHITEBOX_SET,
			name: "GNOI Whitebox Set Controller Connection State",
			val:  10,
		},
		{
			ctr:  GNOI_FACTORY_RESET,
			name: "GNOI Factory Reset",
			val:  9,
		},
		{
			ctr:  GNSI_CREDZ_SET,
			name: "GNSI Credz Set",
			val:  810,
		},
		{
			ctr:  GNSI_CREDZ_CHECKPOINT,
			name: "GNSI Credz Checkpoint",
			val:  800,
		},
		{
			ctr:  DBUS,
			name: "DBUS",
			val:  8,
		},
		{
			ctr:  DBUS_FAIL,
			name: "DBUS fail",
			val:  7,
		},
		{
			ctr:  DBUS_APPLY_PATCH_DB,
			name: "DBUS apply patch db",
			val:  6,
		},
		{
			ctr:  DBUS_APPLY_PATCH_YANG,
			name: "DBUS apply patch yang",
			val:  5,
		},
		{
			ctr:  DBUS_CREATE_CHECKPOINT,
			name: "DBUS create checkpoint",
			val:  4,
		},
		{
			ctr:  DBUS_DELETE_CHECKPOINT,
			name: "DBUS delete checkpoint",
			val:  3,
		},
		{
			ctr:  DBUS_CONFIG_SAVE,
			name: "DBUS config save",
			val:  2,
		},
		{
			ctr:  DBUS_CONFIG_RELOAD,
			name: "DBUS config reload",
			val:  1,
		},
		{
			ctr:  DBUS_STOP_SERVICE,
			name: "DBUS stop service",
			val:  110,
		},
		{
			ctr:  DBUS_RESTART_SERVICE,
			name: "DBUS restart service",
			val:  100,
		},
	}
	// Check counter increment and string name
	for _, r := range refCounts {
		t.Run(r.name, func(t *testing.T) {
			for i := 0; uint64(i) < r.val; i++ {
				IncCounter(r.ctr)
			}
			if r.val != globalCounters[r.ctr] {
				t.Fatalf("Counter %v is %v, expected %v", r.ctr, globalCounters[r.ctr], r.val)
			}
			if r.ctr.String() != r.name {
				t.Fatalf("Counter %v is %v, expected %v", r.ctr, r.ctr.String(), r.name)
			}
		})
	}

	// Check restore from shared mem.
	for i := 0; i < len(globalCounters); i++ {
		globalCounters[i] = 77
	}
	GetMemCounters(&globalCounters)
	for _, r := range refCounts {
		t.Run(r.name+"_restore", func(t *testing.T) {
			if r.val != globalCounters[r.ctr] {
				t.Fatalf("Counter %v is %v, expected %v", r.ctr, globalCounters[r.ctr], r.val)
			}
		})
	}
}

func TestParamNoKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	val, sts := ReqParam(ctx, "foo")
	if sts != false {
		t.Fatalf("ReqParam didn't return false for unknown key")
	}
	if val != nil {
		t.Fatalf("ReqParam didn't return nil for unknown key")
	}
}
