package transformer

import (
	"context"
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
)

func TestSystemPathXfmr(t *testing.T) {
	inParams := XfmrDbToYgPathParams{}
	rv := DbToYangPath_remote_server_path_xfmr(inParams)
	if rv == nil {
		t.Fatalf("Expected error when no db keys provided")
	}

	inParams.tblKeyComp = []string{"foo", "bar"}
	rv = DbToYangPath_remote_server_path_xfmr(inParams)
	if rv == nil {
		t.Fatalf("Expected error when two db keys provided")
	}

	inParams.tblKeyComp = []string{"foo"}
	inParams.tblName = "WrongTable"
	rv = DbToYangPath_remote_server_path_xfmr(inParams)
	if rv != nil || len(inParams.ygPathKeys) != 0 {
		t.Fatalf("Expected no path returned for wrong table")
	}

	inParams.tblName = "SYSLOG_SERVER"
	inParams.ygPathKeys = make(map[string]string)
	rv = DbToYangPath_remote_server_path_xfmr(inParams)
	if rv != nil || len(inParams.ygPathKeys) != 1 {
		t.Fatalf("Expected one path returned")
	}
	path, ok := inParams.ygPathKeys["/openconfig-system:system/logging/remote-servers/remote-server/host"]
	if !ok || path != "foo" {
		t.Fatalf("Path not translated correctly: %v", inParams.ygPathKeys)
	}
}

func TestNormalizeNtpqRemote(t *testing.T) {
	a := "2001:4860:4806::"
	for _, c := range []string{"*", "+", "#", "-", "~"} {
		if a != normalizeNtpqRemote(c+a) {
			t.Fatalf("normalizeNtpqRemote failed to handle: %s", c+a)
		}
	}
	if a != normalizeNtpqRemote(a) {
		t.Fatalf("normalizeNtpqRemote failed to handle noop scenario: %s", a)
	}
}

func TestCallNtpqAndParse(t *testing.T) {
	if err := callNtpqAndParse(); err != nil {
		t.Fatalf("callNtpqAndParse returned an error: %v", err)
	}
	// Config NTP_SERVER entry exists
	rc := db.TransactionalRedisClient(db.StateDB)
	defer db.CloseRedisClient(rc)
	if keys, err := rc.Keys(context.Background(), "NTP_SERVER|*").Result(); err != nil {
		t.Fatalf("redis:Keys returned an error querying for NTP_SERVER: %v", err)
	} else if len(keys) == 0 {
		t.Fatalf("redis:Keys returned no keys, when some expected")
	}
}
