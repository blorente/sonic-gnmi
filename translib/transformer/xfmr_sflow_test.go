package transformer

import (
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
)

func TestNilDbs(t *testing.T) {
	dbs := make([]*db.DB, db.MaxDB)
	if _, err := getSflowColInfoFromDb(dbs); err == nil {
		t.Fatalf("Passing unintialized DBs didn't generate error")
	}
}
