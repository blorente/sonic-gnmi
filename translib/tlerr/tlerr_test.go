package tlerr

import (
	"fmt"
	"testing"

	"github.com/Azure/sonic-mgmt-common/cvl"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
)

type MyError struct{ s string }

func (e *MyError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s", e.s)
}

func TestTranslibErrorToString(t *testing.T) {
	exp := "Translib Redis Error: Cannot open DB"
	got := TranslibDBCannotOpen{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: DB Not Initialized"
	got = TranslibDBNotInit{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: Entry does not exist: Foo"
	got = TranslibRedisClientEntryNotExist{"Foo"}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	cvlErr := cvl.CVLErrorInfo{
		TableName:        "TableWithError",
		ErrCode:          cvl.CVL_SYNTAX_OUT_OF_RANGE,
		CVLErrDetails:    "CVL Error Details",
		Keys:             []string{"Key1", "Key2"},
		Value:            "Field Value",
		Field:            "Field Name",
		Msg:              "Detailed Error Message",
		ConstraintErrMsg: "Constraint Error Message",
		ErrAppTag:        "App Tag",
	}
	exp = fmt.Sprintf("Translib Redis Error: CVL Failure: %d: %v", 123, cvlErr)
	got = TranslibCVLFailure{123, cvlErr}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: Transaction Fails"
	got = TranslibTransactionFail{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: DB Script Fail: Foo"
	got = TranslibDBScriptFail{"Foo"}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: DB Subscribe Fail"
	got = TranslibDBSubscribeFail{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "DB error: Foo"
	var invalidState TranslibDBInvalidState
	invalidState = "Foo"
	got = invalidState.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Foo"
	got = TranslibSyntaxValidationError{123, &MyError{"Foo"}}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Unsupported client version Foo"
	got = TranslibUnsupportedClientVersion{"Foo", "Bar", "Baz"}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = fmt.Sprintf("Translib transformer return %s", false)
	got = TranslibXfmrRetError{false}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: DB Connection Reset"
	got = TranslibDBConnectionReset{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Error: DB Transaction Commands Limit Exceeded"
	got = TranslibDBTxCmdsLim{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Error: DB Resource Lock"
	got = TranslibDBLock{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Redis Error: Not Supported: Foo"
	got = TranslibDBNotSupported{"Foo"}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Timeout Error"
	got = TranslibTimeoutError{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Invalid Session Error"
	got = TranslibInvalidSession{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}

	exp = "Translib Busy"
	got = TranslibBusy{}.Error()
	if exp != got {
		t.Fatalf("Wanted \"%s\", Got \"%s\"", exp, got)
	}
}

func TestDbEntryNotExistError(t *testing.T) {
	if isDBEntryNotExistError(nil) != false {
		t.Fatal("isDBEntryNotExistError(nil) did not return false")
	}

	e := TranslibRedisClientEntryNotExist{"Foo"}
	if isDBEntryNotExistError(e) != true {
		t.Fatal("isDBEntryNotExistError(TranslibRedisClientEntryNotExist) did not return true")
	}
	if isDBEntryNotExistError(&e) != true {
		t.Fatal("isDBEntryNotExistError(&TranslibRedisClientEntryNotExist) did not return true")
	}

	if isDBEntryNotExistError(TranslibDBNotSupported{"Foo"}) != false {
		t.Fatal("isDBEntryNotExistError(TranslibDBNotSupported) did not return false")
	}
}

func TestErrorSeverity(t *testing.T) {
	if ErrorSeverity(nil) != lvl.DEBUG {
		t.Fatal("ErrorSeverity(nil) did not return DEBUG")
	}

	if ErrorSeverity(&MyError{"Foo"}) != lvl.ERROR {
		t.Fatal("ErrorSeverity(&MyError) did not return ERROR")
	}

	if ErrorSeverity(TranslibBusy{}) != lvl.ERROR {
		t.Fatal("ErrorSeverity(TranslibBusy{}) did not return ERROR")
	}

	if ErrorSeverity(&TranslibBusy{}) != lvl.ERROR {
		t.Fatal("ErrorSeverity(&TranslibBusy{}) did not return ERROR")
	}

	if ErrorSeverity(TranslibRedisClientEntryNotExist{"Foo"}) != lvl.DEBUG {
		t.Fatal("ErrorSeverity(TranslibRedisClientEntryNotExist{}) did not return DEBUG")
	}

	if ErrorSeverity(&TranslibRedisClientEntryNotExist{"Foo"}) != lvl.DEBUG {
		t.Fatal("ErrorSeverity(&TranslibRedisClientEntryNotExist{}) did not return DEBUG")
	}
}
