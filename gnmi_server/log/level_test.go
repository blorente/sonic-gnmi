package log

import "testing"

func TestTranslGet(t *testing.T) {
	if ERROR >= WARNING {
		t.Fatal("ERROR >= WARNING")
	}
	if WARNING >= INFO {
		t.Fatal("WARNING >= INFO")
	}
	if INFO >= DEBUG {
		t.Fatal("INFO >= DEBUG")
	}
}
