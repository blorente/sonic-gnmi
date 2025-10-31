package log

import "testing"

func TestTranslLogOrder(t *testing.T) {
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

func TestTranslLogLevel(t *testing.T) {
	if ERROR != 0 {
		t.Fatalf("ERROR == %v (!= 0)", ERROR)
	}
	if WARNING != 1 {
		t.Fatalf("WARNING == %v (!= 1)", WARNING)
	}
	if INFO != 2 {
		t.Fatalf("INFO == %v (!= 2)", INFO)
	}
	if DEBUG != 3 {
		t.Fatalf("DEBUG == %v (!= 3)", DEBUG)
	}
}
