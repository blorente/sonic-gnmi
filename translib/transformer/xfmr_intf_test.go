package transformer

import (
	"reflect"
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/openconfig/ygot/ygot"
)

func TestGetIntfsRoot(t *testing.T) {
	if r := getIntfsRoot(nil); r != nil {
		t.Fatalf("Testing that this operation doesn't cause a crash")
	}
}

func TestEmptyParams(t *testing.T) {
	var xp XfmrParams
	for fn, f := range map[string]interface{}{
		"DbToYang_intf_state_blackhole_xfmr": DbToYang_intf_state_blackhole_xfmr,
		"YangToDb_intf_diag_profile_xfmr":    YangToDb_intf_diag_profile_xfmr,
		"DbToYang_intf_description_xfmr":     DbToYang_intf_description_xfmr} {
		v := reflect.ValueOf(f)
		in := []reflect.Value{reflect.ValueOf(xp)}
		ret := v.Call(in)
		last := ret[len(ret)-1].Interface()
		if _, ok := last.(error); !ok {
			t.Fatalf("Empty XfmrParams didn't generate an error with %s", fn)
		}
	}
}

func TestReadAndParseCounter(t *testing.T) {
	entry := db.Value{
		Field: map[string]string{
			"good_int": "123",
			"bad_int":  "meeple",
		},
	}
	var r uint64
	pr := &r
	if err := readAndParseCounter(&entry, "good_int", &pr); err != nil {
		t.Fatalf("Failed to retrive valid value")
	}
	if err := readAndParseCounter(&entry, "bad_int", &pr); err == nil {
		t.Fatalf("Parse error didn't generate a failure")
	}
	if err := readAndParseCounter(&entry, "missing_int", &pr); err == nil {
		t.Fatalf("Missing field didn't generate a failure")
	}
}

func TestReadAndParseCounters(t *testing.T) {
	entry := db.Value{
		Field: map[string]string{
			"good_int": "123",
		},
	}
	var r uint64
	pr := &r
	fls := []fieldU64LeafPair{
		{"good_int", &pr},
	}
	if e := readAndParseCounters(&entry, fls); e != nil {
		t.Fatalf("Unexpected error")
	}

	fls = []fieldU64LeafPair{
		{"good_int", &pr},
		{"missing_int", &pr},
	}
	if e := readAndParseCounters(&entry, fls); e != nil {
		t.Fatalf("Unexpected error, should ignore miss")
	}

	entry = db.Value{
		Field: map[string]string{
			"good_int": "123",
			"bad_int":  "meeple",
		},
	}
	fls = []fieldU64LeafPair{
		{"good_int", &pr},
		{"bad_int", &pr},
	}
	if e := readAndParseCounters(&entry, fls); e == nil {
		t.Fatalf("Expected a failure from parsing bad_int")
	}
}

func TestDbToYang_intf_eth_aggr_id_xfmr(t *testing.T) {
	var inParams XfmrParams
	inParams.key = "FooBar"
	if _, err := DbToYang_intf_eth_aggr_id_xfmr(inParams); err == nil {
		t.Fatalf("Bad interface name didn't return an error")
	}
}

func TestValidateIntfProvisionedForRelay(t *testing.T) {
	ifName := "FooBar"
	prefixIp := ""
	if _, err := ValidateIntfProvisionedForRelay(nil, ifName, prefixIp, nil); err == nil {
		t.Fatalf("Bad interface name didn't return an error")
	}
}

func TestYangToDb_intf_ip_addr_xfmr(t *testing.T) {
	var inParams XfmrParams
	inParams.uri = "/interfaces/interface[name=Ethernet1/2/3]/subinterfaces/subinterface[index=0]"
	inParams.ygRoot = nil
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Nil yang root didn't return an error")
	}

	var device ocbinds.Device = ocbinds.Device{Interfaces: &ocbinds.OpenconfigInterfaces_Interfaces{Interface: make(map[string]*ocbinds.OpenconfigInterfaces_Interfaces_Interface)}}
	var ygr ygot.GoStruct = &device
	inParams.ygRoot = &ygr
	inParams.uri = "/interfaces/interface[name=Ethernet1/2/3]/subinterfaces/subinterface[index=0]"
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Empty interface yang root didn't return an error")
	}

	device.Interfaces.NewInterface("FooBar")
	inParams.uri = "/interfaces/interface[name=]/subinterfaces/subinterface[index=0]"
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Missing interface name key didn't return an error")
	}

	inParams.uri = "/interfaces/interface[name=FooBar]/subinterfaces/subinterface[index=0]"
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Bad interface name didn't return an error")
	}

	inParams.uri = "/interfaces/interface[name=Ethernet1/2/3]/subinterfaces/subinterface[index=0]"
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Good interface name but not in yang root didn't return an error")
	}

	device.Interfaces.NewInterface("Ethernet1/2/3")
	inParams.uri = "/interfaces/interface[name=Ethernet1/2/3]/subinterfaces/subinterface[index=0]"
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Subinterfaces not in yang root didn't return an error")
	}

	device.Interfaces.Interface["Ethernet1/2/3"].Subinterfaces = &ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces{}
	device.Interfaces.Interface["Ethernet1/2/3"].Subinterfaces.NewSubinterface(1)
	inParams.uri = "/interfaces/interface[name=Ethernet1/2/3]/subinterfaces/subinterface[index=0]"
	if _, err := YangToDb_intf_ip_addr_xfmr(inParams); err == nil {
		t.Fatalf("Subinterface id not in yang root didn't return an error")
	}
}

func TestYangToDb_ipv6_enabled_xfmr(t *testing.T) {
	var inParams XfmrParams
	inParams.uri = "/interfaces/interface[name=FooBar]/subinterfaces/subinterface[index=0]/ipv6/config/enabled"
	if _, err := YangToDb_ipv6_enabled_xfmr(inParams); err == nil {
		t.Fatalf("Invalid interface name didn't return an error")
	}

	inParams.uri = "/interfaces/interface[name=]/subinterfaces/subinterface[index=0]/ipv6/config/enabled"
	if _, err := YangToDb_ipv6_enabled_xfmr(inParams); err == nil {
		t.Fatalf("Missing interface name didn't return an error")
	}

	inParams.uri = "/interfaces/interface[name=Ethernet1/2/3]/subinterfaces/subinterface[index=0]/ipv6/config/enabled"
	if rv, err := YangToDb_ipv6_enabled_xfmr(inParams); err != nil || len(rv) != 0 {
		t.Fatalf("Nil inParams.param return an error or non-empty result")
	}
}
