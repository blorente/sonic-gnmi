package transformer

import (
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/openconfig/ygot/ygot"
)

func TestPathTypeString(t *testing.T) {
	cases := []struct {
		in   PathType
		want string
	}{
		{AllPaths, "AllPaths"},
		{AllCompPaths, "AllComponentPaths"},
		{ConfigPaths, "ConfigPaths"},
		{StatePaths, "StatePaths"},
		{SingularPath, "SingularPath"},
		{RailPaths, "RailPaths"},
	}
	for _, c := range cases {
		got := c.in.String()
		if got != c.want {
			t.Errorf("PathType.String() == %q, want %q", got, c.want)
		}
	}
}

func TestComponentTypeString(t *testing.T) {
	cases := []struct {
		in   componentType
		want string
	}{
		{CompTypeInvalid, "CompTypeInvalid"},
		{CompTypePsu, "CompTypePsu"},
		{CompTypeFan, "CompTypeFan"},
		{CompTypeFanTray, "CompTypeFanTray"},
		{CompTypeFpga, "CompTypeFpga"},
		{CompTypeStorage, "CompTypeStorage"},
		{CompTypeXcvr, "CompTypeXcvr"},
		{CompTypeTemp, "CompTypeTemp"},
		{CompTypeIC, "CompTypeIC"},
		{CompTypeChassis, "CompTypeChassis"},
		{CompTypeNWStack, "CompTypeNWStack"},
		{CompTypeOS, "CompTypeOS"},
		{CompTypeBootLoader, "CompTypeBootLoader"},
		{CompTypePort, "CompTypePort"},
		{CompTypeHwSecurityModule, "CompTypeHwSecurityModule"},
		{CompTypePcie, "CompTypePcie"},
		{CompTypePowerSupplyVR, "CompTypePowerSupplyVR"},
		{CompTypePowerSupplyPB, "CompTypePowerSupplyPB"},
		{CompTypePowerSupplyPH, "CompTypePowerSupplyPH"},
		{CompTypePowerSupplyPSEQ, "CompTypePowerSupplyPSEQ"},
		{CompTypeControllerCard, "CompTypeControllerCard"},
		{CompTypeDiskCarrier, "CompTypeDiskCarrier"},
		{CompTypeEepromStatus, "CompTypeEepromStatus"},
		{CompTypeHumidity, "CompTypeHumidity"},
	}
	for _, c := range cases {
		got := c.in.String()
		if got != c.want {
			t.Errorf("componentType.String() == %q, want %q", got, c.want)
		}
	}
}

func TestGetDbToYangPlatformType(t *testing.T) {
	cases := []struct {
		in   string
		want ocbinds.E_OpenconfigPinsPlatformChassis_PLATFORM_TYPE
		err  bool
	}{
		{"", ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_GENERIC, true},
		{"JunkValue", ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_GENERIC, true},
		{"generic", ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_GENERIC, false},
		{"BX", ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_SWITCH1, false},
	}
	for _, c := range cases {
		got, err := getDbToYangPlatformType(c.in)
		if c.err {
			if err == nil || got != c.want {
				t.Errorf("getDbToYangPlatformType(%s)=%s,%v; want %s,non-nil", c.in, got, err, c.want)
			}
		} else {
			if err != nil || got != c.want {
				t.Errorf("getDbToYangPlatformType(%s)=%s,%v; want %s,nil", c.in, got, err, c.want)
			}
		}
	}
}

func TestGetDbToYangEthPmd(t *testing.T) {
	cases := []struct {
		in   string
		want ocbinds.E_OpenconfigTransportTypes_ETHERNET_PMD_TYPE
	}{
		{"", ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_UNDEFINED},
		{"JunkValue", ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_UNDEFINED},
		{"PMD_UNKNOWN", ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_UNDEFINED},
		{"2X200G_BGR4", ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X200GBASE_BGR4},
		{"100G_PSM4", ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_PSM4},
	}
	for _, c := range cases {
		got := getDbToYangEthPmd(c.in)
		if got != c.want {
			t.Errorf("getDbToYangEthPmd(%s)=%s; want %s", c.in, got, c.want)
		}
	}
}

func TestGetPfmRootObject(t *testing.T) {
	if r := getPfmRootObject(nil); r != nil {
		t.Errorf("Calling getPfmRootObject with nil didn't return nil, %v", r)
	}
}

func testFetchAllPortsFromParentPortWithLanes(t *testing.T) {
	if _, err := fetchAllPortsFromParentPortWithLanes("bad port name", nil, 8); err == nil {
		t.Errorf("Calling fetchAllPortsFromParentPortWithLanes with a bad port name didn't return an error")
	}
}

/* Simple test to cover two negative scenarios with fillAllRailLeaves:
 * Passing empty strings ("") and passing bogus strings (no int/float conversion)
 */
func TestFillAllRailLeaves(t *testing.T) {
	var railEmpty ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail
	var psiEmpty PowerSupplyInfo
	ygot.BuildEmptyTree(&railEmpty)
	ygot.BuildEmptyTree(railEmpty.State)
	fillAllRailLeaves(&railEmpty, psiEmpty, "foo", "bar")
	if *railEmpty.State.Name != "bar" {
		t.Errorf("Unexpected rail name %s", *&railEmpty.State.Name)
	}
	if railEmpty.State.Direction != ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_UNSET {
		t.Errorf("Unexpected direction %v", railEmpty.State.Direction)
	}
	/* All other fields should be unset */
	if railEmpty.State.CommandedVoltage != nil ||
		railEmpty.State.Current != nil ||
		railEmpty.State.PeakCurrent != nil ||
		railEmpty.State.PeakPower != nil ||
		railEmpty.State.PeakPowerInterval != nil ||
		railEmpty.State.PeakVoltage != nil ||
		railEmpty.State.Power != nil ||
		railEmpty.State.StatusVout != nil ||
		railEmpty.State.Temperature != nil ||
		railEmpty.State.Voltage != nil {
		t.Errorf("Unexpected rail contents %#v", railEmpty.State)
	}

	/* Test with bad values for the various string-to-x conversions */
	var rail ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail
	var psi PowerSupplyInfo
	ygot.BuildEmptyTree(&rail)
	ygot.BuildEmptyTree(rail.State)
	psi.Direction = "OUTPUT"
	psi.StatusVout = "foo"
	psi.CommendedVoltage = "foo"
	psi.Energy = "foo"
	psi.Current = "foo"
	psi.Voltage = "foo"
	psi.Power = "foo"
	psi.PeakPower = "foo"
	psi.PeakPowerInterval = "foo"
	psi.PeakVoltage = "foo"
	psi.PeakCurrent = "foo"
	psi.RailTemperature = "foo"
	fillAllRailLeaves(&rail, psi, "foo", "bar")
	if *rail.State.Name != "bar" {
		t.Errorf("Unexpected rail name %s", *&rail.State.Name)
	}
	if rail.State.Direction != ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_OUTPUT {
		t.Errorf("Unexpected direction %v", rail.State.Direction)
	}
	/* All other fields should be unset */
	if rail.State.CommandedVoltage != nil ||
		rail.State.Current != nil ||
		rail.State.PeakCurrent != nil ||
		rail.State.PeakPower != nil ||
		rail.State.PeakPowerInterval != nil ||
		rail.State.PeakVoltage != nil ||
		rail.State.Power != nil ||
		rail.State.StatusVout != nil ||
		rail.State.Temperature != nil ||
		rail.State.Voltage != nil {
		t.Errorf("Unexpected rail contents %#v", rail.State)
	}
}

func TestFillSysFirmwareInfo(t *testing.T) {
	var err error
	comp := &ocbinds.OpenconfigPlatform_Components_Component{}
	ygot.BuildEmptyTree(comp)
	ygot.BuildEmptyTree(comp.Chassis)
	ygot.BuildEmptyTree(comp.Chassis.Alarms)
	ygot.BuildEmptyTree(comp.Chassis.Alarms.State)
	ygot.BuildEmptyTree(comp.Chassis.State)
	ygot.BuildEmptyTree(comp.Chassis.Config)
	name := "chassis"
	sdb, err := db.NewDB(getDBOptions(db.StateDB))
	if err != nil {
		t.Fatal("NewDB failed")
	}
	defer sdb.DeleteDB()
	cdb, err := db.NewDB(getDBOptions(db.ConfigDB))
	if err != nil {
		t.Fatal("NewDB failed")
	}
	defer cdb.DeleteDB()
	singlePathURIs := []string{
		COMP_STATE_FIRM_VER, COMP_STATE_HW_VER, COMP_STATE_MFG_DATE,
		COMP_CONFIG_NAME, FIRMWARE_CHASSIS_OC_NUM_MAC, COMP_STATE_PART_NO,
		COMP_STATE_SERIAL_NO, COMP_STATE_OC_FQ_NAME, FIRMWARE_CHASSIS_OC_PLATFORM,
		FIRMWARE_CHASSIS_OC_BASE_MAC}
	for _, uri := range singlePathURIs {
		t.Run(uri, func(t *testing.T) {
			if err = fillSysFirmwareInfo(comp, name, SingularPath, uri, sdb, cdb); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
