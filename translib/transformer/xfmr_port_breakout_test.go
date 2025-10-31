package transformer

import (
	"testing"

	db "github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/openconfig/ygot/ygot"
)

func TestPopulateChannelGroupsForSameSpeed(t *testing.T) {
	if _, err := populateChannelGroupsForSameSpeed("bad port name", "1x400G"); err == nil {
		t.Errorf("Calling populateChannelGroupsForSameSpeed with a bad port name didn't return an error")
	}
}

func TestPopulateBrkoutMode(t *testing.T) {
	if _, err := populateBrkoutMode(nil, "somePort", "bad IF name"); err == nil {
		t.Errorf("Calling populateBrkoutMode with a bad IF name didn't return an error")
	}
}

func TestModifyPortFootprintWithUnSupportedToMode(t *testing.T) {
	configDb, _ := db.NewDB(db.Options{
		DBNo:               db.ConfigDB,
		InitIndicator:      "CONFIG_DB_INITIALIZED",
		TableNameSeparator: "|",
		KeySeparator:       "|",
	})
	var device ygot.GoStruct = &ocbinds.Device{Interfaces: &ocbinds.OpenconfigInterfaces_Interfaces{}}
	params := NewXfmrParams(PublicXfmrParams{
		YgRoot: &device,
		D:      configDb,
	})
	err := modifyPortFootprint("Ethernet1/1/1", "2x200G", "1x100G", *params)
	if err == nil {
		t.Fatalf("Expecting generatePortInConfigPortsList raise error")
	}
}

func TestModifyPortFootprintWithUnSupportedFromMode(t *testing.T) {
	configDb, _ := db.NewDB(db.Options{
		DBNo:               db.ConfigDB,
		InitIndicator:      "CONFIG_DB_INITIALIZED",
		TableNameSeparator: "|",
		KeySeparator:       "|",
	})
	var device ygot.GoStruct = &ocbinds.Device{Interfaces: &ocbinds.OpenconfigInterfaces_Interfaces{}}
	params := NewXfmrParams(PublicXfmrParams{
		YgRoot: &device,
		D:      configDb,
	})
	err := modifyPortFootprint("Ethernet1/1/1", "1x100G", "2x200G", *params)
	if err == nil {
		t.Fatalf("Expecting generateDeployedPortsList raise error")
	}
}

func TestGenerateDeployedPortsListWithInvalidMode(t *testing.T) {
	var device ygot.GoStruct = &ocbinds.Device{Interfaces: &ocbinds.OpenconfigInterfaces_Interfaces{}}
	_, err := generatePortInConfigPortsList("Ethernet1/1/1", "1x100G", (device).(*ocbinds.Device).Interfaces, nil, REPLACE)
	if err == nil {
		t.Errorf("Expect error with invalid to_mode")
	}

}

func TestHaveLaneSetsChangedWithInvalidMode(t *testing.T) {
	_, err := haveLaneSetsChanged("Ethernet1/1/1", "1x100G", "2X200G")
	if err == nil {
		t.Errorf("Expect error with invalid to_mode")
	}

	_, err = haveLaneSetsChanged("Ethernet1/1/1", "2x200G", "1X100G")
	if err == nil {
		t.Errorf("Expect error with invalid from_mode")
	}

}

func TestYangToDb_port_breakout_subtree_xfmrWithInvalidPlatObj(t *testing.T) {
	var device ygot.GoStruct = &ocbinds.Device{Components: &ocbinds.OpenconfigPlatform_Components{}}
	params := NewXfmrParams(PublicXfmrParams{YgRoot: &device})
	if _, expectErr := YangToDb_port_breakout_subtree_xfmr(*params); expectErr == nil {
		t.Errorf("Expect error with invalid platObj")
	}
}
func TestYangToDb_port_breakout_subtree_xfmrWithInvalidXcvrName(t *testing.T) {
	var device ygot.GoStruct = &ocbinds.Device{Components: &ocbinds.OpenconfigPlatform_Components{
		Component: map[string]*ocbinds.OpenconfigPlatform_Components_Component{
			"1/4": &ocbinds.OpenconfigPlatform_Components_Component{},
		},
	}}
	params := NewXfmrParams(PublicXfmrParams{
		YgRoot: &device,
		Uri:    "openconfig-platform:components/component[name=]/port/openconfig-platform-port:breakout-mode/groups/group[index=0]/config",
	})
	if _, expectErr := YangToDb_port_breakout_subtree_xfmr(*params); expectErr == nil {
		t.Errorf("Expect error with invalid xcvr name")
	}
}
func TestYangToDb_port_breakout_subtree_xfmrWithInvalidGroupIndex(t *testing.T) {
	configDb, _ := db.NewDB(db.Options{
		DBNo:               db.ConfigDB,
		InitIndicator:      "CONFIG_DB_INITIALIZED",
		TableNameSeparator: "|",
		KeySeparator:       "|",
	})
	var device ygot.GoStruct = &ocbinds.Device{Components: &ocbinds.OpenconfigPlatform_Components{
		Component: map[string]*ocbinds.OpenconfigPlatform_Components_Component{
			"1/4": &ocbinds.OpenconfigPlatform_Components_Component{
				Port: &ocbinds.OpenconfigPlatform_Components_Component_Port{
					BreakoutMode: &ocbinds.OpenconfigPlatform_Components_Component_Port_BreakoutMode{
						Groups: &ocbinds.OpenconfigPlatform_Components_Component_Port_BreakoutMode_Groups{
							Group: map[uint8]*ocbinds.OpenconfigPlatform_Components_Component_Port_BreakoutMode_Groups_Group{
								10: &ocbinds.OpenconfigPlatform_Components_Component_Port_BreakoutMode_Groups_Group{},
							},
						},
					},
				},
			},
		},
	}}
	params := NewXfmrParams(PublicXfmrParams{
		YgRoot: &device,
		Uri:    "openconfig-platform:components/component[name=1/4]/port/openconfig-platform-port:breakout-mode/groups/group[index=0]/config",
		D:      configDb,
	})
	if _, expectErr := YangToDb_port_breakout_subtree_xfmr(*params); expectErr == nil {
		t.Errorf("Expect error with invalid group index")
	}
}

func TestSanitizeMode(t *testing.T) {
	to_mode_per_platform, _ := sanitizeMode("2x100G4_1x200G4")
	if to_mode_per_platform != "2x100G(4)+1x200G(4)" {
		t.Errorf("Unexpected to_mode_per_platform %s", to_mode_per_platform)
	}

	to_mode_per_sonic, _ := sanitizeMode("2x100G(4)+1x200G(4)")
	if to_mode_per_sonic != "2x100G4_1x200G4" {
		t.Errorf("Unexpected to_mode_per_sonic %s", to_mode_per_sonic)
	}
}
