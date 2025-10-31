package platform

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

var platformCfgBackup platformConfig

func savePlatformConfig() {
	platformCfgBackup = platformCfg
}

func restorePlatformConfig() {
	platformCfg = platformCfgBackup
}

func loadTestJson(t *testing.T, jsonStr string) error {
	file, err := os.CreateTemp("", "platform_test_tmp-*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(file.Name())
	_, err = file.WriteString(jsonStr)

	return doParsePlatformJson(file.Name())
}

func TestMissingJsonFile(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	err := doParsePlatformJson("/path/to/nonexistent/file")
	if err == nil {
		t.Errorf("No error generated for missing platform.json file")
	}
}

func TestMalformedJsonFile(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	// Missing comma creates an invalid json
	if err := loadTestJson(t, `{"interfaces": {"eth1":{} "eth2":{} }}`); err == nil {
		t.Errorf("No error generated for malformed platform.json file")
	}
}

func TestMalformedIntfName(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	badJsons := []string{
		// Interface naming is required to be <prefix>1/<index> or <prefix>1/<index>/<subindex>
		`{"interfaces": {"eth1":{}, "eth2":{} }}`,
		// Interface index must be an integer
		`{"interfaces": {"ethernetA/B/C":{} }}`,
		// Interface subindex must be an integer
		`{"interfaces": {"ethernet1/2/C":{} }}`,
	}

	for _, badJson := range badJsons {
		if err := loadTestJson(t, badJson); err == nil {
			t.Errorf("No error generated for malformed interface case: \"%s\"", badJson)
		}
	}
}

func TestMalformedIndex(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	badJsons := []string{
		// Index should not be empty
		`{"interfaces": {"ethernet1/7/1":{ "index":"" } }}`,
		`{"interfaces": {"ethernet1/7/1":{ "xedni":"a" } }}`,
		// Index should be an integer
		`{"interfaces": {"ethernet1/7/1":{ "index":"NotAnInteger" } }}`,
		// Index should match the name
		`{"interfaces": {"ethernet1/7/1":{ "index":"12345" } }}`,
	}

	for _, badJson := range badJsons {
		if err := loadTestJson(t, badJson); err == nil {
			t.Errorf("No error generated for malformed interface case: \"%s\"", badJson)
		}
	}
}

func TestMalformedBrkoutMode(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	badJsons := []string{
		// Breakout mode should not be empty
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"" } }}`,
		`{"interfaces": {"ethernet1/7/1":{ "index":"7" } }}`,
		// Default mode should be sane if it is present
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G", "default_brkout_mode":"nonsense mode" } }}`,
		// Default mode should be a single speed if it is present
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G[200G,100G]", "default_brkout_mode":"1x400G[200G]" } }}`,
		// Modes should be sane
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G, 2x400G, 2xOneHundredG" } }}`,
	}

	for _, badJson := range badJsons {
		if err := loadTestJson(t, badJson); err == nil {
			t.Errorf("No error generated for malformed interface case: \"%s\"", badJson)
		}
	}
}

func TestMalformedLanes(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	badJsons := []string{
		// Lanes should be integers
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G", "lanes":"a,b,c" } }}`,
		// Lanes should be have empty entries
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G", "lanes":"1,2,,3" } }}`,
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G", "lanes":"1,2,3," } }}`,
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G", "lanes":",1,2,3" } }}`,
		// Should have enough lanes for all children
		`{"interfaces": {"ethernet1/7/1":{ "index":"7,7,7", "breakout_modes":"1x400G", "lanes": "12,13", "alias_at_lanes": "foo,bar,baz"},
		                 "ethernet1/7/2":{ "index":"7", "breakout_modes":"1x400G"},
		                 "ethernet1/7/3":{ "index":"7", "breakout_modes":"1x400G"} }}`,
	}

	for _, badJson := range badJsons {
		if err := loadTestJson(t, badJson); err == nil {
			t.Errorf("No error generated for malformed interface case: \"%s\"", badJson)
		}
	}
}

func TestMalformedPrimary(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	badJsons := []string{
		// Primary intf missing
		`{"interfaces": {"ethernet1/7/2":{ "index":"7", "breakout_modes":"1x400G" } }}`,
		// Primary intf isn't a primary
		`{"interfaces": {"ethernet1/7/1":{ "index":"7", "breakout_modes":"1x400G" }, "ethernet1/7/2":{ "index":"7", "breakout_modes":"1x400G" }}}`,
	}

	for _, badJson := range badJsons {
		if err := loadTestJson(t, badJson); err == nil {
			t.Errorf("No error generated for malformed interface case: \"%s\"", badJson)
		}
	}
}

func TestMalformedAlias(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()
	badJsons := []string{
		// Subindex references bad alias
		`{"interfaces": {"ethernet1/7/1":{ "index":"7,7", "breakout_modes":"1x400G", "lanes": "12,13", "alias_at_lanes": "foo, bar" },
		                 "ethernet1/7/100":{ "index":"7", "breakout_modes":"1x400G"  } }}`,
	}

	for _, badJson := range badJsons {
		if err := loadTestJson(t, badJson); err == nil {
			t.Errorf("No error generated for malformed interface case: \"%s\"", badJson)
		}
	}
}

func TestSanitizeBreakoutMode(t *testing.T) {
	good := []string{"1x400G", "2x800G[400G]", "8x200G[100G,50G]", "4x800G[100G,50G,40G]", "2x100G(2)+6x200G(6)", "2x200G[100G,50G](2)+6x200G(6)", "2x200G[100G,50G](2)+6x200G[100G](6)", "3x200G(6)+1x800G[400G](2)"}
	bad_single := []string{"1X400G", "1x400g", "400G", "ax400G", "x400G", "1x4O0G", "1x[400G,100G]", "4x800G[100G, 50G, 40G]", "1x400G[]", "1x400G[", "1x400G]", "1x400G(100G,200G)", "1x400G[200G 100G]", "1x400G[200, 100]", "1x400G+1x200G"}
	bad_mixed := []string{"1x400G+1x200G", "2x400G[200G]+4x100G", "2x400G+4x200G[100G]", "2x400G[200G,100G]+4x200G[100G]", "1x400G(a)+1x200G(4)", "1x400G(2)+1x200G)4("}
	bad := append(bad_single, bad_mixed...)

	for _, mode := range good {
		if _, ok := sanitizeBreakoutMode(mode); !ok {
			t.Errorf("Error generated for valid mode: \"%s\"", mode)
		}
	}
	for _, mode := range bad {
		if _, ok := sanitizeBreakoutMode(mode); ok {
			t.Errorf("No error generated for invalid mode: \"%s\"", mode)
		}
	}
}

func TestBrkoutModeToSpeeds(t *testing.T) {
	var cases = []struct {
		mode   string
		speeds []int
	}{
		{mode: "1x400G", speeds: []int{400}},
		{mode: "2x400G[200G,100G]", speeds: []int{100, 200, 400}},
		{mode: "4x200G[100G]", speeds: []int{100, 200}},
		{mode: "1x400G(4)+2x200G(4)", speeds: []int{200, 400}},
		{mode: "2x200G(4)+1x400G(4)", speeds: []int{200, 400}},
		{mode: "8x100G", speeds: []int{100}},
		{mode: "2x800G[400G,200G](4)+1x400G(4)", speeds: []int{200, 400, 800}},
	}
	for _, c := range cases {
		speeds, err := brkoutModeToSpeeds(c.mode)
		if err != nil {
			t.Errorf("Unexpected error %v for mode %s", err, c.mode)
		}
		if len(speeds) != len(c.speeds) {
			t.Errorf("Unexpected speeds %v for mode %s", speeds, c.mode)
		}
		for i, _ := range speeds {
			if speeds[i] != c.speeds[i] {
				t.Errorf("Unexpected speeds %v for mode %s", speeds, c.mode)
			}
		}
	}
}

func TestPlatIntfAlias(t *testing.T) {
	for i := 1; i <= 8; i++ {
		intf := fmt.Sprintf("Ethernet1/1/%d", i)
		expectedAlias := fmt.Sprintf("Eth1/%d", i)
		pIntf, err := platIntfByName(intf)
		if err != nil {
			t.Errorf("Unexpected error %v for interface %s", err, intf)
		}
		if pIntf.alias != expectedAlias {
			t.Errorf("Unexpected alias for interface %s (%#v)", intf, pIntf)
		}
	}
}

func laneSliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIntfsFromBrkoutMode(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()

	_, err := IntfsFromBrkoutMode("foo", "Some Mode")
	if err == nil {
		t.Errorf("No error generated for invalid interface")
	}

	platJson :=
		`{
			"interfaces": {
				"test1/12/1": {
					"lanes": "80,81,82,83,84,85,86,87",
					"index": "12,12,12,12,12,12,12,12",
					"default_brkout_mode": "2x800G",
					"breakout_modes": "2x800G, 4x400G, 8x200G[100G,50G], 1x800G(4)+2x400G(4), 1x800G(4)+4x200G[100G,50G](4), 4x200G[100G,50G](4)+2x400G(4)",
					"alias_at_lanes": "Tst12/1, Tst12/2, Tst12/3, Tst12/4, Tst12/5, Tst12/6, Tst12/7, Tst12/8"
				},
				"test1/12/2": {
					"index": "12",
					"breakout_modes": "1x200G[100G,50G]"
				},
				"test1/12/3": {
					"index": "12",
					"breakout_modes": "1x400G[200G,100G,50G]"
				},
				"test1/12/4": {
					"index": "12",
					"breakout_modes": "1x200G[100G,50G]"
				},
				"test1/12/5": {
					"index": "12",
					"breakout_modes": "1x800G, 2x400G, 4x200G[100G,50G]"
				},
				"test1/12/6": {
					"index": "12",
					"breakout_modes": "1x200G[100G,50G]"
				},
				"test1/12/7": {
					"index": "12",
					"breakout_modes": "1x400G[200G,100G,50G]"
				},
				"test1/12/8": {
					"index": "12",
					"breakout_modes": "1x200G[100G,50G]"
				}
			}
		}`
	file, err := os.CreateTemp("", "platform_test_tmp-*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(file.Name())
	_, err = file.WriteString(platJson)

	err = doParsePlatformJson(file.Name())
	if err != nil {
		t.Errorf("Unexpected error parsing platform json: %v", err)
	}

	/* Check default mode */
	mode := ""
	intfs, err := IntfsFromBrkoutMode("test1/12/1", mode)
	if err != nil {
		t.Errorf("Unexpected error generated for case: \"%s\"", mode)
	}
	if len(intfs) != 2 {
		t.Errorf("Unexpected number of intfs generated for case: \"%s\", %#v", mode, intfs)
	}
	if intfs[0].Name != "test1/12/1" ||
		intfs[0].Index != 12 ||
		!laneSliceEqual(intfs[0].Lanes, []int{80, 81, 82, 83}) ||
		intfs[0].Alias != "Tst12/1" ||
		intfs[0].SpeedMbps != 800000 {
		t.Errorf("Unexpected intf generated for case: \"%s\", %#v", mode, intfs[0])
	}
	if intfs[1].Name != "test1/12/5" ||
		intfs[1].Index != 12 ||
		!laneSliceEqual(intfs[1].Lanes, []int{84, 85, 86, 87}) ||
		intfs[1].Alias != "Tst12/5" ||
		intfs[1].SpeedMbps != 800000 {
		t.Errorf("Unexpected intf generated for case: \"%s\", %#v", mode, intfs[1])
	}

	/* Test with mode specified */
	mode = "2x800G"
	intfs, err = IntfsFromBrkoutMode("test1/12/1", mode)
	if err != nil {
		t.Errorf("Unexpected error generated for case: \"%s\"", mode)
	}
	if len(intfs) != 2 {
		t.Errorf("Unexpected number of intfs generated for case: \"%s\", %#v", mode, intfs)
	}
	if intfs[0].Name != "test1/12/1" ||
		intfs[0].Index != 12 ||
		!laneSliceEqual(intfs[0].Lanes, []int{80, 81, 82, 83}) ||
		intfs[0].Alias != "Tst12/1" ||
		intfs[0].SpeedMbps != 800000 {
		t.Errorf("Unexpected intf generated for case: \"%s\", %#v", mode, intfs[0])
	}
	if intfs[1].Name != "test1/12/5" ||
		intfs[1].Index != 12 ||
		!laneSliceEqual(intfs[1].Lanes, []int{84, 85, 86, 87}) ||
		intfs[1].Alias != "Tst12/5" ||
		intfs[1].SpeedMbps != 800000 {
		t.Errorf("Unexpected intf generated for case: \"%s\", %#v", mode, intfs[1])
	}

	/* Test a more complex mixed mode case */
	mode = "4x50G(4)+2x400G(4)"
	intfs, err = IntfsFromBrkoutMode("test1/12/1", mode)
	if err != nil {
		t.Errorf("Unexpected error generated for case: \"%s\"", mode)
	}
	if len(intfs) != 6 {
		t.Errorf("Unexpected number of intfs generated for case: \"%s\", %#v", mode, intfs)
	}
	for i := 0; i < 4; i++ {
		if intfs[i].Name != fmt.Sprintf("test1/12/%d", i+1) ||
			intfs[i].Index != 12 ||
			!laneSliceEqual(intfs[i].Lanes, []int{80 + i}) ||
			intfs[i].Alias != fmt.Sprintf("Tst12/%d", i+1) ||
			intfs[i].SpeedMbps != 50000 {
			t.Errorf("Unexpected intf[%d] generated for case: \"%s\", %#v", i, mode, intfs[i])
		}
	}
	if intfs[4].Name != "test1/12/5" ||
		intfs[4].Index != 12 ||
		!laneSliceEqual(intfs[4].Lanes, []int{84, 85}) ||
		intfs[4].Alias != "Tst12/5" ||
		intfs[4].SpeedMbps != 400000 {
		t.Errorf("Unexpected intf generated for case: \"%s\", %#v", mode, intfs[4])
	}
	if intfs[5].Name != "test1/12/7" ||
		intfs[5].Index != 12 ||
		!laneSliceEqual(intfs[5].Lanes, []int{86, 87}) ||
		intfs[5].Alias != "Tst12/7" ||
		intfs[5].SpeedMbps != 400000 {
		t.Errorf("Unexpected intf generated for case: \"%s\", %#v", mode, intfs[5])
	}
}

func TestValidSpeedsForIf(t *testing.T) {
	badIntf := "nonsense"
	intfAndSpeed := []struct {
		intf   string
		speeds []string
	}{
		{intf: "Ethernet-BP1/35", speeds: []string{"1000"}},
		{intf: "Ethernet1/1234/1", speeds: []string{"40000", "100000", "200000"}},
		{intf: "Ethernet1/31/7", speeds: []string{"10000", "25000", "50000", "100000", "200000"}},
	}

	_, err := ValidSpeedsForIf(badIntf)
	if err == nil {
		t.Errorf("No error generated for invalid interface %s", badIntf)
	}
	for _, c := range intfAndSpeed {
		speeds, err := ValidSpeedsForIf(c.intf)
		if err != nil {
			t.Errorf("Unexpected error (%v) generated for interface %s", err, c.intf)
		}
		if strings.Join(speeds, ",") != strings.Join(c.speeds, ",") {
			t.Errorf("Unexpected speeds (%v) generated for interface %s", speeds, c.intf)
		}
	}
}

func TestDefaultBrkoutMode(t *testing.T) {
	intf := "Ethernet1/2/1"
	mode, err := DefaultBrkoutMode(intf)
	if err != nil {
		t.Errorf("Unexpected error %v for interface %s", err, intf)
	}
	if mode != "2x400G" {
		t.Errorf("Unexpected default brkout for interface %s (%s)", intf, mode)
	}
}

func TestPlatformIntfLanesStr(t *testing.T) {
	lanes := PlatformIntfLanesStr("ethernet1/1/1")
	if lanes != "" {
		t.Errorf("Unexpected result \"%s\"", lanes)
	}
	lanes = PlatformIntfLanesStr("Ethernet1/1/1")
	if lanes != "9,10,11,12,13,14,15,16" {
		t.Errorf("Unexpected result \"%s\"", lanes)
	}
	lanes = PlatformIntfLanesStr("Ethernet1/1/2")
	if lanes != "10,11,12,13,14,15,16" {
		t.Errorf("Unexpected result \"%s\"", lanes)
	}
	lanes = PlatformIntfLanesStr("Ethernet1/1/3")
	if lanes != "11,12,13,14,15,16" {
		t.Errorf("Unexpected result \"%s\"", lanes)
	}
	lanes = PlatformIntfLanesStr("Ethernet1/1/8")
	if lanes != "16" {
		t.Errorf("Unexpected result \"%s\"", lanes)
	}
}

func TestInterfaceNameFromPort(t *testing.T) {
	ifName := InterfaceNameFromPort("1/1")
	if ifName != "Ethernet1/1/1" {
		t.Errorf("Unexpected result \"%s\"", ifName)
	}
	ifName = InterfaceNameFromPort("1/100000")
	if ifName != "" {
		t.Errorf("Unexpected result \"%s\"", ifName)
	}
}

func TestPortNameFromInterface(t *testing.T) {
	portName, err := PortNameFromInterface("Ethernet1/1/1")
	if err != nil || portName != "1/1" {
		t.Errorf("Unexpected result (PortName %s, err %v)", portName, err)
	}
	portName, err = PortNameFromInterface("Ethernet1/1/100000000")
	if err == nil || portName != "" {
		t.Errorf("Unexpected result (PortName %s, err %v)", portName, err)
	}
}

func TestValidMixedSpeedBrkoutMode(t *testing.T) {
	cases := []struct {
		tgtMode        string
		supportedModes []string
		expected       bool
	}{
		{
			tgtMode:        "1x400G(4)+2x200G(4)",
			supportedModes: []string{"1x400G(4)+4x100G[50G](4)"},
			expected:       false,
		},
		{
			tgtMode:        "1x400G(4)+2x200G(4)",
			supportedModes: []string{"1x400G(4)+4x100G[50G](4)", "1x400G(4)+2x200G[100G](4)"},
			expected:       true,
		},
		{
			tgtMode:        "2x400G(4)+2x200G(4)",
			supportedModes: []string{"4x400G[200G,100G]"},
			expected:       true,
		},
	}
	for _, c := range cases {
		rv := validMixedSpeedBrkoutMode(c.tgtMode, c.supportedModes)
		if rv != c.expected {
			t.Error("Mode %s, supportedModes %v, returned %v instead of %v", c.tgtMode, c.supportedModes, rv, c.expected)
		}
	}
}

func TestPrimaryToChildren(t *testing.T) {
	if _, err := PrimaryIntfToChildIntfs("BadInterface"); err == nil {
		t.Errorf("No error generated for non-existent primary")
	}
	if _, err := PrimaryIntfToChildIntfs("Ethernet1/1/2"); err == nil {
		t.Errorf("No error generated for non-primary")
	}
	group, err := PrimaryIntfToChildIntfs("Ethernet1/1/1")
	if err != nil {
		t.Errorf("Unexpected error %v")
	}
	expected := make([]string, 8)
	for i := 1; i <= 8; i++ {
		expected[i-1] = fmt.Sprintf("Ethernet1/1/%d", i)
	}
	if !reflect.DeepEqual(group, expected) {
		t.Errorf("Expected %v, got %v", expected, group)
	}
}

func TestLaneSetFromBrkoutMode(t *testing.T) {
	savePlatformConfig()
	defer restorePlatformConfig()

	/* Unknown interface should fail */
	if _, err := LaneSetFromBrkoutMode("BadInterface", "1x400G"); err == nil {
		t.Errorf("No error generated for non-existent interface")
	}

	platJsonNoDflt :=
		`{
			"interfaces": {
				"test1/12/1": {
					"lanes": "80,81,82,83,84,85,86,87",
					"index": "12,12,12,12,12,12,12,12",
					"breakout_modes": "2x800G, 4x400G, 8x200G[100G,50G], 1x800G(4)+2x400G(4), 1x800G(4)+4x200G[100G,50G](4), 4x200G[100G,50G](4)+2x400G(4)",
					"alias_at_lanes": "Tst12/1, Tst12/2, Tst12/3, Tst12/4, Tst12/5, Tst12/6, Tst12/7, Tst12/8"
				},
				"test1/12/2": { "index": "12" },
				"test1/12/3": { "index": "12" },
				"test1/12/4": { "index": "12" },
				"test1/12/5": { "index": "12" },
				"test1/12/6": { "index": "12" },
				"test1/12/7": { "index": "12" },
				"test1/12/8": { "index": "12" }
			}
		}`
	/* Same as above but adds a default breakout mode */
	platJson :=
		`{
			"interfaces": {
				"test1/12/1": {
					"lanes": "80,81,82,83,84,85,86,87",
					"index": "12,12,12,12,12,12,12,12",
					"default_brkout_mode": "2x800G",
					"breakout_modes": "2x800G[400G], 4x200G, 8x200G[100G,50G], 1x800G(4)+2x400G(4), 1x800G(4)+4x200G[100G,50G](4), 4x200G[100G,50G](4)+2x400G(4)",
					"alias_at_lanes": "Tst12/1, Tst12/2, Tst12/3, Tst12/4, Tst12/5, Tst12/6, Tst12/7, Tst12/8"
				},
				"test1/12/2": { "index": "12" },
				"test1/12/3": { "index": "12" },
				"test1/12/4": { "index": "12" },
				"test1/12/5": { "index": "12" },
				"test1/12/6": { "index": "12" },
				"test1/12/7": { "index": "12" },
				"test1/12/8": { "index": "12" }
			}
		}`
	/* Uses only four lanes */
	platJsonFourLane :=
		`{
			"interfaces": {
				"test1/12/1": {
					"lanes": "80,81,82,83",
					"index": "12,12,12,12",
					"default_brkout_mode": "2x800G",
					"breakout_modes": "2x800G[400G], 4x200G, 8x200G[100G,50G], 1x800G(4)+2x400G(4), 1x800G(4)+4x200G[100G,50G](4), 4x200G[100G,50G](4)+2x400G(4)",
					"alias_at_lanes": "Tst12/1, Tst12/2, Tst12/3, Tst12/4"
				},
				"test1/12/2": { "index": "12" },
				"test1/12/3": { "index": "12" },
				"test1/12/4": { "index": "12" }
			}
		}`

	if err := loadTestJson(t, platJsonNoDflt); err != nil {
		t.Fatalf("Error %v loading test json", err)
	}
	if _, err := LaneSetFromBrkoutMode("test1/12/2", "1x400G"); err == nil {
		t.Errorf("No error generated for non-primary")
	}
	if _, err := LaneSetFromBrkoutMode("test1/12/1", ""); err == nil {
		t.Errorf("No error generated for no-default case")
	}

	if err := loadTestJson(t, platJson); err != nil {
		t.Fatalf("Error %v loading test json", err)
	}
	/* Check that the default mode (2x800G specified above) is picked up */
	dfltSet, err := LaneSetFromBrkoutMode("test1/12/1", "")
	if err != nil {
		t.Errorf("Unexpected error for default case: %v", err)
	}
	explicitDfltSet, err := LaneSetFromBrkoutMode("test1/12/1", "2x800G")
	if err != nil {
		t.Errorf("Unexpected error for explicit default case: %v", err)
	}
	if !reflect.DeepEqual(dfltSet, explicitDfltSet) {
		t.Errorf("Lane sets for default and explicit default cases not equal: %v, %v", dfltSet, explicitDfltSet)
	}

	/* An alternate speed list should be ignored, not sure why it would ever be provided though... */
	altDfltSet, err := LaneSetFromBrkoutMode("test1/12/1", "2x800G[400G]")
	if err != nil {
		t.Errorf("Unexpected error for alternate default case: %v", err)
	}
	if !reflect.DeepEqual(altDfltSet, explicitDfltSet) {
		t.Errorf("Lane sets for alternate and explicit default cases not equal: %v, %v", altDfltSet, explicitDfltSet)
	}

	cases := []struct {
		mode string
		exp  [][]int
	}{
		/* 2x breakout should give two groups of four lanes each. */
		{mode: "2x800G", exp: [][]int{{80, 81, 82, 83}, {84, 85, 86, 87}}},
		/* 4x breakout should give four groups of two lanes each. */
		{mode: "4x200G", exp: [][]int{{80, 81}, {82, 83}, {84, 85}, {86, 87}}},
		/* 8x breakout should give eight groups of one lane each. */
		{mode: "8x200G", exp: [][]int{{80}, {81}, {82}, {83}, {84}, {85}, {86}, {87}}},
		/* Mixed mode, three interfaces with four, two, and two lanes. */
		{mode: "1x800G(4)+2x400G(4)", exp: [][]int{{80, 81, 82, 83}, {84, 85}, {86, 87}}},
	}
	for _, c := range cases {
		laneSet, err := LaneSetFromBrkoutMode("test1/12/1", c.mode)
		if err != nil {
			t.Errorf("Unexpected error for case: %v", err)
		}
		if !reflect.DeepEqual(laneSet, c.exp) {
			t.Errorf("Lane set for case %s incorrect: want %v, got %v", c.mode, c.exp, laneSet)
		}
	}

	/* A bad case where too many interfaces are requested (unsupported mode). */
	_, err = LaneSetFromBrkoutMode("test1/12/1", "32x800G")
	if err == nil {
		t.Errorf("No error for invalid breakout mode")
	}

	if err := loadTestJson(t, platJsonFourLane); err != nil {
		t.Fatalf("Error %v loading test json", err)
	}
	/* Bad cases where too many lanes are requested (test1/12 only has 4 lanes!). */
	_, err = LaneSetFromBrkoutMode("test1/12/1", "8x200G")
	if err == nil {
		t.Errorf("No error for invalid breakout mode")
	}
	_, err = LaneSetFromBrkoutMode("test1/12/1", "1x800G(4)+2x400G(4)")
	if err == nil {
		t.Errorf("No error for invalid breakout mode")
	}
}
