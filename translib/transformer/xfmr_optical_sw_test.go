package transformer

import (
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
)

// Supplimentary coverage tests for xfmr_optical_sw. See component tests for main coverage cases.

func makeTopologyDbEntry() db.Value {
	return db.Value{
		Field: map[string]string{
			PORT_CONNECTIVITY_KEY:               "INGRESS_EGRESS",
			TOTAL_OPTICAL_MODULE_COUNT_KEY:      "1",
			TOTAL_PORT_COUNT_KEY:                "100",
			INGRESS_PORT_COUNT_KEY:              "50",
			EGRESS_PORT_COUNT_KEY:               "50",
			INGRESS_START_INDEX_KEY:             "1",
			EGRESS_START_INDEX_KEY:              "51",
			INTER_MODULE_CONNECTION_SUPPORT_KEY: "false",
		},
	}
}

func populateCachedOcsTopology() {
	cachedOcsTopology = ocsTopology{
		ConfigCached:            true,
		PortConnectivity:        ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_OpticalSwitchTopology_State_PortConnectivity_INGRESS_EGRESS,
		TotalOpticalModuleCount: 1,
		TotalPortCount:          100,
		IngressPortCount:        50,
		EgressPortCount:         50,
		IngressStartIndex:       1,
		EgressStartIndex:        10001,
		InterModuleSupport:      false,
	}
}

//
// Switch Topology
//

func TestOptSw_GetOrMakeDbClient_NoDb(t *testing.T) {
	testInParams := XfmrParams{
		dbs: [db.MaxDB]*db.DB{},
	}

	dbClient, err := getOrMakeDbClient(testInParams, db.StateDB)

	if err != nil || dbClient == nil {
		t.Fatalf("getOrMakeDbClient did not successfully create a new DB client.")
	}
}

func TestOptSw_ReadAndCacheOcsTopology_NilTopology(t *testing.T) {
	testInParams := XfmrParams{}

	err := readAndCacheOcsTopology(testInParams, nil)

	if err == nil {
		t.Fatalf("readAndCacheOcsTopology did not generate an error when parameters are nil.")
	}
}

func TestOptSw_ProcessTopologyEntries_BadTopologyIntValue(t *testing.T) {
	testDbValue := makeTopologyDbEntry()
	testDbValue.Field[TOTAL_PORT_COUNT_KEY] = "NOTANINT"

	var topology ocsTopology
	err := processTopologyEntries(testDbValue, &topology)

	if err == nil {
		t.Fatalf("processTopologyEntries did not generate an error when an invalid topology value was encountered.")
	}
}

func TestOptSw_ProcessTopologyEntries_MissingTopologyIntValue(t *testing.T) {
	testDbValue := makeTopologyDbEntry()
	delete(testDbValue.Field, TOTAL_PORT_COUNT_KEY)

	var topology ocsTopology
	err := processTopologyEntries(testDbValue, &topology)

	if err == nil {
		t.Fatalf("processTopologyEntries did not generate an error when a missing topology value was encountered.")
	}
}

//
// Port Connections
//

func TestOptSw_CheckPortRangeOk_SlotNumberOutOfRange(t *testing.T) {
	populateCachedOcsTopology()
	testInParams := XfmrParams{}

	if err := checkPortRangeOk(testInParams, 10, -1); err == nil {
		t.Fatalf("CheckPortRangeOk did not generate an error when slot number was negative.")
	}

	if err := checkPortRangeOk(testInParams, 10, cachedOcsTopology.TotalOpticalModuleCount+1); err == nil {
		t.Fatalf("CheckPortRangeOk did not generate an error when slot number exceeded number of occupied optical module slots.")
	}
}

func TestOptSw_CheckPortRangeOk_FailsToCacheOcsTopology(t *testing.T) {
	populateCachedOcsTopology()
	testInParams := XfmrParams{}

	cachedOcsTopology.ConfigCached = false

	if err := checkPortRangeOk(testInParams, 10, -1); err == nil {
		t.Fatalf("CheckPortRangeOk did not generate an error when the OCS topology failed to be cached.")
	}
}

func TestOptSw_ConvertOcsSlotAndPortNumber_BadSlotNumberString(t *testing.T) {
	if _, _, err := convertOcsSlotAndPortNumber("NOTANINT", "123"); err == nil {
		t.Fatalf("convertOcsSlotAndPortNumber did not generate an error when slot number was not an integer.")
	}
}

func TestOptSw_ConvertOcsSlotAndPortNumber_BadPortNumberString(t *testing.T) {
	if _, _, err := convertOcsSlotAndPortNumber("1", "NOTANINT"); err == nil {
		t.Fatalf("convertOcsSlotAndPortNumber did not generate an error when port number was not an integer.")
	}
}

func TestOptSw_ProcessTopologyEntries_BadConnectivityEnum(t *testing.T) {
	testDbValue := makeTopologyDbEntry()
	testDbValue.Field[PORT_CONNECTIVITY_KEY] = "NOTAVALIDENUM"

	var topology ocsTopology
	err := processTopologyEntries(testDbValue, &topology)

	if err == nil {
		t.Fatalf("processTopologyEntries did not generate an error when an invalid port connectivity enum value was encountered.")
	}
}

func TestOptSw_ProcessTopologyEntries_MissingConnectivityEnum(t *testing.T) {
	testDbValue := makeTopologyDbEntry()
	delete(testDbValue.Field, PORT_CONNECTIVITY_KEY)

	var topology ocsTopology
	err := processTopologyEntries(testDbValue, &topology)

	if err == nil {
		t.Fatalf("processTopologyEntries did not generate an error when a missing topology value was encountered.")
	}
}

func TestOptSw_ProcessTopologyEntries_MissingInterModuleSupportField(t *testing.T) {
	testDbValue := makeTopologyDbEntry()
	delete(testDbValue.Field, INTER_MODULE_CONNECTION_SUPPORT_KEY)

	var topology ocsTopology
	err := processTopologyEntries(testDbValue, &topology)

	if err == nil {
		t.Fatalf("processTopologyEntries did not generate an error when a missing topology value was encountered.")
	}
}
