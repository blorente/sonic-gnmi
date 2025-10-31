package transformer

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	"github.com/openconfig/ygot/ygot"
)

// Named OcsPort as Port conflicts with name in xfmr_platform.go
type OcsPorts = ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_Ports
type OcsPort = ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_Ports_Port
type PortConnections = ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_PortConnections
type PortConnection = ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_PortConnections_PortConnection
type OcsPortConfig = ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_Ports_Port_Config
type PortConnectionConfig = ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_PortConnections_PortConnection_Config

const (
	// OCS topology table name.
	OCS_TOPOLOGY_TABLE_NAME = "OCS_TOPOLOGY"

	// OCS topology keys for lookup of valid slot and port ranges.
	PORT_CONNECTIVITY_KEY               = "port-connectivity"
	TOTAL_OPTICAL_MODULE_COUNT_KEY      = "total-optical-module-count"
	TOTAL_PORT_COUNT_KEY                = "total-port-count"
	INGRESS_PORT_COUNT_KEY              = "ingress-port-count"
	EGRESS_PORT_COUNT_KEY               = "egress-port-count"
	INGRESS_START_INDEX_KEY             = "ingress-start-index"
	EGRESS_START_INDEX_KEY              = "egress-start-index"
	INTER_MODULE_CONNECTION_SUPPORT_KEY = "inter-module-connection-support"
)

const (
	// OCS cross connection table name.
	OCS_XCONNECT_TABLE_NAME = "OCS_XCONNECTS"

	// OCS cross connection field names.
	PORT_NAME_KEY      = "port-name"
	PEER_PORT_NAME_KEY = "peer-port-name"
)

const (
	// OCS ports table name.
	OCS_PORTS_TABLE_NAME = "OCS_PORTS"

	// OCS ports field names.
	NAME_KEY        = "name"
	SLOT_NUMBER_KEY = "slot-number"
	PORT_NUMBER_KEY = "port-number"
)

const (
	// OCS port statuses table name.
	OCS_PORT_STATUS_TABLE_NAME = "OCS_PORT_STATUSES"

	// OCS port statuses field names.
	PORT_STATUS_KEY         = "port-status"
	PORT_STATUS_MESSAGE_KEY = "port-status-message"
)

var dbToYangPortConnectivityMap = map[string]ocbinds.E_OpenconfigOpticalSwitch_OpticalSwitch_OpticalSwitchTopology_State_PortConnectivity{
	"INGRESS_EGRESS": ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_OpticalSwitchTopology_State_PortConnectivity_INGRESS_EGRESS,
	"ANY_TO_ANY":     ocbinds.OpenconfigOpticalSwitch_OpticalSwitch_OpticalSwitchTopology_State_PortConnectivity_ANY_TO_ANY,
}

type ocsTopology struct {
	ConfigCached            bool
	PortConnectivity        ocbinds.E_OpenconfigOpticalSwitch_OpticalSwitch_OpticalSwitchTopology_State_PortConnectivity
	TotalOpticalModuleCount int
	TotalPortCount          int
	IngressPortCount        int
	EgressPortCount         int
	IngressStartIndex       int
	EgressStartIndex        int
	InterModuleSupport      bool
}

var cachedOcsTopology ocsTopology

func init() {
}

// Checks port and slot number against the statically configured OCS topology. The first call to this function will issue a DB read of OCS_TOPOLOGY_TABLE_NAME and cache the static configuration values which are enforced through range checks forever after.
func checkPortRangeOk(inParams XfmrParams, portNumber, slotNumber int) error {
	if !cachedOcsTopology.ConfigCached {
		if err := readAndCacheOcsTopology(inParams, &cachedOcsTopology); err != nil {
			return err
		}
	}

	// Check slot number against optic module count.
	if slotNumber < 0 || slotNumber > cachedOcsTopology.TotalOpticalModuleCount {
		return fmt.Errorf("Requested slot-number is outside of valid range. Slot-number was %v, expected 0 < slot-number <= %v", slotNumber, cachedOcsTopology.TotalOpticalModuleCount)
	}

	// Check port number against ingress and egress ranges.
	portInIngressRange := (portNumber < (cachedOcsTopology.IngressStartIndex+cachedOcsTopology.IngressPortCount) && portNumber >= cachedOcsTopology.IngressStartIndex)

	portInEgressRange := (portNumber < (cachedOcsTopology.EgressStartIndex+cachedOcsTopology.EgressPortCount) && portNumber >= cachedOcsTopology.EgressStartIndex)

	if !portInIngressRange && !portInEgressRange {
		return fmt.Errorf("Requested port-number %v is not in a valid ingress or egress range.", portNumber)
	}

	return nil
}

// Gets the DB client from the inParams if it already exists, otherwise, creates a new DB client for the specified database number.
func getOrMakeDbClient(inParams XfmrParams, dbNum db.DBNum) (*db.DB, error) {
	var err error

	dbInst := inParams.dbs[dbNum]
	if dbInst == nil {
		dbInst, err = db.NewDB(getDBOptions(dbNum))
		if err != nil {
			return nil, tlerr.InvalidArgsError{Format: err.Error()}
		}
	}

	return dbInst, nil
}

func processTopologyEntries(entry db.Value, topology *ocsTopology) error {
	// List of topology table key values to read and the location to store int value.
	topologyFieldList := []struct {
		fieldName     string
		valueLocation *int
	}{
		{TOTAL_OPTICAL_MODULE_COUNT_KEY, &cachedOcsTopology.TotalOpticalModuleCount},
		{TOTAL_PORT_COUNT_KEY, &cachedOcsTopology.TotalPortCount},
		{INGRESS_PORT_COUNT_KEY, &cachedOcsTopology.IngressPortCount},
		{EGRESS_PORT_COUNT_KEY, &cachedOcsTopology.EgressPortCount},
		{INGRESS_START_INDEX_KEY, &cachedOcsTopology.IngressStartIndex},
		{EGRESS_START_INDEX_KEY, &cachedOcsTopology.EgressStartIndex},
	}

	// Parse all read topology table values and persist to cached structure.
	for _, topologyField := range topologyFieldList {
		if value, ok := entry.Field[topologyField.fieldName]; ok {
			var err error
			*topologyField.valueLocation, err = strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("Unable to convert value from topology field %v to int. Value was %v. Error %w", topologyField.fieldName, value, err)
			}
		} else {
			return fmt.Errorf("Unable to read topology field %v from StateDB.", topologyField.fieldName)
		}
	}

	// Convert port connectivity enumeration.
	if value, ok := entry.Field[PORT_CONNECTIVITY_KEY]; ok {
		portConnectivityEnum, ok := dbToYangPortConnectivityMap[value]
		if !ok {
			return fmt.Errorf("Unable to convert port connectivity value %v from StateDB to enumeration.", value)
		}
		cachedOcsTopology.PortConnectivity = portConnectivityEnum
	} else {
		return errors.New("Unable to read port connectivity field from StateDB.")
	}

	// Convert inter-module support value.
	if value, ok := entry.Field[INTER_MODULE_CONNECTION_SUPPORT_KEY]; ok {
		cachedOcsTopology.InterModuleSupport = (value == "true")
	} else {
		return errors.New("Unable to read inter-module connectivity support field from StateDB.")
	}

	return nil
}

// Reads and caches the OCS Topology information to the given ocsTopology struct.
func readAndCacheOcsTopology(inParams XfmrParams, topology *ocsTopology) error {
	if topology == nil {
		return errors.New("Invalid topology reference.")
	}

	// Read all OCS topology field values from StateDB
	stateDB, err := getOrMakeDbClient(inParams, db.StateDB)
	if err != nil {
		return err
	}
	entry, dbErr := stateDB.GetEntry(&db.TableSpec{Name: OCS_TOPOLOGY_TABLE_NAME}, db.Key{Comp: []string{""}})
	if dbErr != nil {
		return dbErr
	}

	err = processTopologyEntries(entry, topology)
	if err != nil {
		return err
	}

	topology.ConfigCached = true
	return nil
}

// Gets the root of the optical-switch model ygot structure.
func getOpticalSwitchYgotRoot(s *ygot.GoStruct) *ocbinds.OpenconfigOpticalSwitch_OpticalSwitch {
	deviceObj := (*s).(*ocbinds.Device)
	return deviceObj.OpticalSwitch
}

//
// Port Connections
//

func makeOrLookupPortYgot(optSwRoot *ocbinds.OpenconfigOpticalSwitch_OpticalSwitch, portName string) (*OcsPort, error) {
	var err error
	// Build the Port ygot structure if it does not exist
	if optSwRoot.Ports.Port == nil {
		ygot.BuildEmptyTree(optSwRoot.Ports)
	}

	portObj, ok := optSwRoot.Ports.Port[portName]
	if !ok {
		portObj, err = optSwRoot.Ports.NewPort(portName)
		if err != nil {
			return nil, fmt.Errorf("Unable to create Port ygot structure, Error %w", err)
		}
	}
	ygot.BuildEmptyTree(portObj)

	return portObj, nil
}

// Gets the OCS slot and port number from string.
func convertOcsSlotAndPortNumber(slotStr, portStr string) (slotNum, portNum int32, err error) {
	val, err := strconv.Atoi(slotStr)
	if err != nil {
		return 0, 0, fmt.Errorf("Unable to convert slot value '%v' to int32. Error: %w", slotStr, err)
	}
	slotNumber := int32(val)

	val, err = strconv.Atoi(portStr)
	if err != nil {
		return 0, 0, fmt.Errorf("Unable to convert port value '%v' to int32. Error: %w", slotStr, err)
	}
	portNumber := int32(val)

	return slotNumber, portNumber, nil
}

// Makes a redis key string for the given name. If port name is empty, a wild-card is assumed for the key.
func makePortKey(port string) string {
	var portStr string = "*"
	if port != "" {
		portStr = port
	}

	return portStr
}

// Gets the named path variable from pathInfo. If the variable is not part of the path, returns "*" to indicate wildcard.
func getPathVarOrWildcard(pathInfo *PathInfo, varName string) string {
	pathVar := pathInfo.Var(varName)
	if pathVar == "" {
		pathVar = "*"
	}
	return pathVar
}
