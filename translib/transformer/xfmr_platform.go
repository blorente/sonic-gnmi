//////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Dell, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//////////////////////////////////////////////////////////////////////////

package transformer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/platform"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	"github.com/Azure/sonic-mgmt-common/translib/utils"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
)

const (
	PSU_TBL             = "PSU_INFO"
	FAN_TBL             = "FAN_INFO"
	FAN_TRAY_TBL        = "FAN_DRAWER_INFO"
	TRANSCEIVER_TBL     = "TRANSCEIVER_INFO"
	TRANSCEIVER_STATUS  = "TRANSCEIVER_STATUS"
	TRANSCEIVER_DOM     = "TRANSCEIVER_DOM_SENSOR"
	FPGA_TBL            = "FPGA_INFO"
	PORT_TBL            = "PORT_TABLE"
	BREAKOUT_TBL        = "BREAKOUT_CFG"
	PORT_BREAKOUT       = "PORT_BREAKOUT"
	STORAGE_INFO        = "STORAGE_INFO"
	DISK_CARRIER_TBL    = "DISK_CARRIER_INFO"
	SWITCH_EVENT        = "SWITCH_EVENT"
	NODE_CFG_TBL        = "NODE_CFG"
	SW_COMP_TBL         = "SW_COMP_INFO"
	CHASSIS_CFG         = "CHASSIS_CFG"
	CHASSIS_TBL         = "CHASSIS_INFO"
	HSM_TBL             = "FW_SECURITY_INFO"
	PCIE_TBL            = "PCIE_DEVICE"
	POWER_INFO_TBL      = "POWER_INFO"
	PHYS_ENTITY_TBL     = "PHYSICAL_ENTITY_INFO"
	CONTROLLER_CARD_TBL = "CONTROLLER_CARD_INFO"
	HUMIDITY_SENSOR_TBL = "HUMIDITY_INFO"
	EEPROM_STATUS_TBL   = "EEPROM_STATUS_INFO"
	TEMP_TBL            = "TEMPERATURE_INFO"

	XCVR_LANE_LIMIT = 8

	XCVR_KEY_PREFIX     = "Ethernet"
	STORAGE_NAME_PREFIX = "/*"
	IC_NAME_PREFIX      = "integrated_circuit"
	CHASSIS_PREFIX      = "chassis"
	NW_STACK_PREFIX     = "network_stack"
	OS_PREFIX           = "os"
	BOOTL_PREFIX        = "boot_loader"
	HVL_PREFIX          = "haven"
	DTL_PREFIX          = "dauntless"
	VR_PREFIX           = "voltage_regulator"
	PB_PREFIX           = "power_brick"
	PH_PREFIX           = "hotswap"
	PSEQ_PREFIX         = "power_sequencer"
	FAN_TRAY_PREFIX     = "FanTray"
	FAN_TRAY_SUFFIX     = "_fan_drawer"

	/** Transceiver status values **/
	SFP_STATUS_REMOVED  = "0"
	SFP_STATUS_INSERTED = "1"

	/** Upper-level URIs **/
	COMP     = "/openconfig-platform:components/component"
	COMP_ST  = "/openconfig-platform:components/component/state"
	COMP_CFG = "/openconfig-platform:components/component/config"

	/** Config container name **/
	COMP_CONFIG_NAME = "/openconfig-platform:components/component/config/name"

	/** Supported oc-platform component state URIs **/
	COMP_STATE_DESCR        = "/openconfig-platform:components/component/state/description"
	COMP_STATE_EMPTY        = "/openconfig-platform:components/component/state/empty"
	COMP_STATE_FIRM_VER     = "/openconfig-platform:components/component/state/firmware-version"
	COMP_STATE_HW_VER       = "/openconfig-platform:components/component/state/hardware-version"
	COMP_STATE_LOCATION     = "/openconfig-platform:components/component/state/location"
	COMP_STATE_MFG_DATE     = "/openconfig-platform:components/component/state/mfg-date"
	COMP_STATE_MFG_NAME     = "/openconfig-platform:components/component/state/mfg-name"
	COMP_STATE_NAME         = "/openconfig-platform:components/component/state/name"
	COMP_STATE_OPER_STATUS  = "/openconfig-platform:components/component/state/oper-status"
	COMP_STATE_PART_NO      = "/openconfig-platform:components/component/state/part-no"
	COMP_STATE_REMOVABLE    = "/openconfig-platform:components/component/state/removable"
	COMP_STATE_MODEL_NAME   = "/openconfig-platform:components/component/state/model-name"
	COMP_STATE_SERIAL_NO    = "/openconfig-platform:components/component/state/serial-no"
	COMP_STATE_SW_VER       = "/openconfig-platform:components/component/state/software-version"
	COMP_STATE_TYPE         = "/openconfig-platform:components/component/state/type"
	COMP_STATE_PARENT       = "/openconfig-platform:components/component/state/parent"
	COMP_STATE_OC_FQ_NAME   = "/openconfig-platform:components/component/state/openconfig-pins-platform:fully-qualified-name"
	COMP_STATE_GO_STRG_SIDE = "/openconfig-platform:components/component/state/google-pins-platform:storage-side"
	COMP_STATE_TEMP_CTR     = "/openconfig-platform:components/component/state/temperature"
	COMP_STATE_TEMP         = "/openconfig-platform:components/component/state/temperature/instant"
	COMP_STATE_TEMP_MAX     = "/openconfig-platform:components/component/state/temperature/max"
	COMP_STATE_TEMP_INTV    = "/openconfig-platform:components/component/state/temperature/interval"

	/** Supported PCIe component URIs **/
	COMP_STATE_PCIE                          = "/openconfig-platform:components/component/state/pcie"
	COMP_STATE_PCIE_CORRECTABLE_ERRORS       = "/openconfig-platform:components/component/state/pcie/correctable-errors"
	COMP_STATE_PCIE_CORRECTABLE_ERRORS_TOTAL = "/openconfig-platform:components/component/state/pcie/correctable-errors/total-errors"

	/** Supported Fpga component URIs **/
	FPGA_GO_COMP              = "/openconfig-platform:components/component/google-pins-platform:fpga"
	FPGA_GO_COMP_RESET_COUNT  = "/openconfig-platform:components/component/google-pins-platform:fpga/state/reset-count"
	FPGA_GO_RESET_CAUSE       = "/openconfig-platform:components/component/google-pins-platform:fpga/reset-causes/reset-cause"
	FPGA_GO_RESET_CAUSE_INDEX = "/openconfig-platform:components/component/google-pins-platform:fpga/reset-causes/reset-cause/state/index"
	FPGA_GO_RESET_CAUSE_CAUSE = "/openconfig-platform:components/component/google-pins-platform:fpga/reset-causes/reset-cause/state/cause"
	FPGA_GO_RESET_CAUSE_STATE = "/openconfig-platform:components/component/google-pins-platform:fpga/reset-causes/reset-cause/state"

	/** Supported Storage Component URIs **/
	COMP_STORAGE      = "/openconfig-platform:components/component/storage"
	COMP_STORAGE_ST   = "/openconfig-platform:components/component/storage/state"
	STORAGE_IO_ERRORS = "/openconfig-platform:components/component/storage/state/google-pins-platform:io-errors"
	G_STORAGE_WAF     = "/openconfig-platform:components/component/storage/state/google-pins-platform:write-amplification-factor"
	G_STORAGE_RRER    = "/openconfig-platform:components/component/storage/state/google-pins-platform:raw-read-error-rate"
	G_STORAGE_TP      = "/openconfig-platform:components/component/storage/state/google-pins-platform:throughput-performance"
	G_STORAGE_RSC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:reallocated-sector-count"
	G_STORAGE_POS     = "/openconfig-platform:components/component/storage/state/google-pins-platform:power-on-seconds"
	G_STORAGE_SLL     = "/openconfig-platform:components/component/storage/state/google-pins-platform:ssd-life-left"
	G_STORAGE_PCC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:power-cycle-count"
	G_STORAGE_USCOL   = "/openconfig-platform:components/component/storage/state/google-pins-platform:uncorrectable-sector-count-on-line"
	G_STORAGE_NPS     = "/openconfig-platform:components/component/storage/state/google-pins-platform:num-pure-spare"
	G_STORAGE_NIIB    = "/openconfig-platform:components/component/storage/state/google-pins-platform:num-initial-invalid-block"
	G_STORAGE_STEC    = "/openconfig-platform:components/component/storage/state/google-pins-platform:slc-total-erase-count"
	G_STORAGE_SMAXEC  = "/openconfig-platform:components/component/storage/state/google-pins-platform:slc-max-erase-count"
	G_STORAGE_SMINEC  = "/openconfig-platform:components/component/storage/state/google-pins-platform:slc-min-erase-count"
	G_STORAGE_SAEC    = "/openconfig-platform:components/component/storage/state/google-pins-platform:slc-avg-erase-count"
	G_STORAGE_TTEC    = "/openconfig-platform:components/component/storage/state/google-pins-platform:tlc-total-erase-count"
	G_STORAGE_TMAXEC  = "/openconfig-platform:components/component/storage/state/google-pins-platform:tlc-max-erase-count"
	G_STORAGE_TMINEC  = "/openconfig-platform:components/component/storage/state/google-pins-platform:tlc-min-erase-count"
	G_STORAGE_TAEC    = "/openconfig-platform:components/component/storage/state/google-pins-platform:tlc-avg-erase-count"
	G_STORAGE_WLC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:wear-leveling-count"
	G_STORAGE_PFC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:program-fail-count"
	G_STORAGE_EFC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:erase-fail-count"
	G_STORAGE_PORC    = "/openconfig-platform:components/component/storage/state/google-pins-platform:power-off-retract-count"
	G_STORAGE_HER     = "/openconfig-platform:components/component/storage/state/google-pins-platform:hardware-ecc-recovered"
	G_STORAGE_REC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:reallocation-event-count"
	G_STORAGE_UCEC    = "/openconfig-platform:components/component/storage/state/google-pins-platform:udma-crc-errors-count"
	G_STORAGE_ARS     = "/openconfig-platform:components/component/storage/state/google-pins-platform:available-reserved-space"
	G_STORAGE_WSC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:write-sector-count"
	G_STORAGE_READSC  = "/openconfig-platform:components/component/storage/state/google-pins-platform:read-sector-count"
	G_STORAGE_FWC     = "/openconfig-platform:components/component/storage/state/google-pins-platform:flash-write-count"

	/** Supported POWER SUPPLY URIs **/
	COMP_PS             = "/openconfig-platform:components/component/power-supply"
	COMP_PS_STATE       = "/openconfig-platform:components/component/power-supply/state"
	G_PS_TYPE           = "/openconfig-platform:components/component/power-supply/state/google-pins-platform:type"
	G_PS_FREQ           = "/openconfig-platform:components/component/power-supply/state/google-pins-platform:frequency"
	G_PS_COMMANDED_FREQ = "/openconfig-platform:components/component/power-supply/state/google-pins-platform:commanded-frequency"
	G_PS_MFG_STATUS     = "/openconfig-platform:components/component/power-supply/state/google-pins-platform:manufacturer-status"
	G_PS_STATUS_GPIO    = "/openconfig-platform:components/component/power-supply/state/google-pins-platform:status-gpio"

	G_PS_RAILS              = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails"
	G_PS_RAILS_RAIL         = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail"
	G_PS_RAIL_STATE         = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state"
	G_PS_RAIL_NAME          = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/name"
	G_PS_RAIL_DIRECTION     = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/direction"
	G_PS_RAIL_STATUS_VOUT   = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/status-vout"
	G_PS_RAIL_COMMANDED_VOL = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/commanded-voltage"
	G_PS_RAIL_ENERGY        = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/energy"
	G_PS_RAIL_CURRENT       = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/current"
	G_PS_RAIL_VOLTAGE       = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/voltage"
	G_PS_RAIL_POWER         = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/power"
	G_PS_RAIL_PEAK_POWER    = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/peak-power"
	G_PS_RAIL_PEAK_POWER_IV = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/peak-power-interval"
	G_PS_RAIL_PEAK_VOLTAGE  = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/peak-voltage"
	G_PS_RAIL_PEAK_CURRENT  = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/peak-current"
	G_PS_RAIL_TEMP          = "/openconfig-platform:components/component/power-supply/google-pins-platform:rails/rail/state/temperature"

	/** Supported Fan URIs **/
	COMP_FAN       = "/openconfig-platform:components/component/fan"
	COMP_FAN_ST    = "/openconfig-platform:components/component/fan/state"
	COMP_FAN_SPEED = "/openconfig-platform:components/component/fan/state/openconfig-platform-fan:speed"
	COMP_FAN_SCP   = "/openconfig-platform:components/component/fan/state/google-pins-platform:speed-control-pct"

	/** Supported Xcvr URIs **/
	XCVR_BASE_PREFIX         = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver"
	XCVR_BASE_STATE          = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver/state"
	XCVR_FORM_FACTOR         = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver/state/form-factor"
	XCVR_ETH_PMD             = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver/state/ethernet-pmd"
	XCVR_STATE_LATEST_FW_VER = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver/state/google-pins-platform:latest-available-firmware-version"
	XCVR_BASE_PC             = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver/physical-channels"
	XCVR_BASE_CHANNEL        = "/openconfig-platform:components/component/openconfig-platform-transceiver:transceiver/physical-channels/channel"

	/** Support Temperature Sensor URIs **/
	TEMP_COMP                = "/openconfig-platform:components/component/state/temperature"
	TEMP_INSTANT             = "/openconfig-platform:components/component/state/temperature/instant"
	COMP_SENSOR              = "/openconfig-platform:components/component/google-pins-platform:sensor"
	COMP_SENSOR_ST           = "/openconfig-platform:components/component/google-pins-platform:sensor/state"
	SENSOR_STATE_SENSOR_TYPE = "/openconfig-platform:components/component/google-pins-platform:sensor/state/sensor-type"
	SENSOR_STATE_INSTANT     = "/openconfig-platform:components/component/google-pins-platform:sensor/state/instant"

	/** Supported Integrated Circuit URIs **/
	COMP_IC                    = "/openconfig-platform:components/component/integrated-circuit"
	COMP_IC_CFG                = "/openconfig-platform:components/component/integrated-circuit/config"
	COMP_IC_ST                 = "/openconfig-platform:components/component/integrated-circuit/state"
	COMP_IC_ST_BH              = "/openconfig-platform:components/component/integrated-circuit/state/google-pins-platform:blackhole"
	COMP_IC_ST_CG              = "/openconfig-platform:components/component/integrated-circuit/state/google-pins-platform:congestion"
	COMP_IC_ST_BI              = "/openconfig-platform:components/component/integrated-circuit/state/google-pins-platform:bad-intervals-duration"
	COMP_IC_PLC                = "/openconfig-platform:components/component/integrated-circuit/openconfig-platform-pipeline-counters:pipeline-counters"
	COMP_IC_MEM                = "/openconfig-platform:components/component/integrated-circuit/openconfig-platform-integrated-circuit:memory"
	COMP_IC_MEM_CPE            = "/openconfig-platform:components/component/integrated-circuit/openconfig-platform-integrated-circuit:memory/state/corrected-parity-errors"
	COMP_IC_MEM_TPE            = "/openconfig-platform:components/component/integrated-circuit/openconfig-platform-integrated-circuit:memory/state/total-parity-errors"
	COMP_IC_CSIG_SUPPORTED_CFG = "/opneconfig-platform:components/component/integrated-circuit/google-pins-platform:pins-csig/google-pins-platform:config/google-pins-platform:csig-supported"
	COMP_IC_CSIG_SUPPORTED_ST  = "/openconfig-platform:components/component/integrated-circuit/google-pins-platform:pins-csig/google-pins-platform:state/google-pins-platform:csig-supported"

	/** Supported oc-platform component config URIs **/
	COMP_CFG_OC_FQ_NAME = "/openconfig-platform:components/component/config/openconfig-pins-platform:fully-qualified-name"

	/** Supported Firmware URIs **/
	FIRMWARE_CHASSIS                     = "/openconfig-platform:components/component/chassis"
	FIRMWARE_CHASSIS_ALARMS              = "/openconfig-platform:components/component/chassis/openconfig-pins-platform-chassis:alarms"
	FIRMWARE_CHASSIS_ALARMS_STATE        = "/openconfig-platform:components/component/chassis/openconfig-pins-platform-chassis:alarms/state"
	FIRMWARE_CHASSIS_ALARMS_STATE_STATUS = "/openconfig-platform:components/component/chassis/openconfig-pins-platform-chassis:alarms/state/status"
	FIRMWARE_CHASSIS_CONFIG              = "/openconfig-platform:components/component/chassis/config"
	FIRMWARE_CHASSIS_STATE               = "/openconfig-platform:components/component/chassis/state"
	FIRMWARE_CHASSIS_OC_PLATFORM         = "/openconfig-platform:components/component/chassis/state/openconfig-pins-platform-chassis:platform"
	FIRMWARE_CHASSIS_OC_BASE_MAC         = "/openconfig-platform:components/component/chassis/state/openconfig-pins-platform-chassis:base-mac-address"
	FIRMWARE_CHASSIS_OC_NUM_MAC          = "/openconfig-platform:components/component/chassis/state/openconfig-pins-platform-chassis:mac-address-pool-size"
	FIRMWARE_CHASSIS_OC_CPU_TYPE         = "/openconfig-platform:components/component/chassis/state/openconfig-pins-platform-chassis:cpu-type"

	/** Supported Software Module URIs **/
	COMP_SW_MOD                 = "/openconfig-platform:components/component/software-module"
	COMP_SW_MOD_ST              = "/openconfig-platform:components/component/software-module/state"
	SW_MODULE_STATE_MODULE_TYPE = "/openconfig-platform:components/component/software-module/state/openconfig-platform-software:module-type"

	/** Supported Port URIs **/
	COMP_PORT              = "/openconfig-platform:components/component/port"
	COMP_PORT_CFG          = "/openconfig-platform:components/component/port/config"
	COMP_PORT_ST           = "/openconfig-platform:components/component/port/state"
	PORT_CONFIG_OC_PORT_ID = "/openconfig-platform:components/component/port/config/openconfig-pins-platform-port:port-id"
	PORT_STATE_OC_PORT_ID  = "/openconfig-platform:components/component/port/state/openconfig-pins-platform-port:port-id"

	/** Supported Subcomponent URIs **/
	COMP_SUB = "/openconfig-platform:components/component/subcomponents"

	/** Supported HwSecurityModule URIs **/
	HSM_GO_SUBTREE        = "/openconfig-platform:components/component/google-pins-platform:hardware-security-module"
	HSM_GO_STATE          = "/openconfig-platform:components/component/google-pins-platform:hardware-security-module/state"
	HSM_STATE_GO_ENFORCED = "/openconfig-platform:components/component/google-pins-platform:hardware-security-module/state/secure-payload-enforced"
	HSM_STATE_GO_VER      = "/openconfig-platform:components/component/google-pins-platform:hardware-security-module/state/payload-version"
	HSM_STATE_GO_TYPE     = "/openconfig-platform:components/component/google-pins-platform:hardware-security-module/state/payload-signature-type"

	HSM_FLD_FIRMWARE_VER = "firmware-version"
	HSM_FLD_SERIAL_NO    = "serial-no"
	HSM_FLD_ENFORCED     = "secure-payload-enforced"
	HSM_FLD_PAYLOAD_VER  = "payload-version"
	HSM_FLD_TYPE         = "payload-signature-type"
)

type PSU struct {
	Capacity      string
	Enabled       bool
	Input_Current string
	Input_Voltage string
	Manufacturer  string
	Model_Name    string
	/*
		Output_Current string
		Output_Power   string
		Output_Voltage string
	*/
	Presence      bool
	Part_Number   string
	Serial_Number string
	Status        bool
	Parent        string
	Location      string
	Volt_Type     string
}

type fanInfo struct {
	hardwareRev   string
	isReplaceable bool
	location      string
	mfgDate       string
	model         string
	parent        string
	partNo        string
	presence      bool
	pwm           string
	serial        string
	speed         string
	status        bool
}

type XcvrLane struct {
	RxPowerLane string
	TxBiasLane  string
	TxPowerLane string
	TxDisable   string
}

type Xcvr struct {
	/* Most are strings since media sends 'N/A' when data is not available
	   Conversion will be done before sending along */
	Presence          bool
	Lanes             [XCVR_LANE_LIMIT]XcvrLane
	Temperature       string
	EthPmd            string
	Parent            string
	MfgName           string
	ModuleState       string
	MfgDate           string
	PartNo            string
	SerialNo          string
	HardwareRev       string
	Type              string
	LatestFirmwareVer string
}

type TempSensor struct {
	Crit_High_Threshold string
	Crit_Low_Threshold  string
	Current             string
	High_Threshold      string
	Low_Threshold       string
	Name                string
	Warning_Status      string
	Timestamp           string
}

type HumiditySensor struct {
	Name             string
	Parent           string
	RelativeHumidity string
}

/*Storage structure read from State DB*/
type Storage struct {
	Name                           string
	PartNo                         string
	SerialNo                       string
	Parent                         string
	OperStatus                     string
	IOErrors                       string
	Removable                      string
	WriteAmplificationFactor       string
	RawReadErrorRate               string
	ThroughputPerformance          string
	ReallocatedSectorCount         string
	PowerOnSeconds                 string
	SsdLifeLeft                    string
	PowerCycleCount                string
	UncorrectableSectorCountOnLine string
	NumPureSpare                   string
	NumInitialInvalidBlock         string
	SlcTotalEraseCount             string
	SlcMaxEraseCount               string
	SlcMinEraseCount               string
	SlcAvgEraseCount               string
	TlcTotalEraseCount             string
	TlcMaxEraseCount               string
	TlcMinEraseCount               string
	TlcAvgEraseCount               string
	WearLevelingCount              string
	ProgramFailCount               string
	EraseFailCount                 string
	PowerOffRetractCount           string
	Temperature                    string
	HardwareEccRecovered           string
	ReallocationEventCount         string
	UdmaCrcErrorsCount             string
	AvailableReservedSpace         string
	WriteSectorCount               string
	ReadSectorCount                string
	FlashWriteCount                string
}

type DiskCarrier struct {
	Model_Name    string
	Name          string
	Parent        string
	Part_Number   string
	Serial_Number string
}

/*IC structure read from State DB*/
type IC struct {
	Node_Id                   string
	Name                      string
	Parent                    string
	QualifiedName             string
	UnrecoverableParityErrors uint64
	CorrectedParityErrors     uint64
}

/* CSIG information structure from DB */
type CsigInfo struct {
	csigSupported string
}

/*Port structure read from DB*/
type Port struct {
	Name            string
	Parent          string
	PortID          string
	BreakoutMode    string
	FirmwareVersion string
}

/*Firmware Chassis structure read from State DB*/
type Firmware struct {
	AlarmStatus     bool
	Description     string
	Name            string
	ModelName       string
	FirmwareVersion string
	HardwareVersion string
	MfgDate         string
	OperStatus      string
	PartNo          string
	SerialNo        string
	QualifiedName   string
	BaseMac         string
	MacPoolSize     string
	CpuType         string
}

/*Fpga structure read from State DB*/
type Fpga struct {
	Description     string
	Name            string
	FirmwareVersion string
	MfgName         string
	ResetCauses     []string
	ResetCount      string
}

/*SWCompInfo structure read from State DB*/
type SWCompInfo struct {
	Name            string
	SoftwareVersion string
	Parent          string
	OperStatus      string
	StorageSide     string
}

/*HwSecurityModule structure read from State DB*/
type HwSecurityModule struct {
	Name                 string
	FirmwareVersion      string
	SerialNo             string
	SecPayloadEnforced   string
	PayloadVersion       string
	PayloadSignatureType string
}

type PowerSupplyInfo struct {
	Type                string
	CommandedFrequency  string
	Frequency           string
	ManufacturerStatus  string
	StatusGpio          string
	Location            string
	Temperature         string
	TemperatureMax      string
	TemperatureInterval string
	Direction           string
	StatusVout          string
	CommendedVoltage    string
	Energy              string
	Current             string
	Voltage             string
	Power               string
	PeakPower           string
	PeakPowerInterval   string
	PeakVoltage         string
	PeakCurrent         string
	RailTemperature     string
}

type ControllerCard struct {
	Model_Name    string
	Name          string
	Parent        string
	Part_Number   string
	Serial_Number string
}

type EepromStatus struct {
	Name                 string
	Status               string
	NumCorrectableErrors string
}

type PathType int

const (
	/* Represents all paths under /components/component */
	AllPaths PathType = iota
	/* Represents all paths under a component type, e.g.
	 * /components/component/port or /components/component/fan */
	AllCompPaths
	/* Represents all paths under /components/component/config */
	ConfigPaths
	/* Represents all paths under /components/component/state */
	StatePaths
	/* Represents a path to a specific leaf */
	SingularPath
	/* Represents all paths under /components/component/power-supply/rails/rail */
	RailPaths
)

func (pt PathType) String() string {
	switch pt {
	case AllPaths:
		return "AllPaths"
	case AllCompPaths:
		return "AllComponentPaths"
	case ConfigPaths:
		return "ConfigPaths"
	case StatePaths:
		return "StatePaths"
	case SingularPath:
		return "SingularPath"
	case RailPaths:
		return "RailPaths"
	}
	return fmt.Sprintf("%s", pt)
}

const (
	LaneIndex0 uint16 = iota
	LaneIndex1
	LaneIndex2
	LaneIndex3
	LaneIndex4
	LaneIndex5
	LaneIndex6
	LaneIndex7
)

var platformTypeMap = map[string]ocbinds.E_OpenconfigPinsPlatformChassis_PLATFORM_TYPE{
	"generic":                ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_GENERIC,
	"BX":                     ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_SWITCH1,
	"TA":                     ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_TORSWITCH1,
	"TS":                     ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_TORSWITCH1, // Torswitch2
	"alpine_vs":              ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_ALPINEVS,
	"MS":                     ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_SWITCH4,
	"HL":                     ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_SWITCH2,
	"x86_64-8122_64eh_o-r0":  ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_SWITCH3, // ligthning
	"x86_64-8122_64ehf_o-r0": ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_SWITCH3, // superbolt
}

var resetCauseMap = map[string]ocbinds.E_OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause{
	"power":    ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_POWER,
	"switch":   ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_SWITCH,
	"watchdog": ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_WATCHDOG,
	"software": ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_SOFTWARE,
	"emulator": ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_EMULATOR,
	"cpu":      ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_CPU,
}

var moduleStatusMap = map[string]ocbinds.E_OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus{
	"ModuleReady":          ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_READY,
	"ModulePwrUp":          ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_POWER_UP,
	"ModuleLowPwr":         ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_POWER_LOW,
	"ModulePwrDn":          ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_POWER_DOWN,
	"ModuleFault":          ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_FAULT,
	"ModuleStateUndefined": ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_UNDEFINED,
	"N/A":                  ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_UNKNOWN,
}

var eepromOperStatusMap = map[string]ocbinds.E_OpenconfigPlatformTypes_COMPONENT_OPER_STATUS{
	"GOOD":                ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE,
	"CORRECTABLE_ERROR":   ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE,
	"UNCORRECTABLE_ERROR": ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE,
	"INCOMPLETE_DATA":     ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE,
	"UNEXPECTED_DATA":     ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE,
	"NOT_PROCESSED":       ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE,
}

var cpuTypeMap = map[string]ocbinds.E_OpenconfigPinsPlatformChassis_CPU_TYPE{
	"p2020":      ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_P2020,
	"p2020v2":    ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_P2020V2,
	"p2041":      ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_P2041,
	"hurricane3": ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_HURRICANE3,
	"vega":       ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_VEGA,
	"capitaine":  ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_CAPITAINE,
	"segundo":    ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_SEGUNDO,
	"unknown":    ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_UKNOWN,
}

func getDbToYangPlatformType(platform string) (ocbinds.E_OpenconfigPinsPlatformChassis_PLATFORM_TYPE, error) {
	pf := ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_GENERIC
	if platform == "" {
		return pf, errors.New("chassis platform field not present in State DB")
	}
	if val, ok := platformTypeMap[platform]; ok {
		return val, nil
	}
	return pf, errors.New("chassis platform invalid field value in State DB: " + platform)
}

var dbToYangEthPmdMap = map[string]ocbinds.E_OpenconfigTransportTypes_ETHERNET_PMD_TYPE{
	"10G_LRM":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_10GBASE_LRM,
	"10G_LR":                 ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_10GBASE_LR,
	"10G_ZR":                 ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_10GBASE_ZR,
	"10G_ER":                 ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_10GBASE_ER,
	"10G_SR":                 ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_10GBASE_SR,
	"40G_CR4":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_40GBASE_CR4,
	"40G_SR4":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_40GBASE_SR4,
	"40G_LR4":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_40GBASE_LR4,
	"40G_ER4":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_40GBASE_ER4,
	"40G_PSM4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_40GBASE_PSM4,
	"4X10G_LR":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_4X10GBASE_LR,
	"4X10G_SR":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_4X10GBASE_SR,
	"100G_AOC":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100G_AOC,
	"100G_ACC":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100G_ACC,
	"100G_SR10":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_SR10,
	"100G_SR4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_SR4,
	"100G_LR4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_LR4,
	"100G_ER4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_ER4,
	"100G_CWDM4":             ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_CWDM4,
	"100G_CLR4":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_CLR4,
	"100G_PSM4":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_PSM4,
	"100G_CR4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_CR4,
	"100G_FR":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_100GBASE_FR,
	"200G_BSM8":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_200GBASE_BSM8,
	"400G_ZR":                ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_ZR,
	"400G_LR4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_LR4,
	"400G_FR4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_FR4,
	"400G_LR8":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_LR8,
	"400G_DR4":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_DR4,
	"400G_DR4+":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_DR4,
	"400G_CR8":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_CR8,
	"400G_PSM4":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GMSA_PSM4,
	"400G_PSM8":              ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_PSM8,
	"400G_SR8":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400GBASE_SR8,
	"400G_AOC":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_400G_AOC,
	"2X400G_CR4":             ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_CR4,
	"2X400G_DR4":             ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_DR4,
	"2X400G_2PSM4":           ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_2PSM4,
	"2X400G_PSM4":            ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_PSM4,
	"2X200G_CGR4+":           ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X200GBASE_CGR4_PLUS,
	"2X400G_CDGR4+":          ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_CDGR4_PLUS,
	"2X400G_ENHANCED_CDGR4+": ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_ENHANCED_CDGR4_PLUS,
	"2X400G_FR4":             ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_FR4,
	"2X200G_BGR4":            ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X200GBASE_BGR4,
	"200G_BSM8+":             ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_200GBASE_BSM8_PLUS,
	"800G_VM8":               ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_800GBASE_VM8,
	"2X400G_AOC":             ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_2X400GBASE_AOC,
	"PMD_UNKNOWN":            ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_UNDEFINED,
}

var yangToDbCsigSupportedMap = map[ocbinds.E_OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported]string{
	ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported_NOT_SUPPORTED: "none",
	ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported_SW_SUPPORTED:  "sw_assist",
	ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported_HW_SUPPORTED:  "hw_assist",
}

var dbToYangCsigSupportedMap = map[string]ocbinds.E_OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported{
	"none":      ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported_NOT_SUPPORTED,
	"sw_assist": ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported_SW_SUPPORTED,
	"hw_assist": ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_PinsCsig_Config_CsigSupported_HW_SUPPORTED,
}

func getDbToYangEthPmd(dbVal string) ocbinds.E_OpenconfigTransportTypes_ETHERNET_PMD_TYPE {
	ep := ocbinds.OpenconfigTransportTypes_ETHERNET_PMD_TYPE_ETH_UNDEFINED
	if dbVal == "" {
		log.V(lvl.ERROR).Info("transceiver ethernet-pmd not present in State DB")
		return ep
	}
	val, ok := dbToYangEthPmdMap[dbVal]
	if !ok {
		log.V(lvl.ERROR).Info("transceiver ethernet-pmd invalid field value in State DB: " + dbVal)
		return ep
	}
	return val
}

var temperatureExp = regexp.MustCompile(`(exhaust|inlet|heatsink|dimm|sfp)_sensor(_[0-9]+)?$`)
var cpuExp = regexp.MustCompile(`cpu_sensor_[0-9]+$`)

type componentType int64

const (
	CompTypeInvalid componentType = iota
	CompTypePsu
	CompTypeFan
	CompTypeFanTray
	CompTypeFpga
	CompTypeStorage
	CompTypeDiskCarrier
	CompTypeXcvr
	CompTypeTemp
	CompTypeHumidity
	CompTypeIC
	CompTypeChassis
	CompTypeNWStack
	CompTypeOS
	CompTypeBootLoader
	CompTypePort
	CompTypeHwSecurityModule
	CompTypePcie
	CompTypePowerSupplyVR
	CompTypePowerSupplyPB
	CompTypePowerSupplyPH
	CompTypePowerSupplyPSEQ
	CompTypeControllerCard
	CompTypeEepromStatus
)

func (ct componentType) String() string {
	switch ct {
	case CompTypeInvalid:
		return "CompTypeInvalid"
	case CompTypeControllerCard:
		return "CompTypeControllerCard"
	case CompTypeDiskCarrier:
		return "CompTypeDiskCarrier"
	case CompTypeEepromStatus:
		return "CompTypeEepromStatus"
	case CompTypePsu:
		return "CompTypePsu"
	case CompTypeFan:
		return "CompTypeFan"
	case CompTypeFanTray:
		return "CompTypeFanTray"
	case CompTypeFpga:
		return "CompTypeFpga"
	case CompTypeStorage:
		return "CompTypeStorage"
	case CompTypeXcvr:
		return "CompTypeXcvr"
	case CompTypeTemp:
		return "CompTypeTemp"
	case CompTypeHumidity:
		return "CompTypeHumidity"
	case CompTypeIC:
		return "CompTypeIC"
	case CompTypeChassis:
		return "CompTypeChassis"
	case CompTypeNWStack:
		return "CompTypeNWStack"
	case CompTypeOS:
		return "CompTypeOS"
	case CompTypeBootLoader:
		return "CompTypeBootLoader"
	case CompTypePort:
		return "CompTypePort"
	case CompTypeHwSecurityModule:
		return "CompTypeHwSecurityModule"
	case CompTypePcie:
		return "CompTypePcie"
	case CompTypePowerSupplyVR:
		return "CompTypePowerSupplyVR"
	case CompTypePowerSupplyPB:
		return "CompTypePowerSupplyPB"
	case CompTypePowerSupplyPH:
		return "CompTypePowerSupplyPH"
	case CompTypePowerSupplyPSEQ:
		return "CompTypePowerSupplyPSEQ"
	}
	return fmt.Sprintf("%s", ct)
}

var compTblMap = map[componentType][]string{
	CompTypePsu:              {PSU_TBL, "PSU *"},
	CompTypeFpga:             {FPGA_TBL, "fpga_*"},
	CompTypeFan:              {FAN_TBL, "fan*"},
	CompTypeFanTray:          {FAN_TRAY_TBL, "*"},
	CompTypeXcvr:             {TRANSCEIVER_STATUS, XCVR_KEY_PREFIX + "*"},
	CompTypeTemp:             {TEMP_TBL, "*_sensor*"},
	CompTypeHumidity:         {HUMIDITY_SENSOR_TBL, "humidity_*"},
	CompTypeIC:               {NODE_CFG_TBL, IC_NAME_PREFIX + "*"},
	CompTypeChassis:          {CHASSIS_TBL, CHASSIS_PREFIX},
	CompTypeNWStack:          {SW_COMP_TBL, NW_STACK_PREFIX + "*"},
	CompTypeOS:               {SW_COMP_TBL, OS_PREFIX + "*"},
	CompTypeBootLoader:       {SW_COMP_TBL, BOOTL_PREFIX + "*"},
	CompTypePort:             {PORT_BREAKOUT, ""},
	CompTypeStorage:          {STORAGE_INFO, STORAGE_NAME_PREFIX},
	CompTypeDiskCarrier:      {DISK_CARRIER_TBL, "*"},
	CompTypeHwSecurityModule: {HSM_TBL, "*"},
	CompTypePcie:             {PCIE_TBL, "*"},
	CompTypePowerSupplyVR:    {POWER_INFO_TBL, VR_PREFIX + "*"},
	CompTypePowerSupplyPB:    {POWER_INFO_TBL, PB_PREFIX + "*"},
	CompTypePowerSupplyPH:    {POWER_INFO_TBL, PH_PREFIX + "*"},
	CompTypePowerSupplyPSEQ:  {POWER_INFO_TBL, PSEQ_PREFIX + "*"},
	CompTypeControllerCard:   {CONTROLLER_CARD_TBL, "*"},
	CompTypeEepromStatus:     {EEPROM_STATUS_TBL, "*"},
}

func getCompType(name string, d *db.DB) (componentType, error) {
	compType, err := getCompTypeByName(name)
	if err == nil {
		return compType, nil
	}
	// For some types, derivation doesn't rely on a prefix check, but a table scan.
	componentTableMap := map[componentType]string{
		CompTypeControllerCard: CONTROLLER_CARD_TBL,
		CompTypeStorage:        STORAGE_INFO,
		CompTypeDiskCarrier:    DISK_CARRIER_TBL,
		CompTypePcie:           PCIE_TBL,
		CompTypeEepromStatus:   EEPROM_STATUS_TBL,
	}
	for compType, tblName := range componentTableMap {
		if _, err = d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}}); err == nil {
			return compType, nil
		}
	}
	return CompTypeInvalid, errors.New("unable to derive component type for " + name)
}

func getCompTypeByName(compName string) (componentType, error) {
	switch {
	case validPsuName(&compName):
		return CompTypePsu, nil
	case validFpgaName(&compName):
		return CompTypeFpga, nil
	case validFanName(&compName):
		return CompTypeFan, nil
	case validFanTrayName(compName):
		return CompTypeFanTray, nil
	case validXcvrName(&compName):
		return CompTypeXcvr, nil
	case validTempName(&compName) || validCpuName(compName):
		return CompTypeTemp, nil
	case validHumidityName(compName):
		return CompTypeHumidity, nil
	case validICName(&compName):
		return CompTypeIC, nil
	case strings.HasPrefix(compName, CHASSIS_PREFIX):
		return CompTypeChassis, nil
	case validSWCompName(&compName, NW_STACK_PREFIX):
		return CompTypeNWStack, nil
	case validSWCompName(&compName, OS_PREFIX):
		return CompTypeOS, nil
	case strings.HasPrefix(compName, BOOTL_PREFIX):
		return CompTypeBootLoader, nil
	case validHSMCompName(compName):
		return CompTypeHwSecurityModule, nil
	case len(platform.InterfaceNameFromPort(compName)) > 0:
		return CompTypePort, nil
	case validPSCompName(&compName, VR_PREFIX):
		return CompTypePowerSupplyVR, nil
	case validPSCompName(&compName, PB_PREFIX):
		return CompTypePowerSupplyPB, nil
	case validPSCompName(&compName, PH_PREFIX):
		return CompTypePowerSupplyPH, nil
	case validPSCompName(&compName, PSEQ_PREFIX):
		return CompTypePowerSupplyPSEQ, nil
	default:
		return CompTypeInvalid, fmt.Errorf("component name %s did not match with supported types.", compName)
	}
}

/* Helper to go from a component type to the type specific helper which reads
 * the DB data and populates the ocbinds structs.
 * createCompAndFuncCall - when fetching /components/component
 * getSysComponents - when fetching the following paths:
 *   /components/component[name=<compName>]
 *   /components/component[name=<compName>]/config
 *   /components/component[name=<compName>]/state
 */
func compTypeToFuncCall(cType componentType, compName, subKey string, pfComp *ocbinds.OpenconfigPlatform_Components_Component, targetUriPath string, dbs [db.MaxDB]*db.DB, pType PathType, ygRoot *ygot.GoStruct) error {
	log.V(lvl.DEBUG).Infof("compTypeToFuncCall with name=%s type=%v pType=%v", compName, cType, pType)
	d := dbs[db.StateDB]
	cfgdb := dbs[db.ConfigDB]
	ygot.BuildEmptyTree(pfComp)
	switch cType {
	case CompTypeStorage:
		return fillSysStorageInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypeHwSecurityModule:
		return fillHwSecurityModuleInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypePsu:
		if pType == SingularPath {
			return fillSysPsuInfo(pfComp, compName, false, false, targetUriPath, d)
		} else if pType == AllPaths {
			if err := fillSysPsuInfo(pfComp, compName, true, true, targetUriPath, d); err != nil {
				return err
			}
		}
		return fillSysPsuInfo(pfComp, compName, true, false, targetUriPath, d)
	case CompTypeFan:
		return dbToYangFan(pfComp, compName, targetUriPath, d, cType)
	case CompTypeFanTray:
		return dbToYangFan(pfComp, compName, targetUriPath, d, cType)
	case CompTypeXcvr:
		return fillSysXcvrInfo(pfComp, compName, pType != SingularPath, "", targetUriPath, dbs)
	case CompTypeTemp:
		return fillSysTempInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypeIC:
		return dbToYangIC(pfComp, compName, targetUriPath, dbs, ygRoot)
	case CompTypeFpga:
		return fillSysFpgaInfo(pfComp, compName, pType, targetUriPath, "", d)
	case CompTypeChassis:
		return fillSysFirmwareInfo(pfComp, compName, pType, targetUriPath, d, cfgdb)
	case CompTypeNWStack, CompTypeOS, CompTypeBootLoader:
		return fillSWCompInfo(pfComp, compName, pType, targetUriPath, d, cType)
	case CompTypePort:
		return fillDpbData(pfComp, compName, subKey, pType, targetUriPath, d, cfgdb)
	case CompTypePcie:
		return fillSysPcieInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypePowerSupplyVR, CompTypePowerSupplyPB, CompTypePowerSupplyPH, CompTypePowerSupplyPSEQ:
		return fillPowerSupplyInfo(pfComp, compName, "", pType, targetUriPath, d)
	case CompTypeControllerCard:
		return fillControllerCardInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypeDiskCarrier:
		return fillDiskCarrierInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypeHumidity:
		return fillSysHumidityInfo(pfComp, compName, pType, targetUriPath, d)
	case CompTypeEepromStatus:
		return fillEepromStatusInfo(pfComp, compName, pType, targetUriPath, d)
	}
	return errors.New("Invalid component type")
}

func init() {
	XlateFuncBind("DbToYang_pfm_components_xfmr", DbToYang_pfm_components_xfmr)
	XlateFuncBind("DbToYangPath_pfm_components_path_xfmr", DbToYangPath_pfm_components_path_xfmr)
	XlateFuncBind("Subscribe_pfm_components_xfmr", Subscribe_pfm_components_xfmr)
	XlateFuncBind("YangToDb_pfm_components_xfmr", YangToDb_pfm_components_xfmr)
	XlateFuncBind("YangToDb_pfm_components_ic_csig_xfmr", YangToDb_pfm_components_ic_csig_xfmr)
	XlateFuncBind("DbToYang_pfm_components_ic_csig_xfmr", DbToYang_pfm_components_ic_csig_xfmr)
}

func getPfmRootObject(s *ygot.GoStruct) *ocbinds.OpenconfigPlatform_Components {
	if s == nil {
		return nil
	}
	deviceObj := (*s).(*ocbinds.Device)
	return deviceObj.Components
}

var YangToDb_pfm_components_ic_csig_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	log.V(lvl.DEBUG).Infof("YangToDb_pfm_components_ic_csig_xfmr (requestUri=%v)", inParams.requestUri)

	node := NewPathInfo(inParams.uri).Var("name")
	if len(node) == 0 {
		return nil, nil
	}

	pfmObj := getPfmRootObject(inParams.ygRoot)
	if pfmObj == nil || pfmObj.Component == nil || len(pfmObj.Component) < 1 {
		log.V(lvl.ERROR).Info("YangToDb_pfm_components_ic_csig_xfmr: Empty component.")
		return nil, tlerr.NotSupported("YangToDb_pfm_components_ic_csig_xfmr: Empty component.")
	}

	comp, ok := pfmObj.Component[node]
	if !ok || comp == nil {
		return nil, fmt.Errorf("YangToDb_pfm_components_ic_csig_xfmr: Invalid component name: %s", node)
	}

	var csigSupportedStr string

	if comp.IntegratedCircuit != nil && comp.IntegratedCircuit.PinsCsig != nil && comp.IntegratedCircuit.PinsCsig.Config != nil {
		if csigSupportedStr, ok = yangToDbCsigSupportedMap[comp.IntegratedCircuit.PinsCsig.Config.CsigSupported]; !ok {
			return nil, fmt.Errorf("YangToDb_pfm_components_ic_csig_xfmr, Error: Invalid csig-supported: %s", csigSupportedStr)
		}
	}

	resMap := map[string]map[string]db.Value{
		"HST_CONFIG": {
			"GLOBAL": {
				Field: map[string]string{
					"CSIG_SUPPORTED": csigSupportedStr,
				},
			},
		},
	}
	return resMap, nil
}

var DbToYang_pfm_components_ic_csig_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	log.V(lvl.DEBUG).Infof("DbToYang_pfm_components_ic_csig_xfmr Template: %s ,path: %s, vars: %v", pathInfo.Template, pathInfo.Path, pathInfo.Vars)

	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(targetUriPath, "/openconfig-platform:components/component/integrated-circuit/google-pins-platform:pins-csig") {
		return errors.New("Component not supported")
	}

	node := NewPathInfo(inParams.uri).Var("name")
	if !validICName(&node) {
		return errors.New("Invalid component name: " + node)
	}

	pfmComps := getPfmRootObject(inParams.ygRoot)
	nodeCom, ok := pfmComps.Component[node]
	if !ok || nodeCom == nil {
		return errors.New("Component not present: " + node)
	}
	ygot.BuildEmptyTree(nodeCom)
	ygot.BuildEmptyTree(nodeCom.IntegratedCircuit)
	ygot.BuildEmptyTree(nodeCom.IntegratedCircuit.PinsCsig)
	ygot.BuildEmptyTree(nodeCom.IntegratedCircuit.PinsCsig.Config)
	ygot.BuildEmptyTree(nodeCom.IntegratedCircuit.PinsCsig.State)

	tblName := "HST_CONFIG"
	keyName := "GLOBAL"

	var errs []error

	// Config
	csigInfo, err := getCsigInfoFromDb(keyName, inParams.dbs[db.ConfigDB], tblName)
	if err == nil {
		csigSupportedEnum, ok := dbToYangCsigSupportedMap[csigInfo.csigSupported]
		if !ok {
			return fmt.Errorf("CSIG_SUPPORTED field '%s' read from Config DB not found in dbToYangCsigSupportedMap.", csigInfo.csigSupported)
		}
		csigCfg := nodeCom.IntegratedCircuit.PinsCsig.Config
		csigCfg.CsigSupported = csigSupportedEnum
	} else {
		log.V(lvl.ERROR).Info("Cannot get entry: ", keyName, " in Config DB; Error: ", err)
		errs = append(errs, err)
	}

	// State
	csigInfo, err = getCsigInfoFromDb(keyName, inParams.dbs[db.StateDB], tblName)
	if err == nil {
		csigSupportedEnum, ok := dbToYangCsigSupportedMap[csigInfo.csigSupported]
		if !ok {
			return fmt.Errorf("CSIG_SUPPORTED field '%s' read from State DB not found in dbToYangCsigSupportedMap.", csigInfo.csigSupported)
		}
		csigSt := nodeCom.IntegratedCircuit.PinsCsig.State
		csigSt.CsigSupported = csigSupportedEnum
	} else {
		log.V(lvl.ERROR).Info("Cannot get entry: ", keyName, " in State DB; Error: ", err)
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func fillAllPowerSupplyRailInfo(rails *ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails, d *db.DB, psName string, targetUriPath string) error {
	railKeys, err := d.GetKeysPattern(&(db.TableSpec{Name: POWER_INFO_TBL}), db.Key{Comp: []string{psName, "*"}})
	if err != nil || len(railKeys) == 0 {
		log.V(lvl.DEBUG).Info("Failed to get keys from power supply table.")
		return errors.New("failed to get keys from power supply table.")
	}
	for _, key := range railKeys {
		if key.Len() < 2 {
			continue
		}

		railName := key.Get(1)
		rail, ok := rails.Rail[railName]
		if !ok || rail == nil {
			log.V(lvl.DEBUG).Infof("Rail not present for rail name %v, create a new rail.", railName)
			if rail, err = rails.NewRail(railName); err != nil {
				log.V(lvl.DEBUG).Infof("Rail duplication error when creating rail for %v.", railName)
			}
		}

		ygot.BuildEmptyTree(rail)
		ygot.BuildEmptyTree(rail.State)
		psInfo, err := getPowerSupplyFromDb(psName, railName, d)
		if err == nil {
			fillAllRailLeaves(rail, psInfo, key.Get(0), key.Get(1))
		}
	}
	return nil
}

func fillAllRailLeaves(rail *ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail, psi PowerSupplyInfo, compName string, railName string) {
	var err error

	rail.State.Name = &railName
	psiDirection := strings.ToLower(psi.Direction)
	switch psiDirection {
	case "unset":
		rail.State.Direction = ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_INPUT
	case "input":
		rail.State.Direction = ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_INPUT
	case "output":
		rail.State.Direction = ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_OUTPUT
	default:
		log.V(lvl.DEBUG).Infof("Power supply direction has unexpected value: ", psiDirection)
	}

	if psi.StatusVout != "" {
		if hexNfsVal, err := hexaNumberToUint32(psi.StatusVout); err == nil {
			rail.State.StatusVout = uint32To4Bytes(hexNfsVal)
		}
	}

	if psi.CommendedVoltage != "" {
		if rail.State.CommandedVoltage, err = float32StrTo4Bytes(psi.CommendedVoltage); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.CommendedVoltage, err)
		}
	}

	if psi.Energy != "" {
		if rail.State.Energy, err = float32StrTo4Bytes(psi.Energy); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.Energy, err)
		}
	}

	if psi.Current != "" {
		if rail.State.Current, err = float32StrTo4Bytes(psi.Current); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.Current, err)
		}
	}

	if psi.Voltage != "" {
		if rail.State.Voltage, err = float32StrTo4Bytes(psi.Voltage); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.Voltage, err)
		}
	}

	if psi.Power != "" {
		if rail.State.Power, err = float32StrTo4Bytes(psi.Power); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.Power, err)
		}
	}

	if psi.PeakPower != "" {
		if rail.State.PeakPower, err = float32StrTo4Bytes(psi.PeakPower); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.PeakPower, err)
		}
	}

	if psi.PeakPowerInterval != "" {
		if ppi, err := strconv.ParseUint(psi.PeakPowerInterval, 0, 64); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.PeakPowerInterval, err)
		} else {
			rail.State.PeakPowerInterval = &ppi
		}
	}

	if psi.PeakVoltage != "" {
		if rail.State.PeakVoltage, err = float32StrTo4Bytes(psi.PeakVoltage); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.PeakVoltage, err)
		}
	}

	if psi.PeakCurrent != "" {
		if rail.State.PeakCurrent, err = float32StrTo4Bytes(psi.PeakCurrent); err != nil {
			log.V(lvl.DEBUG).Infof("Error in parsing float32 string \"%s\" to binary: %v", psi.PeakCurrent, err)
		}
	}

	if psi.RailTemperature != "" {
		if float64val, err := strconv.ParseFloat(psi.RailTemperature, 64); err == nil {
			rail.State.Temperature = &float64val
		}
	}
}

func fillPowerSupplyInfo(powerSupplyCom *ocbinds.OpenconfigPlatform_Components_Component, name string, railKey string, pType PathType, targetUriPath string, d *db.DB) error {
	log.V(lvl.DEBUG).Infof("fillPowerSupplyInfo name=%s railKey=%s pType=%v targetUriPath=%s", name, railKey, pType, targetUriPath)
	psi, err := getPowerSupplyFromDb(name, railKey, d)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Power Supply info from State dB", err.Error())
		return err
	}

	psState := powerSupplyCom.PowerSupply.State
	psCompState := powerSupplyCom.State
	psRails := powerSupplyCom.PowerSupply.Rails
	defaultParentVal := CHASSIS_PREFIX
	var psRail *ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail
	var psRailState *ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State

	if railKey != "" {
		psRail = psRails.Rail[railKey]
		ygot.BuildEmptyTree(psRail)
		psRailState = psRail.State
		psRailState.Name = &railKey
	}

	if pType == RailPaths || pType == AllCompPaths || pType == AllPaths {
		// if railKey == "", loop through all the rail keys
		if railKey == "" {
			fillAllPowerSupplyRailInfo(psRails, d, name, targetUriPath)
		} else {
			fillAllRailLeaves(psRail, psi, name, railKey)
		}
	}

	if pType == AllPaths || pType == AllCompPaths {
		powerSupplyCom.Config.Name = &name
	} else if pType == ConfigPaths {
		powerSupplyCom.Config.Name = &name
		return nil
	}

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths {
		psCompState.Name = &name
		psCompState.Location = &psi.Location
		psCompState.Type, _ = psCompState.To_OpenconfigPlatform_Components_Component_State_Type_Union(ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_POWER_SUPPLY)

		if psi.Temperature != "" {
			psCompState.Temperature.Instant = String2Float(psi.Temperature, 0.0)
		}
		if psi.TemperatureMax != "" {
			psCompState.Temperature.Max = String2Float(psi.TemperatureMax, 0.0)
		}
		if psi.TemperatureInterval != "" {
			psCompState.Temperature.Interval = String2Uint(psi.TemperatureInterval, 0)
		}

		psCompState.Parent = &defaultParentVal

		switch {
		case strings.HasPrefix(name, PB_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_BRICK
		case strings.HasPrefix(name, PH_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_HOTSWAP
		case strings.HasPrefix(name, PSEQ_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_SEQUENCER
		case strings.HasPrefix(name, VR_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_VOLTAGE_REGULATOR
		}
		if psi.CommandedFrequency != "" {
			if psState.CommandedFrequency, err = float32StrTo4Bytes(psi.CommandedFrequency); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
		if psi.Frequency != "" {
			if psState.Frequency, err = float32StrTo4Bytes(psi.Frequency); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
		if psi.ManufacturerStatus != "" {
			if hexNfsVal, err := hexaNumberToUint32(psi.ManufacturerStatus); err == nil {
				psState.ManufacturerStatus = uint32To4Bytes(hexNfsVal)
			}
		}
		if psi.StatusGpio != "" {
			if hexGpioVal, err := hexaNumberToUint64(psi.StatusGpio); err == nil {
				psState.StatusGpio = uint64To8Bytes(hexGpioVal)
			}
		}
		return nil
	}

	switch targetUriPath {
	case COMP_CONFIG_NAME:
		powerSupplyCom.Config.Name = &name
	case COMP_STATE_NAME:
		psCompState.Name = &name
	case COMP_STATE_LOCATION:
		psCompState.Location = &psi.Location
	case COMP_STATE_TYPE:
		psCompState.Type, _ = psCompState.To_OpenconfigPlatform_Components_Component_State_Type_Union(ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_POWER_SUPPLY)
	case COMP_STATE_TEMP:
		if psi.Temperature != "" {
			psCompState.Temperature.Instant = String2Float(psi.Temperature, 0.0)
		}
	case COMP_STATE_TEMP_MAX:
		if psi.TemperatureMax != "" {
			psCompState.Temperature.Max = String2Float(psi.TemperatureMax, 0.0)
		}
	case COMP_STATE_TEMP_INTV:
		if psi.TemperatureInterval != "" {
			psCompState.Temperature.Interval = String2Uint(psi.TemperatureInterval, 0)
		}
	case COMP_STATE_PARENT:
		psCompState.Parent = &defaultParentVal
	case G_PS_TYPE:
		switch {
		case strings.HasPrefix(name, PB_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_BRICK
		case strings.HasPrefix(name, PH_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_HOTSWAP
		case strings.HasPrefix(name, PSEQ_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_SEQUENCER
		case strings.HasPrefix(name, VR_PREFIX):
			psState.Type = ocbinds.GooglePinsPlatform_POWER_SUPPLY_TYPE_POWER_VOLTAGE_REGULATOR
		}
	case G_PS_COMMANDED_FREQ:
		if psi.CommandedFrequency != "" {
			if psState.CommandedFrequency, err = float32StrTo4Bytes(psi.CommandedFrequency); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_FREQ:
		if psi.Frequency != "" {
			if psState.Frequency, err = float32StrTo4Bytes(psi.Frequency); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_MFG_STATUS:
		if psi.ManufacturerStatus != "" {
			if hexNfsVal, err := hexaNumberToUint32(psi.ManufacturerStatus); err == nil {
				psState.ManufacturerStatus = uint32To4Bytes(hexNfsVal)
			}
		}
	case G_PS_STATUS_GPIO:
		if hexGpioVal, err := hexaNumberToUint64(psi.StatusGpio); err == nil {
			psState.StatusGpio = uint64To8Bytes(hexGpioVal)
		}
	case G_PS_RAIL_NAME:
		psRailState.Name = &railKey
	case G_PS_RAIL_DIRECTION:
		psiDirection := strings.ToLower(psi.Direction)
		switch psiDirection {
		case "unset":
			psRailState.Direction = ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_UNSET
		case "input":
			psRailState.Direction = ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_INPUT
		case "output":
			psRailState.Direction = ocbinds.OpenconfigPlatform_Components_Component_PowerSupply_Rails_Rail_State_Direction_OUTPUT
		default:
			log.V(lvl.DEBUG).Infof("Power supply direction has unexpected value: ", psiDirection)
		}
	case G_PS_RAIL_STATUS_VOUT:
		if psi.StatusVout != "" {
			if hexNfsVal, err := hexaNumberToUint32(psi.StatusVout); err == nil {
				psRailState.StatusVout = uint32To4Bytes(hexNfsVal)
			}
		}
	case G_PS_RAIL_COMMANDED_VOL:
		if psi.CommendedVoltage != "" {
			if psRailState.CommandedVoltage, err = float32StrTo4Bytes(psi.CommendedVoltage); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_ENERGY:
		if psi.Energy != "" {
			if psRailState.Energy, err = float32StrTo4Bytes(psi.Energy); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_CURRENT:
		if psi.Current != "" {
			if psRailState.Current, err = float32StrTo4Bytes(psi.Current); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_VOLTAGE:
		if psi.Voltage != "" {
			if psRailState.Voltage, err = float32StrTo4Bytes(psi.Voltage); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_POWER:
		if psi.Power != "" {
			if psRailState.Power, err = float32StrTo4Bytes(psi.Power); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_PEAK_POWER:
		if psi.PeakPower != "" {
			if psRailState.PeakPower, err = float32StrTo4Bytes(psi.PeakPower); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_PEAK_POWER_IV:
		if psi.PeakPowerInterval != "" {
			if ppi, err := strconv.ParseUint(psi.PeakPowerInterval, 0, 64); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing string to uint64: ", err)
			} else {
				psRailState.PeakPowerInterval = &ppi
			}
		}
	case G_PS_RAIL_PEAK_VOLTAGE:
		if psi.PeakVoltage != "" {
			if psRailState.PeakVoltage, err = float32StrTo4Bytes(psi.PeakVoltage); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_PEAK_CURRENT:
		if psi.PeakCurrent != "" {
			if psRailState.PeakCurrent, err = float32StrTo4Bytes(psi.PeakCurrent); err != nil {
				log.V(lvl.DEBUG).Infof("Error in parsing float32 to binary: ", err)
			}
		}
	case G_PS_RAIL_TEMP:
		if psi.RailTemperature != "" {
			if float64val, err := strconv.ParseFloat(psi.RailTemperature, 64); err == nil {
				psRailState.Temperature = &float64val
			}
		}
	}

	return nil
}

func getPowerSupplyFromDb(name string, railKey string, d *db.DB) (PowerSupplyInfo, error) {
	var psInfo PowerSupplyInfo
	var ps_comp_err, ps_rail_err error
	psEntry, ps_comp_err := d.GetEntry(&db.TableSpec{Name: POWER_INFO_TBL}, db.Key{Comp: []string{name}})
	if ps_comp_err == nil {
		psInfo = PowerSupplyInfo{
			Temperature:         psEntry.Get("temperature"),
			TemperatureMax:      psEntry.Get("temperature_max"),
			TemperatureInterval: psEntry.Get("temperature_interval"),
			CommandedFrequency:  psEntry.Get("commanded-frequency"),
			Frequency:           psEntry.Get("frequency"),
			ManufacturerStatus:  psEntry.Get("manufacturer-status"),
			StatusGpio:          psEntry.Get("status-gpio"),
			Location:            psEntry.Get("location"),
		}
	} else {
		log.V(lvl.DEBUG).Info("Can't get comp entry: ", name, "; Error: ", ps_comp_err)
	}

	if railKey != "" {
		powerSupplyTBL := POWER_INFO_TBL + d.Opts.KeySeparator + name
		psRailEntry, ps_rail_err := d.GetEntry(&db.TableSpec{Name: powerSupplyTBL}, db.Key{Comp: []string{railKey}})
		if ps_rail_err == nil {
			psInfo.Direction = psRailEntry.Get("direction")
			psInfo.StatusVout = psRailEntry.Get("all-status-vout")
			psInfo.CommendedVoltage = psRailEntry.Get("commanded-voltage")
			psInfo.Energy = psRailEntry.Get("energy")
			psInfo.Current = psRailEntry.Get("current")
			psInfo.Voltage = psRailEntry.Get("voltage")
			psInfo.Power = psRailEntry.Get("power")
			psInfo.PeakPower = psRailEntry.Get("peak-power")
			psInfo.PeakPowerInterval = psRailEntry.Get("peak-power-interval")
			psInfo.PeakVoltage = psRailEntry.Get("peak-voltage")
			psInfo.PeakCurrent = psRailEntry.Get("peak-current")
			psInfo.RailTemperature = psRailEntry.Get("temperature")
		} else {
			log.V(lvl.DEBUG).Info("Can't get rail entry: ", name, " + ", railKey, "; Error: ", ps_rail_err)
		}
	}
	if ps_comp_err != nil && ps_rail_err != nil {
		return PowerSupplyInfo{}, fmt.Errorf("Can't get comp entry: %s; Comp name error: %w; Rail key error: %w", name, ps_comp_err, ps_rail_err)
	}

	return psInfo, nil
}

func getAlpmCounter(ctrsDb, asicDb *db.DB) uint64 {
	var err error

	// Get the oid and field names of the counters
	oid, err := getSwitchOid(asicDb)
	if err != nil {
		log.V(lvl.DEBUG).Infof("Failed to get Switch Oid: %v", err)
		return 0
	}
	fieldNames, err := ctrsDb.GetEntry(&db.TableSpec{Name: "COUNTERS_DEBUG_NAME_SWITCH_STAT_MAP"}, db.Key{Comp: []string{}})
	if err != nil {
		log.V(tlerr.ErrorSeverity(err)).Infof("Failed to get entry: %v", err)
		return 0
	}
	field1 := fieldNames.Get("SAI_IN_DROP_REASON_LPM4_MISS")
	field2 := fieldNames.Get("SAI_IN_DROP_REASON_LPM6_MISS")

	entry, err := ctrsDb.GetEntry(&db.TableSpec{Name: "COUNTERS"}, db.Key{Comp: []string{oid}})
	if err != nil {
		log.V(tlerr.ErrorSeverity(err)).Infof("Failed to get entry: %v", err)
		return 0
	}

	ctr1, err := strconv.ParseUint(entry.Get(field1), 0, 64)
	if err != nil {
		log.V(lvl.DEBUG).Infof("Failed to convert string to uint: %v", err)
		ctr1 = 0
	}
	ctr2, err := strconv.ParseUint(entry.Get(field2), 0, 64)
	if err != nil {
		log.V(lvl.DEBUG).Infof("Failed to convert string to uint: %v", err)
		ctr2 = 0
	}

	return ctr1 + ctr2
}

func getSwitchOid(asicDb *db.DB) (string, error) {
	keys, err := asicDb.GetKeysPattern(&db.TableSpec{Name: "ASIC_STATE"}, db.Key{Comp: []string{"SAI_OBJECT_TYPE_SWITCH*"}})
	if err != nil {
		return "", err
	}
	if len(keys) != 1 {
		return "", tlerr.NotFoundError{Format: "getALPMCounterTable: incorrect number of keys match the pattern"}
	}

	// Key should look like "ASIC_STATE:SAI_OBJECT_TYPE_SWITCH:<Switch OID>"
	key := keys[0]
	if key.Len() != 3 {
		return "", tlerr.NotFoundError{Format: "getALPMCounterTable: invalid key in ASIC DB"}
	}

	return key.Get(1) + ":" + key.Get(2), nil
}

func fillAllSubComponents(scomp *ocbinds.OpenconfigPlatform_Components_Component_Subcomponents, d *db.DB, compName, subKey, targetUriPath string) error {
	log.V(lvl.DEBUG).Infof("fillAllSubComponents: compName: %s, subKey: %s, targetUriPath: %s\n", compName, subKey, targetUriPath)
	key := subKey
	if subKey == "" {
		key = "*"
	}
	subCompKeys, err := d.GetKeysPattern(&(db.TableSpec{Name: "BREAKOUT_PORT_XCVR_CFG"}), db.Key{Comp: []string{compName, key}})
	if err != nil || len(subCompKeys) == 0 {
		return errors.New("failed to get keys from subcomponent table.")
	}
	for _, key := range subCompKeys {
		if key.Len() < 2 {
			continue
		}
		if subComp, err := scomp.NewSubcomponent(key.Get(1)); err == nil {
			// Ignore errors for non-leaf paths
			fillSubComponent(subComp, d, key.Get(0)+d.Opts.KeySeparator+key.Get(1), targetUriPath)
		}
	}
	return nil
}

func fillSubComponent(subcomp *ocbinds.OpenconfigPlatform_Components_Component_Subcomponents_Subcomponent, d *db.DB, subCompKey, targetUriPath string) error {
	log.V(lvl.DEBUG).Infof("fillSubComponent: subCompKey: %s, targetUriPath: %s\n", subCompKey, targetUriPath)
	if subCompKey == "" {
		return errors.New("empty key for subcomponent table.")
	}
	if _, err := d.GetEntry(&db.TableSpec{Name: "BREAKOUT_PORT_XCVR_CFG"}, db.Key{Comp: []string{subCompKey}}); err != nil {
		return fmt.Errorf("cannot get entry with key: %s; Err: %w", subCompKey, err)
	}

	ygot.BuildEmptyTree(subcomp)
	ygot.BuildEmptyTree(subcomp.Config)
	ygot.BuildEmptyTree(subcomp.State)

	if keyList := strings.Split(subCompKey, d.Opts.KeySeparator); len(keyList) == 2 {
		subCompName := keyList[1]
		subcomp.Config.Name = &subCompName
		subcomp.State.Name = &subCompName
	}
	return nil
}

/* Helper for the main subscribe transformer handling the TRANSLATE_EXISTS case. */
func translateExists(inParams XfmrSubscInParams, key string) (XfmrSubscOutParams, error) {
	var result XfmrSubscOutParams
	compType, err := getCompType(key, inParams.dbs[db.StateDB])
	if err != nil {
		return result, err
	}
	tblInfo, ok := compTblMap[compType]
	if !ok {
		return result, errors.New("table not found.")
	}
	cdb := db.StateDB
	tblName := tblInfo[0]
	tblKey := key
	switch compType {
	case CompTypeIC:
		cdb = db.ConfigDB
	case CompTypePort:
		// Convert the port-name (e.g. 1/1) to interface name (Ethernet1/1/1) since that is the DB key
		if ifName := platform.InterfaceNameFromPort(key); len(ifName) > 0 {
			tblKey = ifName
		}
		cdb = db.ConfigDB
		// Should we just update compTblMap for CompTypePort to use this table???
		tblName = BREAKOUT_TBL
	case CompTypePowerSupplyVR, CompTypePowerSupplyPB, CompTypePowerSupplyPH, CompTypePowerSupplyPSEQ:
		// In the case of a power component which only has DB entries for rails and none for
		// the top level component (e.g. POWER_INFO|HotSwap_1|railA, POWER_INFO|HotSwap_1|railB,
		// but no entry for POWER_INFO|HotSwap) we do not want the framework to skip the key because
		// POWER_INFO|HotSwap doesn't exist.  Mark it as virtual to tell the framework to skip
		// that validation.
		result.isVirtualTbl = true
	}
	result.dbDataMap = RedisDbSubscribeMap{cdb: {tblName: {tblKey: {}}}}
	log.V(lvl.DEBUG).Infof("+++ Subscribe_pfm_components_xfmr result: %v %v %v +++", cdb, tblName, tblKey)
	return result, nil
}

/* Given a URI, return a list of component types which apply to it.  For example
 * a URI of "/components/component/port" would return [CompTypePort] while a URI
 * of "/components/component/state/software-version" would return a list of all
 * component types which report software version. */
func cTypesFromUri(uri string) []componentType {
	cTypes := []componentType{}
	if strings.HasPrefix(uri, "/openconfig-platform:components/component/chassis") {
		cTypes = []componentType{CompTypeChassis}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/port") {
		cTypes = []componentType{CompTypePort}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/power-supply") {
		cTypes = []componentType{CompTypePsu, CompTypePowerSupplyVR, CompTypePowerSupplyPB, CompTypePowerSupplyPH, CompTypePowerSupplyPSEQ}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/fan") {
		cTypes = []componentType{CompTypeFan}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/storage") {
		cTypes = []componentType{CompTypeStorage}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/integrated-circuit") {
		cTypes = []componentType{CompTypeIC}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/software-module") {
		cTypes = []componentType{CompTypeNWStack, CompTypeOS}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/state/oper-status") {
		cTypes = []componentType{CompTypeFan, CompTypeNWStack, CompTypeOS}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/oc-transceiver:transceiver") {
		cTypes = []componentType{CompTypeXcvr}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/google-pins-platform:fpga") {
		cTypes = []componentType{CompTypeFpga}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/google-pins-platform:sensor") {
		cTypes = []componentType{CompTypeTemp}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/google-pins-platform:boot-loader") {
		cTypes = []componentType{CompTypeBootLoader}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/google-pins-platform:hardware-security-module") {
		cTypes = []componentType{CompTypeHwSecurityModule}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/state/parent") {
		/* Everything except chassis */
		cTypes = []componentType{CompTypePsu, CompTypeFan, CompTypeFanTray, CompTypeFpga, CompTypeStorage, CompTypeXcvr, CompTypeTemp, CompTypeIC, CompTypeNWStack, CompTypeOS, CompTypeBootLoader, CompTypePort, CompTypeHwSecurityModule, CompTypePcie, CompTypePowerSupplyVR, CompTypePowerSupplyPB, CompTypePowerSupplyPH, CompTypePowerSupplyPSEQ}
	} else if strings.HasPrefix(uri, "/openconfig-platform:components/component/state/software-version") {
		cTypes = []componentType{CompTypeNWStack, CompTypeOS, CompTypeBootLoader}
	}
	return cTypes
}

/* Helper for the main subscribe transformer handling the TRANSLATE_SUBSCRIBE case. */
func translateSubscribe(inParams XfmrSubscInParams, key, targetUriPath string) (XfmrSubscOutParams, error) {
	var result XfmrSubscOutParams
	result.dbDataMap = make(RedisDbSubscribeMap)
	/* Handle TRANSLATE_SUBSCRIBE by expanding the wildcard yang key to a set of
	 * DB tables and keys.  If the key is not a wildcard then identify the set
	 * of DB tables and keys which apply to it. */
	result.isVirtualTbl = false
	result.needCache = true
	result.onChange = OnchangeEnable
	result.nOpts = &notificationOpts{mInterval: 0, pType: OnChange}

	/* Use the requested path to create a positive filter of component types to
	 * process.  Note that a completely empty filter means no filtering is
	 * required. */
	compTypeFilter := []componentType{}
	if key == "*" {
		compTypeFilter = cTypesFromUri(targetUriPath)
	} else {
		cType, err := getCompType(key, inParams.dbs[db.StateDB])
		if err != nil {
			return result, err
		}
		compTypeFilter = []componentType{cType}
	}

	for cType, tblNames := range compTblMap {
		/* An empty filter means no filtering is required. */
		if len(compTypeFilter) > 0 {
			/* Filtering is required, skip all component types not present in the
			 * filter. */
			filteredOut := true
			for _, ct := range compTypeFilter {
				if ct == cType {
					filteredOut = false
					break
				}
			}
			if filteredOut {
				continue
			}
		}

		tblName := tblNames[0]
		tblKey := tblNames[1]
		tblDb := db.StateDB
		if key != "*" {
			/* Generally the DB key will be the component name (yang key), for
			 * the cases where it is not (e.g. Port components) there will be
			 * special casing just below to handle it. */
			tblKey = key
		}

		var subCompTblName, subCompTblKey string
		var subCompDb db.DBNum
		if cType == CompTypeIC {
			/* The integrated-circuit subtree is backed by multiple DB tables but
			 * our compTblMap only captures one.  The special handling here is to
			 * cover all tables.
			 * For wildcard expansion we use the NODE_CFG_TBL in ConfigDB but for
			 * sample cases on the specific counter paths we need the counter
			 * tables. */
			tblDb = db.ConfigDB
			if strings.HasPrefix(targetUriPath, COMP_IC_MEM) {
				result.onChange = OnchangeDisable
			} else if strings.HasPrefix(targetUriPath, COMP_IC_PLC) {
				result.onChange = OnchangeDisable
			}
		} else if cType == CompTypePort {
			/* DB keys here are interface names but the path transformer
			 * will map them back to port names. */
			if strings.HasPrefix(targetUriPath, "/openconfig-platform:components/component/state") ||
				strings.HasPrefix(targetUriPath, "/openconfig-platform:components/component/port/state") {
				tblDb = db.StateDB
				tblName = PORT_BREAKOUT
			} else if strings.HasPrefix(targetUriPath, "/openconfig-platform:components/component/subcomponents") {
				/* Port components are the only component where we support the
				 * subcomponents subtree and populate a transceiver name as the
				 * subcomponent.  Handle that case here if needed. */
				subCompDb = db.ConfigDB
				subCompTblName = "BREAKOUT_PORT_XCVR_CFG"
				subCompKey := NewPathInfo(inParams.uri).Var("name#2")
				if key == "*" {
					subCompTblKey = "*"
				} else if subCompKey != "" && subCompKey != "*" {
					subCompTblKey = key + "|" + subCompKey
				} else {
					subCompTblKey = key + "|" + "*"
				}
			} else {
				tblDb = db.ConfigDB
				tblName = BREAKOUT_TBL
			}

			if key == "*" {
				tblKey = "*"
			} else {
				tblKey = platform.InterfaceNameFromPort(key)
			}
		}

		if result.dbDataMap[tblDb] == nil {
			result.dbDataMap[tblDb] = make(map[string]map[string]map[string]string)
		}
		if result.dbDataMap[tblDb][tblName] == nil {
			result.dbDataMap[tblDb][tblName] = make(map[string]map[string]string)
		}
		if subCompTblName != "" {
			if result.dbDataMap[subCompDb] == nil {
				result.dbDataMap[subCompDb] = make(map[string]map[string]map[string]string)
			}
			if result.dbDataMap[subCompDb][subCompTblName] == nil {
				result.dbDataMap[subCompDb][subCompTblName] = make(map[string]map[string]string)
			}
			result.dbDataMap[subCompDb][subCompTblName][subCompTblKey] = map[string]string{}
		}

		/* Add the DB table and key to the result.  Power components are a
		 * bit tricky as they have multi-level keys so we'll do a bit of
		 * special handling for that case now if needed.  Otherwise just add
		 * the table key to the result. */
		if tblName == POWER_INFO_TBL {
			/* Power components have multi-level keys, e.g. POWER_INFO|<hotswap>
			 * and POWER_INFO|<hotswap>|<rail> which will lead to duplicate
			 * processing and updates if we let the framework expand a simple
			 * wildcard key.  It would expand the wildcard to both the hotswap
			 * component and the hotswap's rail(s) component. */
			keyList, err := inParams.dbs[db.StateDB].GetKeysPattern(&(db.TableSpec{Name: tblName}), db.Key{Comp: []string{tblKey}})
			if err != nil {
				return result, err
			}
			/* First add all components with a single key. */
			for _, k := range keyList {
				if len(k.Comp) != 1 {
					continue
				}
				result.dbDataMap[tblDb][tblName][k.Comp[0]] = map[string]string{}
			}
			/* Check components with two keys for any that were not covered
			 * above and add them exactly once.  This covers the case of a
			 * component that has only rails. */
			added := map[string]bool{}
			for _, k := range keyList {
				if len(k.Comp) != 2 {
					continue
				}
				powerKey := k.Comp[0] + "|" + k.Comp[1]
				if _, ok := result.dbDataMap[tblDb][tblName][k.Comp[0]]; ok {
					continue
				}
				if _, ok := added[k.Comp[0]]; ok {
					continue
				}
				added[k.Comp[0]] = true
				result.dbDataMap[tblDb][tblName][powerKey] = map[string]string{}
			}
		} else {
			if result.dbDataMap[tblDb][tblName][tblKey] == nil {
				result.dbDataMap[tblDb][tblName][tblKey] = map[string]string{}
			}
		}
	}
	if log.V(lvl.DEBUG) {
		for db, _ := range result.dbDataMap {
			for tbl, _ := range result.dbDataMap[db] {
				for k, v := range result.dbDataMap[db][tbl] {
					log.Infof("+++ Subscribe_pfm_components_xfmr result: DB=%d, Table=%s, Key=%s, Flds=%v +++", db, tbl, k, v)
				}
			}
		}
	}
	return result, nil
}

var Subscribe_pfm_components_xfmr SubTreeXfmrSubscribe = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	var result XfmrSubscOutParams
	key := NewPathInfo(inParams.uri).Var("name")

	log.V(lvl.DEBUG).Infof("+++ Subscribe_pfm_components_xfmr uri (%v) key(%s) mode(%v) +++", inParams.uri, key, inParams.subscProc)
	log.V(lvl.DEBUG).Infof("+++ Subscribe_pfm_components_xfmr requestUri (%v) +++", inParams.requestURI)

	pathInfo := NewPathInfo(inParams.requestURI)
	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil {
		return result, err
	}

	if key == "" || strings.Contains(key, "_sensor") {
		/* no need to verify dB data if we are requesting ALL
		   components or if request is for sensor */
		result.isVirtualTbl = true
		return result, err
	}

	if inParams.subscProc == TRANSLATE_EXISTS {
		return translateExists(inParams, key)
	}
	if inParams.subscProc == TRANSLATE_SUBSCRIBE {
		return translateSubscribe(inParams, key, targetUriPath)
	}

	return result, err
}

var DbToYang_pfm_components_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	log.V(lvl.DEBUG).Infof("DbToYang_pfm_components_xfmr: %s, path: %s, vars: %v",
		pathInfo.Template, pathInfo.Path, pathInfo.Vars)

	if !strings.Contains(inParams.requestUri, "/openconfig-platform:components") {
		return errors.New("Component not supported")
	}
	log.V(lvl.DEBUG).Info("inParams.Uri:", inParams.requestUri)
	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil {
		return err
	}

	/* Extract the component name (key), it may be empty ("") if the get is for
	 * the entire list/container (/components/component) */
	compName := pathInfo.Var("name")
	/* Rails and subcomponents may have a second level key */
	subKey := pathInfo.Var("name#2")
	return getSysComponents(getPfmRootObject(inParams.ygRoot), targetUriPath, inParams, compName, subKey)
}

var YangToDb_pfm_components_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	pathInfo := NewPathInfo(inParams.uri)
	key := pathInfo.Var("name")
	if key == "" {
		return nil, nil
	}

	log.V(lvl.DEBUG).Infof("YangToDb_pfm_components_xfmr: uri %s, name %s, requestURI %s, op %v", inParams.uri, key, inParams.requestUri, inParams.oper)
	pfmObj := getPfmRootObject(inParams.ygRoot)
	if pfmObj == nil || pfmObj.Component == nil || len(pfmObj.Component) < 1 {
		return nil, tlerr.NotSupported("YangToDb_pfm_components_xfmr: Empty component.")
	}

	comp, ok := pfmObj.Component[key]
	if !ok || comp == nil {
		return nil, fmt.Errorf("YangToDb_pfm_components_xfmr: Invalid component name: %s", key)
	}

	inParams.key = key
	var tblName, portName string
	cType := CompTypeInvalid
	if validICName(&key) {
		tblName = NODE_CFG_TBL
		cType = CompTypeIC
	} else if key == CHASSIS_PREFIX {
		tblName = CHASSIS_CFG
		cType = CompTypeChassis
	} else {
		/* Not one of the other supported component types, check if it is a port. */
		ifName := platform.InterfaceNameFromPort(key)
		if len(ifName) == 0 {
			return nil, fmt.Errorf("YangToDb_pfm_components_xfmr: Invalid component name: %s", key)
		}
		tblName = BREAKOUT_TBL
		portName = key
		key = ifName
		cType = CompTypePort
	}
	inParams.table = tblName

	memMap := make(map[string]map[string]db.Value)
	if inParams.oper == DELETE {
		if cType == CompTypeIC {
			/* We only support deletion on the following path:
			 * /components/component/integrated-circuit/config/node-id */
			memMap[NODE_CFG_TBL] = map[string]db.Value{key: db.Value{Field: map[string]string{"node-id": ""}}}
		} else if cType == CompTypePort {
			memMap[BREAKOUT_TBL] = map[string]db.Value{key: db.Value{Field: map[string]string{"port-id": ""}}}
		}
	} else {
		if comp.Config != nil {
			fields := db.Value{Field: make(map[string]string)}
			if comp.Config.Name != nil {
				if inParams.key != *comp.Config.Name {
					return nil, fmt.Errorf("Mismatch between component name key: (%s) and name to be configured: (%s)", inParams.key, *comp.Config.Name)
				}
				fields.Set("name", *comp.Config.Name)
			}
			if comp.Config.FullyQualifiedName != nil {
				fields.Set("fully-qualified-name", *comp.Config.FullyQualifiedName)
			}
			memMap[tblName] = map[string]db.Value{key: fields}
		}
		if comp.IntegratedCircuit != nil && comp.IntegratedCircuit.Config != nil && comp.IntegratedCircuit.Config.NodeId != nil {
			if cType != CompTypeIC {
				return nil, fmt.Errorf("Component name \"%s\" not identified as an Integrated Circuit but contains an integrated-circuit subtree..", key)
			}
			dbVal := db.Value{Field: make(map[string]string)}
			if _, ok := memMap[NODE_CFG_TBL]; !ok {
				memMap[NODE_CFG_TBL] = make(map[string]db.Value)
			}
			if _, ok := memMap[NODE_CFG_TBL][key]; !ok {
				memMap[NODE_CFG_TBL][key] = dbVal
			} else {
				dbVal = memMap[NODE_CFG_TBL][key]
			}
			nodeID := *comp.IntegratedCircuit.Config.NodeId
			dbVal.Set("node-id", strconv.FormatUint(nodeID, 10))
		}
		if comp.Port != nil && comp.Port.Config != nil && comp.Port.Config.PortId != nil {
			if cType != CompTypePort {
				return nil, fmt.Errorf("Component name %s is not a Port.", portName)
			}
			dbVal := db.Value{Field: make(map[string]string)}
			if _, ok := memMap[BREAKOUT_TBL]; !ok {
				memMap[BREAKOUT_TBL] = make(map[string]db.Value)
			}
			if _, ok := memMap[BREAKOUT_TBL][key]; !ok {
				memMap[BREAKOUT_TBL][key] = dbVal
			} else {
				dbVal = memMap[BREAKOUT_TBL][key]
			}
			portID := *comp.Port.Config.PortId
			dbVal.Set("port-id", strconv.FormatUint(uint64(portID), 10))
		}
		if comp.Subcomponents != nil && comp.Subcomponents.Subcomponent != nil {
			for subCompKey := range comp.Subcomponents.Subcomponent {
				if comp.Subcomponents.Subcomponent[subCompKey].Config != nil && comp.Subcomponents.Subcomponent[subCompKey].Config.Name != nil {
					if portName != "" {
						portKey := portName + "|" + *comp.Subcomponents.Subcomponent[subCompKey].Config.Name
						memMap["BREAKOUT_PORT_XCVR_CFG"] = map[string]db.Value{portKey: db.Value{Field: map[string]string{"NULL": "NULL"}}}
						log.V(lvl.INFO).Infof("YangToDb_pfm_components_xfmr: memMap: %v\n", memMap)
					}
				}
			}
		}
	}

	// For CHASSIS_CFG|chassis table, the DB operation must be added as an Update to avoid overwriting other fields.
	if tblName == CHASSIS_CFG {
		subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
		subOpMap[db.ConfigDB] = memMap
		updateSubOpDataMap(subOpMap, UPDATE, inParams)
		return nil, nil
	}
	log.V(lvl.DEBUG).Infof("YangToDb_pfm_components_xfmr: result %v", memMap)
	return memMap, nil
}

func createCompAndFuncCall(pfCpts *ocbinds.OpenconfigPlatform_Components, targetUriPath string, compType componentType, inParams XfmrParams, tblName string, tblKey string) {
	var compNames []string
	var err error
	dbs := inParams.dbs
	d := dbs[db.StateDB]
	cfgdb := dbs[db.ConfigDB]
	switch compType {
	case CompTypeIC:
		compNames, err = getAllTableEntries(cfgdb, tblName, tblKey)
	default:
		compNames, err = getAllTableEntries(d, tblName, tblKey)
	}
	if err != nil {
		log.V(lvl.DEBUG).Info(err)
	}

	for _, compAndKey := range compNames {
		compKeys := strings.Split(compAndKey, d.Opts.KeySeparator)
		if len(compKeys) == 0 {
			continue
		}
		comp := compKeys[0]
		pfComp := pfCpts.Component[comp]
		if pfComp == nil {
			pfComp, err = pfCpts.NewComponent(comp)
			if err != nil {
				log.V(lvl.DEBUG).Infof("Component creation failed with NewComponent for comp %v; err = %v", comp, err)
				continue
			}
			ygot.BuildEmptyTree(pfComp)
		}

		if len(compKeys) == 2 {
			secondKey := compKeys[1]

			if compType == CompTypePowerSupplyVR ||
				compType == CompTypePowerSupplyPB ||
				compType == CompTypePowerSupplyPH ||
				compType == CompTypePowerSupplyPSEQ {
				pfPsRails := pfComp.PowerSupply.Rails
				pfPsRail := pfPsRails.Rail[secondKey]
				if pfPsRail == nil {
					pfPsRail, err = pfPsRails.NewRail(secondKey)
					if err != nil {
						log.V(lvl.DEBUG).Infof("Component creation failed with NewRail for rail %v; err = %v", secondKey, err)
						continue
					}
					ygot.BuildEmptyTree(pfPsRail)
				}
			}
		}

		if err = compTypeToFuncCall(compType, comp, "", pfComp, targetUriPath, dbs, AllPaths, inParams.ygRoot); err != nil {
			log.V(lvl.DEBUG).Info(err)
		}
	}
}

/* Main workhorse of the DbToYang transformer.  The get is either for the entire
 * component list (/components/component) or for a specific component name; no
 * wildcards are handled here. */
func getSysComponents(pf_cpts *ocbinds.OpenconfigPlatform_Components, targetUriPath string, inParams XfmrParams, compName, subKey string) error {

	log.V(lvl.DEBUG).Infof("Preparing dB for system components")

	uri := inParams.uri
	dbs := inParams.dbs
	ygRoot := inParams.ygRoot

	var err error
	d := dbs[db.StateDB]
	cfgdb := dbs[db.ConfigDB]
	log.V(lvl.DEBUG).Info("targetUriPath:", targetUriPath)
	switch targetUriPath {
	case COMP:
		log.V(lvl.DEBUG).Infof("compName: %v", compName)
		subCompName := "" /* Get all subcomponents */
		if compName == "" {
			/* Handle all component types except for ports, they will be handled just below. */
			for cType, tbl := range compTblMap {
				if cType == CompTypePort || len(tbl) < 2 {
					continue
				}
				tblName := tbl[0]
				createCompAndFuncCall(pf_cpts, targetUriPath, cType, inParams, tblName, tbl[1])
			}

			ports := platform.PortNames()
			for _, port := range ports {
				pf_comp, err := pf_cpts.NewComponent(port)
				if err != nil {
					log.V(lvl.DEBUG).Infof("Component creation failed with NewComponent for comp %v; err = %v", port, err)
					continue
				}
				log.V(lvl.DEBUG).Info("DPB Adding ", port)
				ygot.BuildEmptyTree(pf_comp)
				if err = fillDpbData(pf_comp, port, subCompName, AllPaths, targetUriPath, d, cfgdb); err != nil {
					log.V(lvl.DEBUG).Info(err)
				}
			}
		} else {
			pf_comp, ok := pf_cpts.Component[compName]
			if !ok || pf_comp == nil {
				return fmt.Errorf("invalid input component name: %s", compName)
			}
			ygot.BuildEmptyTree(pf_comp)
			compType, err := getCompType(compName, d)
			if err != nil {
				return err
			}
			if err = compTypeToFuncCall(compType, compName, subCompName, pf_comp, targetUriPath, dbs, AllPaths, ygRoot); err != nil {
				log.V(lvl.DEBUG).Info(err)
			}
		}
	case COMP_ST:
		compType, err := getCompType(compName, d)
		if err != nil {
			return err
		}
		pf_comp, ok := pf_cpts.Component[compName]
		if !ok || pf_comp == nil {
			return fmt.Errorf("invalid input component name for state path: %s", compName)
		}
		ygot.BuildEmptyTree(pf_comp)
		if compType == CompTypeTemp || compType == CompTypeXcvr || compType == CompTypePowerSupplyPB || compType == CompTypePowerSupplyVR || compType == CompTypePowerSupplyPH || compType == CompTypePowerSupplyPSEQ {
			ygot.BuildEmptyTree(pf_comp.State)
			ygot.BuildEmptyTree(pf_comp.State.Temperature)
		}
		if compType == CompTypePcie || compType == CompTypeEepromStatus {
			ygot.BuildEmptyTree(pf_comp.State)
			ygot.BuildEmptyTree(pf_comp.State.Pcie)
		}
		if err = compTypeToFuncCall(compType, compName, subKey, pf_comp, targetUriPath, dbs, StatePaths, ygRoot); err != nil {
			log.V(lvl.DEBUG).Info(err)
		}

	case COMP_CFG:
		compType, err := getCompType(compName, d)
		if err != nil {
			return err
		}
		if compType == CompTypeIC || compType == CompTypeChassis || compType == CompTypeXcvr || compType == CompTypePort || compType == CompTypePowerSupplyPH {
			pf_comp, ok := pf_cpts.Component[compName]
			if !ok || pf_comp == nil {
				return fmt.Errorf("Invalid component name: %s", compName)
			}
			ygot.BuildEmptyTree(pf_comp)
			if err = compTypeToFuncCall(compType, compName, subKey, pf_comp, targetUriPath, dbs, ConfigPaths, ygRoot); err != nil {
				log.V(lvl.DEBUG).Info(err)
			}
			break
		}
		fallthrough

	default:
		/* The following cases are handled above:
		 *   /components/component
		 *   /components/component[name=<component_name>]
		 *   /components/component[name=<component_name>]/config
		 *   /components/component[name=<component_name>]/state
		 * so the request must be for a specific component's leaf or subtree,
		 * e.g. /components/component[name=integrated_circuit]/integrated-circuit */
		// TODO - Can we de-dup this code with compTypeToFuncCall?  No good way to set pathType...
		compType, err := getCompType(compName, d)
		if err != nil {
			return err
		}
		pf_comp, ok := pf_cpts.Component[compName]
		if !ok || pf_comp == nil {
			return fmt.Errorf("invalid input component name: %s", compName)
		}
		ygot.BuildEmptyTree(pf_comp)
		switch compType {
		case CompTypePsu:
			ygot.BuildEmptyTree(pf_comp.PowerSupply)
			ygot.BuildEmptyTree(pf_comp.PowerSupply.State)
			switch targetUriPath {
			case COMP_PS:
				fallthrough
			case COMP_PS_STATE:
				fillSysPsuInfo(pf_comp, compName, true, true, targetUriPath, d)
			default:
				fillSysPsuInfo(pf_comp, compName, false, true, targetUriPath, d)
			}
		case CompTypeFan:
			dbToYangFan(pf_comp, compName, targetUriPath, d, CompTypeFan)
		case CompTypeFanTray:
			dbToYangFan(pf_comp, compName, targetUriPath, d, CompTypeFan)
		case CompTypeFpga:
			ygot.BuildEmptyTree(pf_comp.Fpga)
			ygot.BuildEmptyTree(pf_comp.Fpga.State)
			switch targetUriPath {
			case FPGA_GO_COMP:
				fallthrough
			case FPGA_GO_RESET_CAUSE:
				fallthrough
			case FPGA_GO_RESET_CAUSE_STATE:
				fillSysFpgaInfo(pf_comp, compName, AllCompPaths, targetUriPath, uri, d)
			default:
				fillSysFpgaInfo(pf_comp, compName, SingularPath, targetUriPath, uri, d)
			}
		case CompTypeStorage:
			ygot.BuildEmptyTree(pf_comp.Storage)
			ygot.BuildEmptyTree(pf_comp.Storage.State)
			switch targetUriPath {
			case COMP_STORAGE:
				return fillSysStorageInfo(pf_comp, compName, AllCompPaths, targetUriPath, d)
			case COMP_STORAGE_ST:
				return fillSysStorageInfo(pf_comp, compName, StatePaths, targetUriPath, d)
			default:
				return fillSysStorageInfo(pf_comp, compName, SingularPath, targetUriPath, d)
			}
		case CompTypeXcvr:
			ygot.BuildEmptyTree(pf_comp.Transceiver)
			ygot.BuildEmptyTree(pf_comp.Transceiver.State)
			// ygot.BuildEmptyTree(pf_comp.Transceiver.Config)

			laneIdx := NewPathInfo(uri).Var("index")
			switch targetUriPath {
			case XCVR_BASE_PREFIX /*XCVR_BASE_CONFIG,*/, XCVR_BASE_STATE, XCVR_BASE_PC, XCVR_BASE_CHANNEL:
				err = fillSysXcvrInfo(pf_comp, compName, true, laneIdx, targetUriPath, dbs)
			default:
				/* For individual components*/
				err = fillSysXcvrInfo(pf_comp, compName, false, laneIdx, targetUriPath, dbs)
			}
		case CompTypeTemp:
			ygot.BuildEmptyTree(pf_comp.Sensor)
			ygot.BuildEmptyTree(pf_comp.Sensor.State)
			switch targetUriPath {
			case COMP_SENSOR:
				fallthrough
			case COMP_SENSOR_ST:
				return fillSysTempInfo(pf_comp, compName, AllCompPaths, targetUriPath, d)
			default:
				return fillSysTempInfo(pf_comp, compName, SingularPath, targetUriPath, d)
			}
		case CompTypeIC:
			return dbToYangIC(pf_comp, compName, targetUriPath, inParams.dbs, inParams.ygRoot)
		case CompTypeChassis:
			ygot.BuildEmptyTree(pf_comp.Chassis)
			ygot.BuildEmptyTree(pf_comp.Chassis.Alarms)
			ygot.BuildEmptyTree(pf_comp.Chassis.Alarms.State)
			ygot.BuildEmptyTree(pf_comp.Chassis.State)
			ygot.BuildEmptyTree(pf_comp.Chassis.Config)
			switch targetUriPath {
			case FIRMWARE_CHASSIS, FIRMWARE_CHASSIS_ALARMS, FIRMWARE_CHASSIS_ALARMS_STATE, FIRMWARE_CHASSIS_STATE:
				return fillSysFirmwareInfo(pf_comp, compName, AllCompPaths, targetUriPath, d, cfgdb)
			case FIRMWARE_CHASSIS_CONFIG:
				return fillSysFirmwareInfo(pf_comp, compName, ConfigPaths, targetUriPath, d, cfgdb)
			default:
				return fillSysFirmwareInfo(pf_comp, compName, SingularPath, targetUriPath, d, cfgdb)
			}
		case CompTypeNWStack:
			fallthrough
		case CompTypeOS:
			fallthrough
		case CompTypeBootLoader:
			ygot.BuildEmptyTree(pf_comp.SoftwareModule)
			ygot.BuildEmptyTree(pf_comp.SoftwareModule.State)
			switch targetUriPath {
			case COMP_SW_MOD:
				fallthrough
			case COMP_SW_MOD_ST:
				return fillSWCompInfo(pf_comp, compName, AllCompPaths, targetUriPath, d, compType)
			default:
				return fillSWCompInfo(pf_comp, compName, SingularPath, targetUriPath, d, compType)
			}
		case CompTypePort:
			ygot.BuildEmptyTree(pf_comp.Port)
			ygot.BuildEmptyTree(pf_comp.Port.State)
			ygot.BuildEmptyTree(pf_comp.Port.Config)
			switch targetUriPath {
			case COMP_PORT:
				fallthrough
			case COMP_PORT_ST:
				fallthrough
			case COMP_PORT_CFG:
				return fillDpbData(pf_comp, compName, subKey, AllCompPaths, targetUriPath, d, cfgdb)
			default:
				return fillDpbData(pf_comp, compName, subKey, SingularPath, targetUriPath, d, cfgdb)
			}
		case CompTypeHwSecurityModule:
			ygot.BuildEmptyTree(pf_comp.HardwareSecurityModule)
			ygot.BuildEmptyTree(pf_comp.HardwareSecurityModule.State)
			switch targetUriPath {
			case HSM_GO_SUBTREE:
				return fillHwSecurityModuleInfo(pf_comp, compName, AllCompPaths, targetUriPath, d)
			case HSM_GO_STATE:
				return fillHwSecurityModuleInfo(pf_comp, compName, StatePaths, targetUriPath, d)
			default:
				return fillHwSecurityModuleInfo(pf_comp, compName, SingularPath, targetUriPath, d)
			}
		case CompTypePcie:
			ygot.BuildEmptyTree(pf_comp.State)
			ygot.BuildEmptyTree(pf_comp.State.Pcie)
			return fillSysPcieInfo(pf_comp, compName, AllPaths, targetUriPath, d)
		case CompTypePowerSupplyPB, CompTypePowerSupplyPH, CompTypePowerSupplyVR, CompTypePowerSupplyPSEQ:
			railKey := NewPathInfo(uri).Var("name#2")
			ygot.BuildEmptyTree(pf_comp.PowerSupply)
			ygot.BuildEmptyTree(pf_comp.PowerSupply.State)
			ygot.BuildEmptyTree(pf_comp.PowerSupply.Rails)

			switch targetUriPath {
			case COMP_PS:
				fillPowerSupplyInfo(pf_comp, compName, railKey, AllCompPaths, targetUriPath, d)
			case COMP_PS_STATE:
				fillPowerSupplyInfo(pf_comp, compName, railKey, StatePaths, targetUriPath, d)
			case G_PS_RAILS:
				fallthrough
			case G_PS_RAILS_RAIL, G_PS_RAIL_STATE:
				fillPowerSupplyInfo(pf_comp, compName, railKey, RailPaths, targetUriPath, d)
			default:
				fillPowerSupplyInfo(pf_comp, compName, railKey, SingularPath, targetUriPath, d)
			}
		case CompTypeControllerCard:
			return fillControllerCardInfo(pf_comp, compName, SingularPath, targetUriPath, d)
		case CompTypeDiskCarrier:
			return fillDiskCarrierInfo(pf_comp, compName, SingularPath, targetUriPath, d)
		case CompTypeHumidity:
			ygot.BuildEmptyTree(pf_comp.Sensor)
			ygot.BuildEmptyTree(pf_comp.Sensor.State)
			switch targetUriPath {
			case COMP_SENSOR:
				fallthrough
			case COMP_SENSOR_ST:
				return fillSysHumidityInfo(pf_comp, compName, AllCompPaths, targetUriPath, d)
			default:
				return fillSysHumidityInfo(pf_comp, compName, SingularPath, targetUriPath, d)
			}
		case CompTypeEepromStatus:
			return fillEepromStatusInfo(pf_comp, compName, SingularPath, targetUriPath, d)
		default:
			return fmt.Errorf("Unhandled Component: %s", compName)
		}
	}

	return err
}

func float32StrTo4Bytes(s string) ([]byte, error) {
	var data []byte
	float64val, err := strconv.ParseFloat(s, 32)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error converting string to float32")
		return data, err
	}
	data = make([]byte, 4)
	/* Using Big Endian (network-order) to pack and unpack data
	 * IMPORTANT: REST server will do a b64 encode before sending the output
	 * The example of encode and decode can be found: https://go.dev/play/p/pr79oRzffY0"
	 * Json test result needs to be modified to b64 decoded result
	 */
	binary.BigEndian.PutUint32(data, math.Float32bits(float32(float64val)))
	return data, err
}

func hexaNumberToUint64(hexaString string) (uint64, error) {
	// replace 0x or 0X with empty String
	numberStr := strings.Replace(hexaString, "0x", "", -1)
	numberStr = strings.Replace(numberStr, "0X", "", -1)
	hexVal, err := strconv.ParseUint(numberStr, 16, 64)
	return hexVal, err
}

func uint64To8Bytes(hexa uint64) []byte {
	var data = make([]byte, 8)
	binary.BigEndian.PutUint64(data, hexa)
	return data
}

func hexaNumberToUint32(hexaString string) (uint32, error) {
	// replace 0x or 0X with empty String
	numberStr := strings.Replace(hexaString, "0x", "", -1)
	numberStr = strings.Replace(numberStr, "0X", "", -1)
	hexVal, err := strconv.ParseUint(numberStr, 16, 32)
	if err != nil {
		return uint32(0), err
	}
	return uint32(hexVal), nil
}

func uint32To4Bytes(hexa uint32) []byte {
	var data = make([]byte, 4)
	binary.BigEndian.PutUint32(data, hexa)
	return data
}

func convertUTF8EndcodedString(s string) string {
	if !utf8.ValidString(s) {
		v := make([]rune, 0, len(s))
		for i, r := range s {
			if r == utf8.RuneError {
				_, size := utf8.DecodeRuneInString(s[i:])
				if size == 1 {
					continue
				}
			}
			v = append(v, r)
		}
		return string(v)
	}
	return s
}

func getSysPsuFromDb(name string, d *db.DB) (PSU, error) {
	var psuInfo PSU
	var err error

	psuEntry, err := d.GetEntry(&db.TableSpec{Name: PSU_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Can't get entry: ", name, "; Error: ", err)
	}

	psuInfo.Enabled = false
	if psuEntry.Get("status") == "true" {
		psuInfo.Enabled = true
	}

	psuInfo.Volt_Type = psuEntry.Get("type")

	psuInfo.Presence = false
	if psuEntry.Get("presence") == "true" {
		psuInfo.Presence = true
	}

	psuInfo.Status = false
	if psuEntry.Get("status") == "true" {
		psuInfo.Status = true
	}

	psuInfo.Parent = entityParent(name, psuEntry.Get("parent"), d)
	psuInfo.Location = entityLocation(name, psuEntry.Get("location"), d)
	psuInfo.Model_Name = convertUTF8EndcodedString(psuEntry.Get("model"))
	psuInfo.Manufacturer = convertUTF8EndcodedString(psuEntry.Get("mfr_id"))
	psuInfo.Part_Number = convertUTF8EndcodedString(psuEntry.Get("part-no"))
	psuInfo.Serial_Number = convertUTF8EndcodedString(psuEntry.Get("serial"))
	return psuInfo, err
}

func fillSysPsuInfo(psuCom *ocbinds.OpenconfigPlatform_Components_Component,
	name string, all bool, getPowerStats bool, targetUriPath string, d *db.DB) error {
	log.V(lvl.DEBUG).Infof("fillSysPsuInfo: name=%s all=%v getPowerStats=%v targetUriPath=%s", name, all, getPowerStats, targetUriPath)
	var err error
	psuInfo, err := getSysPsuFromDb(name, d)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting PSU info from dB")
		return err
	}

	empty := !psuInfo.Presence
	psuEepromState := psuCom.State
	defaultParentVal := CHASSIS_PREFIX
	if all {
		psuCom.Config.Name = &name
		if getPowerStats {
			if err != nil {
				log.V(lvl.DEBUG).Info("float data error")
				return err
			}
			return err
		}
		psuEepromState.Empty = &empty
		psuEepromState.Name = &name

		psuEepromState.Parent = &defaultParentVal
		if psuInfo.Parent != "" && psuInfo.Parent != "N/A" {
			psuEepromState.Parent = &psuInfo.Parent
		}
		psuEepromState.Location = StringWithDefault(&psuInfo.Location, "")

		psuEepromState.Type, _ = psuEepromState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_POWER_SUPPLY)
		psuEepromState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		if psuInfo.Presence {
			if psuInfo.Status {
				psuEepromState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
			} else {
				psuEepromState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
			}
		}

		if psuInfo.Model_Name != "" {
			psuEepromState.ModelName = &psuInfo.Model_Name
			psuEepromState.Description = &psuInfo.Model_Name
			psuEepromState.PartNo = &psuInfo.Model_Name
		}
		if psuInfo.Manufacturer != "" {
			psuEepromState.MfgName = &psuInfo.Manufacturer
		}
		if psuInfo.Serial_Number != "" {
			psuEepromState.SerialNo = &psuInfo.Serial_Number
		}
		if psuInfo.Part_Number != "" {
			psuEepromState.PartNo = &psuInfo.Part_Number
		}

		return err
	}

	switch targetUriPath {
	case COMP_STATE_EMPTY:
		psuEepromState.Empty = &empty
	case COMP_STATE_OPER_STATUS:
		psuEepromState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		if psuInfo.Status {
			psuEepromState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		}
	case COMP_STATE_SERIAL_NO:
		if psuInfo.Serial_Number != "" {
			psuEepromState.SerialNo = &psuInfo.Serial_Number
		}
	case COMP_STATE_DESCR:
		if psuInfo.Model_Name != "" {
			psuEepromState.Description = &psuInfo.Model_Name
		}
	case COMP_STATE_MODEL_NAME:
		if psuInfo.Model_Name != "" {
			psuEepromState.ModelName = &psuInfo.Model_Name
		}
	case COMP_STATE_MFG_NAME:
		if psuInfo.Manufacturer != "" {
			psuEepromState.MfgName = &psuInfo.Manufacturer
		}
	case COMP_STATE_PART_NO:
		if psuInfo.Model_Name != "" {
			psuEepromState.PartNo = &psuInfo.Model_Name
		}
		if psuInfo.Part_Number != "" {
			psuEepromState.PartNo = &psuInfo.Part_Number
		}
	case COMP_STATE_NAME:
		psuEepromState.Name = &name
	case COMP_STATE_PARENT:
		psuEepromState.Parent = &defaultParentVal
		if psuInfo.Parent != "" && psuInfo.Parent != "N/A" {
			psuEepromState.Parent = &psuInfo.Parent
		}
	case COMP_STATE_LOCATION:
		psuEepromState.Location = StringWithDefault(&psuInfo.Location, "")
	case COMP_STATE_TYPE:
		psuEepromState.Type, _ = psuEepromState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_POWER_SUPPLY)
	default:
		return fmt.Errorf("unsupported leaf path for PSU component: %v", targetUriPath)
	}

	return err
}

func validPsuName(name *string) bool {
	if name == nil || *name == "" {
		return false
	}
	valid, _ := regexp.MatchString("PSU [1-9][0-9]*$", *name)
	return valid
}

func validFanName(name *string) bool {
	if name == nil || *name == "" {
		return false
	}
	validFan, _ := regexp.MatchString("fan[1-9][0-9]*$", *name)
	return validFan
}

func validFanTrayName(name string) bool {
	if name == "" {
		return false
	}
	if strings.HasPrefix(name, FAN_TRAY_PREFIX) {
		return true
	} else if strings.HasSuffix(name, FAN_TRAY_SUFFIX) {
		return true
	} else {
		return false
	}
}

func validFpgaName(name *string) bool {
	if name == nil || *name == "" {
		return false
	}
	return strings.HasPrefix(*name, "fpga_")
}

func entityParent(name, default_parent string, d *db.DB) string {
	entityEntry, err := d.GetEntry(&db.TableSpec{Name: PHYS_ENTITY_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		return default_parent
	}
	if parent := entityEntry.Get("parent_name"); parent != "" {
		return parent
	}
	return default_parent
}

func entityLocation(name, default_location string, d *db.DB) string {
	entityEntry, err := d.GetEntry(&db.TableSpec{Name: PHYS_ENTITY_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		return default_location
	}
	if location := entityEntry.Get("position_in_parent"); location != "" {
		return location
	}
	return default_location
}

func fanDbEntry(name string, d *db.DB) (fanInfo, error) {
	var fanInfo fanInfo

	index := strings.TrimPrefix(name, "fan")
	tblName := FAN_TBL
	if validFanTrayName(name) {
		tblName = FAN_TRAY_TBL
		index = strings.TrimSuffix(strings.TrimPrefix(name, FAN_TRAY_PREFIX), FAN_TRAY_SUFFIX)
	}
	dbEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Infof("Failed to get %s from DB, error %v", name, err)
		return fanInfo, err
	}

	fanInfo.hardwareRev = dbEntry.Get("hardware_revision")
	fanInfo.isReplaceable = false
	if removable := dbEntry.Get("is_replaceable"); removable == "true" || removable == "True" {
		fanInfo.isReplaceable = true
	}
	fanInfo.location = index
	fanInfo.mfgDate = dbEntry.Get("mfg_date")
	fanInfo.parent = entityParent(name, dbEntry.Get("drawer_name"), d)
	fanInfo.partNo = dbEntry.Get("part_no")
	fanInfo.model = dbEntry.Get("model")
	fanInfo.presence = false
	if presence := dbEntry.Get("presence"); presence == "true" || presence == "True" {
		fanInfo.presence = true
	}
	fanInfo.pwm = dbEntry.Get("pwm")
	fanInfo.serial = dbEntry.Get("serial")
	fanInfo.speed = dbEntry.Get("speed")
	fanInfo.status = false
	if status := dbEntry.Get("status"); status == "true" || status == "True" {
		fanInfo.status = true
	}

	return fanInfo, nil
}

func dbToYangFan(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, targetUriPath string, d *db.DB, cType componentType) error {
	log.V(lvl.DEBUG).Infof("dbToYangFan: name %s uri %s type %v", name, targetUriPath, cType)

	fanInfo, err := fanDbEntry(name, d)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting fan info from dB")
		return err
	}

	/* Supported fan/fan-tray leaves are:
	 *   - /components/component/state/empty
	 *   - /components/component/state/hardware-version
	 *   - /components/component/state/location
	 *   - /components/component/state/mfg-date
	 *   - /components/component/state/name
	 *   - /components/component/state/oper-status
	 *   - /components/component/state/parent
	 *   - /components/component/state/part-no
	 *   - /components/component/state/removable
	 *   - /components/component/state/serial-no
	 *   - /components/component/state/type
	 *   - /components/component/fan/state/speed
	 *   - /components/component/fan/state/speed-control-pct
	 */
	var allState, allFan bool
	if targetUriPath == COMP {
		allState = true
		allFan = true
	} else if targetUriPath == COMP_ST {
		allState = true
	} else if targetUriPath == COMP_FAN || targetUriPath == COMP_FAN_ST {
		allFan = true
	}

	if targetUriPath == COMP {
		ygot.BuildEmptyTree(comp.State)
		ygot.BuildEmptyTree(comp.Fan)
		ygot.BuildEmptyTree(comp.Fan.State)
	}
	if allState || strings.HasPrefix(targetUriPath, COMP_ST) {
		ygot.BuildEmptyTree(comp.State)
	}
	if allFan || strings.HasPrefix(targetUriPath, COMP_FAN) {
		ygot.BuildEmptyTree(comp.Fan)
		ygot.BuildEmptyTree(comp.Fan.State)
	}

	comp.Name = &name
	if allState || targetUriPath == COMP_STATE_EMPTY {
		empty := !fanInfo.presence
		comp.State.Empty = &empty
	}
	if allState || targetUriPath == COMP_STATE_HW_VER {
		if fanInfo.hardwareRev == "" {
			fanInfo.hardwareRev = "N/A"
		}
		comp.State.HardwareVersion = &fanInfo.hardwareRev
	}
	if allState || targetUriPath == COMP_STATE_LOCATION {
		comp.State.Location = &fanInfo.location
	}
	if allState || targetUriPath == COMP_STATE_MFG_DATE {
		if fanInfo.mfgDate != "" && fanInfo.mfgDate != "N/A" {
			if len(fanInfo.mfgDate) > 10 {
				// MM-DD-YYYY HH:MM:SS -> YYYY-MM-DD
				mfg_date := fanInfo.mfgDate[6:10] + "-" +
					fanInfo.mfgDate[0:2] + "-" + fanInfo.mfgDate[3:5]
				comp.State.MfgDate = &mfg_date
			} else {
				log.V(lvl.WARNING).Infof("fan %s mfg-date expected format: MM-DD-YYYY HH:MM:SS, got %v", name, fanInfo.mfgDate)
			}
		}
	}
	if allState || targetUriPath == COMP_STATE_NAME {
		comp.State.Name = &name
	}
	if allState || targetUriPath == COMP_STATE_OPER_STATUS {
		if fanInfo.presence && fanInfo.status {
			comp.State.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		} else if fanInfo.presence && !fanInfo.status {
			comp.State.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		} else {
			comp.State.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		}
	}
	if allState || targetUriPath == COMP_STATE_PARENT {
		if fanInfo.parent != "" && fanInfo.parent != "N/A" {
			comp.State.Parent = &fanInfo.parent
		} else {
			parentChassis := CHASSIS_PREFIX
			comp.State.Parent = &parentChassis
		}
	}
	if allState || targetUriPath == COMP_STATE_PART_NO {
		if fanInfo.model != "" {
			comp.State.PartNo = &fanInfo.model
		}
		/* Let the partNo field from the DB override the model field from the DB
		 * if it is present. */
		if fanInfo.partNo != "" {
			comp.State.PartNo = &fanInfo.partNo
		}
	}
	if allState || targetUriPath == COMP_STATE_REMOVABLE {
		comp.State.Removable = &fanInfo.isReplaceable
	}
	if allState || targetUriPath == COMP_STATE_SERIAL_NO {
		if fanInfo.serial != "" {
			comp.State.SerialNo = &fanInfo.serial
		}
	}
	if allState || targetUriPath == COMP_STATE_TYPE {
		if cType == CompTypeFan {
			comp.State.Type, _ = comp.State.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_FAN)
		} else if cType == CompTypeFanTray {
			comp.State.Type, _ = comp.State.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_FAN_TRAY)
		}
	}
	if allState || targetUriPath == COMP_STATE_MODEL_NAME {
		if fanInfo.model != "" {
			comp.State.ModelName = &fanInfo.model
		}
	}
	if allFan || targetUriPath == COMP_FAN_SPEED {
		if fanInfo.speed != "" {
			speedU64, err := strconv.ParseUint(fanInfo.speed, 10, 32)
			if err != nil {
				log.V(lvl.DEBUG).Info("Fan %s, failed to convert speed \"%s\" to uint: %v", name, fanInfo.speed, err)
			} else {
				speedU32 := uint32(speedU64)
				comp.Fan.State.Speed = &speedU32
			}
		}
	}
	if allFan || targetUriPath == COMP_FAN_SCP {
		if fanInfo.pwm != "" {
			pwmU64, err := strconv.ParseUint(fanInfo.pwm, 10, 32)
			if err != nil {
				log.V(lvl.DEBUG).Info("Fan %s, failed to convert PWM \"%s\" to uint: %v", name, fanInfo.pwm, err)
			}
			pwmU32 := uint32(pwmU64)
			comp.Fan.State.SpeedControlPct = &pwmU32
		}
	}

	return nil
}

func validXcvrName(name *string) bool {
	if name == nil || *name == "" {
		return false
	}

	/* Expect tranceiver name of form EthernetX, where X is an integer */
	if !strings.HasPrefix(*name, XCVR_KEY_PREFIX) {
		return false
	}

	sp := strings.SplitAfter(*name, "Ethernet")

	if _, err := strconv.Atoi(sp[1]); err != nil {
		return false
	}
	return true
}

func getSysXcvrPresenceFromDb(name string, d *db.DB) (bool, error) {
	xcvrStatusEntry, err := d.GetEntry(&db.TableSpec{Name: TRANSCEIVER_STATUS}, db.Key{Comp: []string{name}})
	if err != nil {
		return false, err
	}
	status := xcvrStatusEntry.Get("status")
	if status == SFP_STATUS_REMOVED {
		return false, nil
	} else if status != SFP_STATUS_INSERTED {
		return false, fmt.Errorf("Unknown status for transceiver %s: %s", name, status)
	}
	return true, nil
}

func getSysXcvrFromDb(name string, d *db.DB) (Xcvr, error) {
	var xcvrInfo Xcvr
	var err error

	xcvrEntry, err := d.GetEntry(&db.TableSpec{Name: TRANSCEIVER_TBL}, db.Key{Comp: []string{name}})

	if err != nil {
		xcvrInfo.Presence = false
		return xcvrInfo, err
	}

	/* Existence of entry implies presence */
	xcvrInfo.Presence = true
	xcvrInfo.EthPmd = xcvrEntry.Get("ethernet-pmd")
	xcvrInfo.Parent = xcvrEntry.Get("parent")
	xcvrInfo.MfgName = xcvrEntry.Get("manufacturer")
	xcvrInfo.MfgDate = xcvrEntry.Get("vendor_date")
	xcvrInfo.PartNo = xcvrEntry.Get("model")
	xcvrInfo.SerialNo = xcvrEntry.Get("serial")
	xcvrInfo.HardwareRev = xcvrEntry.Get("hardware_rev")
	xcvrInfo.Type = xcvrEntry.Get("type")
	xcvrInfo.LatestFirmwareVer = xcvrEntry.Get("latest_fw_rev")

	xcvrDOMEntry, err := d.GetEntry(&db.TableSpec{Name: TRANSCEIVER_DOM}, db.Key{Comp: []string{name}})
	if err != nil {
		xcvrInfo.Presence = false
		return xcvrInfo, err
	}

	for i := 0; i < XCVR_LANE_LIMIT; i++ {
		xcvrInfo.Lanes[i].RxPowerLane = xcvrDOMEntry.Get(fmt.Sprintf("rx%dpower", i+1))
		xcvrInfo.Lanes[i].TxBiasLane = xcvrDOMEntry.Get(fmt.Sprintf("tx%dbias", i+1))
		xcvrInfo.Lanes[i].TxPowerLane = xcvrDOMEntry.Get(fmt.Sprintf("tx%dpower", i+1))
		xcvrInfo.Lanes[i].TxDisable = xcvrDOMEntry.Get(fmt.Sprintf("tx%ddisable", i+1))
	}

	xcvrInfo.Temperature = xcvrDOMEntry.Get("temperature")
	xcvrInfo.ModuleState = xcvrDOMEntry.Get("module_state")

	return xcvrInfo, err
}

func test_if_available(s string) bool {
	return ((s != "") && (s != "N/A") && (s != "n/a"))
}

func convert_form_factor_type(ft string) ocbinds.E_OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE {
	switch {
	case ft == "N/A" || ft == "":
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_UNSET
	case strings.HasPrefix(ft, "Unknown"):
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_OTHER
	case strings.HasPrefix(ft, "SFP"):
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_SFP
	case ft == "XFP":
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_XFP
	case ft == "X2":
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_X2
	case ft == "QSFP":
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_QSFP
	case strings.HasPrefix(ft, "QSFP+"):
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_QSFP_PLUS
	case strings.HasPrefix(ft, "QSFP28"):
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_QSFP28
	case strings.HasPrefix(ft, "OSFP") || strings.HasPrefix(ft, "QSFP-DD"):
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_OSFP
	default:
		return ocbinds.OpenconfigTransportTypes_TRANSCEIVER_FORM_FACTOR_TYPE_UNSET
	}
}

// sfp_type_to_max_lanes_map mapping pulled from third_party/sonic-platform-daemons/sonic-xcvrd/xcvrd/xcvrd.py
var sfpTypeToMaxLanesMap = map[string]int{
	"SFP/SFP+/SFP28":                1,
	"QSFP":                          4,
	"QSFP+ or later":                4,
	"QSFP28 or later":               4,
	"OSFP 8X Pluggable Transceiver": 8,
	"QSFP-DD Double Density 8X Pluggable Transceiver": 8,
}

func convAndFillDBValues(rxpField, txpField, txbField, txdisableField string, channel *ocbinds.OpenconfigPlatform_Components_Component_Transceiver_PhysicalChannels_Channel) {
	if rxpField != "" {
		if rxPower, err := strconv.ParseFloat(rxpField, 64); err == nil {
			channel.State.InputPower.Instant = &rxPower
		} else {
			log.V(lvl.DEBUG).Infof("Error converting rxPower (\"%s\") from string to float64", rxpField)
		}
	}
	if txpField != "" {
		if txPower, err := strconv.ParseFloat(txpField, 64); err == nil {
			channel.State.OutputPower.Instant = &txPower
		} else {
			log.V(lvl.DEBUG).Infof("Error converting txPower (\"%s\") from string to float64", txpField)
		}
	}
	if txbField != "" {
		if txBias, err := strconv.ParseFloat(txbField, 64); err == nil {
			channel.State.LaserBiasCurrent.Instant = &txBias
		} else {
			log.V(lvl.DEBUG).Infof("Error converting txBias (\"%s\") from string to float64", txbField)
		}
	}
	txLaserEnable := false
	if txdisableField == "False" || txdisableField == "false" {
		txLaserEnable = true
	}
	channel.State.TxLaser = &txLaserEnable
}

func fetchAllPortsFromParentPortWithLanes(port string, appstdb *db.DB, maxLanes int) (map[uint16]string, error) {
	laneToPortMap := make(map[uint16]string)
	childPortList, err := platform.PrimaryIntfToChildIntfs(port)
	if err != nil {
		return nil, err
	}
	// Loop through all possible child ports and add to laneToPortMap only if
	// the Appl State DB entry is present and has the "lanes" field.
	ts := db.TableSpec{Name: PORT_TBL}
	for _, intfName := range childPortList {
		key := db.Key{Comp: []string{intfName}}
		dbEntry, err := appstdb.GetEntry(&ts, key)
		if err != nil {
			continue
		}
		dbLanes := dbEntry.Get("lanes")
		if dbLanes == "" {
			continue
		}

		lanesList := strings.Split(dbLanes, ",")
		// Convert the lane set to physical channels list based on maxLanes per xcvr type:
		// E.g. ["9","10","11","12"] gets converted to ["0","1","2","3"]
		// For SFP+ ports, ["257"] / ["258"] gets converted to ["0"]
		for _, lane := range lanesList {
			val, err := strconv.ParseUint(lane, 10, 16)
			if err != nil {
				log.V(lvl.DEBUG).Info("error in strconv: " + err.Error())
				return nil, err
			}
			laneToPortMap[(uint16(val)-1)%uint16(maxLanes)] = intfName
		}
	}
	// len(laneToPortMap) should be atleast 1 to account for parent port itself.
	if len(laneToPortMap) == 0 {
		log.V(lvl.DEBUG).Info("no ports found in Appl State DB for parent port " + port)
		return nil, tlerr.InvalidArgs("no ports found in Appl State DB for parent port " + port)
	}
	return laneToPortMap, nil
}

func findAssociatedIntfForXcvrLane(xcvrName string, laneIdx uint16, maxLanes int, appstdb *db.DB) string {
	// Derive the parent port from transceiver name.
	parentIntfName := strings.Replace(xcvrName, "Ethernet", "Ethernet1/", 1)
	if maxLanes != 1 {
		parentIntfName += "/1"
	}
	// Fetch all existing child ports from parent port in Appl State DB.
	var err error
	if appstdb == nil {
		appstdb, err = db.NewDB(getDBOptions(db.ApplStateDB))
		if err != nil {
			log.V(lvl.DEBUG).Info("could not create ApplStateDB instance: " + err.Error())
			return ""
		}
		defer appstdb.DeleteDB()
	}
	// laneToPortMap should return <laneIndex: interfaceName> mapping.
	laneToPortMap, err := fetchAllPortsFromParentPortWithLanes(parentIntfName, appstdb, maxLanes)
	if err != nil {
		return ""
	}
	// Lookup the associated interface using laneIdx.
	intf, ok := laneToPortMap[laneIdx]
	if !ok {
		log.V(lvl.DEBUG).Infof("invalid lane %v for parent port %v and xcvr %v", laneIdx, parentIntfName, xcvrName)
		return ""
	}
	return intf
}

func fillXcvrLaneInfo(xcvrCom *ocbinds.OpenconfigPlatform_Components_Component, laneIdx uint16, xcvrInfo Xcvr, name string, maxLanes int, d *db.DB) (err error) {
	channel, ok := xcvrCom.Transceiver.PhysicalChannels.Channel[laneIdx]
	if !ok || channel == nil {
		channel, err = xcvrCom.Transceiver.PhysicalChannels.NewChannel(laneIdx)
		if err != nil {
			return fmt.Errorf("cannot create channel object: %w", err)
		}
	}
	ygot.BuildEmptyTree(channel)
	ygot.BuildEmptyTree(channel.Config)
	ygot.BuildEmptyTree(channel.State)
	channel.Config.Index = &laneIdx
	channel.State.Index = &laneIdx

	// Fetch the associated interface for the xcvr lane. In case of errors, don't break and keep
	// processing subsequent paths; default value of "" is returned and used.
	associatedIntf := findAssociatedIntfForXcvrLane(name, laneIdx, maxLanes, d)
	channel.State.AssociatedInterface = &associatedIntf

	if laneIdx < XCVR_LANE_LIMIT {
		lane := &xcvrInfo.Lanes[laneIdx]
		convAndFillDBValues(lane.RxPowerLane, lane.TxPowerLane, lane.TxBiasLane, lane.TxDisable, channel)
	} else {
		return errors.New("lane index is invalid.")
	}
	return nil
}

func fillSysXcvrInfo(xcvrCom *ocbinds.OpenconfigPlatform_Components_Component,
	name string, all bool, laneIdx string, targetUriPath string, dbs [db.MaxDB]*db.DB) error {
	var err error

	log.V(lvl.DEBUG).Infof("fillSysXcvrInfo: name %s, all %v laneIdx %s targetUriPath %s", name, all, laneIdx, targetUriPath)

	d := dbs[db.StateDB]
	if d == nil {
		d, err = db.NewDB(getDBOptions(db.StateDB))
		if err != nil {
			return tlerr.InvalidArgsError{Format: err.Error()}
		}
		defer d.DeleteDB()
	}
	cfgdb := dbs[db.ConfigDB]
	if cfgdb == nil {
		cfgdb, err = db.NewDB(getDBOptions(db.ConfigDB))
		if err != nil {
			return tlerr.InvalidArgsError{Format: err.Error()}
		}
		defer cfgdb.DeleteDB()
	}

	xcvrPresence, err := getSysXcvrPresenceFromDb(name, d)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting transceiver status from dB")
		return err
	}
	nm := name
	xcvrEEPROMState := xcvrCom.State
	xcvrEEPROMState.Name = &nm
	if !xcvrPresence {
		p := !xcvrPresence
		xcvrEEPROMState.Empty = &p
		return nil
	}

	xcvrInfo, err := getSysXcvrFromDb(name, d)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting transceiver info from dB")
		return err
	}

	if xcvrInfo.Type != "" && laneIdx != "" {
		maxLanes, ok := sfpTypeToMaxLanesMap[xcvrInfo.Type]
		if !ok {
			return errors.New("could not find the max number of lanes for transceiver.")
		}
		idx, err := strconv.ParseUint(laneIdx, 10, 16)
		if err != nil {
			return err
		}
		if idx >= uint64(maxLanes) {
			return errors.New("lane index greater than the max number of lanes for transceiver.")
		}
		fillXcvrLaneInfo(xcvrCom, uint16(idx), xcvrInfo, name, maxLanes, dbs[db.ApplStateDB])
	}

	xcvrState := xcvrCom.Transceiver.State

	if all {

		/* Top level */
		xcvrEEPROMState.Name = &nm
		xcvrCom.Config.Name = &nm

		/* Present state */
		p := !xcvrInfo.Presence
		xcvrEEPROMState.Empty = &p

		q := true
		xcvrEEPROMState.Removable = &q
		/* Not present */
		if p {
			return err
		}

		xcvrEEPROMState.Type, _ = xcvrCom.State.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_TRANSCEIVER)

		if test_if_available(xcvrInfo.Parent) {
			xcvrEEPROMState.Parent = &xcvrInfo.Parent
		}

		/* Vendor info */
		if xcvrInfo.SerialNo != "" {
			xcvrEEPROMState.SerialNo = &xcvrInfo.SerialNo
		}
		if xcvrInfo.PartNo != "" {
			xcvrEEPROMState.PartNo = &xcvrInfo.PartNo
		}
		if xcvrInfo.MfgName != "" {
			xcvrEEPROMState.MfgName = &xcvrInfo.MfgName
		}
		if xcvrInfo.HardwareRev != "" {
			xcvrEEPROMState.HardwareVersion = &xcvrInfo.HardwareRev
			// Using the 'hardware_rev' field to also populate the firmware-version path.
			xcvrEEPROMState.FirmwareVersion = &xcvrInfo.HardwareRev
		}
		if xcvrInfo.LatestFirmwareVer != "" {
			xcvrState.LatestAvailableFirmwareVersion = &xcvrInfo.LatestFirmwareVer
		}
		if xcvrInfo.MfgDate != "" {
			xcvrEEPROMState.MfgDate = &xcvrInfo.MfgDate
		}
		if xcvrInfo.ModuleState != "" {
			xcvrState.ModuleStatus = ocbinds.OpenconfigPlatform_Components_Component_Transceiver_State_ModuleStatus_MODULE_STATUS_UNKNOWN
			if v, ok := moduleStatusMap[xcvrInfo.ModuleState]; ok {
				xcvrState.ModuleStatus = v
			}
		}
		xcvrEEPROMState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		if xcvrInfo.Temperature != "" {
			if float64val, err := strconv.ParseFloat(xcvrInfo.Temperature, 64); err == nil {
				xcvrEEPROMState.Temperature.Instant = &float64val
			}
		}

		/* Physical-Channels level */
		if xcvrInfo.Type != "" && laneIdx == "" {
			if maxLanes, ok := sfpTypeToMaxLanesMap[xcvrInfo.Type]; ok {
				for i := 0; i < maxLanes; i++ {
					fillXcvrLaneInfo(xcvrCom, uint16(i), xcvrInfo, name, maxLanes, dbs[db.ApplStateDB])
				}
			} else {
				log.V(lvl.DEBUG).Info("Could not find the max number of lanes for transceiver.")
			}
		}

		if xcvrInfo.Type != "" {
			xcvrState.FormFactor = convert_form_factor_type(xcvrInfo.Type)
		}
		xcvrState.EthernetPmd = getDbToYangEthPmd(xcvrInfo.EthPmd)

		/*
		       Pending YANG updates
		   if (test_if_available(xcvrInfo.Module_Lane_Count)){
		       tmp, err := strconv.ParseUint(xcvrInfo.Module_Lane_Count, 10, 64)
		       if err == nil {
		           q := uint32(tmp)
		           xcvrState.ModuleLaneCount = &q
		       }
		   }
		   if (test_if_available(xcvrInfo.Lpmode)){
		       tmp, err := strconv.ParseBool(xcvrInfo.Lpmode)
		       if err == nil {
		           xcvrState.Lpmode = &tmp
		       }
		   }


		   if (test_if_available(xcvrInfo.Media_Interface)){
		       xcvrState.MediaInterface = &xcvrInfo.Media_Interface
		   }
		   if (test_if_available(xcvrInfo.Cable_Type)){
		       xcvrState.CableType = &xcvrInfo.Cable_Type
		   }

		   if (test_if_available(xcvrInfo.Qsa_Adapter_Type)){
		       xcvrState.QsaAdapterType = &xcvrInfo.Qsa_Adapter_Type
		   }
		*/
		return err
	}

	switch targetUriPath {
	case COMP_STATE_EMPTY:
		q := !xcvrInfo.Presence
		xcvrEEPROMState.Empty = &q
	case COMP_STATE_NAME:
		nm := name
		xcvrEEPROMState.Name = &nm
	case COMP_CONFIG_NAME:
		nm := name
		xcvrCom.Config.Name = &nm
	case COMP_STATE_TYPE:
		xcvrEEPROMState.Type, _ = xcvrCom.State.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_TRANSCEIVER)
	case COMP_STATE_PARENT:
		if test_if_available(xcvrInfo.Parent) {
			xcvrEEPROMState.Parent = &xcvrInfo.Parent
		}
	case COMP_STATE_SERIAL_NO:
		if xcvrInfo.SerialNo != "" {
			xcvrEEPROMState.SerialNo = &xcvrInfo.SerialNo
		}
	case COMP_STATE_PART_NO:
		if xcvrInfo.PartNo != "" {
			xcvrEEPROMState.PartNo = &xcvrInfo.PartNo
		}
	case COMP_STATE_MFG_NAME:
		if xcvrInfo.MfgName != "" {
			xcvrEEPROMState.MfgName = &xcvrInfo.MfgName
		}
	case COMP_STATE_HW_VER:
		if xcvrInfo.HardwareRev != "" {
			xcvrEEPROMState.HardwareVersion = &xcvrInfo.HardwareRev
		}
	// Using the 'hardware_rev' field to also populate the firmware-version path.
	case COMP_STATE_FIRM_VER:
		if xcvrInfo.HardwareRev != "" {
			xcvrEEPROMState.FirmwareVersion = &xcvrInfo.HardwareRev
		}
	case COMP_STATE_MFG_DATE:
		if xcvrInfo.MfgDate != "" {
			xcvrEEPROMState.MfgDate = &xcvrInfo.MfgDate
		}
	case COMP_STATE_OPER_STATUS:
		xcvrEEPROMState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
	case COMP_STATE_TEMP:
		if xcvrInfo.Temperature != "" {
			float64val, err := strconv.ParseFloat(xcvrInfo.Temperature, 64)
			if err != nil {
				return err
			}
			xcvrEEPROMState.Temperature.Instant = &float64val
		}

	case COMP_STATE_REMOVABLE:
		q := true
		xcvrEEPROMState.Removable = &q

	case XCVR_FORM_FACTOR:
		if xcvrInfo.Type != "" {
			xcvrState.FormFactor = convert_form_factor_type(xcvrInfo.Type)
		}
	case XCVR_ETH_PMD:
		xcvrState.EthernetPmd = getDbToYangEthPmd(xcvrInfo.EthPmd)
	case XCVR_STATE_LATEST_FW_VER:
		if xcvrInfo.LatestFirmwareVer != "" {
			xcvrState.LatestAvailableFirmwareVersion = &xcvrInfo.LatestFirmwareVer
		}
	}
	return err
}

/* Get a list of all table entries available */
func getAllTableEntries(d *db.DB, tblName string, key string) ([]string, error) {
	if tblName == "" || key == "" {
		return nil, errors.New("getAllTableEntries: empty table name or key.")
	}
	keyList, err := d.GetKeysPattern(&(db.TableSpec{Name: tblName}), db.Key{Comp: []string{key}})
	if err != nil {
		return nil, err
	}
	var ret []string
	for _, v := range keyList {
		if len(v.Comp) == 0 {
			continue
		}
		ret = append(ret, strings.Join(v.Comp, d.Opts.KeySeparator))
	}
	return ret, nil
}

func validTempName(name *string) bool {
	if name == nil || *name == "" {
		return false
	}
	// For TEMPERATURE_INFO, SONiC supported keys are "exhaust_sensor_x", "inlet_sensor_x", "heatsink_sensor_x", "dimm_sensor_x",
	// "exhaust_sensor", "inlet_sensor", "heatsink_sensor", and "dimm_sensor"
	if valid := temperatureExp.MatchString(*name); valid {
		return valid
	}
	if valid, err := regexp.MatchString("TEMP [1-9][0-9]*$", *name); err == nil {
		return valid
	}
	return false
}

func validCpuName(name string) bool {
	if name == "" {
		return false
	}
	// For CPU component, SONiC supported keys are "cpu_sensor_x", and "cpu_sensor"
	if valid := cpuExp.MatchString(name); valid {
		return valid
	}
	return false
}

func validHumidityName(name string) bool {
	if name == "" {
		return false
	}
	return strings.HasPrefix(name, "humidity_")
}

func getSysTempFromDb(name string, d *db.DB) (TempSensor, error) {
	tempEntry, err := d.GetEntry(&db.TableSpec{Name: TEMP_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		return TempSensor{}, err
	}

	tempInfo := TempSensor{
		Current:             tempEntry.Get("temperature"),
		Name:                tempEntry.Get("name"),
		Crit_High_Threshold: tempEntry.Get("critical_high_threshold"),
		Crit_Low_Threshold:  tempEntry.Get("critical_low_threshold"),
		High_Threshold:      tempEntry.Get("high_threshold"),
		Low_Threshold:       tempEntry.Get("low_threshold"),
		Timestamp:           tempEntry.Get("timestamp"),
		Warning_Status:      tempEntry.Get("warning_status"),
	}

	return tempInfo, nil
}

/* This function converts the timestamp stored in the dB to the pattern accepted
 * by the IETF timestamp pattern specified in the YANG model
 */
func convertToIetfTime(time string) string {
	time = time[:8] + "T" + time[9:]
	time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + time[8:] + "Z"
	return time
}

func fillSysTempInfo(temp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, d *db.DB) error {
	isCpuSensor := validCpuName(name)
	tempInfo, err := getSysTempFromDb(name, d)
	if err != nil {
		return err
	}

	tempCom := temp.State.Temperature
	tempState := temp.State
	tempSensorState := temp.Sensor.State
	defaultParentVal := CHASSIS_PREFIX

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths || targetUriPath == TEMP_COMP {
		tempState.Name = &name
		tempState.Location = &name
		temp.Config.Name = &name
		tempState.Parent = &defaultParentVal
		tempState.Type, _ = tempState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_SENSOR)
		if isCpuSensor {
			tempState.Type, _ = tempState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_CPU)
		}
		if tempInfo.Name != "" {
			tempState.Name = &tempInfo.Name
		}
		if tempInfo.Current != "" {
			cur, terr := strconv.ParseFloat(tempInfo.Current, 64)
			if terr != nil {
				log.V(lvl.DEBUG).Infof("Error in ParseFloat conversion for tempInfo.Current (%v): %v", tempInfo.Current, terr)
			} else {
				tempCom.Instant = &cur
			}
		}

		if pType == StatePaths {
			return nil
		}
		// Sensor State Sensor Type
		if !isCpuSensor {
			switch {
			case strings.HasPrefix(name, "dimm_sensor"):
				tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_DIMM_TEMPERATURE_SENSOR
			case strings.HasPrefix(name, "exhaust_sensor"):
				tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_EXHAUST_TEMPERATURE_SENSOR
			case strings.HasPrefix(name, "heatsink_sensor"):
				tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_HEAT_SINK_TEMPERATURE_SENSOR
			case strings.HasPrefix(name, "inlet_sensor"):
				tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_INLET_TEMPERATURE_SENSOR
			}
		}
		return nil
	}

	switch targetUriPath {
	case COMP_CONFIG_NAME:
		temp.Config.Name = &name
	case COMP_STATE_NAME:
		tempState.Name = &name
		if tempInfo.Name != "" {
			tempState.Name = &tempInfo.Name
		}
	case COMP_STATE_LOCATION:
		tempState.Location = &name
	case COMP_STATE_PARENT:
		tempState.Parent = &defaultParentVal
	case COMP_STATE_TYPE:
		tempState.Type, _ = tempState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_SENSOR)
		if isCpuSensor {
			tempState.Type, _ = tempState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_CPU)
		}
	case SENSOR_STATE_SENSOR_TYPE:
		if isCpuSensor {
			return errors.New("invalid component for sensor/state/sensor-type path.")
		}
		switch {
		case strings.HasPrefix(name, "dimm_sensor"):
			tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_DIMM_TEMPERATURE_SENSOR
		case strings.HasPrefix(name, "exhaust_sensor"):
			tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_EXHAUST_TEMPERATURE_SENSOR
		case strings.HasPrefix(name, "heatsink_sensor"):
			tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_HEAT_SINK_TEMPERATURE_SENSOR
		case strings.HasPrefix(name, "inlet_sensor"):
			tempSensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_INLET_TEMPERATURE_SENSOR
		default:
			return fmt.Errorf("invalid component name for sensor type: %s", name)
		}
	case TEMP_INSTANT:
		if tempInfo.Current != "" {
			cur, terr := strconv.ParseFloat(tempInfo.Current, 64)
			if terr != nil {
				return terr
			}
			tempCom.Instant = &cur
		}
	default:
		return fmt.Errorf("unsupported leaf path for Sensor/CPU component: %v", targetUriPath)
	}

	return nil
}

func getSysHumidityFromDb(name string, d *db.DB) (HumiditySensor, error) {
	sensorEntry, err := d.GetEntry(&db.TableSpec{Name: HUMIDITY_SENSOR_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return HumiditySensor{}, err
	}

	return HumiditySensor{
		Name:             sensorEntry.Get("channel-name"),
		Parent:           sensorEntry.Get("parent"),
		RelativeHumidity: sensorEntry.Get("relative-humidity"),
	}, nil
}

func fillSysHumidityInfo(humidity *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, d *db.DB) error {
	humidityInfo, err := getSysHumidityFromDb(name, d)
	if err != nil {
		return err
	}

	humidityState := humidity.State
	humiditySensorState := humidity.Sensor.State
	defaultParentVal := CHASSIS_PREFIX

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths || targetUriPath == COMP_SENSOR {
		humidityState.Name = StringWithDefault(&humidityInfo.Name, name)
		humidity.Config.Name = humidityState.Name
		humidityState.Parent = StringWithDefault(&humidityInfo.Parent, defaultParentVal)
		humidityState.Type, _ = humidityState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_SENSOR)

		if pType == StatePaths {
			return nil
		}

		// Sensor State
		humiditySensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_HUMIDITY_SENSOR
		humiditySensorState.Instant = String2Float(humidityInfo.RelativeHumidity, 0)
		return nil
	}

	switch targetUriPath {
	case COMP_CONFIG_NAME:
		humidity.Config.Name = StringWithDefault(&humidityInfo.Name, name)
	case COMP_STATE_NAME:
		humidityState.Name = StringWithDefault(&humidityInfo.Name, name)
	case COMP_STATE_PARENT:
		humidityState.Parent = StringWithDefault(&humidityInfo.Parent, defaultParentVal)
	case COMP_STATE_TYPE:
		humidityState.Type, _ = humidityState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_SENSOR)
	case SENSOR_STATE_SENSOR_TYPE:
		humiditySensorState.SensorType = ocbinds.GooglePinsPlatform_SENSOR_TYPE_HUMIDITY_SENSOR
	case SENSOR_STATE_INSTANT:
		humiditySensorState.Instant = String2Float(humidityInfo.RelativeHumidity, 0)
	default:
		return fmt.Errorf("unsupported leaf path for Sensor/CPU component: %v", targetUriPath)
	}

	return nil
}

func getSysStorageFromDb(name string, d *db.DB, tblName string) (Storage, error) {
	/* Backend is populating STORAGE_INFO in: sonic-platform-daemons/sonic-componentd/scripts/componentd */
	storageEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return Storage{}, err
	}
	storageInfo := Storage{
		Name:                           storageEntry.Get("name"),
		IOErrors:                       storageEntry.Get("io-errors"),
		PartNo:                         storageEntry.Get("part-no"),
		Parent:                         storageEntry.Get("parent"),
		SerialNo:                       storageEntry.Get("serial-no"),
		Removable:                      storageEntry.Get("removable"),
		OperStatus:                     storageEntry.Get("oper-status"),
		WriteAmplificationFactor:       storageEntry.Get("write-amplification-factor"),
		RawReadErrorRate:               storageEntry.Get("raw-read-error-rate"),
		ThroughputPerformance:          storageEntry.Get("throughput-performance"),
		ReallocatedSectorCount:         storageEntry.Get("reallocated-sector-count"),
		PowerOnSeconds:                 storageEntry.Get("power-on-time"),
		SsdLifeLeft:                    storageEntry.Get("ssd-life-left"),
		PowerCycleCount:                storageEntry.Get("power-cycle-count"),
		UncorrectableSectorCountOnLine: storageEntry.Get("uncorrectable-sector-count"),
		NumPureSpare:                   storageEntry.Get("num-pure-spare"),
		NumInitialInvalidBlock:         storageEntry.Get("num-initial-invalid-block"),
		SlcTotalEraseCount:             storageEntry.Get("slc-total-erase-count"),
		SlcMaxEraseCount:               storageEntry.Get("slc-max-erase-count"),
		SlcMinEraseCount:               storageEntry.Get("slc-min-erase-count"),
		SlcAvgEraseCount:               storageEntry.Get("slc-avg-erase-count"),
		TlcTotalEraseCount:             storageEntry.Get("tlc-total-erase-count"),
		TlcMaxEraseCount:               storageEntry.Get("tlc-max-erase-count"),
		TlcMinEraseCount:               storageEntry.Get("tlc-min-erase-count"),
		TlcAvgEraseCount:               storageEntry.Get("tlc-avg-erase-count"),
		WearLevelingCount:              storageEntry.Get("wear-leveling-count"),
		ProgramFailCount:               storageEntry.Get("program-fail-count"),
		EraseFailCount:                 storageEntry.Get("erase-fail-count"),
		PowerOffRetractCount:           storageEntry.Get("power-off-retract-count"),
		Temperature:                    storageEntry.Get("temperature-celsius"),
		HardwareEccRecovered:           storageEntry.Get("hardware-ecc-recovered"),
		ReallocationEventCount:         storageEntry.Get("reallocation-event-count"),
		UdmaCrcErrorsCount:             storageEntry.Get("udma-crc-error"),
		AvailableReservedSpace:         storageEntry.Get("available-reserved-space"),
		WriteSectorCount:               storageEntry.Get("host-writes"),
		ReadSectorCount:                storageEntry.Get("read-sector-count"),
		FlashWriteCount:                storageEntry.Get("nand-writes"),
	}
	return storageInfo, nil
}

func getHwSecurityModuleFromDb(name string, d *db.DB, tblName string) (HwSecurityModule, error) {
	hsmEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return HwSecurityModule{}, err
	}
	hsmInfo := HwSecurityModule{
		Name:                 hsmEntry.Get("name"),
		FirmwareVersion:      hsmEntry.Get(HSM_FLD_FIRMWARE_VER),
		SerialNo:             hsmEntry.Get(HSM_FLD_SERIAL_NO),
		SecPayloadEnforced:   hsmEntry.Get(HSM_FLD_ENFORCED),
		PayloadVersion:       hsmEntry.Get(HSM_FLD_PAYLOAD_VER),
		PayloadSignatureType: hsmEntry.Get(HSM_FLD_TYPE),
	}
	return hsmInfo, nil
}

func validICName(name *string) bool {
	if name == nil || *name == "" {
		return false
	}
	// Expect node name of form integrated-circuitX, where X is an integer
	if !strings.HasPrefix(*name, IC_NAME_PREFIX) {
		return false
	}

	sp := strings.SplitAfter(*name, IC_NAME_PREFIX)
	if len(sp) < 2 {
		return false
	}

	if _, err := strconv.Atoi(sp[1]); err != nil {
		return false
	}
	return true
}

func icDbEntry(name string, d *db.DB, tblName string) IC {
	var nodeInfo IC

	switch tblName {
	case NODE_CFG_TBL:
		nodeEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
		if err != nil {
			log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
			return nodeInfo
		}
		nodeInfo.Node_Id = nodeEntry.Get("node-id")
		nodeInfo.Name = nodeEntry.Get("name")
		nodeInfo.Parent = nodeEntry.Get("parent")
		nodeInfo.QualifiedName = nodeEntry.Get("fully-qualified-name")
	case SWITCH_EVENT:
		switchEvents, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{"PARITY_ERROR"}})
		if err != nil {
			log.V(lvl.DEBUG).Info("Cant get parity errors from switch event table ", err)
			return nodeInfo
		}
		nodeInfo.UnrecoverableParityErrors = *String2Uint(switchEvents.Get("unrecoverable_parity_errors"), 0)
		nodeInfo.CorrectedParityErrors = *String2Uint(switchEvents.Get("recovered_parity_errors"), 0)
	}
	return nodeInfo
}

func populateBlackholeCounters(d *db.DB, ygRoot *ygot.GoStruct, bh *ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_State_Blackhole) error {
	entry, err := d.GetEntry(
		&db.TableSpec{Name: "COUNTERS_BLACKHOLE"},
		db.Key{Comp: []string{"SWITCH_COUNTERS"}})
	if err == nil {
		fls := []fieldU64LeafPair{
			{"BLACKHOLE_IN_DISCARD_EVENTS", &bh.InDiscardEvents},
			{"BLACKHOLE_OUT_DISCARD_EVENTS", &bh.OutDiscardEvents},
			{"BLACKHOLE_IN_ERROR_EVENTS", &bh.InErrorEvents},
			{"BLACKHOLE_LPM_MISS_EVENTS", &bh.LpmMissEvents},
			{"BLACKHOLE_FEC_NOT_CORRECTABLE_EVENTS", &bh.FecNotCorrectableEvents},
			{"BLACKHOLE_MEMORY_ERROR_EVENTS", &bh.MemoryErrorEvents},
			{"BLACKHOLE", &bh.BlackholeEvents},
		}
		if e := readAndParseCounters(&entry, fls); e != nil {
			return e
		}

		ts, ok := entry.Field["SWITCH_STAT_TIME_STAMP_USEC"]
		if !ok || ts == "" {
			return nil
		}
		if usec, err := strconv.ParseInt(ts, 10, 64); err == nil {
			utils.UpdateYGSTimestamp(*ygRoot, bh, usec*1000)
		}
	} else if !tlerr.IsTranslibRedisClientEntryNotExist(err) {
		return err
	}

	return nil
}

func populateBadIntervalCounters(d *db.DB, icState *ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_State) error {
	entry, err := d.GetEntry(
		&db.TableSpec{Name: "COUNTERS_PORT_BAD_INTERVALS_PARAM"},
		db.Key{Comp: []string{"GLOBAL"}})
	if err != nil {
		if !tlerr.IsTranslibRedisClientEntryNotExist(err) {
			return err
		}
	}
	if e := readAndParseCounter(&entry, "INTERVAL_MSEC", &icState.BadIntervalsDuration); e != nil {
		switch e.(type) {
		case tlerr.NotFoundError:
		default:
			return e
		}
	}
	return nil
}

func populateCongestionCounters(d *db.DB, ygRoot *ygot.GoStruct, cong *ocbinds.OpenconfigPlatform_Components_Component_IntegratedCircuit_State_Congestion) error {
	entry, err := d.GetEntry(
		&db.TableSpec{Name: "COUNTERS_CONGESTION"},
		db.Key{Comp: []string{"SWITCH_COUNTERS"}})
	if err == nil {
		fls := []fieldU64LeafPair{
			{"CONGESTION", &cong.CongestionEvents},
		}
		if e := readAndParseCounters(&entry, fls); e != nil {
			return e
		}

		ts, ok := entry.Field["SWITCH_STAT_TIME_STAMP_USEC"]
		if !ok || ts == "" {
			return nil
		}
		if usec, err := strconv.ParseInt(ts, 10, 64); err == nil {
			utils.UpdateYGSTimestamp(*ygRoot, cong, usec*1000)
		}
	} else if !tlerr.IsTranslibRedisClientEntryNotExist(err) {
		return err
	}
	return nil
}

/* Filling in the config and state info for integrated circuits available in Redis DB */
func dbToYangIC(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, targetUriPath string, dbs [db.MaxDB]*db.DB, ygRoot *ygot.GoStruct) error {
	/* Integrated-circuits have the following subtrees to populate:
	 *   ...component/config
	 *   ...component/state
	 *   ...component/integrated-circuit
	 *   ...component/integrated-circuit/config
	 *   ...component/integrated-circuit/state
	 *   ...component/integrated-circuit/state/blackhole
	 *   ...component/integrated-circuit/state/congestion
	 *   ...component/integrated-circuit/pipeline-counters
	 *   ...component/integrated-circuit/memory
	 * Decide now which subtrees to fill based on the request. */
	var all, allIc, compSt, compCfg, icCfg, icSt, icPlc, icMem bool
	var icStBh, icStCg, icStBi bool
	if targetUriPath == COMP {
		all = true
	} else if strings.HasPrefix(targetUriPath, COMP_CFG) {
		compCfg = true
	} else if strings.HasPrefix(targetUriPath, COMP_ST) {
		compSt = true
	} else if strings.HasPrefix(targetUriPath, COMP_IC_CFG) {
		icCfg = true
	} else if strings.HasPrefix(targetUriPath, COMP_IC_ST) {
		icSt = true
		if strings.HasPrefix(targetUriPath, COMP_IC_ST_BH) {
			icStBh = true
		} else if strings.HasPrefix(targetUriPath, COMP_IC_ST_CG) {
			icStCg = true
		} else if strings.HasPrefix(targetUriPath, COMP_IC_ST_BI) {
			icStBi = true
		}
	} else if strings.HasPrefix(targetUriPath, COMP_IC_PLC) {
		icPlc = true
	} else if strings.HasPrefix(targetUriPath, COMP_IC_MEM) {
		icMem = true
	} else if strings.HasPrefix(targetUriPath, COMP_IC) {
		allIc = true
	}
	log.V(lvl.DEBUG).Infof("dbToYangIC: name %s targetUriPath %s", name, targetUriPath)
	if !compSt && !compCfg {
		ygot.BuildEmptyTree(comp.IntegratedCircuit)
		if all || allIc || icCfg {
			ygot.BuildEmptyTree(comp.IntegratedCircuit.Config)
		}
		if all || allIc || icSt {
			ygot.BuildEmptyTree(comp.IntegratedCircuit.State)
			if !icStCg {
				ygot.BuildEmptyTree(comp.IntegratedCircuit.State.Blackhole)
			}
			if !icStBh {
				ygot.BuildEmptyTree(comp.IntegratedCircuit.State.Congestion)
			}
		}
		if all || allIc || icPlc {
			ygot.BuildEmptyTree(comp.IntegratedCircuit.PipelineCounters)
			ygot.BuildEmptyTree(comp.IntegratedCircuit.PipelineCounters.Drop)
			ygot.BuildEmptyTree(comp.IntegratedCircuit.PipelineCounters.Drop.LookupBlock.State)
		}
		if all || allIc || icMem {
			ygot.BuildEmptyTree(comp.IntegratedCircuit.Memory)
			ygot.BuildEmptyTree(comp.IntegratedCircuit.Memory.State)
		}
	}

	var err error
	cfgDb := dbs[db.ConfigDB]
	ctrsDb := dbs[db.CountersDB]
	asicDb := dbs[db.AsicDB]

	// Ignore error if unable to retrieve parity error counts from counters db
	nodeMem := icDbEntry(name, ctrsDb, SWITCH_EVENT)
	nodeCfg := icDbEntry(name, cfgDb, NODE_CFG_TBL)
	noRoute := getAlpmCounter(ctrsDb, asicDb)

	/* Handle component config paths.  Note that we always need "name" since
	 * is the list key. */
	comp.Config.Name = &name
	if all || compCfg {
		switch targetUriPath {
		case COMP, COMP_CFG, COMP_CFG_OC_FQ_NAME:
			if nodeCfg.QualifiedName != "" {
				comp.Config.FullyQualifiedName = &nodeCfg.QualifiedName
			}
		}
	}

	/* Handle component state paths: name, type, parent, fully-qualified-name */
	if all || compSt {
		var stName, stType, stParent, stFQName bool
		switch targetUriPath {
		case COMP_ST:
			stName, stType, stParent, stFQName = true, true, true, true
		case COMP_STATE_NAME:
			stName = true
		case COMP_STATE_TYPE:
			stType = true
		case COMP_STATE_PARENT:
			stParent = true
		case COMP_STATE_OC_FQ_NAME:
			stFQName = true
		default:
			/* Unsupported path or /components/component */
		}
		if all || stName {
			comp.State.Name = &name
			if nodeCfg.Name != "" {
				comp.State.Name = &nodeCfg.Name
			}
		}
		if all || stType {
			comp.State.Type, _ = comp.State.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_INTEGRATED_CIRCUIT)
		}
		if all || stParent {
			parentChassis := CHASSIS_PREFIX
			comp.State.Parent = &parentChassis
			if nodeCfg.Parent != "" {
				comp.State.Parent = &nodeCfg.Parent
			}
		}
		if all || stFQName {
			if nodeCfg.QualifiedName != "" {
				comp.State.FullyQualifiedName = &nodeCfg.QualifiedName
			}
		}
	}

	/* Handle component integrated-circuit config paths */
	if all || allIc || icCfg {
		nodeID, err := strconv.ParseUint(nodeCfg.Node_Id, 10, 64)
		if err != nil {
			log.V(lvl.WARNING).Infof("string conversion failed for IC %s node-id \"%s\": %v", name, nodeCfg.Node_Id, err)
		} else {
			comp.IntegratedCircuit.Config.NodeId = &nodeID
		}
	}

	/* Handle component integrated-circuit state paths */
	if all || allIc || icSt {
		allState := all || allIc || (!icStBh && !icStCg && !icStBi)
		if allState || icStBh {
			if err = populateBlackholeCounters(ctrsDb, ygRoot, comp.IntegratedCircuit.State.Blackhole); err != nil {
				log.V(lvl.WARNING).Infof("Error populating BH counters, uri=%s, name=%s, err=%v", targetUriPath, name, err)
				return err
			}
		}
		if allState || icStCg {
			if err = populateCongestionCounters(ctrsDb, ygRoot, comp.IntegratedCircuit.State.Congestion); err != nil {
				log.V(lvl.WARNING).Infof("Error populating CG counters, uri=%s, name=%s, err=%v", targetUriPath, name, err)
				return err
			}
		}
		if allState || icStBi {
			if err = populateBadIntervalCounters(ctrsDb, comp.IntegratedCircuit.State); err != nil {
				log.V(lvl.WARNING).Infof("Error populating CG counters, uri=%s, name=%s, err=%v", targetUriPath, name, err)
				return err
			}
		}
		if allState {
			nodeID, err := strconv.ParseUint(nodeCfg.Node_Id, 10, 64)
			if err != nil {
				log.V(lvl.WARNING).Infof("string conversion failed for IC %s node-id \"%s\": %v", name, nodeCfg.Node_Id, err)
			} else {
				comp.IntegratedCircuit.State.NodeId = &nodeID
			}
		}
	}

	/* Handle component integrated-circuit pipeline counter paths (only the ALPM
	 * miss counter. */
	if all || allIc || icPlc {
		comp.IntegratedCircuit.PipelineCounters.Drop.LookupBlock.State.NoRoute = &noRoute
	}

	/* Handle component integrated-circuit memory paths */
	if all || allIc || icMem {
		totalParityErrors := nodeMem.CorrectedParityErrors + nodeMem.UnrecoverableParityErrors
		if targetUriPath != COMP_IC_MEM_TPE {
			comp.IntegratedCircuit.Memory.State.CorrectedParityErrors = &nodeMem.CorrectedParityErrors
		}
		if targetUriPath != COMP_IC_MEM_CPE {
			comp.IntegratedCircuit.Memory.State.TotalParityErrors = &totalParityErrors
		}
	}
	return nil
}

func StringWithDefault(val *string, dval string) *string {
	if *val != "" {
		return val
	}
	return &dval
}

/* Filling in the state info for storage available in Redis DB */
func fillSysStorageInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB) error {
	statePresent := true
	storageInfo, err := getSysStorageFromDb(name, stdb, STORAGE_INFO)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Storage info from State DB: ", err.Error())
		statePresent = false
		if pType == StatePaths {
			return err
		}
	}
	if (pType == AllPaths || pType == AllCompPaths) && !statePresent {
		return fmt.Errorf("State DB entries not found for all paths; Error: %w", err)
	}
	compState := comp.State
	ygot.BuildEmptyTree(compState)
	compTemp := compState.Temperature
	storageState := comp.Storage.State

	if targetUriPath == COMP || targetUriPath == COMP_CFG || targetUriPath == COMP_CONFIG_NAME {
		comp.Config.Name = &name
	}

	// Filling in state values: name, type and io-errors
	if ((pType == AllPaths || pType == AllCompPaths) && statePresent) || pType == StatePaths {
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_STORAGE)
		compState.Name = StringWithDefault(&storageInfo.Name, name)
		compState.PartNo = StringWithDefault(&storageInfo.PartNo, "Missing in State DB")
		compState.SerialNo = StringWithDefault(&storageInfo.SerialNo, "Missing in State DB")
		compState.Parent = StringWithDefault(&storageInfo.Parent, "Missing in State DB")
		compState.Removable = String2Bool(&storageInfo.Removable, false)
		switch storageInfo.OperStatus {
		case "ACTIVE":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		case "INACTIVE":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		case "DISABLED":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		}
		compState.Parent = StringWithDefault(&storageInfo.Parent, "Missing in State DB")
		compTemp.Instant = String2Float(storageInfo.Temperature, 0.0)
		if storageInfo.IOErrors != "" {
			ioErrors, err := strconv.ParseUint(storageInfo.IOErrors, 10, 64)
			if err == nil {
				storageState.IoErrors = &ioErrors
			}
		}
		// Note: Ideal value of WriteAmplificationFactor is 1.0,
		// which means the amount of data written to the flash memory is equal to the data written by the host
		storageState.WriteAmplificationFactor = String2Float(storageInfo.WriteAmplificationFactor, 1.00)
		storageState.RawReadErrorRate = String2Float(storageInfo.RawReadErrorRate, 0.0)
		storageState.ThroughputPerformance = String2Float(storageInfo.ThroughputPerformance, 0.0)
		storageState.ReallocatedSectorCount = String2Uint(storageInfo.ReallocatedSectorCount, 0)
		storageState.PowerOnSeconds = String2Uint(storageInfo.PowerOnSeconds, 0)
		// Note: For new disk, SSD Lift Left should be 100
		storageState.SsdLifeLeft = String2Uint(storageInfo.SsdLifeLeft, 100)
		storageState.PowerCycleCount = String2Uint32(storageInfo.PowerCycleCount, 0)
		storageState.UncorrectableSectorCountOnLine = String2Uint32(storageInfo.UncorrectableSectorCountOnLine, 0)
		storageState.NumPureSpare = String2Uint32(storageInfo.NumPureSpare, 0)
		storageState.NumInitialInvalidBlock = String2Uint32(storageInfo.NumInitialInvalidBlock, 0)
		storageState.SlcTotalEraseCount = String2Uint32(storageInfo.SlcTotalEraseCount, 0)
		storageState.SlcMaxEraseCount = String2Uint32(storageInfo.SlcMaxEraseCount, 0)
		storageState.SlcMinEraseCount = String2Uint32(storageInfo.SlcMinEraseCount, 0)
		storageState.SlcAvgEraseCount = String2Uint32(storageInfo.SlcAvgEraseCount, 0)
		storageState.TlcTotalEraseCount = String2Uint32(storageInfo.TlcTotalEraseCount, 0)
		storageState.TlcMaxEraseCount = String2Uint32(storageInfo.TlcMaxEraseCount, 0)
		storageState.TlcMinEraseCount = String2Uint32(storageInfo.TlcMinEraseCount, 0)
		storageState.TlcAvgEraseCount = String2Uint32(storageInfo.TlcAvgEraseCount, 0)
		storageState.WearLevelingCount = String2Uint32(storageInfo.WearLevelingCount, 0)
		storageState.ProgramFailCount = String2Uint32(storageInfo.ProgramFailCount, 0)
		storageState.EraseFailCount = String2Uint32(storageInfo.EraseFailCount, 0)
		storageState.PowerOffRetractCount = String2Uint32(storageInfo.PowerOffRetractCount, 0)
		storageState.HardwareEccRecovered = String2Uint32(storageInfo.HardwareEccRecovered, 0)
		storageState.ReallocationEventCount = String2Uint32(storageInfo.ReallocationEventCount, 0)
		storageState.UdmaCrcErrorsCount = String2Uint32(storageInfo.UdmaCrcErrorsCount, 0)
		storageState.AvailableReservedSpace = String2Uint32(storageInfo.AvailableReservedSpace, 0)
		storageState.WriteSectorCount = String2Uint32(storageInfo.WriteSectorCount, 0)
		storageState.ReadSectorCount = String2Uint32(storageInfo.ReadSectorCount, 0)
		storageState.FlashWriteCount = String2Uint32(storageInfo.FlashWriteCount, 0)

	}

	if pType != SingularPath {
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		compState.Name = StringWithDefault(&storageInfo.Name, name)
	case COMP_STATE_PART_NO:
		compState.PartNo = StringWithDefault(&storageInfo.PartNo, "Missing in State DB")
	case COMP_STATE_SERIAL_NO:
		compState.SerialNo = StringWithDefault(&storageInfo.SerialNo, "Missing in State DB")
	case COMP_STATE_TYPE:
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_STORAGE)
	case COMP_STATE_PARENT:
		compState.Parent = StringWithDefault(&storageInfo.Parent, "Missing in State DB")
	case COMP_STATE_OPER_STATUS:
		switch storageInfo.OperStatus {
		case "ACTIVE":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		case "INACTIVE":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		case "DISABLED":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		}
	case COMP_STATE_REMOVABLE:
		compState.Removable = String2Bool(&storageInfo.Removable, false)
	case COMP_STATE_TEMP_CTR, COMP_STATE_TEMP:
		compTemp.Instant = String2Float(storageInfo.Temperature, 0.0)
	case STORAGE_IO_ERRORS:
		if storageInfo.IOErrors == "" {
			return errors.New("storage io-errors field not present in State DB")
		}
		ioErrors, err := strconv.ParseUint(storageInfo.IOErrors, 10, 64)
		if err != nil {
			return fmt.Errorf("string conversion failed for io-errors: %s; Error: %w", storageInfo.IOErrors, err)
		}
		storageState.IoErrors = &ioErrors
	case G_STORAGE_WAF:
		storageState.WriteAmplificationFactor = String2Float(storageInfo.WriteAmplificationFactor, 1.00)
	case G_STORAGE_RRER:
		storageState.RawReadErrorRate = String2Float(storageInfo.RawReadErrorRate, 0.0)
	case G_STORAGE_TP:
		storageState.ThroughputPerformance = String2Float(storageInfo.ThroughputPerformance, 0.0)
	case G_STORAGE_RSC:
		storageState.ReallocatedSectorCount = String2Uint(storageInfo.ReallocatedSectorCount, 0)
	case G_STORAGE_POS:
		storageState.PowerOnSeconds = String2Uint(storageInfo.PowerOnSeconds, 0)
	case G_STORAGE_SLL:
		storageState.SsdLifeLeft = String2Uint(storageInfo.SsdLifeLeft, 100)
	case G_STORAGE_PCC:
		storageState.PowerCycleCount = String2Uint32(storageInfo.PowerCycleCount, 0)
	case G_STORAGE_USCOL:
		storageState.UncorrectableSectorCountOnLine = String2Uint32(storageInfo.UncorrectableSectorCountOnLine, 0)
	case G_STORAGE_NPS:
		storageState.NumPureSpare = String2Uint32(storageInfo.NumPureSpare, 0)
	case G_STORAGE_NIIB:
		storageState.NumInitialInvalidBlock = String2Uint32(storageInfo.NumInitialInvalidBlock, 0)
	case G_STORAGE_STEC:
		storageState.SlcTotalEraseCount = String2Uint32(storageInfo.SlcTotalEraseCount, 0)
	case G_STORAGE_SMAXEC:
		storageState.SlcMaxEraseCount = String2Uint32(storageInfo.SlcMaxEraseCount, 0)
	case G_STORAGE_SMINEC:
		storageState.SlcMinEraseCount = String2Uint32(storageInfo.SlcMinEraseCount, 0)
	case G_STORAGE_SAEC:
		storageState.SlcAvgEraseCount = String2Uint32(storageInfo.SlcAvgEraseCount, 0)
	case G_STORAGE_TTEC:
		storageState.TlcTotalEraseCount = String2Uint32(storageInfo.TlcTotalEraseCount, 0)
	case G_STORAGE_TMAXEC:
		storageState.TlcMaxEraseCount = String2Uint32(storageInfo.TlcMaxEraseCount, 0)
	case G_STORAGE_TMINEC:
		storageState.TlcMinEraseCount = String2Uint32(storageInfo.TlcMinEraseCount, 0)
	case G_STORAGE_TAEC:
		storageState.TlcAvgEraseCount = String2Uint32(storageInfo.TlcAvgEraseCount, 0)
	case G_STORAGE_WLC:
		storageState.WearLevelingCount = String2Uint32(storageInfo.WearLevelingCount, 0)
	case G_STORAGE_PFC:
		storageState.ProgramFailCount = String2Uint32(storageInfo.ProgramFailCount, 0)
	case G_STORAGE_EFC:
		storageState.EraseFailCount = String2Uint32(storageInfo.EraseFailCount, 0)
	case G_STORAGE_PORC:
		storageState.PowerOffRetractCount = String2Uint32(storageInfo.PowerOffRetractCount, 0)
	case G_STORAGE_HER:
		storageState.HardwareEccRecovered = String2Uint32(storageInfo.HardwareEccRecovered, 0)
	case G_STORAGE_REC:
		storageState.ReallocationEventCount = String2Uint32(storageInfo.ReallocationEventCount, 0)
	case G_STORAGE_UCEC:
		storageState.UdmaCrcErrorsCount = String2Uint32(storageInfo.UdmaCrcErrorsCount, 0)
	case G_STORAGE_ARS:
		storageState.AvailableReservedSpace = String2Uint32(storageInfo.AvailableReservedSpace, 0)
	case G_STORAGE_WSC:
		storageState.WriteSectorCount = String2Uint32(storageInfo.WriteSectorCount, 0)
	case G_STORAGE_READSC:
		storageState.ReadSectorCount = String2Uint32(storageInfo.ReadSectorCount, 0)
	case G_STORAGE_FWC:
		storageState.FlashWriteCount = String2Uint32(storageInfo.FlashWriteCount, 0)
	}
	return nil
}

func getDiskCarrierInfoFromDb(name string, d *db.DB) (DiskCarrier, error) {
	diskCarrierEntry, err := d.GetEntry(&db.TableSpec{Name: DISK_CARRIER_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return DiskCarrier{}, err
	}
	return DiskCarrier{
		Model_Name:    diskCarrierEntry.Get("model"),
		Name:          diskCarrierEntry.Get("name"),
		Parent:        diskCarrierEntry.Get("parent"),
		Part_Number:   diskCarrierEntry.Get("part-no"),
		Serial_Number: diskCarrierEntry.Get("serial"),
	}, nil
}

/* Filling in the state info for disk carrier components available in Redis DB */
func fillDiskCarrierInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB) error {
	diskCarrierInfo, err := getDiskCarrierInfoFromDb(name, stdb)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting disk carrier info from State DB: ", err.Error())
		return err
	}
	carrierEepromState := comp.State
	defaultVal := ""
	defaultParentVal := CHASSIS_PREFIX

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths {
		// Filling in state values
		carrierEepromState.Name = StringWithDefault(&diskCarrierInfo.Name, name)
		carrierEepromState.Description = StringWithDefault(&diskCarrierInfo.Model_Name, defaultVal)
		carrierEepromState.ModelName = StringWithDefault(&diskCarrierInfo.Model_Name, defaultVal)
		carrierEepromState.Parent = StringWithDefault(&diskCarrierInfo.Parent, defaultParentVal)
		carrierEepromState.PartNo = StringWithDefault(&diskCarrierInfo.Part_Number, diskCarrierInfo.Model_Name)
		carrierEepromState.SerialNo = StringWithDefault(&diskCarrierInfo.Serial_Number, defaultVal)
		carrierEepromState.Type, _ = carrierEepromState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_STORAGE)
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		carrierEepromState.Name = StringWithDefault(&diskCarrierInfo.Name, name)
	case COMP_STATE_DESCR:
		carrierEepromState.Description = StringWithDefault(&diskCarrierInfo.Model_Name, defaultVal)
	case COMP_STATE_MODEL_NAME:
		carrierEepromState.ModelName = StringWithDefault(&diskCarrierInfo.Model_Name, defaultVal)
	case COMP_STATE_PARENT:
		carrierEepromState.Parent = StringWithDefault(&diskCarrierInfo.Parent, defaultParentVal)
	case COMP_STATE_TYPE:
		carrierEepromState.Type, _ = carrierEepromState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_STORAGE)
	case COMP_STATE_PART_NO:
		carrierEepromState.PartNo = StringWithDefault(&diskCarrierInfo.Part_Number, diskCarrierInfo.Model_Name)
	case COMP_STATE_SERIAL_NO:
		carrierEepromState.SerialNo = StringWithDefault(&diskCarrierInfo.Serial_Number, defaultVal)
	}
	return nil
}

var string2PayloadSignatureType = map[string]ocbinds.E_OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType{
	"IMAGE_DEV":                ocbinds.OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType_DEV,
	"IMAGE_BREAKOUT":           ocbinds.OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType_DEV,
	"IMAGE_TEST":               ocbinds.OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType_DEV,
	"IMAGE_PROD":               ocbinds.OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType_PROD,
	"IMAGE_UNSIGNED_INTEGRITY": ocbinds.OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType_UNSIGNED,
}

func String2PayloadSignatureType(t *string) ocbinds.E_OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType {
	if v, ok := string2PayloadSignatureType[*t]; ok {
		return v
	}
	return ocbinds.OpenconfigPlatform_Components_Component_HardwareSecurityModule_State_PayloadSignatureType_INVALID
}

func String2Bool(b *string, dflt bool) *bool {
	v, err := strconv.ParseBool(*b)
	if err != nil {
		log.V(lvl.ERROR).Info(err)
		return &dflt
	}
	return &v
}

/* Filling in the state info for HwSecurityModule available in Redis DB */
func fillHwSecurityModuleInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB) error {
	statePresent := true
	hsmInfo, err := getHwSecurityModuleFromDb(name, stdb, HSM_TBL)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting HwSecurityModule info from State DB: ", err.Error())
		statePresent = false
		if pType == StatePaths {
			return err
		}
	}
	if (pType == AllPaths || pType == AllCompPaths) && !statePresent {
		return fmt.Errorf("State DB entries not found for all paths; Error: %w", err)
	}
	compState := comp.State
	hsmState := comp.HardwareSecurityModule.State
	defaultParentVal := CHASSIS_PREFIX

	// Filling in state _all_ values.
	if ((pType == AllPaths || pType == AllCompPaths) && statePresent) || pType == StatePaths {
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_HARDWARE_SECURITY_MODULE)
		compState.Name = StringWithDefault(&hsmInfo.Name, name)
		compState.FirmwareVersion = StringWithDefault(&hsmInfo.FirmwareVersion, "Missing in State DB")
		compState.SerialNo = StringWithDefault(&hsmInfo.SerialNo, "Missing in State DB")
		compState.Parent = &defaultParentVal
		hsmState.SecurePayloadEnforced = String2Bool(&hsmInfo.SecPayloadEnforced, false)
		hsmState.PayloadSignatureType = String2PayloadSignatureType(&hsmInfo.PayloadSignatureType)
		hsmState.PayloadVersion = StringWithDefault(&hsmInfo.PayloadVersion, "Missing in State DB")
	}

	if pType != SingularPath {
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		compState.Name = StringWithDefault(&hsmInfo.Name, name)
	case COMP_STATE_FIRM_VER:
		compState.FirmwareVersion = StringWithDefault(&hsmInfo.FirmwareVersion, "Missing in State DB")
	case COMP_STATE_SERIAL_NO:
		compState.SerialNo = StringWithDefault(&hsmInfo.SerialNo, "Missing in State DB")
	case COMP_STATE_TYPE:
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_HARDWARE_SECURITY_MODULE)
	case COMP_STATE_PARENT:
		compState.Parent = &defaultParentVal
	case HSM_STATE_GO_ENFORCED:
		hsmState.SecurePayloadEnforced = String2Bool(&hsmInfo.SecPayloadEnforced, false)
	case HSM_STATE_GO_TYPE:
		hsmState.PayloadSignatureType = String2PayloadSignatureType(&hsmInfo.PayloadSignatureType)
	case HSM_STATE_GO_VER:
		hsmState.PayloadVersion = StringWithDefault(&hsmInfo.PayloadVersion, "Missing in State DB")
	}
	return nil
}

func getSysDpbFromDb(name string, d *db.DB, tblName string) (Port, error) {
	portEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return Port{}, err
	}

	portInfo := Port{
		Name:            portEntry.Get("name"),
		Parent:          portEntry.Get("parent"),
		PortID:          portEntry.Get("port-id"),
		BreakoutMode:    portEntry.Get("brkout_mode"),
		FirmwareVersion: portEntry.Get("serdes-firmware-version"),
	}
	return portInfo, nil
}

/* Filling in the state info for ports available in Redis DB */
func fillDpbData(comp *ocbinds.OpenconfigPlatform_Components_Component, name, subKey string, pType PathType, targetUriPath string, stdb *db.DB, cfgdb *db.DB) error {
	ifName := platform.InterfaceNameFromPort(name)
	portInfo, err := getSysDpbFromDb(ifName, stdb, PORT_BREAKOUT)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Port info from State DB: ", err.Error())
		if pType == StatePaths {
			return err
		}
	}
	portCfg, err := getSysDpbFromDb(ifName, cfgdb, BREAKOUT_TBL)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Port info from Config DB: ", err.Error())
		if pType == ConfigPaths {
			return err
		}
	}
	compState := comp.State
	portCompState := comp.Port.State
	portCompCfg := comp.Port.Config
	defaultParentVal := IC_NAME_PREFIX + "0"

	if pType != SingularPath {
		if pType == AllPaths || pType == AllCompPaths {
			// Port Config State ID
			if portCfg.PortID != "" {
				tmp, _ := strconv.ParseUint(portCfg.PortID, 10, 32)
				portID := uint32(tmp)
				portCompCfg.PortId = &portID
			}
			// Port State Port ID
			if portInfo.PortID != "" {
				tmp, _ := strconv.ParseUint(portInfo.PortID, 10, 32)
				portID := uint32(tmp)
				portCompState.PortId = &portID
			}
			// Subcomponents
			if targetUriPath == COMP || strings.HasPrefix(targetUriPath, COMP_SUB) {
				ygot.BuildEmptyTree(comp.Subcomponents)
				fillAllSubComponents(comp.Subcomponents, cfgdb, name, subKey, targetUriPath)
			}
		}
		if pType == AllPaths && portCfg.BreakoutMode != "" {
			// Port breakout
			if entry, err := cfgdb.GetEntry(&db.TableSpec{Name: BREAKOUT_TBL}, db.Key{Comp: []string{ifName}}); err == nil {
				if mode, err := sanitizeMode(entry.Get("brkout_mode")); err == nil {
					if chGrps, err := populateChannelGroups(ifName, mode); err == nil {
						// Populate for all indices.
						for idx, grp := range chGrps {
							if err := fillChGroup(idx, grp, comp.Port.BreakoutMode.Groups); err != nil {
								log.V(lvl.ERROR).Info(err)
							}
						}
					} else {
						log.V(lvl.DEBUG).Info("Cannot get channel groups: ", err)
					}
				} else {
					log.V(lvl.DEBUG).Info("Cannot get mode: ", err)
				}
			} else {
				log.V(lvl.DEBUG).Info("Cannot get entry: ", name, "; Error: ", err)
			}
		}
		// Filling in state values: name, parent, and type
		// State Type
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_PORT)
		// State Name
		compState.Name = &name
		if portInfo.Name != "" {
			compState.Name = &portInfo.Name
		}
		// State Parent
		compState.Parent = &defaultParentVal
		if portInfo.Parent != "" {
			compState.Parent = &portInfo.Parent
		}
		// State Firmware Version
		if portInfo.FirmwareVersion != "" {
			compState.FirmwareVersion = &portInfo.FirmwareVersion
		}
		// Config Name
		comp.Config.Name = &name
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		compState.Name = &name
		if portInfo.Name != "" {
			compState.Name = &portInfo.Name
		}
	case COMP_STATE_PARENT:
		compState.Parent = &defaultParentVal
		if portInfo.Parent != "" {
			compState.Parent = &portInfo.Parent
		}
	case COMP_STATE_FIRM_VER:
		if portInfo.FirmwareVersion != "" {
			compState.FirmwareVersion = &portInfo.FirmwareVersion
		}
	case COMP_STATE_TYPE:
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_PORT)
	case COMP_CONFIG_NAME:
		comp.Config.Name = &name
	case PORT_CONFIG_OC_PORT_ID:
		if portCfg.PortID == "" {
			return errors.New("port-id field not present in Config DB")
		}
		tmp, err := strconv.ParseUint(portCfg.PortID, 10, 32)
		if err != nil {
			return fmt.Errorf("string conversion failed for port-id: %s; Error: %w", portCfg.PortID, err)
		}
		portID := uint32(tmp)
		portCompCfg.PortId = &portID
	case PORT_STATE_OC_PORT_ID:
		if portInfo.PortID == "" {
			return errors.New("port-id field not present in State DB")
		}
		tmp, err := strconv.ParseUint(portInfo.PortID, 10, 32)
		if err != nil {
			return fmt.Errorf("string conversion failed for port-id: %s; Error: %w", portInfo.PortID, err)
		}
		portID := uint32(tmp)
		portCompState.PortId = &portID
	}

	if strings.HasPrefix(targetUriPath, COMP_SUB) {
		ygot.BuildEmptyTree(comp.Subcomponents)
		fillAllSubComponents(comp.Subcomponents, cfgdb, name, subKey, targetUriPath)
	}
	return nil
}

func getSysFirmwareFromDb(name string, d *db.DB, tblName string) (Firmware, error) {
	firmwareEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return Firmware{}, err
	}

	firmwareInfo := Firmware{AlarmStatus: false}
	if firmwareEntry.Get("alarm_status") == "true" {
		firmwareInfo.AlarmStatus = true
	}
	firmwareInfo.Description = firmwareEntry.Get("description")
	firmwareInfo.Name = firmwareEntry.Get("name")

	firmwareInfo.FirmwareVersion = firmwareEntry.Get("firmware-version")
	firmwareInfo.HardwareVersion = firmwareEntry.Get("hardware-version")
	firmwareInfo.MfgDate = firmwareEntry.Get("manufacture_date")
	firmwareInfo.OperStatus = firmwareEntry.Get("oper-status")
	firmwareInfo.PartNo = firmwareEntry.Get("part-no")
	firmwareInfo.SerialNo = firmwareEntry.Get("serial")
	firmwareInfo.QualifiedName = firmwareEntry.Get("fully-qualified-name")
	firmwareInfo.ModelName = firmwareEntry.Get("product_name")
	firmwareInfo.BaseMac = firmwareEntry.Get("base_mac_addr")
	firmwareInfo.MacPoolSize = firmwareEntry.Get("mac_addr_num")
	firmwareInfo.CpuType = firmwareEntry.Get("cpu-type")

	return firmwareInfo, nil
}

func getSysFpgaFromDb(name string, d *db.DB, tblName string) (Fpga, error) {
	fpgaEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return Fpga{}, err
	}
	fpgaInfo := Fpga{
		Name:            fpgaEntry.Get("name"),
		Description:     fpgaEntry.Get("description"),
		MfgName:         fpgaEntry.Get("mfg_name"),
		FirmwareVersion: fpgaEntry.Get("firmware-version"),
		ResetCount:      fpgaEntry.Get("reset_count"),
	}
	for i := 0; i < 8; i++ {
		reset_cause_key := "reset_cause_d" + strconv.Itoa(i)
		reset_cause := fpgaEntry.Get(reset_cause_key)
		if reset_cause == "" {
			log.V(lvl.DEBUG).Infof("Only last %d fpga reset causes are recorded.", i)
			break
		}
		fpgaInfo.ResetCauses = append(fpgaInfo.ResetCauses, reset_cause)
	}
	return fpgaInfo, nil
}

/* Filling in the config and state info for firmware chassis available in Redis DB */
func fillSysFirmwareInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB, cfgdb *db.DB) error {
	statePresent, cfgPresent := true, true
	firmwareInfo, err := getSysFirmwareFromDb(name, stdb, CHASSIS_TBL)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Chassis info from State DB: ", err.Error())
		statePresent = false
		if pType == StatePaths {
			return err
		}
	}
	firmwareCfg, err := getSysFirmwareFromDb(name, cfgdb, CHASSIS_CFG)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Chassis info from Config DB: ", err.Error())
		cfgPresent = false
		if pType == ConfigPaths {
			return err
		}
	}
	if (pType == AllPaths || pType == AllCompPaths) && !statePresent && !cfgPresent {
		return fmt.Errorf("Config and State DB entries not found for all paths; Error: %w", err)
	}

	compState := comp.State
	compCfg := comp.Config
	firmwareCh := comp.Chassis.State
	defaultVal := ""

	if ((pType == AllPaths || pType == AllCompPaths) && statePresent) || pType == StatePaths {
		//Chassis Alarms State Status
		comp.Chassis.Alarms.State.Status = &firmwareInfo.AlarmStatus
		// Filling in state values
		// State Type
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_CHASSIS)
		// State Name
		compState.Name = &name
		if firmwareInfo.Name != "" {
			compState.Name = &firmwareInfo.Name
		}
		// State Description
		if firmwareInfo.Description != "" {
			compState.Description = &firmwareInfo.Description
		}
		// State Firmware Version
		compState.FirmwareVersion = &defaultVal
		if firmwareInfo.FirmwareVersion != "" {
			compState.FirmwareVersion = &firmwareInfo.FirmwareVersion
		}
		// State Hardware Version
		if firmwareInfo.HardwareVersion != "" {
			compState.HardwareVersion = &firmwareInfo.HardwareVersion
		}
		// State Mfg Date
		if len(firmwareInfo.MfgDate) > 10 {
			mfg_date := firmwareInfo.MfgDate[6:10] + "-" +
				firmwareInfo.MfgDate[0:2] + "-" + firmwareInfo.MfgDate[3:5]
			compState.MfgDate = &mfg_date
		}
		// State Oper Status
		if firmwareInfo.OperStatus != "" {
			switch firmwareInfo.OperStatus {
			case "active":
				compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
			case "inactive":
				compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
			case "disabled":
				compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
			}
		}
		// State Part No
		if firmwareInfo.PartNo != "" {
			compState.PartNo = &firmwareInfo.PartNo
		}
		// State Serial No
		if firmwareInfo.SerialNo != "" {
			compState.SerialNo = &firmwareInfo.SerialNo
		}
		// State Fully Qualified Name
		if firmwareInfo.QualifiedName != "" {
			compState.FullyQualifiedName = &firmwareInfo.QualifiedName
		}
		// State Model Name
		if firmwareInfo.ModelName != "" {
			compState.ModelName = &firmwareInfo.ModelName
		}
		if pType == StatePaths {
			return nil
		}

		// Chassis State Platform
		firmwareCh.Platform = ocbinds.OpenconfigPinsPlatformChassis_PLATFORM_TYPE_GENERIC
		oc_platform, err := getDbToYangPlatformType(firmwareInfo.ModelName)
		if err == nil {
			firmwareCh.Platform = oc_platform
		}
		// Chassis State Base Mac Address
		if firmwareInfo.BaseMac != "" {
			firmwareCh.BaseMacAddress = &firmwareInfo.BaseMac
		}
		// Chassis State Mac Address Pool Size
		if firmwareInfo.MacPoolSize != "" {
			poolSize, err := strconv.ParseUint(firmwareInfo.MacPoolSize, 10, 64)
			if err != nil {
				return fmt.Errorf("string conversion failed for mac pool size: %s; Error: %w", firmwareInfo.MacPoolSize, err)
			}
			poolSizeVal := uint32(poolSize)
			firmwareCh.MacAddressPoolSize = &poolSizeVal
		}
		// Chassis Cpu Type
		firmwareCh.CpuType = ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_UKNOWN
		if firmwareInfo.CpuType != "" {
			if cpu_type, ok := cpuTypeMap[firmwareInfo.CpuType]; ok {
				firmwareCh.CpuType = cpu_type
			}
		}
	}

	if ((pType == AllPaths || pType == AllCompPaths) && cfgPresent) || pType == ConfigPaths {
		// Filling in config values
		// Config Name
		compCfg.Name = &name
		if firmwareCfg.Name != "" {
			compCfg.Name = &firmwareCfg.Name
		}
		// Config Fully Qualified Name
		if firmwareCfg.QualifiedName != "" {
			compCfg.FullyQualifiedName = &firmwareCfg.QualifiedName
		}
	}

	if pType != SingularPath {
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_DESCR:
		if firmwareInfo.Description == "" {
			return errors.New("description field not present in State DB")
		}
		compState.Description = &firmwareInfo.Description
	case COMP_STATE_NAME:
		compState.Name = &name
		if firmwareInfo.Name != "" {
			compState.Name = &firmwareInfo.Name
		}
	case COMP_STATE_TYPE:
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_CHASSIS)
	case COMP_STATE_FIRM_VER:
		if firmwareInfo.FirmwareVersion == "" {
			return errors.New("firmware_version field not present in State DB")
		}
		compState.FirmwareVersion = &firmwareInfo.FirmwareVersion
	case COMP_STATE_HW_VER:
		if firmwareInfo.HardwareVersion == "" {
			return errors.New("hardware_version field not present in State DB")
		}
		compState.HardwareVersion = &firmwareInfo.HardwareVersion
	case COMP_STATE_MFG_DATE:
		if len(firmwareInfo.MfgDate) < 11 {
			return errors.New("mfg_date field not present in State DB")
		}
		mfg_date := firmwareInfo.MfgDate[6:10] + "-" +
			firmwareInfo.MfgDate[0:2] + "-" + firmwareInfo.MfgDate[3:5]
		compState.MfgDate = &mfg_date
	case COMP_STATE_OPER_STATUS:
		if firmwareInfo.OperStatus == "" {
			return errors.New("oper_status field not present in State DB")
		}
		switch firmwareInfo.OperStatus {
		case "active":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		case "inactive":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		case "disabled":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		default:
			return fmt.Errorf("oper_status invalid field value in State DB: %s", firmwareInfo.OperStatus)
		}
	case COMP_STATE_PART_NO:
		if firmwareInfo.PartNo == "" {
			return errors.New("part_no field not present in State DB")
		}
		compState.PartNo = &firmwareInfo.PartNo
	case COMP_STATE_SERIAL_NO:
		if firmwareInfo.SerialNo == "" {
			return errors.New("serial_no field not present in State DB")
		}
		compState.SerialNo = &firmwareInfo.SerialNo
	case COMP_STATE_OC_FQ_NAME:
		if firmwareInfo.QualifiedName == "" {
			return errors.New("fully_qualified_name field not present in State DB")
		}
		compState.FullyQualifiedName = &firmwareInfo.QualifiedName
	case FIRMWARE_CHASSIS_OC_PLATFORM:
		oc_platform, err := getDbToYangPlatformType(firmwareInfo.ModelName)
		if err != nil {
			return err
		}
		firmwareCh.Platform = oc_platform
	case FIRMWARE_CHASSIS_OC_BASE_MAC:
		if firmwareInfo.BaseMac == "" {
			return errors.New("base-mac-address field not present in State DB")
		}
		firmwareCh.BaseMacAddress = &firmwareInfo.BaseMac
	case FIRMWARE_CHASSIS_OC_NUM_MAC:
		if firmwareInfo.MacPoolSize == "" {
			return errors.New("mac-address-pool-size field not present in State DB")
		}
		poolSize, err := strconv.ParseUint(firmwareInfo.MacPoolSize, 10, 64)
		if err != nil {
			return fmt.Errorf("string conversion failed for mac pool size: %s; Error: %w", firmwareInfo.MacPoolSize, err)
		}
		poolSizeVal := uint32(poolSize)
		firmwareCh.MacAddressPoolSize = &poolSizeVal
	case COMP_STATE_MODEL_NAME:
		if firmwareInfo.ModelName == "" {
			return errors.New("component state model-name field not present in State DB")
		}
		compState.ModelName = &firmwareInfo.ModelName
	case COMP_CONFIG_NAME:
		compCfg.Name = &name
		if firmwareCfg.Name != "" {
			compCfg.Name = &firmwareCfg.Name
		}
	case COMP_CFG_OC_FQ_NAME:
		if firmwareCfg.QualifiedName == "" {
			return errors.New("fully_qualified_name field not present in Config DB")
		}
		compCfg.FullyQualifiedName = &firmwareCfg.QualifiedName
	case FIRMWARE_CHASSIS_ALARMS_STATE_STATUS:
		comp.Chassis.Alarms.State.Status = &firmwareInfo.AlarmStatus
	case FIRMWARE_CHASSIS_OC_CPU_TYPE:
		firmwareCh.CpuType = ocbinds.OpenconfigPinsPlatformChassis_CPU_TYPE_CPU_TYPE_UKNOWN
		if firmwareInfo.CpuType != "" {
			if cpu_type, ok := cpuTypeMap[firmwareInfo.CpuType]; ok {
				firmwareCh.CpuType = cpu_type
			}
		}
	}
	return nil
}

/* Filling in the state info for fpga available in Redis DB */
func fillSysFpgaInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, uri string, stdb *db.DB) error {
	log.V(lvl.DEBUG).Info("fillSysFpgaInfo: populating Fpga info in state paths")

	statePresent, fpgaPresent := true, true
	fpgaParent := CHASSIS_PREFIX
	fpgaInfo, err := getSysFpgaFromDb(name, stdb, FPGA_TBL)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting Fpga info from State DB: ", err.Error())
		statePresent = false
		fpgaPresent = false
		if pType == StatePaths || pType == AllPaths {
			return err
		}
	}

	if (pType == AllPaths || pType == AllCompPaths) && !statePresent && !fpgaPresent {
		return fmt.Errorf("State DB entries not found for all paths; Error: %w", err)
	}

	compState := comp.State
	defaultVal := ""
	if ((pType == AllPaths || pType == AllCompPaths) && statePresent) || pType == StatePaths {
		log.V(lvl.DEBUG).Info("State paths ... ")
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_FPGA)
		// State Name
		compState.Name = &name
		if fpgaInfo.Name != "" {
			compState.Name = &fpgaInfo.Name
		}
		// State Description
		if fpgaInfo.Description != "" {
			compState.Description = &fpgaInfo.Description
		}
		// State Firmware Version
		compState.FirmwareVersion = &defaultVal
		if fpgaInfo.FirmwareVersion != "" {
			compState.FirmwareVersion = &fpgaInfo.FirmwareVersion
		}
		// State Mfg Name
		if fpgaInfo.MfgName != "" {
			compState.MfgName = &fpgaInfo.MfgName
		}
		// State Parent
		compState.Parent = &fpgaParent
		if pType == StatePaths {
			return nil
		}
	}

	if ((pType == AllPaths || pType == AllCompPaths) && fpgaPresent) || pType == AllCompPaths {
		compFpga := comp.Fpga

		// State Reset Count
		if fpgaInfo.ResetCount != "" {
			reset_count, err := strconv.ParseInt(fpgaInfo.ResetCount, 10, 0)
			if err != nil {
				log.V(lvl.DEBUG).Infof("Failed to convert FPGA reset-count to int: %s", fpgaInfo.ResetCount)
				reset_count = 0
			}
			reset_count8 := uint8(reset_count)
			compFpga.State.ResetCount = &reset_count8
		}

		resetCauseIndex := NewPathInfo(uri).Var("index")
		log.V(lvl.DEBUG).Infof("Reset Cause index %s", resetCauseIndex)
		if resetCauseIndex != "" {
			index, err := strconv.ParseInt(resetCauseIndex, 10, 0)
			if err != nil || len(fpgaInfo.ResetCauses) <= int(index) {
				log.V(lvl.DEBUG).Infof("reset cause index not present in State DB for index %v", resetCauseIndex)
				return errors.New("reset cause index not present in State DB for index " + resetCauseIndex)
			}
			var resetCauseObj *ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause
			if fpgaInfo.ResetCauses != nil {
				var ok bool
				ocIndex := uint8(index)
				if resetCauseObj, ok = compFpga.ResetCauses.ResetCause[ocIndex]; !ok {
					var err error
					log.V(lvl.DEBUG).Info("Allocating new Reset cause object")
					resetCauseObj, err = compFpga.ResetCauses.NewResetCause(ocIndex)
					if err != nil {
						log.V(lvl.DEBUG).Infof("unable to allocate new reset cause %d", ocIndex)
						return err
					}
				}
				ygot.BuildEmptyTree(resetCauseObj)
				if resetCauseObj.State == nil {
					ygot.BuildEmptyTree(resetCauseObj.State)
				}
				resetCauseObj.State.Index = &ocIndex
				if resetCauseObj.State.Cause, ok = resetCauseMap[fpgaInfo.ResetCauses[index]]; !ok {
					resetCauseObj.State.Cause = ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_UNSET
				}
			}
		} else {
			// State Fpga Reset Causes
			if fpgaInfo.ResetCauses != nil {
				for index, resetCause := range fpgaInfo.ResetCauses {
					ocIndex := uint8(index)
					resetCauseObj, err := compFpga.ResetCauses.NewResetCause(ocIndex)
					if err != nil {
						log.V(lvl.DEBUG).Infof("unable to allocate new reset cause %d", ocIndex)
						return err
					}
					log.V(lvl.DEBUG).Info("Allocating new Reset cause object")
					ygot.BuildEmptyTree(resetCauseObj)
					if resetCauseObj.State == nil {
						ygot.BuildEmptyTree(resetCauseObj.State)
					}
					resetCauseObj.State.Index = &ocIndex
					ok := false
					if resetCauseObj.State.Cause, ok = resetCauseMap[resetCause]; !ok {
						resetCauseObj.State.Cause = ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_UNSET
					}
				}
			}
		}
	}

	if pType != SingularPath {
		log.V(lvl.DEBUG).Info("Not singular path. Returning .. ")
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_DESCR:
		if fpgaInfo.Description == "" {
			return errors.New("description field not present in State DB")
		}
		compState.Description = &fpgaInfo.Description
	case COMP_STATE_NAME:
		compState.Name = &name
		if fpgaInfo.Name != "" {
			compState.Name = &fpgaInfo.Name
		}
	case COMP_STATE_TYPE:
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_FPGA)
	case COMP_STATE_FIRM_VER:
		if fpgaInfo.FirmwareVersion == "" {
			return errors.New("firmware_version field not present in State DB")
		}
		compState.FirmwareVersion = &fpgaInfo.FirmwareVersion
	case COMP_STATE_MFG_NAME:
		if len(fpgaInfo.MfgName) < 1 {
			return errors.New("mfg_name field not present in State DB")
		}
		compState.MfgName = &fpgaInfo.MfgName
	case COMP_STATE_PARENT:
		compState.Parent = &fpgaParent
	case FPGA_GO_COMP_RESET_COUNT:
		if len(fpgaInfo.ResetCount) < 1 {
			return errors.New("reset_count field not present in State DB")
		}
		reset_count, err := strconv.ParseInt(fpgaInfo.ResetCount, 10, 0)
		if err != nil {
			return errors.New("Failed to convert FPGA reset-count to int")
		}
		reset_count8 := uint8(reset_count)
		comp.Fpga.State.ResetCount = &reset_count8
	case FPGA_GO_RESET_CAUSE_INDEX:
		compFpga := comp.Fpga
		index, err := strconv.ParseInt(NewPathInfo(uri).Var("index"), 10, 0)
		if err != nil || len(fpgaInfo.ResetCauses) < int(index) {
			return errors.New("reset cause index not present in State DB")
		}
		ocIndex := uint8(index)
		var resetCauseObj *ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause
		var ok bool
		if resetCauseObj, ok = compFpga.ResetCauses.ResetCause[ocIndex]; !ok {
			var err error
			log.V(lvl.DEBUG).Info("Allocating new Reset cause object")
			resetCauseObj, err = compFpga.ResetCauses.NewResetCause(ocIndex)
			if err != nil {
				log.V(lvl.DEBUG).Infof("unable to allocate new reset cause %d", ocIndex)
				return err
			}
		}
		ygot.BuildEmptyTree(resetCauseObj)
		if resetCauseObj.State == nil {
			ygot.BuildEmptyTree(resetCauseObj.State)
		}
		resetCauseObj.Index = &ocIndex
		resetCauseObj.State.Index = &ocIndex
	case FPGA_GO_RESET_CAUSE_CAUSE:
		compFpga := comp.Fpga
		indexStr := NewPathInfo(uri).Var("index")
		index, err := strconv.ParseInt(indexStr, 10, 0)
		if err != nil || len(fpgaInfo.ResetCauses) <= int(index) {
			log.V(lvl.DEBUG).Infof("reset cause index not present in State DB for index %v", indexStr)
			return errors.New("reset cause index not present in State DB for index " + indexStr)
		}
		ocIndex := uint8(index)
		var resetCauseObj *ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause
		var ok bool
		if resetCauseObj, ok = compFpga.ResetCauses.ResetCause[ocIndex]; !ok {
			var err error
			log.V(lvl.DEBUG).Info("Allocating new Reset cause object")
			resetCauseObj, err = compFpga.ResetCauses.NewResetCause(ocIndex)
			if err != nil {
				log.V(lvl.DEBUG).Infof("unable to allocate new reset cause %d", ocIndex)
				return err
			}
		}
		ygot.BuildEmptyTree(resetCauseObj)
		if resetCauseObj.State == nil {
			ygot.BuildEmptyTree(resetCauseObj.State)
		}
		if resetCauseObj.State.Cause, ok = resetCauseMap[fpgaInfo.ResetCauses[index]]; !ok {
			resetCauseObj.State.Cause = ocbinds.OpenconfigPlatform_Components_Component_Fpga_ResetCauses_ResetCause_State_Cause_UNSET
		}
	default:
		log.V(lvl.DEBUG).Info("Default case")
	}

	return nil
}

func validSWCompName(name *string, prefix string) bool {
	if name == nil || *name == "" {
		return false
	}
	// Expect node name of form network_stackX or osX, where X is an integer (either 0 or 1)
	if !strings.HasPrefix(*name, prefix) {
		return false
	}

	sp := strings.SplitAfter(*name, prefix)
	if len(sp) < 2 {
		return false
	}

	if val, err := strconv.Atoi(sp[1]); err != nil || val > 1 {
		return false
	}
	return true
}

func validPSCompName(name *string, prefix string) bool {
	if name == nil || *name == "" {
		return false
	}
	if !strings.HasPrefix(*name, prefix) {
		return false
	}
	return true
}

func validHSMCompName(name string) bool {
	for _, p := range []string{HVL_PREFIX, DTL_PREFIX} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func getSWCompInfoFromDb(name string, d *db.DB, tblName string) (SWCompInfo, error) {
	swcEntry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cannot get entry: ", name, "; Error: ", err)
		return SWCompInfo{}, err
	}

	swcInfo := SWCompInfo{
		Name:            swcEntry.Get("name"),
		SoftwareVersion: swcEntry.Get("software-version"),
		Parent:          swcEntry.Get("parent"),
		OperStatus:      swcEntry.Get("oper-status"),
		StorageSide:     swcEntry.Get("storage-side"),
	}

	return swcInfo, nil
}

/* Filling in the state info for software components available in Redis DB */
func fillSWCompInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB, cType componentType) error {
	swcInfo, err := getSWCompInfoFromDb(name, stdb, SW_COMP_TBL)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting SW Comp info from State DB: ", err.Error())
		return err
	}
	compState := comp.State
	swModuleState := comp.SoftwareModule.State
	defaultVal := ""
	defaultParentVal := CHASSIS_PREFIX

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths {
		// Filling in state values
		// State Name
		compState.Name = &name
		// State Software Version
		compState.SoftwareVersion = &defaultVal
		if swcInfo.SoftwareVersion != "" {
			compState.SoftwareVersion = &swcInfo.SoftwareVersion
		}
		// State Parent
		compState.Parent = &defaultParentVal
		if swcInfo.Parent != "" {
			compState.Parent = &swcInfo.Parent
		}
		// State Type
		switch cType {
		case CompTypeOS:
			compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_SOFTWARE_COMPONENT_OPERATING_SYSTEM)
		case CompTypeBootLoader:
			compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_SOFTWARE_COMPONENT_BOOT_LOADER)
			return nil
		case CompTypeNWStack:
			compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_SOFTWARE_COMPONENT_SOFTWARE_MODULE)
		}
		// State Oper Status
		compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		switch swcInfo.OperStatus {
		case "active":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		case "inactive":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		}
		// State Storage Side
		switch swcInfo.StorageSide {
		case "a":
			compState.StorageSide = ocbinds.OpenconfigPlatform_Components_Component_State_StorageSide_SIDE_A
		case "b":
			compState.StorageSide = ocbinds.OpenconfigPlatform_Components_Component_State_StorageSide_SIDE_B
		}
		if pType == StatePaths {
			return nil
		}
		// SW Module State Module Type
		if cType == CompTypeNWStack {
			swModuleState.ModuleType = ocbinds.OpenconfigPlatformSoftware_SOFTWARE_MODULE_TYPE_USERSPACE_PACKAGE_BUNDLE
		}
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		compState.Name = &name
	case COMP_STATE_SW_VER:
		if swcInfo.SoftwareVersion == "" {
			return errors.New("software_version field not present in State DB")
		}
		compState.SoftwareVersion = &swcInfo.SoftwareVersion
	case COMP_STATE_PARENT:
		compState.Parent = &defaultParentVal
		if swcInfo.Parent != "" {
			compState.Parent = &swcInfo.Parent
		}
	case COMP_STATE_TYPE:
		switch cType {
		case CompTypeOS:
			compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_SOFTWARE_COMPONENT_OPERATING_SYSTEM)
		case CompTypeBootLoader:
			compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_SOFTWARE_COMPONENT_BOOT_LOADER)
		case CompTypeNWStack:
			compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
				ocbinds.OpenconfigPlatformTypes_OPENCONFIG_SOFTWARE_COMPONENT_SOFTWARE_MODULE)
		default:
			return errors.New("invalid component type for software component")
		}
	case COMP_STATE_OPER_STATUS:
		if cType == CompTypeBootLoader {
			return errors.New("invalid path for bootloader component.")
		}
		switch swcInfo.OperStatus {
		case "active":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		case "inactive":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_INACTIVE
		case "disabled":
			compState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_DISABLED
		case "":
			return errors.New("oper_status field not present in State DB")
		default:
			return errors.New("oper_status invalid field value in State DB: " + swcInfo.OperStatus)
		}
	case COMP_STATE_GO_STRG_SIDE:
		if cType == CompTypeBootLoader {
			return errors.New("invalid path for bootloader component.")
		}
		switch swcInfo.StorageSide {
		case "a":
			compState.StorageSide = ocbinds.OpenconfigPlatform_Components_Component_State_StorageSide_SIDE_A
		case "b":
			compState.StorageSide = ocbinds.OpenconfigPlatform_Components_Component_State_StorageSide_SIDE_B
		case "":
			return errors.New("storage-side field not present in State DB")
		default:
			return errors.New("storage-side invalid field value in State DB: " + swcInfo.StorageSide)
		}
	case SW_MODULE_STATE_MODULE_TYPE:
		if cType != CompTypeNWStack {
			return errors.New("invalid component for software-module/state/module-type path.")
		}
		swModuleState.ModuleType = ocbinds.OpenconfigPlatformSoftware_SOFTWARE_MODULE_TYPE_USERSPACE_PACKAGE_BUNDLE
	}
	return nil
}

func String2Uint(b string, dflt uint64) *uint64 {
	v, err := strconv.ParseUint(b, 10, 64)
	if err != nil {
		if b != "" {
			log.V(lvl.DEBUG).Infof("string conversion failure for %v; err = %v", b, err)
		}
		return &dflt
	}
	return &v
}

func String2Uint32(b string, dflt uint32) *uint32 {
	v, err := strconv.ParseUint(b, 10, 32)
	if err != nil {
		if b != "" {
			log.V(lvl.DEBUG).Infof("string conversion failure for %v; err = %v", b, err)
		}
		return &dflt
	}
	vv := uint32(v)
	return &vv
}

func String2Float(b string, dflt float64) *float64 {
	v, err := strconv.ParseFloat(b, 64)
	if err != nil {
		if b != "" {
			log.V(lvl.DEBUG).Infof("string conversion failure for %v; err = %v", b, err)
		}
		return &dflt
	}
	return &v
}

func fillSysPcieInfo(pcieComp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, d *db.DB) error {
	pcieParent := CHASSIS_PREFIX
	pcieInfo, err := d.GetEntry(&db.TableSpec{Name: PCIE_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get PCIE table info: ", err)
		return err
	}

	if targetUriPath == COMP || targetUriPath == COMP_ST || targetUriPath == COMP_STATE_PARENT {
		pcieComp.State.Parent = &pcieParent
	}

	// Updating the tree directly to avoid string case matches for each individual pcie path.
	// PCIE Fatal errors
	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths ||
		targetUriPath == COMP_STATE_PCIE ||
		strings.Contains(targetUriPath, "fatal-errors") {
		ygot.BuildEmptyTree(pcieComp.State.Pcie.FatalErrors)
		pcieComp.State.Pcie.FatalErrors.TotalErrors = String2Uint(pcieInfo.Get("fatal|TOTAL_ERR_FATAL"), 0)
		pcieComp.State.Pcie.FatalErrors.UndefinedErrors = String2Uint(pcieInfo.Get("fatal|Undefined"), 0)
		pcieComp.State.Pcie.FatalErrors.DataLinkErrors = String2Uint(pcieInfo.Get("fatal|DLP"), 0)
		pcieComp.State.Pcie.FatalErrors.SurpriseDownErrors = String2Uint(pcieInfo.Get("fatal|SDES"), 0)
		pcieComp.State.Pcie.FatalErrors.PoisonedTlpErrors = String2Uint(pcieInfo.Get("fatal|TLP"), 0)
		pcieComp.State.Pcie.FatalErrors.FlowControlProtocolErrors = String2Uint(pcieInfo.Get("fatal|FCP"), 0)
		pcieComp.State.Pcie.FatalErrors.CompletionTimeoutErrors = String2Uint(pcieInfo.Get("fatal|CmpltTO"), 0)
		pcieComp.State.Pcie.FatalErrors.CompletionAbortErrors = String2Uint(pcieInfo.Get("fatal|CmpltAbrt"), 0)
		pcieComp.State.Pcie.FatalErrors.UnexpectedCompletionErrors = String2Uint(pcieInfo.Get("fatal|UnxCmplt"), 0)
		pcieComp.State.Pcie.FatalErrors.ReceiverOverflowErrors = String2Uint(pcieInfo.Get("fatal|RxOF"), 0)
		pcieComp.State.Pcie.FatalErrors.MalformedTlpErrors = String2Uint(pcieInfo.Get("fatal|MalfTLP"), 0)
		pcieComp.State.Pcie.FatalErrors.EcrcErrors = String2Uint(pcieInfo.Get("fatal|ECRC"), 0)
		pcieComp.State.Pcie.FatalErrors.UnsupportedRequestErrors = String2Uint(pcieInfo.Get("fatal|UnsupReq"), 0)
		pcieComp.State.Pcie.FatalErrors.AcsViolationErrors = String2Uint(pcieInfo.Get("fatal|ACSViol"), 0)
		pcieComp.State.Pcie.FatalErrors.InternalErrors = String2Uint(pcieInfo.Get("fatal|UncorrIntErr"), 0)
		pcieComp.State.Pcie.FatalErrors.BlockedTlpErrors = String2Uint(pcieInfo.Get("fatal|BlockedTLP"), 0)
		pcieComp.State.Pcie.FatalErrors.AtomicOpBlockedErrors = String2Uint(pcieInfo.Get("fatal|AtomicOpBlocked"), 0)
		pcieComp.State.Pcie.FatalErrors.TlpPrefixBlockedErrors = String2Uint(pcieInfo.Get("fatal|TLPBlockedErr"), 0)
	}

	// PCIE Non-fatal errors
	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths ||
		targetUriPath == COMP_STATE_PCIE ||
		strings.Contains(targetUriPath, "non-fatal-errors") {
		ygot.BuildEmptyTree(pcieComp.State.Pcie.NonFatalErrors)
		pcieComp.State.Pcie.NonFatalErrors.TotalErrors = String2Uint(pcieInfo.Get("non_fatal|TOTAL_ERR_NONFATAL"), 0)
		pcieComp.State.Pcie.NonFatalErrors.UndefinedErrors = String2Uint(pcieInfo.Get("non_fatal|Undefined"), 0)
		pcieComp.State.Pcie.NonFatalErrors.DataLinkErrors = String2Uint(pcieInfo.Get("non_fatal|DLP"), 0)
		pcieComp.State.Pcie.NonFatalErrors.SurpriseDownErrors = String2Uint(pcieInfo.Get("non_fatal|SDES"), 0)
		pcieComp.State.Pcie.NonFatalErrors.PoisonedTlpErrors = String2Uint(pcieInfo.Get("non_fatal|TLP"), 0)
		pcieComp.State.Pcie.NonFatalErrors.FlowControlProtocolErrors = String2Uint(pcieInfo.Get("non_fatal|FCP"), 0)
		pcieComp.State.Pcie.NonFatalErrors.CompletionTimeoutErrors = String2Uint(pcieInfo.Get("non_fatal|CmpltTO"), 0)
		pcieComp.State.Pcie.NonFatalErrors.CompletionAbortErrors = String2Uint(pcieInfo.Get("non_fatal|CmpltAbrt"), 0)
		pcieComp.State.Pcie.NonFatalErrors.UnexpectedCompletionErrors = String2Uint(pcieInfo.Get("non_fatal|UnxCmplt"), 0)
		pcieComp.State.Pcie.NonFatalErrors.ReceiverOverflowErrors = String2Uint(pcieInfo.Get("non_fatal|RxOF"), 0)
		pcieComp.State.Pcie.NonFatalErrors.MalformedTlpErrors = String2Uint(pcieInfo.Get("non_fatal|MalfTLP"), 0)
		pcieComp.State.Pcie.NonFatalErrors.EcrcErrors = String2Uint(pcieInfo.Get("non_fatal|ECRC"), 0)
		pcieComp.State.Pcie.NonFatalErrors.UnsupportedRequestErrors = String2Uint(pcieInfo.Get("non_fatal|UnsupReq"), 0)
		pcieComp.State.Pcie.NonFatalErrors.AcsViolationErrors = String2Uint(pcieInfo.Get("non_fatal|ACSViol"), 0)
		pcieComp.State.Pcie.NonFatalErrors.InternalErrors = String2Uint(pcieInfo.Get("non_fatal|UncorrIntErr"), 0)
		pcieComp.State.Pcie.NonFatalErrors.BlockedTlpErrors = String2Uint(pcieInfo.Get("non_fatal|BlockedTLP"), 0)
		pcieComp.State.Pcie.NonFatalErrors.AtomicOpBlockedErrors = String2Uint(pcieInfo.Get("non_fatal|AtomicOpBlocked"), 0)
		pcieComp.State.Pcie.NonFatalErrors.TlpPrefixBlockedErrors = String2Uint(pcieInfo.Get("non_fatal|TLPBlockedErr"), 0)
	}

	// PCIE Correctable errors
	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths ||
		targetUriPath == COMP_STATE_PCIE ||
		strings.Contains(targetUriPath, "correctable-errors") {
		ygot.BuildEmptyTree(pcieComp.State.Pcie.CorrectableErrors)
		pcieComp.State.Pcie.CorrectableErrors.TotalErrors = String2Uint(pcieInfo.Get("correctable|TOTAL_ERR_COR"), 0)
		pcieComp.State.Pcie.CorrectableErrors.ReceiverErrors = String2Uint(pcieInfo.Get("correctable|RxErr"), 0)
		pcieComp.State.Pcie.CorrectableErrors.BadTlpErrors = String2Uint(pcieInfo.Get("correctable|BadTLP"), 0)
		pcieComp.State.Pcie.CorrectableErrors.BadDllpErrors = String2Uint(pcieInfo.Get("correctable|BadDLLP"), 0)
		pcieComp.State.Pcie.CorrectableErrors.RelayRolloverErrors = String2Uint(pcieInfo.Get("correctable|Rollover"), 0)
		pcieComp.State.Pcie.CorrectableErrors.ReplayTimeoutErrors = String2Uint(pcieInfo.Get("correctable|Timeout"), 0)
		pcieComp.State.Pcie.CorrectableErrors.AdvisoryNonFatalErrors = String2Uint(pcieInfo.Get("correctable|NonFatalErr"), 0)
		pcieComp.State.Pcie.CorrectableErrors.InternalErrors = String2Uint(pcieInfo.Get("correctable|CorrIntErr"), 0)
		pcieComp.State.Pcie.CorrectableErrors.HdrLogOverflowErrors = String2Uint(pcieInfo.Get("correctable|HeaderOF"), 0)
	}

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths ||
		targetUriPath == COMP_STATE_PCIE ||
		strings.Contains(targetUriPath, "pins-link-speed") ||
		strings.Contains(targetUriPath, "pins-link-width") {
		pcieComp.State.Pcie.PinsLinkSpeed = String2Float(pcieInfo.Get("link_speed"), 0)
		pcieComp.State.Pcie.PinsLinkWidth = String2Uint32(pcieInfo.Get("link_width"), 0)
	}

	return nil
}

func getCsigInfoFromDb(key string, d *db.DB, tblName string) (CsigInfo, error) {
	var csigInfo CsigInfo

	entry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{key}})
	if err != nil {
		return csigInfo, err
	}

	csigInfo.csigSupported = entry.Get("CSIG_SUPPORTED")

	return csigInfo, nil
}

func getControllerCardInfoFromDb(name string, d *db.DB) (ControllerCard, error) {
	cardEntry, err := d.GetEntry(&db.TableSpec{Name: CONTROLLER_CARD_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return ControllerCard{}, err
	}

	return ControllerCard{
		Name:          cardEntry.Get("name"),
		Model_Name:    cardEntry.Get("model"),
		Parent:        cardEntry.Get("parent"),
		Part_Number:   cardEntry.Get("part-no"),
		Serial_Number: cardEntry.Get("serial"),
	}, nil
}

/* Filling in the state info for controller card components available in Redis DB */
func fillControllerCardInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB) error {
	cardInfo, err := getControllerCardInfoFromDb(name, stdb)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting controller card info from State DB: ", err.Error())
		return err
	}
	cardState := comp.State
	defaultVal := ""
	defaultParentVal := CHASSIS_PREFIX

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths {
		// Filling in state values
		cardState.Name = StringWithDefault(&cardInfo.Name, name)
		cardState.Description = StringWithDefault(&cardInfo.Model_Name, defaultVal)
		cardState.ModelName = StringWithDefault(&cardInfo.Model_Name, defaultVal)
		cardState.Parent = StringWithDefault(&cardInfo.Parent, defaultParentVal)
		cardState.PartNo = StringWithDefault(&cardInfo.Part_Number, cardInfo.Model_Name)
		cardState.SerialNo = StringWithDefault(&cardInfo.Serial_Number, defaultVal)
		cardState.Type, _ = cardState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_CONTROLLER_CARD)
		// If this code is running, the controller card is functioning, so report ACTIVE
		cardState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		cardState.Name = StringWithDefault(&cardInfo.Name, name)
	case COMP_STATE_DESCR:
		cardState.Description = StringWithDefault(&cardInfo.Model_Name, defaultVal)
	case COMP_STATE_MODEL_NAME:
		cardState.ModelName = StringWithDefault(&cardInfo.Model_Name, defaultVal)
	case COMP_STATE_PARENT:
		cardState.Parent = StringWithDefault(&cardInfo.Parent, defaultParentVal)
	case COMP_STATE_TYPE:
		cardState.Type, _ = cardState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_CONTROLLER_CARD)
	case COMP_STATE_OPER_STATUS:
		// If this code is running, the controller card is functioning, so report ACTIVE
		cardState.OperStatus = ocbinds.OpenconfigPlatformTypes_COMPONENT_OPER_STATUS_ACTIVE
	case COMP_STATE_PART_NO:
		cardState.PartNo = StringWithDefault(&cardInfo.Part_Number, cardInfo.Model_Name)
	case COMP_STATE_SERIAL_NO:
		cardState.SerialNo = StringWithDefault(&cardInfo.Serial_Number, defaultVal)
	}
	return nil
}

func getEepromStatusInfoFromDb(name string, d *db.DB) (EepromStatus, error) {
	statusEntry, err := d.GetEntry(&db.TableSpec{Name: EEPROM_STATUS_TBL}, db.Key{Comp: []string{name}})
	if err != nil {
		log.V(lvl.DEBUG).Info("Cant get entry: ", name, "; Error: ", err)
		return EepromStatus{}, err
	}

	return EepromStatus{
		Name:                 statusEntry.Get("name"),
		Status:               statusEntry.Get("status"),
		NumCorrectableErrors: statusEntry.Get("num-correctable-errors"),
	}, nil
}

/* Filling in the state info for eeprom status components available in Redis DB */
func fillEepromStatusInfo(comp *ocbinds.OpenconfigPlatform_Components_Component,
	name string, pType PathType, targetUriPath string, stdb *db.DB) error {
	statusInfo, err := getEepromStatusInfoFromDb(name, stdb)
	if err != nil {
		log.V(lvl.DEBUG).Info("Error Getting eeprom status info from State DB: ", err.Error())
		return err
	}
	compState := comp.State
	defaultParentVal := CHASSIS_PREFIX

	if pType == AllPaths || pType == AllCompPaths || pType == StatePaths {
		// Filling in state values
		compState.Name = StringWithDefault(&statusInfo.Name, name)
		compState.Parent = &defaultParentVal
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_EEPROM)
		var ok bool
		if compState.OperStatus, ok = eepromOperStatusMap[statusInfo.Status]; !ok {
			return fmt.Errorf("Failed to convert %s eeprom status (got \"%s\")", name, statusInfo.Status)
		}
		ygot.BuildEmptyTree(compState.Pcie)
		ygot.BuildEmptyTree(compState.Pcie.CorrectableErrors)
		compState.Pcie.CorrectableErrors.TotalErrors = String2Uint(statusInfo.NumCorrectableErrors, 0)
		return nil
	}

	switch targetUriPath {
	case COMP_STATE_NAME:
		compState.Name = StringWithDefault(&statusInfo.Name, name)
	case COMP_STATE_PARENT:
		compState.Parent = &defaultParentVal
	case COMP_STATE_TYPE:
		compState.Type, _ = compState.To_OpenconfigPlatform_Components_Component_State_Type_Union(
			ocbinds.OpenconfigPlatformTypes_OPENCONFIG_HARDWARE_COMPONENT_EEPROM)
	case COMP_STATE_OPER_STATUS:
		var ok bool
		if compState.OperStatus, ok = eepromOperStatusMap[statusInfo.Status]; !ok {
			return fmt.Errorf("Failed to convert %s eeprom status (got \"%s\")", name, statusInfo.Status)
		}
	case COMP_STATE_PCIE:
		fallthrough
	case COMP_STATE_PCIE_CORRECTABLE_ERRORS:
		fallthrough
	case COMP_STATE_PCIE_CORRECTABLE_ERRORS_TOTAL:
		ygot.BuildEmptyTree(compState.Pcie)
		ygot.BuildEmptyTree(compState.Pcie.CorrectableErrors)
		fmt.Println("SallyLyu compState.Pcie.CorrectableErrors ", compState.Pcie.CorrectableErrors)
		compState.Pcie.CorrectableErrors.TotalErrors = String2Uint(statusInfo.NumCorrectableErrors, 0)
	}
	return nil
}

var DbToYangPath_pfm_components_path_xfmr PathXfmrDbToYangFunc = func(inParams XfmrDbToYgPathParams) error {
	rootPath := COMP

	log.V(lvl.DEBUG).Infof("DbToYangPath_pfm_path_xfmr: inParams: %#v", inParams)

	if len(inParams.tblKeyComp) == 0 {
		return fmt.Errorf("Invalid tblKeyCom for pfm path xmfr:%v", inParams.tblKeyComp)
	}

	tblKey := inParams.tblKeyComp[0]
	var subKey string
	if len(inParams.tblKeyComp) == 2 {
		subKey = inParams.tblKeyComp[1]
	}
	switch inParams.tblName {
	case PORT_BREAKOUT:
		// PORT_BREAKOUT uses the same db and db key as CompTypeXcvr, but with different yang key
		var portName string
		if inParams.db != nil {
			// Try to get port name from DB first.
			if entry, err := inParams.db.GetEntry(&db.TableSpec{Name: PORT_BREAKOUT}, db.Key{Comp: inParams.tblKeyComp}); err == nil {
				portName = entry.Get("name")
			}
		}
		if portName == "" {
			log.V(lvl.DEBUG).Info("No port name found for port breakout:", tblKey)
			// Try to get port name from platform.json.
			var err error
			if portName, err = platform.PortNameFromInterface(tblKey); err != nil {
				log.V(lvl.WARNING).Info("No port name found for port breakout:", tblKey, err)
				return err
			}
		}
		inParams.ygPathKeys[rootPath+"/name"] = portName

	case BREAKOUT_TBL:
		portName, err := platform.PortNameFromInterface(tblKey)
		if err != nil {
			log.V(lvl.WARNING).Infof("No port name found for interface \"%s\", error %v", tblKey, err)
			return err
		}
		inParams.ygPathKeys[rootPath+"/name"] = portName
	case POWER_INFO_TBL:
		inParams.ygPathKeys[rootPath+"/name"] = tblKey
		if subKey != "" {
			railName := subKey
			inParams.ygPathKeys[rootPath+"/power-supply/google-pins-platform:rails/rail/name"] = railName
		}
	case "BREAKOUT_PORT_XCVR_CFG":
		inParams.ygPathKeys[rootPath+"/name"] = tblKey
		if subKey != "" {
			inParams.ygPathKeys[rootPath+"/subcomponents/subcomponent/name"] = subKey
		}
	default:
		inParams.ygPathKeys[rootPath+"/name"] = tblKey
	}

	log.V(lvl.DEBUG).Info("DbToYangPath_pfm_path_xfmr:- params.ygPathKeys: ", inParams.ygPathKeys)

	return nil
}
