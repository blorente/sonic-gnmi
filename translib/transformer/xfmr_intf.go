//////////////////////////////////////////////////////////////////////////
//
// Copyright 2019 Dell, Inc.
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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"os"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/platform"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	"github.com/Azure/sonic-mgmt-common/translib/utils"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
)

func init() {
	XlateFuncBind("intf_table_xfmr", intf_table_xfmr)
	XlateFuncBind("YangToDb_intf_tbl_key_xfmr", YangToDb_intf_tbl_key_xfmr)
	XlateFuncBind("DbToYang_intf_tbl_key_xfmr", DbToYang_intf_tbl_key_xfmr)
	XlateFuncBind("YangToDb_intf_mtu_xfmr", YangToDb_intf_mtu_xfmr)
	XlateFuncBind("DbToYang_intf_mtu_xfmr", DbToYang_intf_mtu_xfmr)
	XlateFuncBind("DbToYang_intf_admin_status_xfmr", DbToYang_intf_admin_status_xfmr)
	XlateFuncBind("YangToDb_intf_enabled_xfmr", YangToDb_intf_enabled_xfmr)
	XlateFuncBind("DbToYang_intf_enabled_xfmr", DbToYang_intf_enabled_xfmr)
	XlateFuncBind("DbToYang_intf_eth_aggr_id_xfmr", DbToYang_intf_eth_aggr_id_xfmr)
	XlateFuncBind("YangToDb_intf_eth_port_config_xfmr", YangToDb_intf_eth_port_config_xfmr)
	XlateFuncBind("DbToYang_intf_eth_port_config_xfmr", DbToYang_intf_eth_port_config_xfmr)
	//XlateFuncBind("Subscribe_intf_eth_port_config_xfmr", Subscribe_intf_eth_port_config_xfmr)
	//XlateFuncBind("DbToYangPath_intf_eth_port_config_path_xfmr", DbToYangPath_intf_eth_port_config_path_xfmr)
	XlateFuncBind("DbToYang_intf_eth_auto_neg_xfmr", DbToYang_intf_eth_auto_neg_xfmr)
	XlateFuncBind("DbToYang_intf_eth_port_speed_xfmr", DbToYang_intf_eth_port_speed_xfmr)

	XlateFuncBind("DbToYang_intf_get_counters_xfmr", DbToYang_intf_get_counters_xfmr)
	XlateFuncBind("DbToYang_intf_get_ether_counters_xfmr", DbToYang_intf_get_ether_counters_xfmr)
	XlateFuncBind("Subscribe_intf_get_counters_xfmr", Subscribe_intf_get_counters_xfmr)
	//XlateFuncBind("DbToYangPath_intf_get_counters_path_xfmr", DbToYangPath_intf_get_counters_path_xfmr)
	XlateFuncBind("Subscribe_intf_get_ether_counters_xfmr", Subscribe_intf_get_ether_counters_xfmr)

	XlateFuncBind("YangToDb_intf_subintfs_xfmr", YangToDb_intf_subintfs_xfmr)
	XlateFuncBind("DbToYang_intf_subintfs_xfmr", DbToYang_intf_subintfs_xfmr)

	XlateFuncBind("YangToDb_subintf_ip_addr_key_xfmr", YangToDb_subintf_ip_addr_key_xfmr)
	XlateFuncBind("DbToYang_subintf_ip_addr_key_xfmr", DbToYang_subintf_ip_addr_key_xfmr)
	XlateFuncBind("YangToDb_intf_ip_addr_xfmr", YangToDb_intf_ip_addr_xfmr)
	XlateFuncBind("DbToYang_intf_ip_addr_xfmr", DbToYang_intf_ip_addr_xfmr)

	XlateFuncBind("intf_subintfs_table_xfmr", intf_subintfs_table_xfmr)
	XlateFuncBind("YangToDb_subif_index_xfmr", YangToDb_subif_index_xfmr)
	XlateFuncBind("DbToYang_subif_index_xfmr", DbToYang_subif_index_xfmr)
	XlateFuncBind("DbToYangPath_intf_ip_path_xfmr", DbToYangPath_intf_ip_path_xfmr)
	XlateFuncBind("Subscribe_intf_ip_addr_xfmr", Subscribe_intf_ip_addr_xfmr)

	XlateFuncBind("YangToDb_subintf_ipv6_tbl_key_xfmr", YangToDb_subintf_ipv6_tbl_key_xfmr)
	XlateFuncBind("DbToYang_subintf_ipv6_tbl_key_xfmr", DbToYang_subintf_ipv6_tbl_key_xfmr)
	XlateFuncBind("YangToDb_ipv6_enabled_xfmr", YangToDb_ipv6_enabled_xfmr)
	XlateFuncBind("DbToYang_ipv6_enabled_xfmr", DbToYang_ipv6_enabled_xfmr)

	XlateFuncBind("intf_post_xfmr", intf_post_xfmr)
	XlateFuncBind("intf_pre_xfmr", intf_pre_xfmr)

	// Not in upstream
	XlateFuncBind("YangToDb_intf_name_xfmr", YangToDb_intf_name_xfmr)
	XlateFuncBind("DbToYang_intf_name_xfmr", DbToYang_intf_name_xfmr)
	XlateFuncBind("DbToYang_intf_hardware_port_xfmr", DbToYang_intf_hardware_port_xfmr)
	XlateFuncBind("DbToYang_intf_transceiver_xfmr", DbToYang_intf_transceiver_xfmr)
	XlateFuncBind("DbToYang_intf_physical_channel_xfmr", DbToYang_intf_physical_channel_xfmr)
	XlateFuncBind("DbToYang_intf_last_change_xfmr", DbToYang_intf_last_change_xfmr)
	XlateFuncBind("YangToDb_intf_diag_profile_xfmr", YangToDb_intf_diag_profile_xfmr)
	XlateFuncBind("DbToYang_intf_diag_profile_xfmr", DbToYang_intf_diag_profile_xfmr)
	XlateFuncBind("YangToDb_intf_loopback_mode_xfmr", YangToDb_intf_loopback_mode_xfmr)
	XlateFuncBind("DbToYang_intf_loopback_mode_xfmr", DbToYang_intf_loopback_mode_xfmr)
	XlateFuncBind("YangToDb_intf_type_xfmr", YangToDb_intf_type_xfmr)
	XlateFuncBind("DbToYang_intf_type_xfmr", DbToYang_intf_type_xfmr)
	XlateFuncBind("YangToDb_pins_if_health_indicator_xfmr", YangToDb_pins_if_health_indicator_xfmr)
	XlateFuncBind("DbToYang_pins_if_health_indicator_xfmr", DbToYang_pins_if_health_indicator_xfmr)
	XlateFuncBind("YangToDb_intf_bridge_id_xfmr", YangToDb_intf_bridge_id_xfmr)
	XlateFuncBind("DbToYang_intf_bridge_id_xfmr", DbToYang_intf_bridge_id_xfmr)
	XlateFuncBind("YangToDb_pins_if_id_xfmr", YangToDb_pins_if_id_xfmr)
	XlateFuncBind("DbToYang_pins_if_id_xfmr", DbToYang_pins_if_id_xfmr)
	XlateFuncBind("DbToYang_pins_ifindex_xfmr", DbToYang_pins_ifindex_xfmr)
	XlateFuncBind("DbToYang_intf_hw_vendor_id_xfmr", DbToYang_intf_hw_vendor_id_xfmr)
	XlateFuncBind("DbToYang_intf_oper_status_xfmr", DbToYang_intf_oper_status_xfmr)
	XlateFuncBind("YangToDb_intf_fqin_xfmr", YangToDb_intf_fqin_xfmr)
	XlateFuncBind("DbToYang_intf_fqin_xfmr", DbToYang_intf_fqin_xfmr)
	XlateFuncBind("YangToDb_intf_ecmp_hash_offset_xfmr", YangToDb_intf_ecmp_hash_offset_xfmr)
	XlateFuncBind("DbToYang_intf_ecmp_hash_offset_xfmr", DbToYang_intf_ecmp_hash_offset_xfmr)
	XlateFuncBind("YangToDb_intf_ecmp_hash_algorithm_xfmr", YangToDb_intf_ecmp_hash_algorithm_xfmr)
	XlateFuncBind("DbToYang_intf_ecmp_hash_algorithm_xfmr", DbToYang_intf_ecmp_hash_algorithm_xfmr)
	XlateFuncBind("YangToDb_intf_port_direction_xfmr", YangToDb_intf_port_direction_xfmr)
	XlateFuncBind("DbToYang_intf_port_direction_xfmr", DbToYang_intf_port_direction_xfmr)
	XlateFuncBind("YangToDb_intf_csig_tag_action_xfmr", YangToDb_intf_csig_tag_action_xfmr)
	XlateFuncBind("DbToYang_intf_csig_tag_action_xfmr", DbToYang_intf_csig_tag_action_xfmr)
	XlateFuncBind("DbToYang_intf_eth_duplex_mode_xfmr", DbToYang_intf_eth_duplex_mode_xfmr)
	XlateFuncBind("DbToYang_intf_eth_mac_address_xfmr", DbToYang_intf_eth_mac_address_xfmr)
	XlateFuncBind("DbToYang_intf_eth_negotiated_port_speed_xfmr", DbToYang_intf_eth_negotiated_port_speed_xfmr)
	XlateFuncBind("DbToYang_intf_eth_forwarding_viable_xfmr", DbToYang_intf_eth_forwarding_viable_xfmr)
	XlateFuncBind("DbToYang_intf_eth_fec_mode_xfmr", DbToYang_intf_eth_fec_mode_xfmr)
	XlateFuncBind("DbToYang_intf_eth_sfec_mode_xfmr", DbToYang_intf_eth_sfec_mode_xfmr)
	XlateFuncBind("DbToYang_intf_eth_ingress_delay_xfmr", DbToYang_intf_eth_ingress_delay_xfmr)
	XlateFuncBind("DbToYang_intf_eth_ingress_delay_applied_xfmr", DbToYang_intf_eth_ingress_delay_applied_xfmr)
	XlateFuncBind("DbToYang_intf_eth_egress_delay_xfmr", DbToYang_intf_eth_egress_delay_xfmr)
	XlateFuncBind("DbToYang_intf_eth_egress_delay_applied_xfmr", DbToYang_intf_eth_egress_delay_applied_xfmr)
	XlateFuncBind("DbToYang_intf_eth_state_pfc_enable_xfmr", DbToYang_intf_eth_state_pfc_enable_xfmr)
	XlateFuncBind("DbToYang_intf_eth_sfec_oper_mode_xfmr", DbToYang_intf_eth_sfec_oper_mode_xfmr)
	XlateFuncBind("DbToYang_intf_eth_link_training_xfmr", DbToYang_intf_eth_link_training_xfmr)
	XlateFuncBind("DbToYang_intf_eth_xcvr_qualified_xfmr", DbToYang_intf_eth_xcvr_qualified_xfmr)
	XlateFuncBind("YangToDb_intf_hold_time_config_xfmr", YangToDb_intf_hold_time_config_xfmr)
	XlateFuncBind("DbToYang_intf_hold_time_config_xfmr", DbToYang_intf_hold_time_config_xfmr)
	XlateFuncBind("DbToYang_intf_hold_time_down_xfmr", DbToYang_intf_hold_time_down_xfmr)
	XlateFuncBind("DbToYang_intf_hold_time_up_xfmr", DbToYang_intf_hold_time_up_xfmr)
	XlateFuncBind("DbToYang_ipv4_enabled_xfmr", DbToYang_ipv4_enabled_xfmr)
	XlateFuncBind("DbToYang_intf_ipv6_counters_xfmr", DbToYang_intf_ipv6_counters_xfmr)
	XlateFuncBind("DbToYang_intf_ipv4_counters_xfmr", DbToYang_intf_ipv4_counters_xfmr)
	XlateFuncBind("YangToDb_subintf_ipv4_tbl_key_xfmr", YangToDb_subintf_ipv4_tbl_key_xfmr)
	XlateFuncBind("YangToDb_intf_encoded_id_xfmr", YangToDb_intf_encoded_id_xfmr)
	XlateFuncBind("DbToYang_intf_encoded_id_xfmr", DbToYang_intf_encoded_id_xfmr)
	XlateFuncBind("DbToYang_intf_description_xfmr", DbToYang_intf_description_xfmr)
	XlateFuncBind("DbToYangPath_intf_path_xfmr", DbToYangPath_intf_path_xfmr)
	XlateFuncBind("DbToYang_intf_mgmt_xfmr", DbToYang_intf_mgmt_xfmr)
	XlateFuncBind("DbToYang_intf_cpu_xfmr", DbToYang_intf_cpu_xfmr)
	XlateFuncBind("DbToYang_intf_eth_ingress_timestamp_xfmr", DbToYang_intf_eth_ingress_timestamp_xfmr)
	XlateFuncBind("DbToYang_intf_eth_egress_timestamp_xfmr", DbToYang_intf_eth_egress_timestamp_xfmr)
	XlateFuncBind("DbToYang_intf_eth_pfc_xfmr", DbToYang_intf_eth_pfc_xfmr)
	XlateFuncBind("Subscribe_intf_eth_pfc_xfmr", Subscribe_intf_eth_pfc_xfmr)
	XlateFuncBind("YangToDb_intf_aied_link_damping_xfmr", YangToDb_intf_aied_link_damping_xfmr)
	XlateFuncBind("DbToYang_intf_aied_link_damping_xfmr", DbToYang_intf_aied_link_damping_xfmr)
	XlateFuncBind("Subscribe_intf_aied_link_damping_xfmr", Subscribe_intf_aied_link_damping_xfmr)
	XlateFuncBind("DbToYang_intf_state_blackhole_xfmr", DbToYang_intf_state_blackhole_xfmr)
	XlateFuncBind("DbToYang_intf_state_hst_xfmr", DbToYang_intf_state_hst_xfmr)
}

const (
	PORT_ADMIN_STATUS = "admin_status"
	PORT_SPEED        = "speed"
	PORT_AUTONEG      = "autoneg"

	PORTCHANNEL_INTERFACE_TN = "PORTCHANNEL_INTERFACE"
	PORTCHANNEL_MEMBER_TN    = "PORTCHANNEL_MEMBER"
	DEFAULT_MTU              = 9100
)

const (
	PIPE        = "|"
	COLON       = ":"
	ETHERNET    = "Eth"
	PORTCHANNEL = "PortChannel"
	MGMT        = "eth"
	MGMT_BOND   = "bond"
	LOOPBACK    = "Loopback"
	CPU         = "CPU"
	BRIDGE      = "br"
)

const (
	HARDWARE_PORT              = "hardware-port"
	LAG_TABLE_ALIAS            = "alias"
	PORT_INDEX                 = "index"
	PORT_TN                    = "PORT"
	PORT_MTU                   = "mtu"
	PORT_LOOPBACK_MODE         = "loopback-mode"
	PORT_HEALTH_INDICATOR      = "health_indicator"
	PORT_HOLD_TIME_UP          = "hold_time_up"
	PORT_HOLD_TIME_DOWN        = "hold_time_down"
	PORT_ADMIN_STATUS_STATE    = "admin_status_state"
	ADV_PORT_SPEED             = "adv_speeds"
	PORT_FEC                   = "fec"
	ADV_PORT_FEC               = "adv_extended_fec_modes"
	PORT_SFEC_MODE             = "sfec_mode"
	PORT_SFEC_OPER_MODE        = "sfec_oper_mode"
	PORT_INGRESS_DELAY         = "ingress-delay"
	PORT_EGRESS_DELAY          = "egress-delay"
	PORT_INGRESS_DELAY_APPLIED = "ingress-delay-applied"
	PORT_EGRESS_DELAY_APPLIED  = "egress-delay-applied"
	PORT_INGRESS_TIMESTAMP     = "ingress_timestamp_enable"
	PORT_EGRESS_TIMESTAMP      = "egress_timestamp_enable"
	PORT_PFC_ENABLE            = "pfc_asym"
	PORT_LANES                 = "lanes"
	PORT_OPER_STATUS           = "oper_status"
	PORT_PRESENCE              = "presence"
	PORT_LAST_CHANGE           = "last-change"
	PORT_MAC_ADDR              = "mac-address"
	PORT_NEGOTIATED_SPEED      = "negotiated-port-speed"
	PORT_FWD_VIABLE            = "forwarding-viable"
	PORT_LINK_TRAINING         = "standalone-link-training"
	PORT_FQIN                  = "fully-qualified-interface-name"
	PORTCHANNEL_TN             = "PORTCHANNEL"
	LAG_TABLE_TN               = "LAG_TABLE"
	LAG_MEMBER_TABLE_TN        = "LAG_MEMBER_TABLE"
	LOOPBACK_TN                = "LOOPBACK"
	LOOPBACK_INTERFACE_TN      = "LOOPBACK_INTERFACE"
	UNNUMBERED                 = "unnumbered"
	PORT_UNDER_TEST            = "under_test"
	QOS_PORT_TN                = "QOS_PORT"
	HARDWARE_VENDOR_ID         = "hardware_vendor_id"
	UNKNOWN                    = "unknown"
	DEFAULT_L2_HEADER_SIZE     = 22
)

type TblData struct {
	portTN   string
	memberTN string
	intfTN   string
	keySep   string
}

type PopulateIntfCounters func(inParams XfmrParams, itfName string, counters interface{}) error

type CounterData struct {
	OIDTN            string
	CountersTN       string
	PopulateCounters PopulateIntfCounters
}

type IntfTblData struct {
	cfgDb       TblData
	appDb       TblData
	appStateDb  TblData
	stateDb     TblData
	CountersHdl CounterData
}

var IntfTypeTblMap = map[E_InterfaceType]IntfTblData{
	IntfTypeEthernet: IntfTblData{
		cfgDb:       TblData{portTN: "PORT", intfTN: "INTERFACE", keySep: PIPE},
		appDb:       TblData{portTN: "PORT_TABLE", intfTN: "INTF_TABLE", keySep: COLON},
		appStateDb:  TblData{portTN: "PORT_TABLE", intfTN: "INTF_TABLE", keySep: COLON},
		stateDb:     TblData{portTN: "PORT_TABLE", intfTN: "INTERFACE_TABLE", keySep: PIPE},
		CountersHdl: CounterData{OIDTN: "COUNTERS_PORT_NAME_MAP", CountersTN: "COUNTERS", PopulateCounters: populatePortCounters},
	},
	IntfTypePortChannel: IntfTblData{
		cfgDb:       TblData{portTN: "PORTCHANNEL", intfTN: "PORTCHANNEL_INTERFACE", memberTN: "PORTCHANNEL_MEMBER", keySep: PIPE},
		appDb:       TblData{portTN: "LAG_TABLE", intfTN: "INTF_TABLE", keySep: COLON, memberTN: "LAG_MEMBER_TABLE"},
		appStateDb:  TblData{portTN: "LAG_TABLE", intfTN: "INTF_TABLE", keySep: COLON, memberTN: "LAG_MEMBER_TABLE"},
		stateDb:     TblData{portTN: "LAG_TABLE", intfTN: "INTERFACE_TABLE", keySep: PIPE},
		CountersHdl: CounterData{OIDTN: "COUNTERS_PORT_NAME_MAP", CountersTN: "COUNTERS", PopulateCounters: populatePortChannelCounters},
	},
	IntfTypeMgmt: IntfTblData{
		cfgDb:       TblData{portTN: "MGMT_PORT", intfTN: "MGMT_INTERFACE", keySep: PIPE},
		appDb:       TblData{portTN: "MGMT_PORT_TABLE", intfTN: "MGMT_INTF_TABLE", keySep: COLON},
		appStateDb:  TblData{portTN: "MGMT_PORT_TABLE", intfTN: "MGMT_INTF_TABLE", keySep: COLON},
		stateDb:     TblData{portTN: "MGMT_PORT_TABLE", intfTN: "MGMT_INTERFACE_TABLE", keySep: PIPE},
		CountersHdl: CounterData{OIDTN: "", CountersTN: "", PopulateCounters: populateMGMTPortCounters},
	},
	IntfTypeLoopback: IntfTblData{
		cfgDb:       TblData{portTN: "LOOPBACK", intfTN: "LOOPBACK_INTERFACE", keySep: PIPE},
		appDb:       TblData{portTN: "INTF_TABLE", intfTN: "INTF_TABLE", keySep: COLON},
		appStateDb:  TblData{portTN: "INTF_TABLE", intfTN: "INTF_TABLE", keySep: COLON},
		CountersHdl: CounterData{OIDTN: "", CountersTN: "", PopulateCounters: populateMGMTPortCounters},
	},
	IntfTypeCpu: IntfTblData{
		cfgDb:       TblData{portTN: "CPU_PORT", keySep: PIPE},
		appDb:       TblData{portTN: "PORT_TABLE", keySep: COLON},
		appStateDb:  TblData{portTN: "PORT_TABLE", keySep: COLON},
		stateDb:     TblData{portTN: "PORT_TABLE", keySep: PIPE},
		CountersHdl: CounterData{OIDTN: "COUNTERS_PORT_NAME_MAP", CountersTN: "COUNTERS", PopulateCounters: populatePortCounters},
	},
	IntfTypeSubIntf: IntfTblData{
		cfgDb:      TblData{portTN: "VLAN_SUB_INTERFACE", intfTN: "VLAN_SUB_INTERFACE", keySep: PIPE},
		appDb:      TblData{portTN: "PORT_TABLE", intfTN: "INTF_TABLE", keySep: COLON},
		stateDb:    TblData{portTN: "PORT_TABLE", intfTN: "INTERFACE_TABLE", keySep: PIPE},
		appStateDb: TblData{portTN: "PORT_TABLE", intfTN: "INTF_TABLE", keySep: COLON},
	},
	IntfTypeMgmtBond: IntfTblData{
		cfgDb:       TblData{portTN: "MGMT_PORT", intfTN: "MGMT_INTERFACE", keySep: PIPE},
		appDb:       TblData{portTN: "MGMT_PORT_TABLE", intfTN: "MGMT_INTF_TABLE", keySep: COLON},
		appStateDb:  TblData{portTN: "MGMT_PORT_TABLE", intfTN: "MGMT_INTF_TABLE", keySep: COLON},
		stateDb:     TblData{portTN: "MGMT_PORT_TABLE", intfTN: "MGMT_INTERFACE_TABLE", keySep: PIPE},
		CountersHdl: CounterData{OIDTN: "", CountersTN: "", PopulateCounters: populateMGMTPortCounters},
	},
	IntfTypeBridge: IntfTblData{
		cfgDb:      TblData{portTN: "BRIDGE", memberTN: "BRIDGE_MEMBER", keySep: PIPE},
		appStateDb: TblData{portTN: "BRIDGE_TABLE", memberTN: "BRIDGE_MEMBER_TABLE", keySep: COLON},
	},
}

var dbIdToTblMap = map[db.DBNum][]string{
	db.ConfigDB:    {"PORT", "MGMT_PORT", "VLAN", "PORTCHANNEL", "LOOPBACK", "VXLAN_TUNNEL", "VLAN_SUB_INTERFACE", "CPU_PORT", "BRIDGE"},
	db.ApplDB:      {"PORT_TABLE", "MGMT_PORT_TABLE", "VLAN_TABLE", "LAG_TABLE"},
	db.StateDB:     {"PORT_TABLE", "MGMT_PORT_TABLE", "LAG_TABLE"},
	db.ApplStateDB: {"PORT_TABLE", "MGMT_PORT_TABLE", "VLAN_TABLE", "LAG_TABLE", "BRIDGE_TABLE"},
}

var intfOCToSpeedMap = map[ocbinds.E_OpenconfigIfEthernet_ETHERNET_SPEED]string{
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_10MB:   "10",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_100MB:  "100",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_1GB:    "1000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_2500MB: "2500",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_5GB:    "5000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_10GB:   "10000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_25GB:   "25000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_40GB:   "40000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_50GB:   "50000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_100GB:  "100000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_200GB:  "200000",
	ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_400GB:  "400000",
}

var yangToDbDuplexMap = map[ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode]string{
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_UNSET: "UNSET",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_FULL:  "FULL",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_HALF:  "HALF",
}

var dbToYangDuplexMap = map[string]ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode{
	"UNSET": ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_UNSET,
	"FULL":  ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_FULL,
	"HALF":  ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_HALF,
}

var dbToYangFecModeMap = map[string]ocbinds.E_OpenconfigIfEthernet_INTERFACE_FEC{
	"none":              ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_DISABLED,
	"fc":                ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_FC,
	"rs528":             ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_RS528,
	"rs544":             ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_RS544,
	"rs544-interleaved": ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_RS544_2X_INTERLEAVE,
}

var yangToDbFecModeMap = map[ocbinds.E_OpenconfigIfEthernet_INTERFACE_FEC]string{
	ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_DISABLED:            "none",
	ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_FC:                  "fc",
	ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_RS528:               "rs528",
	ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_RS544:               "rs544",
	ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_RS544_2X_INTERLEAVE: "rs544-interleaved",
}

var dbToYangSfecModeMap = map[string]ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode{
	"disable": ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode_DISABLE,
	"auto":    ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode_AUTO,
}

var dbToYangSfecOperModeMap = map[string]ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_SfecOperMode{
	"enable":  ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_SfecOperMode_ENABLED,
	"disable": ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_SfecOperMode_DISABLED,
	UNKNOWN:   ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_SfecOperMode_UNKNOWN,
}

var yangToDbSfecModeMap = map[ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode]string{
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode_DISABLE: "disable",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode_AUTO:    "auto",
}

var yangToDbEcmpHashAlgorithmMap = map[ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm]string{
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC:       "crc",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_XOR:       "xor",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_RANDOM:    "random",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_32LO:  "crc_32lo",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_32HI:  "crc_32hi",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_CCITT: "crc_ccitt",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_XOR:   "crc_xor",
}

var dbToYangEcmpHashAlgorithmMap = map[string]ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm{
	"crc":       ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC,
	"xor":       ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_XOR,
	"random":    ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_RANDOM,
	"crc_32lo":  ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_32LO,
	"crc_32hi":  ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_32HI,
	"crc_ccitt": ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_CCITT,
	"crc_xor":   ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm_CRC_XOR,
}

var yangToDbPortDirectionMap = map[ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection]string{
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_UNKNOWN:            "unknown",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_HOST_FACING:        "host_facing",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_FABRIC_FACING:      "fabric_facing",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_VENDOR_HOST_FACING: "vendor_host_facing",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_FABRIC_DOWNLINK:    "fabric_downlink",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_FABRIC_UPLINK:      "fabric_uplink",
}

var dbToYangPortDirectionMap = map[string]ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection{
	"unknown":            ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_UNKNOWN,
	"host_facing":        ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_HOST_FACING,
	"fabric_facing":      ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_FABRIC_FACING,
	"vendor_host_facing": ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_VENDOR_HOST_FACING,
	"fabric_downlink":    ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_FABRIC_DOWNLINK,
	"fabric_uplink":      ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection_FABRIC_UPLINK,
}

var yangToDbCsigTagActionMap = map[ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction]string{
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction_STRIP_TAG: "strip_tag",
	ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction_KEEP_TAG:  "keep_tag",
}

var dbToYangCsigTagActionMap = map[string]ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction{
	"strip_tag": ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction_STRIP_TAG,
	"keep_tag":  ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction_KEEP_TAG,
}

type E_InterfaceType int64

const (
	IntfTypeUnset       E_InterfaceType = 0
	IntfTypeEthernet    E_InterfaceType = 1
	IntfTypePortChannel E_InterfaceType = 2
	IntfTypeMgmt        E_InterfaceType = 3
	IntfTypeLoopback    E_InterfaceType = 4
	IntfTypeSubIntf     E_InterfaceType = 5
	IntfTypeCpu         E_InterfaceType = 6
	IntfTypeMgmtBond    E_InterfaceType = 7
	IntfTypeBridge      E_InterfaceType = 8
)

type E_InterfaceSubType int64

const (
	IntfSubTypeUnset E_InterfaceSubType = 0
)

var IF_TYPE_MAP = map[E_InterfaceType]ocbinds.E_IETFInterfaces_InterfaceType{
	IntfTypeUnset:       ocbinds.IETFInterfaces_InterfaceType_UNSET,
	IntfTypeEthernet:    ocbinds.IETFInterfaces_InterfaceType_ethernetCsmacd,
	IntfTypeMgmt:        ocbinds.IETFInterfaces_InterfaceType_ethernetCsmacd,
	IntfTypeMgmtBond:    ocbinds.IETFInterfaces_InterfaceType_ieee8023adLag,
	IntfTypePortChannel: ocbinds.IETFInterfaces_InterfaceType_ieee8023adLag,
	IntfTypeLoopback:    ocbinds.IETFInterfaces_InterfaceType_softwareLoopback,
	IntfTypeCpu:         ocbinds.IETFInterfaces_InterfaceType_ethernetCsmacd,
	IntfTypeBridge:      ocbinds.IETFInterfaces_InterfaceType_bridge,
}

var pcs = make(map[string]bool)
var pcMembers = make(map[string]bool)

func getIntfTypeByName(name string) (E_InterfaceType, E_InterfaceSubType, error) {
	if strings.HasPrefix(name, ETHERNET) {
		return IntfTypeEthernet, IntfSubTypeUnset, nil
	} else if strings.HasPrefix(name, MGMT) {
		return IntfTypeMgmt, IntfSubTypeUnset, nil
	} else if strings.HasPrefix(name, MGMT_BOND) {
		return IntfTypeMgmtBond, IntfSubTypeUnset, nil
	} else if strings.HasPrefix(name, PORTCHANNEL) {
		if strings.Contains(name, ".") {
			return IntfTypeSubIntf, IntfSubTypeUnset, nil
		}
		return IntfTypePortChannel, IntfSubTypeUnset, nil
	} else if strings.HasPrefix(name, LOOPBACK) {
		return IntfTypeLoopback, IntfSubTypeUnset, nil
	} else if name == CPU {
		return IntfTypeCpu, IntfSubTypeUnset, nil
	} else if strings.HasPrefix(name, BRIDGE) {
		return IntfTypeBridge, IntfSubTypeUnset, nil
	} else {
		return IntfTypeUnset, IntfSubTypeUnset, errors.New("Interface name prefix not matched with supported types")
	}
}

func getIntfsRoot(s *ygot.GoStruct) *ocbinds.OpenconfigInterfaces_Interfaces {
	if s == nil {
		return nil
	}
	deviceObj := (*s).(*ocbinds.Device)
	return deviceObj.Interfaces
}

func getPortTableNameByDBId(intftbl IntfTblData, curDb db.DBNum) (string, error) {

	tblName := intftbl.cfgDb.portTN

	switch curDb {
	case db.ApplStateDB:
		tblName = intftbl.appStateDb.portTN
	case db.ApplDB:
		tblName = intftbl.appDb.portTN
	case db.StateDB:
		tblName = intftbl.stateDb.portTN
	}

	return tblName, nil
}

/* Perform action based on the operation and Interface type wrt Interface name key */
/* It should handle only Interface name key xfmr operations */
func performIfNameKeyXfmrOp(inParams *XfmrParams, requestUriPath *string, ifName *string, ifType E_InterfaceType, subintfid uint32) error {
	var err error
	switch inParams.oper {
	case DELETE:
		if strings.HasPrefix(*requestUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface") && subintfid != 0 {
			errStr := "Subinterfaces not supported"
			log.Error(errStr)
			return tlerr.NotSupported(errStr)
		}

		if *requestUriPath == "/openconfig-interfaces:interfaces/interface" {
			switch ifType {
			case IntfTypePortChannel:
				err := deleteLagIntfAndMembers(inParams, ifName)
				if err != nil {
					log.V(lvl.ERROR).Infof("Deleting LAG: %s failed! Err:%v", *ifName, err)
					return tlerr.InvalidArgsError{Format: err.Error()}
				}
			case IntfTypeEthernet:
				err = validateIntfExists(inParams.d, IntfTypeTblMap[IntfTypeEthernet].cfgDb.portTN, *ifName)
				if err != nil {
					// Not returning error from here since mgmt infra will return "Resource not found" error in case of non existence entries
					return nil
				}
			case IntfTypeBridge:
				if validateIntfExists(inParams.d, IntfTypeTblMap[IntfTypeBridge].cfgDb.portTN, *ifName) != nil {
					return tlerr.InvalidArgsError{Format: "Bridge Interface: " + *ifName + " doesn't exist and cannot be deleted"}
				}
				if err := deleteBridgeIntf(inParams, ifName); err != nil {
					log.V(lvl.ERROR).Infof("Deleting Bridge interface: %s failed! Err:%s", *ifName, err.Error())
					return tlerr.InvalidArgsError{Format: err.Error()}
				}
			default:
				return tlerr.InvalidArgsError{Format: "Invalid interface for delete:" + *ifName}
			}
		}
	case CREATE:
		fallthrough
	case UPDATE, REPLACE:
		if ifType == IntfTypeEthernet {
			// Validate existence of physical ports for UPDATE Set config only for DPB (b/204217582)
			if inParams.oper == UPDATE {
				if err = validateIntfExists(inParams.d, IntfTypeTblMap[IntfTypeEthernet].cfgDb.portTN, *ifName); err != nil {
					return tlerr.InvalidArgsError{Format: "Interface " + *ifName + " cannot be configured; err = " + err.Error()}
				}
			}
			if inParams.oper == REPLACE {
				if *requestUriPath == "/openconfig-interfaces:interfaces/interface" ||
					*requestUriPath == "/openconfig-interfaces:interfaces/interface/config" {
					// OC interfaces yang does not have attributes to set Physical interface critical attributes like speed, alias, lanes, index.
					// Replace/PUT request without the critical attributes would end up in deletion of the same in PORT table, which cannot be allowed.
					// Hence block the Replace/PUT request for Physical interfaces alone.
					return tlerr.NotSupported("Replace/PUT request not allowed for Physical interfaces")
				}
			}
		}
		/* Google: removing this check; a replace should be able to create an entry.  And why only port channels?!
		if ifType == IntfTypePortChannel {
			if (inParams.oper == UPDATE) || (inParams.oper == REPLACE) {
				err = validateIntfExists(inParams.d, IntfTypeTblMap[IntfTypePortChannel].cfgDb.portTN, *ifName)
				if err != nil { //No Matching PortChannel to UPDATE/REPLACE
					errStr := "PortChannel: " + *ifName + " does not exist"
					return tlerr.InvalidArgsError{Format: errStr}
				}
			}
		}
		*/
	}
	return err
}

/* Validate interface in L3 mode, if true return error */
/* Google: Removing this code from upstream as it is not used (yet?)
func validateL3ConfigExists(d *db.DB, ifName *string) error {
	intfType, _, ierr := getIntfTypeByName(*ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return errors.New("Invalid interface type IntfTypeUnset")
	}
	intTbl := IntfTypeTblMap[intfType]
	IntfMapObj, err := d.GetEntry(&db.TableSpec{Name: intTbl.cfgDb.intfTN}, db.Key{Comp: []string{*ifName}})
	if err == nil && IntfMapObj.IsPopulated() {
		errStr := "L3 Configuration exists for Interface: " + *ifName

		// L3 config exists if interface in interface table
		return tlerr.InvalidArgsError{Format: errStr}
	}
	return nil
}
*/

func processIntfTableRemoval(d *db.DB, ifName string, tblName string, intfMap map[string]db.Value) {
	intfKey, _ := d.GetKeysByPattern(&db.TableSpec{Name: tblName}, "*"+ifName)
	if len(intfKey) != 0 {
		key := ifName
		intfMap[key] = db.Value{Field: map[string]string{}}
	}
}

/* Validate whether intf exists in DB */
func validateIntfExists(d *db.DB, intfTs string, ifName string) error {
	if len(ifName) == 0 {
		return errors.New("Length of Interface name is zero")
	}
	entry, err := d.GetEntry(&db.TableSpec{Name: intfTs}, db.Key{Comp: []string{ifName}})
	if err != nil || !entry.IsPopulated() {
		return tlerr.InvalidArgsError{Format: "Invalid Interface:" + ifName}
	}
	return nil
}

func updateDefaultMtu(inParams *XfmrParams, ifName *string, ifType E_InterfaceType, resMap map[string]string) error {
	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	intfMap := make(map[string]map[string]db.Value)

	intTbl := IntfTypeTblMap[ifType]
	resMap["mtu"] = strconv.FormatUint(uint64(DEFAULT_MTU-DEFAULT_L2_HEADER_SIZE), 10)

	intfMap[intTbl.cfgDb.portTN] = make(map[string]db.Value)
	intfMap[intTbl.cfgDb.portTN][*ifName] = db.Value{Field: resMap}

	subOpMap[db.ConfigDB] = intfMap
	inParams.subOpDataMap[UPDATE] = &subOpMap
	return nil
}

func getDbToYangSpeed(speed string) (ocbinds.E_OpenconfigIfEthernet_ETHERNET_SPEED, error) {
	portSpeed := ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_SPEED_UNKNOWN
	var err error = errors.New("Not found in port speed map")
	for k, v := range intfOCToSpeedMap {
		if speed == v {
			portSpeed = k
			err = nil
		}
	}
	return portSpeed, err
}

var intf_table_xfmr TableXfmrFunc = func(inParams XfmrParams) ([]string, error) {
	var tblList []string
	var err error

	pathInfo := NewPathInfo(inParams.uri)

	targetUriPath, err := getYangPathFromUri(pathInfo.Path)

	ifName := pathInfo.Var("name")
	if ifName == "" || ifName == "*" {
		log.V(lvl.DEBUG).Info("TableXfmrFunc - intf_table_xfmr Intf key is not present")

		if db, ok := dbIdToTblMap[inParams.curDb]; !ok {
			log.V(lvl.ERROR).Info("TableXfmrFunc - intf_table_xfmr db id entry not present")
			return tblList, errors.New("Key not present")
		} else {
			return db, nil
		}
	}

	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return tblList, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	intTbl := IntfTypeTblMap[intfType]
	log.V(lvl.DEBUG).Info("TableXfmrFunc - targetUriPath : ", targetUriPath)

	if inParams.oper == DELETE && (targetUriPath == "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4" ||
		targetUriPath == "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6") {
		return tblList, tlerr.New("DELETE operation not allowed on  this container")

	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/config") {
		tblList = append(tblList, intTbl.cfgDb.portTN)
	} else if intfType != IntfTypePortChannel &&
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation") {
		//Checking interface type at container level, if not PortChannel type return nil
		return nil, nil
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/state/counters") {
		tblList = append(tblList, "NONE")
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/pfc") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/google-pins-interfaces:pfc") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/pfc") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/google-pins-interfaces:pfc") {
		tblList = append(tblList, "NONE")
	} else if strings.HasSuffix(targetUriPath, "transceiver-qualified") {
		tblList = append(tblList, intTbl.stateDb.portTN)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/hold-time/state") {
		tblList = append(tblList, intTbl.appStateDb.portTN)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/addresses/address/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/addresses/address/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/addresses/address/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/addresses/address/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/unnumbered/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/unnumbered/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/unnumbered/config") {
		tblList = append(tblList, intTbl.cfgDb.intfTN)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/addresses/address/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/addresses/address/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/addresses/address/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/addresses/address/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/unnumbered/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/unnumbered/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/unnumbered/state") {
		tblList = append(tblList, intTbl.appStateDb.intfTN)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/addresses") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/addresses") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/addresses") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/addresses") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6") {
		tblList = append(tblList, intTbl.cfgDb.intfTN)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet") {
		if inParams.oper != DELETE {
			tblList = append(tblList, intTbl.cfgDb.portTN)
		}
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/hold-time") {
		tblList = append(tblList, intTbl.cfgDb.portTN)
	} else if targetUriPath == "/openconfig-interfaces:interfaces/interface" {
		tblList = append(tblList, intTbl.cfgDb.portTN)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface") {
		if inParams.oper != DELETE {
			tblList = append(tblList, intTbl.cfgDb.portTN)
		}
	} else {
		err = errors.New("Invalid URI")
	}

	log.V(lvl.DEBUG).Infof("TableXfmrFunc - Uri: (%v), targetUriPath: %s, tblList: (%v)\r\n", inParams.uri, targetUriPath, tblList)

	return tblList, err
}

var YangToDb_intf_tbl_key_xfmr KeyXfmrYangToDb = func(inParams XfmrParams) (string, error) {
	var err error

	pathInfo := NewPathInfo(inParams.uri)
	requestUriPath, _ := getYangPathFromUri(inParams.requestUri)
	log.V(lvl.DEBUG).Infof("YangToDb_intf_tbl_key_xfmr: inParams.uri: %s, pathInfo: %s, inParams.requestUri: %s", inParams.uri, pathInfo, requestUriPath)

	reqpathInfo := NewPathInfo(inParams.requestUri)
	ifName := pathInfo.Var("name")
	idx := reqpathInfo.Var("index")
	var i32 uint32
	i32 = 0
	if idx != "" {
		i64, _ := strconv.ParseUint(idx, 10, 32)
		i32 = uint32(i64)
	}
	if ifName != "" && ifName != "*" {
		log.V(lvl.DEBUG).Info("YangToDb_intf_tbl_key_xfmr: ifName: ", ifName)
		intfType, _, ierr := getIntfTypeByName(ifName)
		if ierr != nil {
			log.V(lvl.ERROR).Infof("Extracting Interface type for Interface: %s failed!", ifName)
			return "", tlerr.New(ierr.Error())
		}
		err = performIfNameKeyXfmrOp(&inParams, &requestUriPath, &ifName, intfType, i32)
		if err != nil {
			return "", tlerr.InvalidArgsError{Format: err.Error()}
		}
	}
	return ifName, err
}

var DbToYang_intf_tbl_key_xfmr KeyXfmrDbToYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	/* Code for DBToYang - Key xfmr. */
	log.V(lvl.DEBUG).Info("Entering DbToYang_intf_tbl_key_xfmr")
	res_map := make(map[string]interface{})

	log.V(lvl.DEBUG).Info("DbToYang_intf_tbl_key_xfmr: Interface Name = ", inParams.key)
	res_map["name"] = inParams.key
	return res_map, nil
}

var DbToYang_intf_admin_status_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_admin_status_xfmr - Invalid interface type IntfTypeUnset")
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, errors.New("DbToYang_intf_admin_status_xfmr: Invalid interface type " + strconv.Itoa(int(intfType)))
	}
	intTbl := IntfTypeTblMap[intfType]
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_admin_status_xfmr table name not found : ", intTbl)
		return nil, errors.New("DbToYang_intf_admin_status_xfmr table name not found")
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	dbField := PORT_ADMIN_STATUS
	adminStatus, ok := prtInst.Field[dbField]
	if !ok {
		log.V(lvl.ERROR).Info("Admin status field not found in DB for interface " + ifName)
		return nil, errors.New("Admin status field not found in DB for interface " + ifName)
	}
	var status ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_State_AdminStatus
	if adminStatus == "up" {
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_AdminStatus_UP
	} else {
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_AdminStatus_DOWN
	}
	result := make(map[string]interface{})
	result["admin-status"] = ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_State_AdminStatus.ΛMap(status)["E_OpenconfigInterfaces_Interfaces_Interface_State_AdminStatus"][int64(status)].Name
	return result, nil
}

var DbToYang_intf_enabled_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return nil, fmt.Errorf("DbToYang_intf_enabled_xfmr Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	if intfType == IntfTypeCpu || intfType == IntfTypeLoopback || intfType == IntfTypeMgmt || intfType == IntfTypeMgmtBond {
		return nil, errors.New("DbToYang_intf_enabled_xfmr: Invalid Interface Type")
	}
	intTbl := IntfTypeTblMap[intfType]
	tblName, err := getPortTableNameByDBId(intTbl, db.ConfigDB)
	if err != nil {
		return nil, errors.New("DbToYang_intf_enabled_xfmr table name not found. Err: " + err.Error())
	}
	// This is a bookkeeping attribute, it is always fetched from the ConfigDB
	cfgDB := inParams.dbs[db.ConfigDB]
	if cfgDB == nil {
		cfgDB, err = db.NewDB(getDBOptions(db.ConfigDB))
		if err != nil {
			return nil, tlerr.InvalidArgsError{Format: err.Error()}
		}
		defer cfgDB.DeleteDB()
	}
	prtInst, dbErr := cfgDB.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{ifName}})
	if dbErr != nil {
		return nil, dbErr
	}
	adminStatus, ok := prtInst.Field[PORT_ADMIN_STATUS]
	if !ok {
		return nil, errors.New("Admin status field not found in DB")
	}
	result := make(map[string]interface{})
	if adminStatus == "up" {
		result["enabled"] = true
	} else {
		result["enabled"] = false
	}
	return result, nil
}

var YangToDb_intf_mtu_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	var ifName string
	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || len(intfsObj.Interface) < 1 {
		return res_map, nil
	} else {
		for infK := range intfsObj.Interface {
			ifName = infK
		}
	}
	intfType, _, _ := getIntfTypeByName(ifName)
	if inParams.oper == DELETE {
		log.V(lvl.DEBUG).Infof("Updating the Interface: %s with default MTU", ifName)
		if intfType == IntfTypeLoopback {
			log.V(lvl.DEBUG).Infof("MTU not supported for Loopback Interface Type: %d", intfType)
			return res_map, nil
		}
		/* Note: For the mtu delete request, res_map with delete operation and
		   subOp map with update operation (default MTU value) is filled. This is because, transformer default
		   updates the result DS for delete oper with table and key. This needs to be fixed by transformer
		   for deletion of an attribute */
		err := updateDefaultMtu(&inParams, &ifName, intfType, res_map)
		if err != nil {
			log.V(lvl.ERROR).Infof("Updating Default MTU for Interface: %s failed", ifName)
			return res_map, err
		}
		return res_map, nil
	}
	// Handles all the operations other than Delete
	intfTypeVal, _ := inParams.param.(*uint16)
	intTypeValStr := strconv.FormatUint(uint64(*intfTypeVal)-uint64(DEFAULT_L2_HEADER_SIZE), 10)

	if IntfTypePortChannel == intfType {
		/* Apply the MTU to all the portchannel member ports */
		updateMemberPortsMtu(&inParams, &ifName, &intTypeValStr)
	} else if IntfTypeEthernet == intfType {
		/* Do not allow MTU configuration on a portchannel member port */
		/* Google: Removing this check from upstream since we do explicitly configure MTUs on port channel members
		lagId, _ := retrievePortChannelAssociatedWithIntf(&inParams, &ifName)
		if lagId != nil {
			log.Infof("%s is member of %s", ifName, *lagId)
			errStr := "Configuration not allowed when port is member of Portchannel."
			return nil, tlerr.InvalidArgsError{Format: errStr}
		}
		*/
	}

	res_map["mtu"] = intTypeValStr
	return res_map, nil
}

var DbToYang_intf_mtu_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_mtu_xfmr - Invalid interface type IntfTypeUnset")
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel && intfType != IntfTypeBridge {
		return nil, errors.New("DbToYang_intf_mtu_xfmr: Invalid interface type " + strconv.Itoa(int(intfType)))
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		log.V(lvl.DEBUG).Info("DbToYang_intf_mtu_xfmr interface type not found : ", intfType)
		return nil, errors.New("interface type not found.")
	}

	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_mtu_xfmr table name not found")
		return nil, errors.New("table name not found. Err: " + err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}
	mtuStr, ok := prtInst.Field["mtu"]
	if !ok {
		log.V(lvl.ERROR).Info("DbToYang_intf_mtu_xfmr MTU is not found in DB")
		return nil, errors.New("DbToYang_intf_mtu_xfmr MTU is not found in DB")
	}
	result := make(map[string]interface{})
	mtuVal, err := strconv.ParseFloat(mtuStr, 64)
	if err != nil {
		return result, err
	}
	result["mtu"] = mtuVal + DEFAULT_L2_HEADER_SIZE
	return result, nil
}

// YangToDb_intf_eth_port_config_xfmr handles port-speed, auto-neg, aggregate-id, port-fec xor fec-mode, sfec-mode, unreliable-los and enable-pfc config.
var YangToDb_intf_eth_port_config_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	var err error
	var lagStr string
	memMap := make(map[string]map[string]db.Value)
	resMap := make(map[string]string)

	pathInfo := NewPathInfo(inParams.uri)
	requestUriPath := (NewPathInfo(inParams.requestUri)).YangPath
	ifName := pathInfo.Var("name")

	log.V(lvl.DEBUG).Infof("YangToDb_intf_eth_port_config_xfmr: inParams.uri: %s, pathInfo: %s, inParams.requestUri: %s, InParams.oper %v", inParams.uri, pathInfo, requestUriPath, inParams.oper)
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: "Invalid Interface " + ifName}
	}
	if intfType == IntfTypeBridge {
		// These config paths do not apply to bridge interfaces.
		return nil, nil
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		log.V(lvl.ERROR).Info("YangToDb_intf_eth_port_config_xfmr interface type not found : ", intfType)
		return nil, errors.New("interface type not found.")
	}

	intfsObj := getIntfsRoot(inParams.ygRoot)
	intfObj := intfsObj.Interface[ifName]

	// Need to differentiate between config container delete and any other attribute delete
	if inParams.oper == DELETE {
		/* Handles 3 cases
		   case 1: Deletion request at top-level container / list
		   case 2: Deletion request at ethernet container level
		   case 3: Deletion request at ethernet/config container level */

		//case 1
		if intfObj.Ethernet == nil ||
			//case 2
			intfObj.Ethernet.Config == nil ||
			//case 3
			(intfObj.Ethernet.Config != nil && requestUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config") {

			// Delete all the Vlans for Interface and member port removal from port-channel
			lagId, err := retrievePortChannelAssociatedWithIntf(&inParams, &ifName)
			if lagId != nil {
				log.V(lvl.DEBUG).Infof("%s is member of %s", ifName, *lagId)
			}
			if err != nil {
				errStr := "Retrieveing PortChannel associated with Interface: " + ifName + " failed!"
				return nil, errors.New(errStr)
			}
			if lagId != nil {
				lagStr = *lagId
				intTbl := IntfTypeTblMap[IntfTypePortChannel]
				tblName, _ := getMemTableNameByDBId(intTbl, inParams.curDb)

				dbValue := db.Value{Field: map[string]string{"NULL": "NULL"}}
				intfKey := lagStr + "|" + ifName
				tblMap := map[string]db.Value{intfKey: dbValue}
				return map[string]map[string]db.Value{tblName: tblMap}, nil
			}
			return nil, err
		}
	}

	/* Handle AggregateId config */
	if intfObj.Ethernet.Config.AggregateId != nil {
		if !strings.HasPrefix(ifName, ETHERNET) {
			return nil, errors.New("Invalid config request")
		}
		intTbl := IntfTypeTblMap[IntfTypePortChannel]
		tblName, _ := getMemTableNameByDBId(intTbl, inParams.curDb)

		switch inParams.oper {
		case CREATE:
			fallthrough
		case REPLACE:
			fallthrough
		case UPDATE:
			lagId := intfObj.Ethernet.Config.AggregateId
			lagStr = *lagId
			pcMembers[lagStr+"|"+ifName] = true
			log.V(lvl.DEBUG).Infof("Add member port %s", lagStr)

			intfType, _, err := getIntfTypeByName(ifName)
			if intfType != IntfTypeEthernet || err != nil {
				intfTypeStr := strconv.Itoa(int(intfType))
				errStr := "Invalid interface type" + intfTypeStr
				log.V(lvl.DEBUG).Info(errStr)
				return nil, tlerr.InvalidArgsError{Format: errStr}
			}

			/* Check if given iface already part of another PortChannel */
			intf_lagId, _ := retrievePortChannelAssociatedWithIntf(&inParams, &ifName)
			if intf_lagId != nil && *intf_lagId != lagStr {
				return nil, tlerr.InvalidArgsError{Format: ifName + " already member of " + *intf_lagId}
			}
			/* Check if the PortChannel is being created. */
			_, ok := intfsObj.Interface[lagStr]
			if !ok {
				/* Check if PortChannel exists */
				err = validateIntfExists(inParams.d, intTbl.cfgDb.portTN, lagStr)
				if err != nil {
					return nil, err
				}
			}

		case DELETE:
			lagId, err := retrievePortChannelAssociatedWithIntf(&inParams, &ifName)
			if lagId != nil {
				log.V(lvl.DEBUG).Infof("%s is member of %s", ifName, *lagId)
			}
			if lagId == nil || err != nil {
				return nil, nil
			}
			lagStr = *lagId
		} /* End of switch case */
		if len(lagStr) != 0 {
			intfKey := lagStr + "|" + ifName
			if _, ok := memMap[tblName]; !ok {
				memMap[tblName] = make(map[string]db.Value)
			}
			memMap[tblName][intfKey] = db.Value{Field: map[string]string{"NULL": "NULL"}}
		}
	}

	/* Handle PortSpeed config */
	if intfObj.Ethernet.Config.PortSpeed != 0 {
		portSpeed := intfObj.Ethernet.Config.PortSpeed
		val, ok := intfOCToSpeedMap[portSpeed]
		if ok {
			if err = validateSpeed(inParams.d, ifName, val); err == nil {
				resMap[PORT_SPEED] = val
				resMap[ADV_PORT_SPEED] = val
			}
		} else {
			err = tlerr.InvalidArgs("Invalid speed %s", val)
		}
	}
	// Handle FEC config. fec-mode and port-fec are mutually exclusive
	fecModeSet := intfObj.Ethernet.Config.FecMode != ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_UNSET
	if fecModeSet {
		fecMode := intfObj.Ethernet.Config.FecMode
		if inParams.oper == DELETE {
			fecMode = ocbinds.OpenconfigIfEthernet_INTERFACE_FEC_FEC_DISABLED
		}
		if fecModeVal, ok := yangToDbFecModeMap[fecMode]; !ok {
			err = tlerr.InvalidArgs("Invalid fec-mode %s", fecMode)
			log.V(lvl.ERROR).Info("Did not find fec-mode entry")
		} else {
			resMap[PORT_FEC] = fecModeVal
			resMap[ADV_PORT_FEC] = fecModeVal
			log.V(lvl.DEBUG).Infof("Setting fec-mode: %s", fecModeVal)
		}
	}

	/* Handle duplex-mode config */
	if strings.Contains(inParams.requestUri, "duplex-mode") {
		duplex := intfObj.Ethernet.Config.DuplexMode
		val, ok := yangToDbDuplexMap[duplex]
		if !ok {
			err = tlerr.InvalidArgs("Invalid unreliable duplex %s", duplex)
			log.V(lvl.ERROR).Infof("Did not find valid duplex configuration entry")
		} else {
			/* Need the number of lanes */
			log.V(lvl.DEBUG).Infof("Configuring duplex of port %s to %s", ifName, val)
			resMap["duplex-mode"] = val
		}
	}
	/* Handle AutoNegotiate config */
	if intfObj.Ethernet.Config.AutoNegotiate != nil {
		autoNeg := intfObj.Ethernet.Config.AutoNegotiate
		var enStr string
		if *autoNeg {
			enStr = "on"
		} else {
			enStr = "off"
		}
		resMap[PORT_AUTONEG] = enStr
	}
	/* Handle Enable PFC config */
	if intfObj.Ethernet.Config.EnablePfcRx != nil {
		pfc := intfObj.Ethernet.Config.EnablePfcRx
		var enPfcStr string
		/* TODO(b/361822295): Uncomment and read from PORT_QOS_MAP after CVL changes
		if _, ok := memMap["PORT_QOS_MAP"]; !ok {
			memMap["PORT_QOS_MAP"] = make(map[string]db.Value)
		}
		*/
		if *pfc {
			enPfcStr = "on"
			/* TODO(b/361822295): Uncomment and read from PORT_QOS_MAP after CVL changes
			// PORT_QOS_MAP
			subOpMap := map[db.DBNum]map[string]map[string]db.Value{
				db.ConfigDB: map[string]map[string]db.Value{
					"PORT_QOS_MAP": map[string]db.Value{
							ifName: db.Value{
								Field: map[string]string{
									"pfc_enable":       "0,1,2,3,4,5,6,7",
									"pfcwd_sw_enable":  "0,1,2,3,4,5,6,7",
									"pfc_to_queue_map": "default_pfc_to_queue_map",
								},
							},
						},
					},
				}
			updateSubOpDataMap(subOpMap, REPLACE, inParams)
			*/
			// PFC_WD
			if _, ok := memMap["PFC_WD"]; !ok {
				memMap["PFC_WD"] = make(map[string]db.Value)
			}
			memMap["PFC_WD"] = map[string]db.Value{
				ifName: db.Value{
					Field: map[string]string{
						"action":           "forward",
						"detection_time":   "1000",
						"restoration_time": "1000",
					}}}
		} else {
			enPfcStr = "off"
			/* TODO(b/361822295): Uncomment and read from PORT_QOS_MAP after CVL changes
			if entry, err := inParams.d.GetEntry(&db.TableSpec{Name: "PORT_QOS_MAP"}, db.Key{Comp: []string{ifName}}); err == nil && entry.IsPopulated() {
				subOpMap := map[db.DBNum]map[string]map[string]db.Value{
					db.ConfigDB: map[string]map[string]db.Value{
						"PORT_QOS_MAP": map[string]db.Value{
							ifName: db.Value{
								Field: map[string]string{
									"pfc_enable":       "",
									"pfcwd_sw_enable":  "",
									"pfc_to_queue_map": "",
								},
							},
						},
					},
				}
				updateSubOpDataMap(subOpMap, DELETE, inParams)
			}
			*/
			// Delete PFC_WD entry for the interface.
			if entry, err := inParams.d.GetEntry(&db.TableSpec{Name: "PFC_WD"}, db.Key{Comp: []string{ifName}}); err == nil && entry.IsPopulated() {
				subOpMap := map[db.DBNum]map[string]map[string]db.Value{
					db.ConfigDB: map[string]map[string]db.Value{
						"PFC_WD": map[string]db.Value{
							ifName: db.Value{},
						},
					},
				}
				updateSubOpDataMap(subOpMap, DELETE, inParams)
			}
		}
		resMap[PORT_PFC_ENABLE] = enPfcStr
	}
	/* Handle sfec-mode config */
	sfecMode := intfObj.Ethernet.Config.SfecMode
	if sfecMode != ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode_UNSET {
		if inParams.oper == DELETE {
			sfecMode = ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode_UNSET
		}
		sfecModeVal, ok := yangToDbSfecModeMap[sfecMode]
		if !ok {
			err = tlerr.InvalidArgs("Invalid sfec-mode %s", sfecMode)
			log.V(lvl.ERROR).Info("Did not find sfec-mode entry")
		}
		resMap[PORT_SFEC_MODE] = sfecModeVal
		log.V(lvl.DEBUG).Infof("Setting fec-mode: %s", sfecModeVal)
	}
	/* Handle Mac-address config */
	if intfObj.Ethernet.Config.MacAddress != nil {
		macAddr := *(intfObj.Ethernet.Config.MacAddress)
		resMap["mac-address"] = macAddr
	}
	/* Handle Forwarding-viable config */
	if intfObj.Ethernet.Config.ForwardingViable != nil {
		fwdViable := intfObj.Ethernet.Config.ForwardingViable
		fwdViableStr := "true"
		if !(*fwdViable) {
			fwdViableStr = "false"
		}
		resMap[PORT_FWD_VIABLE] = fwdViableStr
	}
	/* Handle Link Training config */
	if intfObj.Ethernet.Config.StandaloneLinkTraining != nil {
		linkTrainingStr := "on"
		if !(*(intfObj.Ethernet.Config.StandaloneLinkTraining)) {
			linkTrainingStr = "off"
		}
		resMap["link_training"] = linkTrainingStr
	}
	if intfObj.Ethernet.Config.IngressDelay != nil {
		bits := binary.BigEndian.Uint32(intfObj.Ethernet.Config.IngressDelay)
		f := math.Float32frombits(bits)
		resMap[PORT_INGRESS_DELAY] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}
	if intfObj.Ethernet.Config.EgressDelay != nil {
		bits := binary.BigEndian.Uint32(intfObj.Ethernet.Config.EgressDelay)
		f := math.Float32frombits(bits)
		resMap[PORT_EGRESS_DELAY] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}

	if intfObj.Ethernet.Config.InsertEgressTimestamp != nil {
		egressTimestamp := strconv.FormatBool(*intfObj.Ethernet.Config.InsertEgressTimestamp)
		resMap[PORT_EGRESS_TIMESTAMP] = egressTimestamp
	}

	if intfObj.Ethernet.Config.InsertIngressTimestamp != nil {
		ingressTimestamp := strconv.FormatBool(*intfObj.Ethernet.Config.InsertIngressTimestamp)
		resMap[PORT_INGRESS_TIMESTAMP] = ingressTimestamp
	}

	if len(resMap) > 0 {
		memMap[intTbl.cfgDb.portTN] = map[string]db.Value{
			ifName: db.Value{
				Field: resMap,
			},
		}
	}
	return memMap, err
}

// DbToYang_intf_eth_port_config_xfmr is to handle DB to yang translation of port-speed, auto-neg and aggregate-id config.
var DbToYang_intf_eth_port_config_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	var err error
	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	uriIfName := pathInfo.Var("name")
	ifName := uriIfName

	log.V(3).Infof("DbToYang_intf_eth_port_config_xfmr: Interface name : %s ", ifName)

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		errStr := "Invalid Interface"
		err = tlerr.InvalidArgsError{Format: errStr}
		return err
	}
	intTbl := IntfTypeTblMap[intfType]
	tblName := intTbl.cfgDb.portTN
	entry, dbErr := inParams.dbs[db.ConfigDB].GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{ifName}})
	if dbErr != nil {
		errStr := "Invalid Interface"
		err = tlerr.InvalidArgsError{Format: errStr}
		return err
	}
	targetUriPath := pathInfo.YangPath
	if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config") {
		get_cfg_obj := false
		var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
		if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
			var ok bool = false
			if intfObj, ok = intfsObj.Interface[uriIfName]; !ok {
				intfObj, _ = intfsObj.NewInterface(uriIfName)
			}
			ygot.BuildEmptyTree(intfObj)
		} else {
			ygot.BuildEmptyTree(intfsObj)
			intfObj, _ = intfsObj.NewInterface(uriIfName)
			ygot.BuildEmptyTree(intfObj)
		}
		ygot.BuildEmptyTree(intfObj.Ethernet)
		ygot.BuildEmptyTree(intfObj.Ethernet.Config)

		if targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config" {
			get_cfg_obj = true
		}
		var errStr string

		if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/openconfig-if-aggregate:aggregate-id" {
			is_id_populated := false
			intf_lagId, _ := retrievePortChannelAssociatedWithIntf(&inParams, &ifName)
			if intf_lagId != nil {
				if strings.HasPrefix(*intf_lagId, "PortChannel") {
					intfObj.Ethernet.Config.AggregateId = intf_lagId
					is_id_populated = true
				}
			}
			if !is_id_populated {
				errStr = "aggregate-id not set"
			}

			// subscribe for aggregate-id needs "Resource not found" for delete notification
			if (targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/openconfig-if-aggregate:aggregate-id") && (!is_id_populated) {
				err = tlerr.NotFoundError{Format: "Resource not found"}
				return err
			}
		}

		if entry.IsPopulated() {
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/auto-negotiate" {
				autoNeg, ok := entry.Field[PORT_AUTONEG]
				if ok {
					var oc_auto_neg bool
					if autoNeg == "on" || autoNeg == "true" {
						oc_auto_neg = true
					} else {
						oc_auto_neg = false
					}
					intfObj.Ethernet.Config.AutoNegotiate = &oc_auto_neg
				} else {
					errStr = "auto-negotiate not set"
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/port-speed" {
				speed, ok := entry.Field[PORT_SPEED]
				portSpeed := ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_UNSET
				if ok {
					portSpeed, err = getDbToYangSpeed(speed)
					intfObj.Ethernet.Config.PortSpeed = portSpeed
				} else {
					errStr = "port-speed not set"
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/duplex-mode" {
				if duplex, ok := entry.Field["duplex-mode"]; !ok {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: duplex-mode not set in DB, returning default duplex-mode for : %s", ifName)
				} else {
					oc_duplex, err := getDbToYangDuplex(duplex)
					if err != nil {
						log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: duplex-mode field not found in DB")
					} else {
						intfObj.Ethernet.Config.DuplexMode = oc_duplex
					}
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/mac-address" {
				if macAddr, ok := entry.Field["mac-address"]; ok {
					intfObj.Ethernet.Config.MacAddress = &macAddr
				} else {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: mac-address not set in DB, returning default mac-address for : %s", ifName)
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/standalone-link-training" {
				if linkTraining, ok := entry.Field["link_training"]; ok {
					linkTrainingVal := linkTraining == "on"
					intfObj.Ethernet.Config.StandaloneLinkTraining = &linkTrainingVal
				} else {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: link_training not set in DB, returning default link_training for : %s", ifName)
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/fec-mode" {
				if fec, ok := entry.Field[PORT_FEC]; !ok {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: port-fec field not found in DB")
					log.V(lvl.DEBUG).Info("DbToYang_intf_eth_port_config_xfmr: fec-mode field not found in DB")
				} else {
					if fecMode, ok := dbToYangFecModeMap[fec]; !ok {
						log.V(lvl.DEBUG).Info("DbToYang_intf_eth_port_config_xfmr: fec-mode field not found in lookup table")
					} else {
						intfObj.Ethernet.Config.FecMode = fecMode
					}
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:forwarding-viable" {
				if fwdViable, ok := entry.Field[PORT_FWD_VIABLE]; ok {
					fwdViableVal := fwdViable != "false"
					intfObj.Ethernet.Config.ForwardingViable = &fwdViableVal
				} else {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: forwarding-viable not set in DB, returning default forwarding-viable for : %s", ifName)
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:sfec-mode" {
				if sfec_mode, ok := entry.Field[PORT_SFEC_MODE]; ok {
					if sfecMode, ok := dbToYangSfecModeMap[sfec_mode]; !ok {
						log.V(lvl.DEBUG).Info("DbToYang_intf_eth_port_config_xfmr: sfec-mode field not found in lookup table")
					} else {
						intfObj.Ethernet.Config.SfecMode = sfecMode
					}
				} else {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: sfec_mode not set in DB")
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:ingress-delay" {
				if ingress_delay, ok := entry.Field[PORT_INGRESS_DELAY]; ok {
					if intfObj.Ethernet.Config.IngressDelay, err = float32StrTo4Bytes(ingress_delay); err != nil {
						log.V(lvl.DEBUG).Infof("Error in converting ingress_delay float32-str to binary: ", err)
					}
				} else {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: ingress-delay not set in DB")
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:egress-delay" {
				if egress_delay, ok := entry.Field[PORT_EGRESS_DELAY]; ok {
					if intfObj.Ethernet.Config.EgressDelay, err = float32StrTo4Bytes(egress_delay); err != nil {
						log.V(lvl.DEBUG).Infof("Error in converting egress-delay float32-str to binary: ", err)
					}
				} else {
					log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_port_config_xfmr: egress-delay not set in DB")
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:insert-ingress-timestamp" {
				if ingressTimestamp, ok := entry.Field[PORT_INGRESS_TIMESTAMP]; ok && ingressTimestamp != "" && intfType == IntfTypeEthernet {
					if insertIngressTimestamp, err := strconv.ParseBool(ingressTimestamp); err != nil {
						log.V(lvl.DEBUG).Infof("Error in converting insert-ingress-imestamp str to bool: ", err)
					} else {
						intfObj.Ethernet.Config.InsertIngressTimestamp = &insertIngressTimestamp
					}
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:insert-egress-timestamp" {
				if egressTimestamp, ok := entry.Field[PORT_EGRESS_TIMESTAMP]; ok && egressTimestamp != "" && intfType == IntfTypeEthernet {
					if insertEgressTimestamp, err := strconv.ParseBool(egressTimestamp); err != nil {
						log.V(lvl.DEBUG).Infof("Error in converting insert-egress-imestamp str to bool: ", err)
					} else {
						intfObj.Ethernet.Config.InsertEgressTimestamp = &insertEgressTimestamp
					}
				}
			}
			if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/config/google-pins-if:enable-pfc-rx" {
				if pfc, ok := entry.Field[PORT_PFC_ENABLE]; ok {
					pfcEnable := pfc == "on"
					intfObj.Ethernet.Config.EnablePfcRx = &pfcEnable
				} else {
					log.V(lvl.DEBUG).Info("pfc enable not set")
				}
			}

		} else {
			errStr = "Attribute not set"
		}
		if !get_cfg_obj && errStr != "" {
			err = tlerr.InvalidArgsError{Format: errStr}
		}
	}

	return err
}

/* Google: removing this code as the top level table transformer and key transformer handle it
var Subscribe_intf_eth_port_config_xfmr SubTreeXfmrSubscribe = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	var err error
	var result XfmrSubscOutParams

	if inParams.subscProc == TRANSLATE_SUBSCRIBE {
		log.V(3).Info("Subscribe_intf_eth_port_config_xfmr: inParams.subscProc: ", inParams.subscProc)

		pathInfo := NewPathInfo(inParams.uri)
		targetUriPath := pathInfo.YangPath

		log.V(3).Infof("Subscribe_intf_eth_port_config_xfmr:- URI:%s pathinfo:%s ", inParams.uri, pathInfo.Path)
		log.V(3).Infof("Subscribe_intf_eth_port_config_xfmr:- Target URI path:%s", targetUriPath)

		// to handle the TRANSLATE_SUBSCRIBE
		result.nOpts = new(notificationOpts)
		result.nOpts.pType = OnChange
		result.nOpts.mInterval = 15
		result.isVirtualTbl = false
		result.needCache = true

		ifName := pathInfo.StringVar("name", "*")

		log.V(3).Info("Subscribe_intf_eth_port_config_xfmr: ifName: ", ifName)

		result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {
			"PORT": {ifName: {"autoneg": "auto-negotiate", "speed": "port-speed"}}}}

		log.V(3).Info("Subscribe_intf_eth_port_config_xfmr: result ", result)
	}
	return result, err
}

var DbToYangPath_intf_eth_port_config_path_xfmr PathXfmrDbToYangFunc = func(params XfmrDbToYgPathParams) error {
	log.V(3).Info("DbToYangPath_intf_eth_port_config_path_xfmr: params: ", params)

	intfRoot := "/openconfig-interfaces:interfaces/interface"

	if params.tblName != "PORT" {
		log.Info("DbToYangPath_intf_eth_port_config_path_xfmr: from wrong table: ", params.tblName)
		return nil
	}

	if (params.tblName == "PORT") && (len(params.tblKeyComp) > 0) {
		params.ygPathKeys[intfRoot+"/name"] = params.tblKeyComp[0]
	} else {
		log.Info("DbToYangPath_intf_eth_port_config_path_xfmr, wrong param: tbl ", params.tblName, " key ", params.tblKeyComp)
		return nil
	}

	log.Info("DbToYangPath_intf_eth_port_config_path_xfmr: params.ygPathkeys: ", params.ygPathKeys)

	return nil
}
*/

var DbToYang_intf_eth_auto_neg_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return nil, fmt.Errorf("DbToYang_intf_eth_auto_neg_xfmr - Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	intTbl := IntfTypeTblMap[intfType]

	tblName, _ := getPortTableNameByDBId(intTbl, inParams.curDb)
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	autoNeg, ok := prtInst.Field[PORT_AUTONEG]
	if !ok {
		return nil, errors.New("auto-negotiate field not found in DB")
	}
	result := make(map[string]interface{})
	result["auto-negotiate"] = autoNeg == "on"
	return result, nil
}

var DbToYang_intf_eth_port_speed_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_eth_port_speed_xfmr - Invalid interface type IntfTypeUnset")
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, nil
	}

	intTbl := IntfTypeTblMap[intfType]

	tblName, _ := getPortTableNameByDBId(intTbl, inParams.curDb)
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	speed, ok := prtInst.Field[PORT_SPEED]
	if !ok {
		return nil, errors.New("speed field not found in DB for port " + inParams.key)
	}
	portSpeed, err := getDbToYangSpeed(speed)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_port_speed_xfmr: Speed field not found in map; err = " + err.Error())
	}
	result := make(map[string]interface{})
	result["port-speed"] = ocbinds.E_OpenconfigIfEthernet_ETHERNET_SPEED.ΛMap(portSpeed)["E_OpenconfigIfEthernet_ETHERNET_SPEED"][int64(portSpeed)].Name
	return result, nil
}

var DbToYang_intf_eth_aggr_id_xfmr = func(inParams XfmrParams) (map[string]interface{}, error) {
	var err error
	result := make(map[string]interface{})

	log.V(lvl.DEBUG).Info("DbToYang_intf_eth_aggr_id_xfmr, interface name ", inParams.key)

	intfType, _, ierr := getIntfTypeByName(inParams.key)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_eth_aggr_id_xfmr - Invalid interface type IntfTypeUnset")
		return result, errors.New("Invalid interface type IntfTypeUnset")
	}

	if IntfTypeEthernet != intfType {
		return result, nil
	}

	intf_lagId, _ := retrievePortChannelAssociatedWithIntf(&inParams, &inParams.key)
	if intf_lagId != nil {
		lagPrefix := "PortChannel"
		if strings.HasPrefix(*intf_lagId, lagPrefix) {
			result["aggregate-id"] = intf_lagId
		}
	}

	log.Infof("DbToYang_intf_eth_aggr_id_xfmr result %v", result)

	return result, err
}

func getIntfCountersTblKey(d *db.DB, ifKey string) (string, error) {
	var oid string

	portOidCountrTblTs := &db.TableSpec{Name: "COUNTERS_PORT_NAME_MAP"}
	ifCountInfo, err := d.GetMapAll(portOidCountrTblTs)
	if err != nil {
		log.V(lvl.ERROR).Info("Port-OID (Counters) get for all the interfaces failed!")
		return oid, err
	}
	if !ifCountInfo.IsPopulated() {
		return "", errors.New("Get for OID info from all the interfaces from Counters DB failed!")
	}
	if oid, ok := ifCountInfo.Field[ifKey]; ok {
		return oid, nil
	}
	return "", errors.New("OID info not found from Counters DB for interface " + ifKey)
}

func getCounters(entry *db.Value, entry_backup *db.Value, attr string, counter_val **uint64) error {

	var ok bool = false
	var err error
	val1, ok := entry.Field[attr]
	if !ok {
		return errors.New("Attr " + attr + "doesn't exist in IF table Map!")
	}
	val2, ok := entry_backup.Field[attr]
	if !ok {
		return errors.New("Attr " + attr + "doesn't exist in IF backup table Map!")
	}

	if len(val1) > 0 {
		v, _ := strconv.ParseUint(val1, 10, 64)
		v_backup, _ := strconv.ParseUint(val2, 10, 64)
		val := v - v_backup
		*counter_val = &val
		return nil
	}
	return err
}

var portCntList []string = []string{"in-octets", "in-unknown-protos", "in-unicast-pkts", "in-broadcast-pkts", "in-multicast-pkts",
	"in-errors", "in-discards", "in-fcs-errors", "in-pkts", "out-octets", "out-unicast-pkts",
	"out-broadcast-pkts", "out-multicast-pkts", "out-errors", "out-discards",
	"out-pkts",
	"last-clear", "carrier-transitions",
	"in-buffer-discards", "out-ecn-marked-pkts"}

var etherCntList []string = []string{"in-oversize-frames", "in-maxsize-exceeded", "in-undersize-frames",
	"in-jabber-frames", "in-fragment-frames", "fec-uncorrectable-words",
	"fec-correctable-words", "fec-symbol-errors", "fec-codeword-without-symbol-errors-count", "fec-codeword-with-1-symbol-error-count",
	"fec-codeword-with-2-symbol-errors-count", "fec-codeword-with-3-symbol-errors-count", "fec-codeword-with-4-symbol-errors-count",
	"fec-codeword-with-5-symbol-errors-count", "fec-codeword-with-6-symbol-errors-count", "fec-codeword-with-7-symbol-errors-count",
	"fec-codeword-with-8-symbol-errors-count", "fec-codeword-with-9-symbol-errors-count", "fec-codeword-with-10-symbol-errors-count",
	"fec-codeword-with-11-symbol-errors-count", "fec-codeword-with-12-symbol-errors-count", "fec-codeword-with-13-symbol-errors-count",
	"fec-codeword-with-14-symbol-errors-count", "fec-codeword-with-15-symbol-errors-count", "fec-codeword-with-16-symbol-errors-count"}
var subV4CntList = []string{"in-pkts", "out-pkts", "in-multicast-pkts", "out-multicast-pkts"}
var subV6CntList = []string{"in-discarded-pkts", "out-discarded-pkts", "in-pkts", "out-pkts", "in-multicast-pkts", "out-multicast-pkts"}

func getSpecificCounterAttr(targetUriPath string, entry *db.Value, entry_backup *db.Value, counter interface{}, portEntry *db.Value) (bool, error) {

	var e error
	var ok bool
	var counter_val *ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters
	var eth_counter_val *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_Counters
	var v4_sub_counter_val *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv4_State_Counters
	var v6_sub_counter_val *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv6_State_Counters

	switch {
	case strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/state/counters"):
		if counter_val, ok = counter.(*ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters); !ok {
			log.V(lvl.DEBUG).Infof(targetUriPath + " OpenconfigInterfaces_Interfaces_Interface_State_Counters is not valid")
			return true, nil
		}
	case strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/state/counters"),
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state/counters"):
		if v4_sub_counter_val, ok = counter.(*ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv4_State_Counters); !ok {
			log.V(lvl.DEBUG).Infof(targetUriPath + " OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv4_State_Counters is not valid")
			return true, nil
		}
	case strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters"),
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters"):
		if v6_sub_counter_val, ok = counter.(*ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv6_State_Counters); !ok {
			log.V(lvl.DEBUG).Infof(targetUriPath + " OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv6_State_Counters")
			return true, nil
		}
	case strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/state/counters"),
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters"):
		if eth_counter_val, ok = counter.(*ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_Counters); !ok {
			log.V(lvl.DEBUG).Infof(targetUriPath + " OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_Counters")
			return true, nil
		}
	default:
		log.V(lvl.DEBUG).Infof(targetUriPath + " - Not an valid interface counter paths")
		return true, nil
	}

	switch targetUriPath {
	case "/openconfig-interfaces:interfaces/interface/state/counters/in-octets":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_OCTETS", &counter_val.InOctets)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-unknown-protos":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_UNKNOWN_PROTOS", &counter_val.InUnknownProtos)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-unicast-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_UCAST_PKTS", &counter_val.InUnicastPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-broadcast-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_BROADCAST_PKTS", &counter_val.InBroadcastPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-multicast-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_MULTICAST_PKTS", &counter_val.InMulticastPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-errors":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_ERRORS", &counter_val.InErrors)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-discards":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_DISCARDS", &counter_val.InDiscards)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-buffer-discards":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IN_DROPPED_PKTS", &counter_val.InBufferDiscards)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-fcs-errors":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ETHER_STATS_CRC_ALIGN_ERRORS", &counter_val.InFcsErrors)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/in-pkts":
		var inNonUCastPkt, inUCastPkt *uint64
		var in_pkts uint64

		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_NON_UCAST_PKTS", &inNonUCastPkt)
		if e == nil {
			e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_UCAST_PKTS", &inUCastPkt)
			if e != nil {
				return true, e
			}
			in_pkts = *inUCastPkt + *inNonUCastPkt
			counter_val.InPkts = &in_pkts
			return true, e
		} else {
			return true, e
		}

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-octets":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_OCTETS", &counter_val.OutOctets)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-unicast-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_UCAST_PKTS", &counter_val.OutUnicastPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-broadcast-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_BROADCAST_PKTS", &counter_val.OutBroadcastPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-multicast-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_MULTICAST_PKTS", &counter_val.OutMulticastPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-errors":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_ERRORS", &counter_val.OutErrors)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-discards":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_DISCARDS", &counter_val.OutDiscards)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/state/counters/last-clear":
		timestampStr := (entry_backup.Field["LAST_CLEAR_TIMESTAMP"])
		timestamp, _ := strconv.ParseUint(timestampStr, 10, 64)
		counter_val.LastClear = &timestamp
		return true, nil

	case "/openconfig-interfaces:interfaces/interface/state/counters/carrier-transitions":
		transitionStr, ok := portEntry.Field["num-status-changes"]
		if !ok || transitionStr == "" {
			return true, tlerr.NotFound("num-status-changes field not found in Appl State DB.")
		}
		transitions, err := strconv.ParseUint(transitionStr, 10, 64)
		if err != nil {
			return true, err
		}
		counter_val.CarrierTransitions = &transitions
		return true, nil

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-pkts":
		var outNonUCastPkt, outUCastPkt *uint64
		var out_pkts uint64

		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_NON_UCAST_PKTS", &outNonUCastPkt)
		if e == nil {
			e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_OUT_UCAST_PKTS", &outUCastPkt)
			if e != nil {
				return true, e
			}
			out_pkts = *outUCastPkt + *outNonUCastPkt
			counter_val.OutPkts = &out_pkts
			return true, e
		} else {
			return true, e
		}

	case "/openconfig-interfaces:interfaces/interface/state/counters/out-ecn-marked-pkts",
		"/openconfig-interfaces:interfaces/interface/state/counters/google-pins-interfaces:out-ecn-marked-pkts":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ECN_MARKED_PACKETS", &counter_val.OutEcnMarkedPkts)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/in-oversize-frames",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/in-oversize-frames":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ETHER_RX_OVERSIZE_PKTS", &eth_counter_val.InOversizeFrames)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/in-maxsize-exceeded",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/in-maxsize-exceeded":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ETHER_RX_OVERSIZE_PKTS", &eth_counter_val.InMaxsizeExceeded)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/in-undersize-frames",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/in-undersize-frames":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ETHER_STATS_UNDERSIZE_PKTS", &eth_counter_val.InUndersizeFrames)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/in-jabber-frames",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/in-jabber-frames":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ETHER_STATS_JABBERS", &eth_counter_val.InJabberFrames)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/in-fragment-frames",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/in-fragment-frames":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_ETHER_STATS_FRAGMENTS", &eth_counter_val.InFragmentFrames)
		return true, e

	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-uncorrectable-words",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-uncorrectable-words",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-uncorrectable-words",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-uncorrectable-words":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_NOT_CORRECTABLE_FRAMES", &eth_counter_val.FecUncorrectableWords)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-correctable-words",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-correctable-words",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-correctable-words",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-correctable-words":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CORRECTABLE_FRAMES", &eth_counter_val.FecCorrectableWords)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-symbol-errors",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-symbol-errors",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-symbol-errors",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-symbol-errors":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_SYMBOL_ERRORS", &eth_counter_val.FecSymbolErrors)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-without-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-without-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-without-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-without-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S0", &eth_counter_val.FecCodewordWithoutSymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-1-symbol-error-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-1-symbol-error-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-1-symbol-error-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-1-symbol-error-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S1", &eth_counter_val.FecCodewordWith_1SymbolErrorCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-2-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-2-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-2-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-2-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S2", &eth_counter_val.FecCodewordWith_2SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-3-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-3-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-3-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-3-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S3", &eth_counter_val.FecCodewordWith_3SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-4-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-4-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-4-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-4-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S4", &eth_counter_val.FecCodewordWith_4SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-5-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-5-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-5-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-5-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S5", &eth_counter_val.FecCodewordWith_5SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-6-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-6-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-6-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-6-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S6", &eth_counter_val.FecCodewordWith_6SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-7-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-7-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-7-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-7-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S7", &eth_counter_val.FecCodewordWith_7SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-8-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-8-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-8-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-8-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S8", &eth_counter_val.FecCodewordWith_8SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-9-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-9-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-9-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-9-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S9", &eth_counter_val.FecCodewordWith_9SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-10-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-10-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-10-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-10-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S10", &eth_counter_val.FecCodewordWith_10SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-11-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-11-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-11-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-11-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S11", &eth_counter_val.FecCodewordWith_11SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-12-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-12-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-12-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-12-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S12", &eth_counter_val.FecCodewordWith_12SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-13-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-13-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-13-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-13-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S13", &eth_counter_val.FecCodewordWith_13SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-14-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-14-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-14-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-14-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S14", &eth_counter_val.FecCodewordWith_14SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-15-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-15-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-15-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-15-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S15", &eth_counter_val.FecCodewordWith_15SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters/fec-codeword-with-16-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/ethernet/state/counters/google-pins-interfaces:fec-codeword-with-16-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/fec-codeword-with-16-symbol-errors-count",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters/google-pins-interfaces:fec-codeword-with-16-symbol-errors-count":
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IF_IN_FEC_CODEWORD_ERRORS_S16", &eth_counter_val.FecCodewordWith_16SymbolErrorsCount)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state/counters/out-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/state/counters/out-pkts":
		var outNonUCastPkt, outUCastPkt *uint64
		ygot.BuildEmptyTree(v4_sub_counter_val)
		if e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IP_OUT_NON_UCAST_PKTS", &outNonUCastPkt); e != nil {
			return true, e
		}
		if e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IP_OUT_UCAST_PKTS", &outUCastPkt); e != nil {
			return true, e
		}
		out_pkts := *outUCastPkt + *outNonUCastPkt
		v4_sub_counter_val.OutPkts = &out_pkts
		return true, nil
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state/counters/in-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/state/counters/in-pkts":
		ygot.BuildEmptyTree(v4_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IP_IN_RECEIVES", &v4_sub_counter_val.InPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state/counters/in-multicast-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/state/counters/in-multicast-pkts":
		ygot.BuildEmptyTree(v4_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IP_IN_NON_UCAST_PKTS", &v4_sub_counter_val.InMulticastPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state/counters/out-multicast-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/state/counters/out-multicast-pkts":
		ygot.BuildEmptyTree(v4_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IP_OUT_NON_UCAST_PKTS", &v4_sub_counter_val.OutMulticastPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters/out-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters/out-pkts":
		var outNonUCastPkt, outUCastPkt *uint64

		ygot.BuildEmptyTree(v6_sub_counter_val)
		if e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_OUT_NON_UCAST_PKTS", &outNonUCastPkt); e != nil {
			return true, e
		}
		if e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_OUT_UCAST_PKTS", &outUCastPkt); e != nil {
			return true, e
		}
		out_pkts := *outUCastPkt + *outNonUCastPkt
		v6_sub_counter_val.OutPkts = &out_pkts
		return true, nil
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters/in-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters/in-pkts":
		ygot.BuildEmptyTree(v6_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_IN_RECEIVES", &v6_sub_counter_val.InPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters/in-multicast-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters/in-multicast-pkts":
		ygot.BuildEmptyTree(v6_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_IN_MCAST_PKTS", &v6_sub_counter_val.InMulticastPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters/out-multicast-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters/out-multicast-pkts":
		ygot.BuildEmptyTree(v6_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_OUT_MCAST_PKTS", &v6_sub_counter_val.OutMulticastPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters/in-discarded-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters/in-discarded-pkts":
		ygot.BuildEmptyTree(v6_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_IN_DISCARDS", &v6_sub_counter_val.InDiscardedPkts)
		return true, e
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters/out-discarded-pkts",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters/out-discarded-pkts":
		ygot.BuildEmptyTree(v6_sub_counter_val)
		e = getCounters(entry, entry_backup, "SAI_PORT_STAT_IPV6_OUT_DISCARDS", &v6_sub_counter_val.OutDiscardedPkts)
		return true, e
	default:
		log.V(lvl.ERROR).Infof(targetUriPath + " - Not an interface state counter attribute")
	}
	return false, nil
}

var DbToYang_intf_get_counters_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	var err error

	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	targetUriPath, err := getYangPathFromUri(inParams.uri)
	log.V(lvl.DEBUG).Info("targetUriPath is ", targetUriPath)

	if !strings.Contains(targetUriPath, "/openconfig-interfaces:interfaces/interface/state/counters") {
		log.V(lvl.ERROR).Infof("%s is redundant", targetUriPath)
		return err
	}

	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.ERROR).Info("DbToYang_intf_get_counters_xfmr - Invalid interface type IntfTypeUnset")
		return fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	intTbl := IntfTypeTblMap[intfType]
	if intTbl.CountersHdl.PopulateCounters == nil {
		log.V(lvl.ERROR).Infof("Counters for Interface: %s not supported!", ifName)
		return nil
	}
	var state_counters *ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters

	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		var ok bool = false
		var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
		if intfObj, ok = intfsObj.Interface[ifName]; !ok {
			intfObj, _ = intfsObj.NewInterface(ifName)
			ygot.BuildEmptyTree(intfObj)
		}
		ygot.BuildEmptyTree(intfObj)
		if intfObj.State == nil || intfObj.State.Counters == nil {
			ygot.BuildEmptyTree(intfObj.State)
		}
		state_counters = intfObj.State.Counters
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, _ := intfsObj.NewInterface(ifName)
		ygot.BuildEmptyTree(intfObj)
		state_counters = intfObj.State.Counters
	}

	err = intTbl.CountersHdl.PopulateCounters(inParams, "", state_counters)
	log.V(lvl.DEBUG).Info("DbToYang_intf_get_counters_xfmr - ", state_counters)

	return err
}

var Subscribe_intf_get_counters_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	log.V(lvl.DEBUG).Info("Entering Subscribe_intf_get_counters_xfmr")

	result := XfmrSubscOutParams{
		isVirtualTbl: false,
		needCache:    true,
		onChange:     OnchangeDisable,
		dbDataMap:    make(RedisDbSubscribeMap),
		nOpts:        &notificationOpts{mInterval: 1, pType: Sample}, // Counters can only support Sample.
	}

	defer log.V(lvl.DEBUG).Info("Returning Subscribe_intf_get_counters_xfmr, result:", result)

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	if ifName != "*" {
		intfType, _, err := getIntfTypeByName(ifName)
		if err != nil {
			return result, err
		}
		tblName, err := getPortTableNameByDBId(IntfTypeTblMap[intfType], db.ConfigDB)
		if err != nil {
			return result, errors.New("Subscribe_intf_get_counters_xfmr table name not found. Err: " + err.Error())
		}
		result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {tblName: {ifName: {}}}}
		return result, nil
	}

	// wildcard key
	result.dbDataMap[db.ConfigDB] = make(map[string]map[string]map[string]string)
	for _, tblName := range dbIdToTblMap[db.ConfigDB] {
		result.dbDataMap[db.ConfigDB][tblName] = map[string]map[string]string{ifName: {}}
	}

	return result, nil
}

/* Google: removing this code as it is ununsed
var DbToYangPath_intf_get_counters_path_xfmr PathXfmrDbToYangFunc = func(params XfmrDbToYgPathParams) error {
	log.Info("DbToYangPath_intf_get_counters_path_xfmr: params: ", params)

	intfRoot := "/openconfig-interfaces:interfaces/interface"

	if params.tblName != "COUNTERS_PORT_NAME_MAP" {
		log.Info("DbToYangPath_intf_get_counters_path_xfmr: from wrong table: ", params.tblName)
		return nil
	}

	if (params.tblName == "COUNTERS_PORT_NAME_MAP") && (len(params.tblKeyComp) > 0) {
		params.ygPathKeys[intfRoot+"/name"] = params.tblKeyComp[0]
	} else {
		log.Info("DbToYangPath_intf_get_counters_path_xfmr, wrong param: tbl ", params.tblName, " key ", params.tblKeyComp)
		return nil
	}

	log.Info("DbToYangPath_intf_get_counters_path_xfmr: params.ygPathkeys: ", params.ygPathKeys)

	return nil
}*/

var populatePortCounters PopulateIntfCounters = func(inParams XfmrParams, ifName string, counter interface{}) error {
	pathInfo := NewPathInfo(inParams.uri)
	if ifName == "" {
		ifName = pathInfo.Var("name")
	}

	targetUriPath, err := getYangPathFromUri(pathInfo.Path)

	log.V(lvl.DEBUG).Info("PopulateIntfCounters : inParams.curDb : ", inParams.curDb, "D: ", inParams.d, "DB index : ", inParams.dbs[inParams.curDb])
	oid, oiderr := getIntfCountersTblKey(inParams.dbs[inParams.curDb], ifName)
	if oiderr != nil {
		return oiderr
	}
	cntTs := &db.TableSpec{Name: "COUNTERS"}
	entry, dbErr := inParams.dbs[inParams.curDb].GetEntry(cntTs, db.Key{Comp: []string{oid}})
	if dbErr != nil {
		return dbErr
	}
	CounterData := entry
	cntTs_cp := &db.TableSpec{Name: "COUNTERS_BACKUP"}
	entry_backup, dbErr := inParams.dbs[inParams.curDb].GetEntry(cntTs_cp, db.Key{Comp: []string{oid}})
	if dbErr != nil {
		m := make(map[string]string)
		log.V(lvl.DEBUG).Info("PopulateIntfCounters : not able find the oid entry in DB COUNTERS_BACKUP table")
		/* Frame backup data with 0 as counter values */
		for attr := range entry.Field {
			m[attr] = "0"
		}
		m["LAST_CLEAR_TIMESTAMP"] = "0"
		entry_backup = db.Value{Field: m}
	}
	CounterBackUpData := entry_backup
	portEntry, dbErr := inParams.dbs[db.ApplStateDB].GetEntry(&db.TableSpec{Name: "PORT_TABLE"}, db.Key{Comp: []string{ifName}})
	if dbErr != nil {
		return dbErr
	}

	switch targetUriPath {
	case "/openconfig-interfaces:interfaces/interface/state/counters":
		for _, attr := range portCntList {
			uri := targetUriPath + "/" + attr
			if ok, err := getSpecificCounterAttr(uri, &CounterData, &CounterBackUpData, counter, &portEntry); !ok || err != nil {
				log.V(lvl.DEBUG).Info("Get Counter URI failed :", uri)
			}
		}
	case "/openconfig-interfaces:interfaces/interface/ethernet/state/counters",
		"/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters":
		for _, attr := range etherCntList {
			uri := targetUriPath + "/" + attr
			if ok, err := getSpecificCounterAttr(uri, &CounterData, &CounterBackUpData, counter, &portEntry); !ok || err != nil {
				log.V(lvl.DEBUG).Info("Get Ethernet Counter URI failed :", uri)
			}
		}
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/state/counters",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/state/counters":
		for _, attr := range subV4CntList {
			uri := targetUriPath + "/" + attr
			if ok, err := getSpecificCounterAttr(uri, &CounterData, &CounterBackUpData, counter, &portEntry); !ok || err != nil {
				log.V(lvl.DEBUG).Info("Get subinterface IPv4 Counter URI failed :", uri)
			}
		}
	case "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/state/counters",
		"/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/state/counters":
		for _, attr := range subV6CntList {
			uri := targetUriPath + "/" + attr
			if ok, err := getSpecificCounterAttr(uri, &CounterData, &CounterBackUpData, counter, &portEntry); !ok || err != nil {
				log.V(lvl.DEBUG).Info("Get subinterface IPv6 Counter URI failed :", uri)
			}
		}

	default:
		_, err = getSpecificCounterAttr(targetUriPath, &CounterData, &CounterBackUpData, counter, &portEntry)
	}

	if err != nil {
		return err
	}

	// Use counter specific timestamp if there is one.
	ts, ok := CounterData.Field["PORT_STAT_TIME_STAMP_USEC"]
	if !ok || ts == "" {
		return nil
	}
	if usec, err := strconv.ParseInt(ts, 10, 64); err == nil {
		utils.UpdateYGSTimestamp(*inParams.ygRoot, counter.(ygot.GoStruct), usec*1000)
	} else {
		log.V(lvl.DEBUG).Infof("Invalid timestamp for port %s, %v", ifName, ts)
	}

	return nil
}

var populatePortChannelCounters PopulateIntfCounters = func(inParams XfmrParams, ifName string, counter interface{}) error {
	pathInfo := NewPathInfo(inParams.uri)
	if ifName == "" {
		ifName = pathInfo.Var("name")
	}

	members, err := getMembers(inParams.dbs[db.StateDB], ifName)
	if err != nil {
		return fmt.Errorf("%w; getMembers() for %s failed", err, ifName)
	}

	state_counters, ok := counter.(*ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters)
	if !ok {
		return fmt.Errorf("Expected counter to be of type OpenconfigInterfaces_Interfaces_Interface_State_Counters, wasn't...")
	}

	for _, member := range members {
		var mcounters ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters
		populatePortCounters(inParams, member, &mcounters)
		sumStateCounters(state_counters, &mcounters)
	}
	return nil
}

var YangToDb_intf_counters_key KeyXfmrYangToDb = func(inParams XfmrParams) (string, error) {
	var entry_key string
	var err error
	pathInfo := NewPathInfo(inParams.uri)
	intfName := pathInfo.Var("name")
	oid, oiderr := getIntfCountersTblKey(inParams.dbs[inParams.curDb], intfName)

	if oiderr == nil {
		entry_key = oid
	}
	return entry_key, err
}

var DbToYang_intf_counters_key KeyXfmrDbToYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	rmap := make(map[string]interface{})
	var err error
	return rmap, err
}

var DbToYang_intf_get_ether_counters_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	var err error

	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	targetUriPath, err := getYangPathFromUri(inParams.uri)
	log.V(lvl.DEBUG).Infof("DbToYang_intf_get_ether_counters_xfmr - entering... ifName=%s targetUriPath=%s", ifName, targetUriPath)

	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_get_ether_counters_xfmr - Invalid interface type IntfTypeUnset")
		return fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	if intfType == IntfTypeMgmt || intfType == IntfTypeMgmtBond || intfType == IntfTypeCpu || intfType == IntfTypeLoopback {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_get_ether_counters_xfmr - Ether Stats not supported for intfType %v", intfType)
		return errors.New("Ethernet counters not supported.")
	}

	if !strings.Contains(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/state/counters") &&
		!strings.Contains(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/state/counters") {
		log.V(lvl.ERROR).Infof("%s is redundant", targetUriPath)
		return err
	}

	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
	var eth_counters *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_Counters

	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		var ok bool = false
		if intfObj, ok = intfsObj.Interface[ifName]; !ok {
			intfObj, _ = intfsObj.NewInterface(ifName)
		}
		ygot.BuildEmptyTree(intfObj)
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, _ = intfsObj.NewInterface(ifName)
		ygot.BuildEmptyTree(intfObj)
	}

	ygot.BuildEmptyTree(intfObj.Ethernet)
	ygot.BuildEmptyTree(intfObj.Ethernet.State)
	ygot.BuildEmptyTree(intfObj.Ethernet.State.Counters)
	eth_counters = intfObj.Ethernet.State.Counters

	return populatePortCounters(inParams, "", eth_counters)
}

var Subscribe_intf_get_ether_counters_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	log.V(lvl.DEBUG).Info("Entering Subscribe_intf_get_ether_counters_xfmr")

	result := XfmrSubscOutParams{
		isVirtualTbl: false,
		needCache:    true,
		onChange:     OnchangeDisable,
		dbDataMap:    make(RedisDbSubscribeMap),
		nOpts:        &notificationOpts{mInterval: 1, pType: Sample}, // Counters can only support Sample.
	}

	ifName := NewPathInfo(inParams.uri).Var("name")
	if ifName == "" {
		ifName = "*"
	}
	result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {"PORT": {ifName: {}}}}
	log.V(lvl.DEBUG).Infof("Returning Subscribe_intf_get_ether_counters_xfmr, result.dbDataMap=%v", result.dbDataMap)

	return result, nil
}

var intf_post_xfmr PostXfmrFunc = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {

	requestUriPath, _ := getYangPathFromUri(inParams.requestUri)
	retDbDataMap := (*inParams.dbDataMap)[inParams.curDb]
	log.V(lvl.DEBUG).Info("Entering intf_post_xfmr")

	if inParams.oper != REPLACE || requestUriPath != "/openconfig-interfaces:interfaces" {
		return retDbDataMap, nil
	}

	cfgDB := inParams.dbs[db.ConfigDB]
	if _, ok := retDbDataMap["PORT"]; ok {
		attrList := []string{"lanes", "alias", "index"}
		for intf := range retDbDataMap["PORT"] {
			appendExistingDBAttr(cfgDB, "PORT", intf, attrList, retDbDataMap)
		}
	}
	if _, ok := retDbDataMap["MGMT_PORT"]; ok {
		attrList := []string{"bond-downdelay", "bond-lacp-rate", "bond-miimon", "bond-mode", "bond-slaves", "bond-updelay"}
		for intf := range retDbDataMap["MGMT_PORT"] {
			appendExistingDBAttr(cfgDB, "MGMT_PORT", intf, attrList, retDbDataMap)
		}
	}

	intfsObj := getIntfsRoot(inParams.ygRoot)

	// Delete PortChannels and Bridges removed from the config
	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	subOpMap[db.ConfigDB] = make(map[string]map[string]db.Value)

	// PORTCHANNEL|* tables
	deletePCs, err := getPCTablesForDeletion(cfgDB, PORTCHANNEL_TN, 1, pcs)
	if err == nil && len(deletePCs) > 0 {
		subOpMap[db.ConfigDB][PORTCHANNEL_TN] = deletePCs
	}

	// PORTCHANNEL_INTERFACE|* tables
	deleteIntfs, err := getPCTablesForDeletion(cfgDB, PORTCHANNEL_INTERFACE_TN, 1, pcs)
	if err == nil && len(deleteIntfs) > 0 {
		subOpMap[db.ConfigDB][PORTCHANNEL_INTERFACE_TN] = deleteIntfs
	}

	// PORTCHANNEL_MEMBER|*|* tables
	deleteMems, err := getPCTablesForDeletion(cfgDB, PORTCHANNEL_MEMBER_TN, 2, pcMembers)
	if err == nil && len(deleteMems) > 0 {
		subOpMap[db.ConfigDB][PORTCHANNEL_MEMBER_TN] = deleteMems
	}

	// UMF_TRUNK_QUEUE|<pc>|<qid> tables
	deleteTrunkQueues, err := getTrunkQueueTablesForDeletion(cfgDB, pcs)
	if err == nil && len(deleteMems) > 0 {
		subOpMap[db.ConfigDB]["UMF_TRUNK_QUEUE"] = deleteTrunkQueues
	}

	// BRIDGE|* tables
	bridgeTN := IntfTypeTblMap[IntfTypeBridge].cfgDb.portTN
	bridgeKeys, err := cfgDB.GetKeys(&db.TableSpec{Name: bridgeTN})
	if err == nil {
		deleteBridges := map[string]db.Value{}
		for _, key := range bridgeKeys {
			br := key.Get(0)
			if _, ok := intfsObj.Interface[br]; !ok {
				deleteBridges[br] = db.Value{}
			}
		}
		if len(deleteBridges) > 0 {
			subOpMap[db.ConfigDB][bridgeTN] = deleteBridges
		}
	}

	// BRIDGE_MEMBER|*|* tables
	bridgeMemTN := IntfTypeTblMap[IntfTypeBridge].cfgDb.memberTN
	bridgeMemKeys, err := cfgDB.GetKeys(&db.TableSpec{Name: bridgeMemTN})
	if err == nil {
		deleteBridgeMems := map[string]db.Value{}
		for _, key := range bridgeMemKeys {
			br := key.Get(0)
			intf := key.Get(1)

			if _, ok := intfsObj.Interface[br]; !ok {
				deleteBridgeMems[br+"|"+intf] = db.Value{}
				continue
			}
			if intfObj, ok := intfsObj.Interface[intf]; !ok || intfObj.Config == nil || intfObj.Config.BridgeId == nil || *(intfObj.Config.BridgeId) != br {
				deleteBridgeMems[br+"|"+intf] = db.Value{}
			}
		}
		if len(deleteBridgeMems) > 0 {
			subOpMap[db.ConfigDB][bridgeMemTN] = deleteBridgeMems
		}
	}

	if len(subOpMap[db.ConfigDB]) > 0 {
		updateSubOpDataMap(subOpMap, DELETE, inParams)
	}
	log.V(lvl.DEBUG).Infof("PortChannel Cleanup:\nPCs: %v\nPCMembers: %v\nsubOpMap: %v", pcs, pcMembers, subOpMap)

	additionalConfig := make(map[string]map[string]db.Value)
	// Handle PortChannel members@ field.
	if pcToMembers := pcToMembersMap(); len(pcToMembers) > 0 {
		additionalConfig[PORTCHANNEL_TN] = map[string]db.Value{}
		for pc, mems := range pcToMembers {
			slices.Sort(mems)
			additionalConfig[PORTCHANNEL_TN][pc] = db.Value{Field: map[string]string{"members@": strings.Join(mems, ",")}}
		}
	}

	// Handle default interfaces config.
	for _, intf := range intfsObj.Interface {
		ifName := intf.Name
		if ifName == nil {
			continue
		}
		intfType, _, err := getIntfTypeByName(*ifName)
		if err != nil || intfType != IntfTypeEthernet {
			continue
		}
		intTbl, _ := IntfTypeTblMap[intfType]
		if _, ok := additionalConfig[intTbl.cfgDb.portTN]; !ok {
			additionalConfig[intTbl.cfgDb.portTN] = map[string]db.Value{}
		}

		data := db.Value{Field: map[string]string{}}
		// Disable link damping for interfaces that do not have link damping config
		if intf.PenaltyBasedAied == nil {
			data.Set("link_event_damping_algorithm", "disabled")
		}
		// Set Default learn_mode for all front panel ports
		data.Set("learn_mode", "disable")
		additionalConfig[intTbl.cfgDb.portTN][*ifName] = data
	}
	if len(additionalConfig) > 0 {
		updateSubOpDataMap(map[db.DBNum]map[string]map[string]db.Value{
			db.ConfigDB: additionalConfig,
		}, REPLACE, inParams)
	}
	log.V(lvl.DEBUG).Infof("Setting additional config for interfaces: %v", additionalConfig)
	return retDbDataMap, nil
}

var intf_pre_xfmr PreXfmrFunc = func(inParams XfmrParams) error {
	var err error
	requestUriPath, _ := getYangPathFromUri(inParams.requestUri)
	if inParams.oper == REPLACE && requestUriPath == "/openconfig-interfaces:interfaces" {
		pcs = make(map[string]bool)
		pcMembers = make(map[string]bool)
	}
	if inParams.oper == DELETE {
		switch requestUriPath {
		case "/openconfig-interfaces:interfaces":
			return tlerr.InvalidArgsError{Format: "Delete operation not supported for this path - " + requestUriPath}
		case "/openconfig-interfaces:interfaces/interface":
			pathInfo := NewPathInfo(inParams.uri)
			if len(pathInfo.Vars) == 0 {
				return tlerr.InvalidArgsError{Format: "Delete operation not supported for this path - " + requestUriPath}
			}
		}
	}
	return err
}

var intf_subintfs_table_xfmr TableXfmrFunc = func(inParams XfmrParams) ([]string, error) {
	var tblList []string
	log.V(lvl.DEBUG).Info("intf_subintfs_table_xfmr: URI: ", inParams.uri)

	pathInfo := NewPathInfo(inParams.uri)

	idx := pathInfo.Var("index")

	if idx == "" || idx == "*" {
		if inParams.oper == GET || inParams.oper == DELETE {
			if inParams.dbDataMap != nil {
				(*inParams.dbDataMap)[db.ConfigDB]["SUBINTF_TBL"] = make(map[string]db.Value)
				(*inParams.dbDataMap)[db.ConfigDB]["SUBINTF_TBL"]["0"] = db.Value{Field: make(map[string]string)}
				tblList = append(tblList, "SUBINTF_TBL")
			}
			log.V(lvl.DEBUG).Info("intf_subintfs_table_xfmr - Subinterface get operation ")
		}
	} else {
		if idx == "0" {
			if inParams.dbDataMap != nil {
				(*inParams.dbDataMap)[db.ConfigDB]["SUBINTF_TBL"] = make(map[string]db.Value)
				(*inParams.dbDataMap)[db.ConfigDB]["SUBINTF_TBL"]["0"] = db.Value{Field: make(map[string]string)}
				(*inParams.dbDataMap)[db.ConfigDB]["SUBINTF_TBL"]["0"].Field["NULL"] = "NULL"
			}
			tblList = append(tblList, "SUBINTF_TBL")
		}
		log.V(lvl.DEBUG).Info("intf_subintfs_table_xfmr - Subinterface get operation ")
	}

	return tblList, nil
}

var YangToDb_intf_subintfs_xfmr KeyXfmrYangToDb = func(inParams XfmrParams) (string, error) {
	var subintf_key string
	var err error

	log.V(lvl.DEBUG).Info("YangToDb_intf_subintfs_xfmr - inParams.uri ", inParams.uri)

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return ifName, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}

	idx := pathInfo.Var("index")

	if idx != "0" {
		subintf_key = ifName + "." + idx
	} else {
		subintf_key = idx
	}

	log.V(lvl.DEBUG).Info("YangToDb_intf_subintfs_xfmr - return subintf_key ", subintf_key)
	return subintf_key, err
}

var DbToYang_intf_subintfs_xfmr KeyXfmrDbToYang = func(inParams XfmrParams) (map[string]interface{}, error) {

	log.V(lvl.DEBUG).Info("Entering DbToYang_intf_subintfs_xfmr")
	var idx string

	if strings.Contains(inParams.key, ".") {
		key_split := strings.Split(inParams.key, ".")
		idx = key_split[1]
	} else {
		idx = inParams.key
	}

	rmap := make(map[string]interface{})
	i64, _ := strconv.ParseUint(idx, 10, 32)
	rmap["index"] = i64

	log.V(lvl.DEBUG).Info("DbToYang_intf_subintfs_xfmr rmap ", rmap)
	return rmap, nil
}

var YangToDb_subintf_ip_addr_key_xfmr KeyXfmrYangToDb = func(inParams XfmrParams) (string, error) {
	log.V(lvl.DEBUG).Info("Entering YangToDb_subintf_ip_addr_key_xfmr")
	var inst_key string
	pathInfo := NewPathInfo(inParams.uri)
	inst_key = pathInfo.Var("ip")
	log.V(lvl.DEBUG).Info("Interface IP: ", inst_key)
	return inst_key, nil
}

var DbToYang_subintf_ip_addr_key_xfmr KeyXfmrDbToYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("Entering DbToYang_subintf_ip_addr_key_xfmr")
	rmap := make(map[string]interface{})
	return rmap, nil
}

var YangToDb_intf_csig_tag_action_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	if inParams.oper == DELETE {
		return res_map, nil
	}
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		// Only supported for singletons.
		log.V(lvl.ERROR).Info("YangToDb_intf_csig_tag_action_xfmr, Error: CSIG tag action is only supported for singletons.")
		return nil, nil
	}

	csigTagActionEnum, ok := inParams.param.(ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction)
	if !ok {
		return nil, nil
	}
	csigTagActionStr, ok := yangToDbCsigTagActionMap[csigTagActionEnum]
	if !ok {
		return nil, errors.New("YangToDb_intf_csig_tag_action_xfmr, Error: Invalid csig-tag-action.")
	}

	res_map["csig_tag_action"] = csigTagActionStr
	return res_map, nil
}

var DbToYang_intf_csig_tag_action_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("DbToYang_intf_csig_tag_action_xfmr: inParams: ", inParams)

	prtInst, err := retrieveDbEntryForSingletonInterface(inParams)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	csigTagActionStr, ok := prtInst.Field["csig_tag_action"]
	if !ok {
		return nil, nil
	}
	csigTagActionEnum, ok := dbToYangCsigTagActionMap[csigTagActionStr]
	if !ok {
		return nil, errors.New("csig_tag_action field read from DB not found in dbToYangCsigTagActionMap.")
	}
	result["csig-tag-action"] = ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction.ΛMap(csigTagActionEnum)["E_OpenconfigInterfaces_Interfaces_Interface_Config_CsigTagAction"][int64(csigTagActionEnum)].Name
	return result, nil
}

var YangToDb_subif_index_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	var err error

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	res_map["parent"] = ifName

	log.V(lvl.DEBUG).Info("YangToDb_subif_index_xfmr: res_map:", res_map)
	return res_map, err
}

var DbToYang_subif_index_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	res_map := make(map[string]interface{})

	pathInfo := NewPathInfo(inParams.uri)
	id := pathInfo.Var("index")
	log.V(lvl.DEBUG).Info("DbToYang_subif_index_xfmr: Sub-interface Index = ", id)
	i64, _ := strconv.ParseUint(id, 10, 32)
	res_map["index"] = i64
	return res_map, nil
}

/* Get interface to IP mapping for all interfaces in the given table */
/*
func getCachedAllIntfIpMap(dbCl *db.DB, tblName string, ipv4 bool, ipv6 bool, ip string, tblPattern *db.Table) (map[string]map[string]db.Value, error) {
	var err error
	all := true
	intfIpMap := make(map[string]map[string]db.Value)
	if !ipv4 || !ipv6 {
		all = false
	}
	log.V(3).Info("Inside getCachedAllIntfIpMap: Get Interface IP Info from table cache to Internal DS")

	//Get keys from tblPattern
	keys, err := tblPattern.GetKeys()
	if err != nil {
		return intfIpMap, err
	}

	for _, key := range keys {
		ifName := key.Get(0)
		intfType, _, ierr := getIntfTypeByName(ifName)
		if intfType == IntfTypeUnset || ierr != nil {
			continue
		}

		if !all {
			ipB, _, _ := parseCIDR(key.Get(1))
			if (validIPv4(ipB.String()) && (!ipv4)) ||
				(validIPv6(ipB.String()) && (!ipv6)) {
				continue
			}
			if ip != "" {
				if ipB.String() != ip {
					continue
				}
			}
		}

		ipInfo, _ := tblPattern.GetEntry(db.Key{Comp: []string{ifName, key.Get(1)}})

		if _, ok := intfIpMap[ifName]; !ok {
			intfIpMap[ifName] = make(map[string]db.Value)
		}

		intfIpMap[ifName][key.Get(1)] = ipInfo
	}
	return intfIpMap, err
}

func handleAllIntfIPGetForTable(inParams XfmrParams, tblName string, isAppDb bool) error {
	var err error
	intfsObj := getIntfsRoot(inParams.ygRoot)
	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface

	var tblPattern db.Table

	currDb := inParams.dbs[db.ConfigDB]
	if isAppDb {
		currDb = inParams.dbs[db.ApplDB]
	}

	dbTbl := db.TableSpec{Name: tblName, CompCt: 2}
	keyPattern := db.Key{Comp: []string{"*", "*"}}
	tblPattern, err = currDb.GetTablePattern(&dbTbl, keyPattern)

	if err != nil {
		log.Error("handleAllIntfIPGetForTable: GetTablePattern() returns err: %v", err)
		return nil
	}

	var intfIpMap map[string]map[string]db.Value
	if isAppDb {
		intfIpMap, err = getCachedAllIntfIpMap(inParams.dbs[db.ApplDB], tblName, true, true, "", &tblPattern)
	} else {
		intfIpMap, err = getCachedAllIntfIpMap(inParams.dbs[db.ConfigDB], tblName, true, true, "", &tblPattern)
	}

	if log.V(3) {
		log.Infof("handleAllIntfIPGetForTable, tbl: %v, intfIpMap: %v", tblName, intfIpMap)
	}

	if len(intfIpMap) == 0 {
		return nil
	}

	i32 := uint32(0)

	// YGOT filling
	for intfName, ipMapDB := range intfIpMap {

		var name string
		name = *(&intfName)

		if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
			var ok bool = false
			if intfObj, ok = intfsObj.Interface[name]; !ok {
				intfObj, _ = intfsObj.NewInterface(name)
			}
			ygot.BuildEmptyTree(intfObj)
			if intfObj.Subinterfaces == nil {
				ygot.BuildEmptyTree(intfObj.Subinterfaces)
			}
		} else {
			ygot.BuildEmptyTree(intfsObj)
			intfObj, _ = intfsObj.NewInterface(name)
			ygot.BuildEmptyTree(intfObj)
		}

		if log.V(3) {
			log.Infof("handleAllIntfIPGetForTable, intfName: %v, name: %v, subidx: %v, ipmap: %v", intfName, name, i32, ipMapDB)
		}

		if isAppDb {
			convertIpMapToOC(ipMapDB, intfObj, true, i32)
		} else {
			convertIpMapToOC(ipMapDB, intfObj, false, i32)
		}
	}
	return nil
}
*/

// ValidateIntfProvisionedForRelay helper function to validate IP address deletion if DHCP relay is provisioned
func ValidateIntfProvisionedForRelay(d *db.DB, ifName string, prefixIp string, entry *db.Value) (bool, error) {
	var tblList string

	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("ValidateIntfProvisionedForRelay - Invalid interface type IntfTypeUnset")
		return false, errors.New("Invalid InterfaceType")
	}

	// get all the IP addresses on this interface, refer to the intf table name
	intTbl := IntfTypeTblMap[intfType]
	tblList = intTbl.cfgDb.intfTN

	if entry == nil {
		ent, dbErr := d.GetEntry(&db.TableSpec{Name: tblList}, db.Key{Comp: []string{ifName}})
		entry = &ent
		if dbErr != nil {
			log.Warning("Failed to read entry from config DB, " + tblList + " " + ifName)
			return false, nil
		}
	}

	//check if dhcp_sever is provisioned for ipv4
	if strings.Contains(prefixIp, ".") || strings.Contains(prefixIp, "ipv4") {
		log.V(2).Info("ValidateIntfProvisionedForRelay  - IPv4Check")
		log.V(2).Info(entry)
		if len(entry.Field["dhcp_servers@"]) > 0 {
			return true, nil
		}
		//} else if (strings.Contains(prefixIp, ":") && numIpv6 < 2) || strings.Contains(prefixIp, "ipv6") {
	} else if (strings.Contains(prefixIp, ":")) || strings.Contains(prefixIp, "ipv6") {
		//check if dhcpv6_sever is provisioned for ipv6
		log.V(2).Info("ValidateIntfProvisionedForRelay  - IPv6Check")
		log.V(2).Info(entry)
		if len(entry.Field["dhcpv6_servers@"]) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func handleIntfIPGetByTargetURI(inParams XfmrParams, targetUriPath string, ifName string, intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface) error {
	var ipMap map[string]db.Value
	var err error

	pathInfo := NewPathInfo(inParams.uri)
	ipAddr := pathInfo.Var("ip")
	idx := pathInfo.Var("index")
	i32 := uint32(0)
	if idx != "0" {
		i64, _ := strconv.ParseUint(idx, 10, 32)
		i32 = uint32(i64)
	}
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return ierr
	}
	intTbl := IntfTypeTblMap[intfType]

	if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/addresses/address/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/addresses/address/config") {
		ipMap, err = getIntfIpByName(inParams.dbs[db.ConfigDB], intTbl.cfgDb.intfTN, ifName, true, false, ipAddr)
		log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv4 config ipMap - : ", ipMap)
		convertIpMapToOC(ipMap, intfObj, false, i32)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/addresses/address/config") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/addresses/address/config") {
		ipMap, err = getIntfIpByName(inParams.dbs[db.ConfigDB], intTbl.cfgDb.intfTN, ifName, false, true, ipAddr)
		log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv6 config ipMap - : ", ipMap)
		convertIpMapToOC(ipMap, intfObj, false, 0)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/addresses/address/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/addresses/address/state") {
		ipMap, err = getIntfIpByName(inParams.dbs[db.ApplStateDB], intTbl.appStateDb.intfTN, ifName, true, false, ipAddr)
		log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv4 state ipMap - : ", ipMap)
		convertIpMapToOC(ipMap, intfObj, true, 0)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/addresses/address/state") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/addresses/address/state") {
		ipMap, err = getIntfIpByName(inParams.dbs[db.ApplStateDB], intTbl.appStateDb.intfTN, ifName, false, true, ipAddr)
		log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv6 state ipMap - : ", ipMap)
		convertIpMapToOC(ipMap, intfObj, true, 0)
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv4/addresses") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv4/addresses") {
		ipMap, err = getIntfIpByName(inParams.dbs[db.ConfigDB], intTbl.cfgDb.intfTN, ifName, true, false, ipAddr)
		if err == nil {
			log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv4 config ipMap - : ", ipMap)
			convertIpMapToOC(ipMap, intfObj, false, i32)
		}
		ipMap, err = getIntfIpByName(inParams.dbs[db.ApplStateDB], intTbl.appStateDb.intfTN, ifName, true, false, ipAddr)
		if err == nil {
			log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv4 state ipMap - : ", ipMap)
			convertIpMapToOC(ipMap, intfObj, true, i32)
		}
	} else if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/ipv6/addresses") ||
		strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces/subinterface/openconfig-if-ip:ipv6/addresses") {
		ipMap, err = getIntfIpByName(inParams.dbs[db.ConfigDB], intTbl.cfgDb.intfTN, ifName, false, true, ipAddr)
		if err == nil {
			log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv6 config ipMap - : ", ipMap)
			convertIpMapToOC(ipMap, intfObj, false, i32)
		}
		ipMap, err = getIntfIpByName(inParams.dbs[db.ApplStateDB], intTbl.appStateDb.intfTN, ifName, false, true, ipAddr)
		if err == nil {
			log.V(lvl.DEBUG).Info("handleIntfIPGetByTargetURI : ipv6 state ipMap - : ", ipMap)
			convertIpMapToOC(ipMap, intfObj, true, i32)
		}
	}
	return err
}

func convertIpMapToOC(intfIpMap map[string]db.Value, ifInfo *ocbinds.OpenconfigInterfaces_Interfaces_Interface, isState bool, subintfid uint32) error {
	var subIntf *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface
	var err error

	if _, ok := ifInfo.Subinterfaces.Subinterface[subintfid]; !ok {
		_, err = ifInfo.Subinterfaces.NewSubinterface(subintfid)
		if err != nil {
			log.V(lvl.ERROR).Info("Creation of subinterface subtree failed!")
			return err
		}
	}

	subIntf = ifInfo.Subinterfaces.Subinterface[subintfid]
	ygot.BuildEmptyTree(subIntf)
	ygot.BuildEmptyTree(subIntf.Ipv4)
	ygot.BuildEmptyTree(subIntf.Ipv6)

	for ipKey, _ := range intfIpMap {
		log.V(lvl.DEBUG).Info("IP address = ", ipKey)
		ipB, ipNetB, _ := parseCIDR(ipKey)
		v4Flag := false
		v6Flag := false

		var v4Address *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv4_Addresses_Address
		var v6Address *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv6_Addresses_Address
		if validIPv4(ipB.String()) {
			if _, ok := subIntf.Ipv4.Addresses.Address[ipB.String()]; !ok {
				_, err = subIntf.Ipv4.Addresses.NewAddress(ipB.String())
			}
			v4Address = subIntf.Ipv4.Addresses.Address[ipB.String()]
			v4Flag = true
		} else if validIPv6(ipB.String()) {
			if _, ok := subIntf.Ipv6.Addresses.Address[ipB.String()]; !ok {
				_, err = subIntf.Ipv6.Addresses.NewAddress(ipB.String())
			}
			v6Address = subIntf.Ipv6.Addresses.Address[ipB.String()]
			v6Flag = true
		} else {
			log.V(lvl.DEBUG).Info("Invalid IP address " + ipB.String())
			continue
		}
		if err != nil {
			log.V(lvl.ERROR).Info("Creation of address subtree failed!")
			return err
		}
		if v4Flag {
			ygot.BuildEmptyTree(v4Address)
			ipStr := new(string)
			*ipStr = ipB.String()
			v4Address.Ip = ipStr
			ipNetBNum := ipNetB.Bits()
			prfxLen := new(uint8)
			*prfxLen = uint8(ipNetBNum)
			if isState {
				v4Address.State.Ip = ipStr
				v4Address.State.PrefixLength = prfxLen
			} else {
				v4Address.Config.Ip = ipStr
				v4Address.Config.PrefixLength = prfxLen
			}
		}
		if v6Flag {
			ygot.BuildEmptyTree(v6Address)
			ipStr := new(string)
			*ipStr = ipB.String()
			v6Address.Ip = ipStr
			ipNetBNum := ipNetB.Bits()
			prfxLen := new(uint8)
			*prfxLen = uint8(ipNetBNum)
			if isState {
				v6Address.State.Ip = ipStr
				v6Address.State.PrefixLength = prfxLen
			} else {
				v6Address.Config.Ip = ipStr
				v6Address.Config.PrefixLength = prfxLen
			}
		}
	}
	return err
}

/*
Google: the upstream version of DbToYang_intf_ip_addr_xfmr includes some clever

	caching of values which we do not need.  Skipping this for now but should
	revisit.  TODO

	var DbToYang_intf_ip_addr_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
		var err error
		intfsObj := getIntfsRoot(inParams.ygRoot)
		pathInfo := NewPathInfo(inParams.uri)
		uriIfName := pathInfo.Var("name")
		ifName := uriIfName

		targetUriPath := pathInfo.YangPath
		log.Infof("DbToYang_intf_ip_addr_xfmr: uri:%v path:%v", inParams.uri, targetUriPath)

		reqPathInfo := NewPathInfo(inParams.requestUri)
		requestUriPath := reqPathInfo.YangPath
		var reqUriIfName string = reqPathInfo.Var("name")

		if (inParams.oper == GET) &&
			((requestUriPath == "/openconfig-interfaces:interfaces" || requestUriPath == "/openconfig-interfaces:interfaces/interface") && reqUriIfName == "") {
			_, present := inParams.txCache.Load("interface_subinterface_ip_read_once")
			if present {
				log.Info("DbToYang_intf_ip_addr_xfmr, top level GET, interface_subinterface_ip_read_once already cached")
				return nil
			}

			intfTypeList := [2]E_InterfaceType{IntfTypeEthernet, IntfTypePortChannel}

			// Get IP from all configDb table interfaces
			for i := 0; i < len(intfTypeList); i++ {
				intfTbl := IntfTypeTblMap[intfTypeList[i]]

				handleAllIntfIPGetForTable(inParams, intfTbl.cfgDb.intfTN, false)
			}

			// Get IP from applDb INTF_TABLE interfaces
			handleAllIntfIPGetForTable(inParams, "INTF_TABLE", true)

			inParams.txCache.Store("interface_subinterface_ip_read_once", true)
			return nil
		} else {
			// Handle GET requests for given interface
			var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
			ifName = *(&uriIfName)

			if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces") {
				if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
					var ok bool = false
					if intfObj, ok = intfsObj.Interface[uriIfName]; !ok {
						intfObj, _ = intfsObj.NewInterface(uriIfName)
					}
					ygot.BuildEmptyTree(intfObj)
					if intfObj.Subinterfaces == nil {
						ygot.BuildEmptyTree(intfObj.Subinterfaces)
					}
				} else {
					ygot.BuildEmptyTree(intfsObj)
					intfObj, _ = intfsObj.NewInterface(uriIfName)
					ygot.BuildEmptyTree(intfObj)
				}

				err = handleIntfIPGetByTargetURI(inParams, targetUriPath, ifName, intfObj)
				if err != nil {
					return err
				}

			} else {
				err = errors.New("Invalid URI : " + targetUriPath)
			}
		}

		return err
	}
*/
var DbToYang_intf_ip_addr_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	var err error
	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	targetUriPath, err := getYangPathFromUri(inParams.uri)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("DbToYang_intf_ip_addr_xfmr: targetUriPath is ", targetUriPath)

	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface

	if strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/subinterfaces") {
		if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
			var ok bool
			if intfObj, ok = intfsObj.Interface[ifName]; !ok {
				intfObj, _ = intfsObj.NewInterface(ifName)
			}
			ygot.BuildEmptyTree(intfObj)
			ygot.BuildEmptyTree(intfObj.Subinterfaces)
		} else {
			ygot.BuildEmptyTree(intfsObj)
			intfObj, _ = intfsObj.NewInterface(ifName)
			ygot.BuildEmptyTree(intfObj)
		}

		return handleIntfIPGetByTargetURI(inParams, targetUriPath, ifName, intfObj)
	}
	return errors.New("Invalid URI : " + targetUriPath)
}

var YangToDb_intf_ip_addr_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	var err error
	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	subIntfmap := make(map[string]map[string]db.Value)
	subIntfmap_del := make(map[string]map[string]db.Value)
	var value db.Value

	pathInfo := NewPathInfo(inParams.uri)
	uriIfName := pathInfo.Var("name")
	idx := pathInfo.Var("index")
	i64, err := strconv.ParseUint(idx, 10, 32)
	i32 := uint32(i64)
	ifName := uriIfName

	sonicIfName := &uriIfName

	log.V(lvl.DEBUG).Infof("YangToDb_intf_ip_addr_xfmr: Interface name retrieved from alias : %s is %s", ifName, *sonicIfName)
	ifName = *sonicIfName
	intfType, _, ierr := getIntfTypeByName(ifName)

	if intfType == IntfTypeBridge {
		// These config paths do not apply to bridge interfaces.
		return nil, nil
	}

	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || len(intfsObj.Interface) < 1 {
		log.V(lvl.DEBUG).Info("YangToDb_intf_subintf_ip_xfmr : IntfsObj/interface list is empty.")
		return subIntfmap, errors.New("IntfsObj/Interface is not specified")
	}

	if ifName == "" {
		errStr := "Interface KEY not present"
		log.V(lvl.DEBUG).Info("YangToDb_intf_subintf_ip_xfmr : " + errStr)
		return subIntfmap, errors.New(errStr)
	}

	if intfType == IntfTypeUnset || ierr != nil {
		errStr := "Invalid interface type IntfTypeUnset"
		log.V(lvl.DEBUG).Info("YangToDb_intf_subintf_ip_xfmr : " + errStr)
		return subIntfmap, errors.New(errStr)
	}
	/* Set invokeCRUSubtreeOnce flag to invoke subtree once */
	if inParams.invokeCRUSubtreeOnce != nil {
		*inParams.invokeCRUSubtreeOnce = true
	}

	/* Validate if DHCP_Relay is provisioned on the interface */
	prefixType := ""
	if strings.Contains(inParams.uri, "ipv4") {
		prefixType = "ipv4"
	} else if strings.Contains(inParams.uri, "ipv6") {
		prefixType = "ipv6"
	}

	if inParams.oper == DELETE {
		dhcpProv, _ := ValidateIntfProvisionedForRelay(inParams.d, ifName, prefixType, nil)
		if dhcpProv {
			errStr := "IP address cannot be deleted. DHCP Relay is configured on the interface."
			return subIntfmap, tlerr.InvalidArgsError{Format: errStr}
		}
	}

	if _, ok := intfsObj.Interface[uriIfName]; !ok {
		errStr := "Interface entry not found in Ygot tree, ifname: " + ifName
		log.V(lvl.DEBUG).Info("YangToDb_intf_subintf_ip_xfmr : " + errStr)
		return subIntfmap, errors.New(errStr)
	}

	intTbl := IntfTypeTblMap[intfType]
	tblName, _ := getIntfTableNameByDBId(intTbl, inParams.curDb)
	intfObj := intfsObj.Interface[uriIfName]

	if intfObj.Subinterfaces == nil || len(intfObj.Subinterfaces.Subinterface) < 1 {
		// Handling the scenario for Interface instance delete at interfaces/interface[name] level or subinterfaces container level
		if inParams.oper == DELETE {
			log.V(lvl.DEBUG).Info("Top level Interface instance delete or subinterfaces container delete for Interface: ", ifName)
			return intf_ip_addr_del(inParams.d, ifName, tblName, nil)
		}
		errStr := "SubInterface node doesn't exist"
		log.V(lvl.DEBUG).Info("YangToDb_intf_subintf_ip_xfmr : " + errStr)
		err = tlerr.InvalidArgsError{Format: errStr}
		return subIntfmap, err
	}
	if _, ok := intfObj.Subinterfaces.Subinterface[i32]; !ok {
		log.V(lvl.DEBUG).Info("YangToDb_intf_subintf_ip_xfmr : No IP address handling required")
		errStr := "SubInterface index 0 doesn't exist"
		err = tlerr.InvalidArgsError{Format: errStr}
		return subIntfmap, err
	}

	subIntfObj := intfObj.Subinterfaces.Subinterface[i32]
	if inParams.oper == DELETE {
		return intf_ip_addr_del(inParams.d, ifName, tblName, subIntfObj)
	}

	entry, dbErr := inParams.d.GetEntry(&db.TableSpec{Name: intTbl.cfgDb.intfTN}, db.Key{Comp: []string{ifName}})
	if dbErr != nil || !entry.IsPopulated() {
		ifdb := make(map[string]string)
		ifdb["NULL"] = "NULL"
		value := db.Value{Field: ifdb}
		if _, ok := subIntfmap[tblName]; !ok {
			subIntfmap[tblName] = make(map[string]db.Value)
		}
		subIntfmap[tblName][ifName] = value

	}
	if subIntfObj.Ipv4 != nil && subIntfObj.Ipv4.Addresses != nil {
		for ip := range subIntfObj.Ipv4.Addresses.Address {
			addr := subIntfObj.Ipv4.Addresses.Address[ip]
			if addr.Config != nil {
				if addr.Config.Ip == nil {
					addr.Config.Ip = new(string)
					*addr.Config.Ip = ip
				}
				log.V(lvl.DEBUG).Info("Ip:=", *addr.Config.Ip)
				if addr.Config.PrefixLength == nil {
					log.Error("Prefix Length empty!")
					errStr := "Prefix Length not present"
					err = tlerr.InvalidArgsError{Format: errStr}
					return subIntfmap, err
				}
				log.V(lvl.DEBUG).Info("prefix:=", *addr.Config.PrefixLength)

				if !validIPv4(*addr.Config.Ip) {
					errStr := "Invalid IPv4 address " + *addr.Config.Ip
					err = tlerr.InvalidArgsError{Format: errStr}
					return subIntfmap, err
				}

				ipPref := *addr.Config.Ip + "/" + strconv.Itoa(int(*addr.Config.PrefixLength))
				/* Google: Permit overlapping IPs
				// Check for IP overlap
				overlapIP, oerr = validateIpOverlap(inParams.d, ifName, ipPref, tblName, true) */

				ipEntry, _ := inParams.d.GetEntry(&db.TableSpec{Name: intTbl.cfgDb.intfTN}, db.Key{Comp: []string{ifName, ipPref}})
				ipMap, _ := getIntfIpByName(inParams.d, intTbl.cfgDb.intfTN, ifName, true, false, "")

				m := make(map[string]string)
				alrdyCfgredIP, primaryIpAlrdyCfgred, err := utlValidateIpTypeForCfgredDiffIp(m, ipMap, &ipEntry, &ipPref, &ifName)
				if err != nil {
					return nil, err
				}
				// Primary IP config already happened and replacing it with new one
				if primaryIpAlrdyCfgred && len(alrdyCfgredIP) != 0 && alrdyCfgredIP != ipPref {
					subIntfmap_del[tblName] = make(map[string]db.Value)
					key := ifName + "|" + alrdyCfgredIP
					subIntfmap_del[tblName][key] = value
					subOpMap[db.ConfigDB] = subIntfmap_del
					log.V(lvl.DEBUG).Info("subOpMap: ", subOpMap)
					inParams.subOpDataMap[DELETE] = &subOpMap
				}

				intf_key := intf_intf_tbl_key_gen(ifName, *addr.Config.Ip, int(*addr.Config.PrefixLength), "|")
				m["family"] = "IPv4"

				value := db.Value{Field: m}
				if _, ok := subIntfmap[tblName]; !ok {
					subIntfmap[tblName] = make(map[string]db.Value)
				}
				subIntfmap[tblName][intf_key] = value
				if log.V(3) {
					log.Info("tblName :", tblName, " intf_key: ", intf_key, " data : ", value)
				}
			}
		}
	}
	if subIntfObj.Ipv6 != nil && subIntfObj.Ipv6.Addresses != nil {
		for ip := range subIntfObj.Ipv6.Addresses.Address {
			addr := subIntfObj.Ipv6.Addresses.Address[ip]
			if addr.Config != nil {
				if addr.Config.Ip == nil {
					addr.Config.Ip = new(string)
					*addr.Config.Ip = ip
				}
				log.V(lvl.DEBUG).Info("Ipv6 IP:=", *addr.Config.Ip)
				if addr.Config.PrefixLength == nil {
					log.Error("Prefix Length empty!")
					errStr := "Prefix Length not present"
					err = tlerr.InvalidArgsError{Format: errStr}
					return subIntfmap, err
				}
				log.V(lvl.DEBUG).Info("Ipv6 prefix:=", *addr.Config.PrefixLength)

				if !validIPv6(*addr.Config.Ip) {
					errStr := "Invalid IPv6 address " + *addr.Config.Ip
					err = tlerr.InvalidArgsError{Format: errStr}
					return subIntfmap, err
				}

				/* Google: Permit overlapping IPs
				// Check for IPv6 overlap
				ipPref := *addr.Config.Ip + "/" + strconv.Itoa(int(*addr.Config.PrefixLength))
				overlapIP, oerr = validateIpOverlap(inParams.d, ifName, ipPref, tblName, true) */

				m := make(map[string]string)

				intf_key := intf_intf_tbl_key_gen(ifName, *addr.Config.Ip, int(*addr.Config.PrefixLength), "|")

				m["family"] = "IPv6"

				value := db.Value{Field: m}
				if _, ok := subIntfmap[tblName]; !ok {
					subIntfmap[tblName] = make(map[string]db.Value)
				}
				subIntfmap[tblName][intf_key] = value
				log.V(lvl.DEBUG).Info("tblName :", tblName, "intf_key: ", intf_key, "data : ", value)
			}
		}
	}

	/* Google: Permit overlapping IPs
	if oerr != nil {
		if overlapIP == "" {
			log.Error(oerr)
			return nil, tlerr.InvalidArgsError{Format: oerr.Error()}
		} else {
			subIntfmap_del[tblName] = make(map[string]db.Value)
			key := ifName + "|" + overlapIP
			subIntfmap_del[tblName][key] = value
			subOpMap[db.ConfigDB] = subIntfmap_del
			log.V(lvl.DEBUG).Info("subOpMap: ", subOpMap)
			inParams.subOpDataMap[DELETE] = &subOpMap
		}
	} */

	// In case of a Set-Replace for subinterface/ip*/addresses subtree, do an Update instead to
	// preserve system-written fields in MGMT_INTERFACE
	if inParams.oper == REPLACE &&
		(intfType == IntfTypeMgmt || intfType == IntfTypeMgmtBond) {
		updateSubOpDataMap(map[db.DBNum]map[string]map[string]db.Value{
			db.ConfigDB: subIntfmap,
		}, UPDATE, inParams)
		return nil, nil
	}

	log.V(lvl.DEBUG).Infof("YangToDb_intf_subintf_ip_xfmr : subIntfmap : ", subIntfmap)
	return subIntfmap, err
}

/* Google: Permit overlapping IPs
// Check for IP overlap
func validateIpOverlap(d *db.DB, intf string, ipPref string, tblName string, isIntfIp bool) (string, error) {
	log.V(lvl.DEBUG).Info("Checking for IP overlap ....")

	ipA, ipNetA, err := parseCIDR(ipPref)
	if err != nil {
		log.Info("Failed to parse IP address: ", ipPref)
		return "", err
	}

	var allIntfKeys []db.Key

	for key := range IntfTypeTblMap {
		intTbl := IntfTypeTblMap[key]
		keys, err := d.GetKeys(&db.TableSpec{Name: intTbl.cfgDb.intfTN})
		if err != nil {
			log.Info("Failed to get keys; err=%v", err)
			return "", err
		}
		allIntfKeys = append(allIntfKeys, keys...)
	}

	if len(allIntfKeys) > 0 {
		for _, key := range allIntfKeys {
			if len(key.Comp) < 2 {
				continue
			}
			ipB, ipNetB, perr := parseCIDR(key.Get(1))
			//Check if key has IP, if not continue
			if perr != nil {
				continue
			}
			if ipNetA.Contains(ipB) || ipNetB.Contains(ipA) {
				if log.V(3) {
					log.Info("IP: ", ipPref, " overlaps with ", key.Get(1), " of ", key.Get(0))
				}

				//Handle IP overlap across different interface
				_, _, ierr := getIntfTypeByName(key.Get(0))
				if ierr != nil {
					log.Errorf("Extracting Interface type for Interface: %s failed!", key.Get(0))
					return "", ierr
				}
				if intf != key.Get(0) {
					errStr := "IP " + ipPref + " overlaps with IP or IP Anycast " + key.Get(1) + " of Interface " + key.Get(0)
					return "", errors.New(errStr)
				}
			}
		}
	}
	return "", nil
} */

func utlCheckAndRetrievePrimaryIPConfigured(ipMap map[string]db.Value) (bool, string) {
	for ipKey, _ := range ipMap {
		return true, ipKey
	}
	return false, ""
}

func utlValidateIpTypeForCfgredDiffIp(m map[string]string, ipMap map[string]db.Value, ipEntry *db.Value, ipPref *string, ifName *string) (string, bool, error) {
	dbgStr := "IPv4 address"
	checkPrimIPCfgred, cfgredPrimIP := utlCheckAndRetrievePrimaryIPConfigured(ipMap)
	if checkPrimIPCfgred && !ipEntry.IsPopulated() {
		infoStr := "Primary " + dbgStr + " is already configured for interface: " + *ifName
		log.V(lvl.DEBUG).Info(infoStr)
		return cfgredPrimIP, true, nil
	}

	return "", false, nil
}

func intf_intf_tbl_key_gen(intfName string, ip string, prefixLen int, keySep string) string {
	return intfName + keySep + ip + "/" + strconv.Itoa(prefixLen)
}

func parseCIDR(ipPref string) (netip.Addr, netip.Prefix, error) {
	prefIdx := strings.LastIndexByte(ipPref, '/')
	if prefIdx <= 0 {
		return netip.Addr{}, netip.Prefix{}, fmt.Errorf("Invalid Prefix(%q): no'/'", ipPref)
	}
	prefLen, _ := strconv.Atoi(ipPref[prefIdx+1:])
	ipA, err := netip.ParseAddr(ipPref[:prefIdx])
	if err != nil {
		log.Infof("parseCIDR: Failed to parse IP address:%s : err : %s ", ipPref, err)
		return netip.Addr{}, netip.Prefix{}, fmt.Errorf("Failed to parse IP address: %s", ipPref)
	}

	ipNetA, _ := ipA.Prefix(prefLen)
	return ipA, ipNetA, nil
}

func getIntfIpByName(dbCl *db.DB, tblName string, ifName string, ipv4 bool, ipv6 bool, ip string) (map[string]db.Value, error) {
	var err error
	intfIpMap := make(map[string]db.Value)
	all := true
	if !ipv4 || !ipv6 {
		all = false
	}
	log.V(lvl.DEBUG).Info("Updating Interface IP Info from DB to Internal DS for Interface Name : ", ifName)

	keys, err := doGetIntfIpKeys(dbCl, tblName, ifName)
	log.V(lvl.DEBUG).Infof("Found %d keys for (%v)(%v)", len(keys), tblName, ifName)
	if err != nil {
		return intfIpMap, err
	}
	for _, key := range keys {
		if len(key.Comp) < 2 {
			continue
		}
		if key.Get(0) != ifName {
			continue
		}
		if len(key.Comp) > 2 {
			for i := range key.Comp {
				if i == 0 || i == 1 {
					continue
				}
				key.Comp[1] = key.Comp[1] + ":" + key.Comp[i]
			}
		}
		if !all {
			ipB, _, _ := parseCIDR(key.Get(1))
			if (validIPv4(ipB.String()) && (!ipv4)) ||
				(validIPv6(ipB.String()) && (!ipv6)) {
				continue
			}
			if ip != "" {
				if ipB.String() != ip {
					continue
				}
			}
		}

		ipInfo, _ := dbCl.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{key.Get(0), key.Get(1)}})
		intfIpMap[key.Get(1)] = ipInfo
	}
	return intfIpMap, err
}

/* Get all IP keys for given interface */
func doGetIntfIpKeys(d *db.DB, tblName string, intfName string) ([]db.Key, error) {
	ts := db.TableSpec{Name: tblName + d.Opts.KeySeparator + intfName, CompCt: 2}
	ipKeys, err := d.GetKeys(&ts)
	log.V(lvl.DEBUG).Infof("doGetIntfIpKeys for %s with %v - %v", intfName, ts, ipKeys)
	return ipKeys, err
}

func validIPv4(ipAddress string) bool {
	/* Dont allow ip addresses that start with "0." or "255."*/
	if strings.HasPrefix(ipAddress, "0.") || strings.HasPrefix(ipAddress, "255.") {
		log.V(lvl.ERROR).Info("validIP: IP is reserved ", ipAddress)
		return false
	}

	ip := net.ParseIP(ipAddress)
	ipAddress = strings.Trim(ipAddress, " ")

	re, _ := regexp.Compile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)
	if re.MatchString(ipAddress) {
		return validIP(ip)
	}
	return false
}

func validIPv6(ipAddress string) bool {
	ip := net.ParseIP(ipAddress)
	ipAddress = strings.Trim(ipAddress, " ")

	re, _ := regexp.Compile(`(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))`)
	if re.MatchString(ipAddress) {
		return validIP(ip)
	}
	return false
}

func validIP(ip net.IP) bool {
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() {
		return false
	}
	return true
}

func getIntfTableNameByDBId(intftbl IntfTblData, curDb db.DBNum) (string, error) {

	var tblName string

	switch curDb {
	case db.ConfigDB:
		tblName = intftbl.cfgDb.intfTN
	case db.ApplStateDB:
		tblName = intftbl.appStateDb.intfTN
	case db.ApplDB:
		tblName = intftbl.appDb.intfTN
	case db.StateDB:
		tblName = intftbl.stateDb.intfTN
	default:
		tblName = intftbl.cfgDb.intfTN
	}

	return tblName, nil
}

func intf_ip_addr_del(d *db.DB, ifName string, tblName string, subIntf *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface) (map[string]map[string]db.Value, error) {
	subIntfmap := make(map[string]map[string]db.Value)
	intfIpMap := make(map[string]db.Value)

	// Handles the case when the delete request at subinterfaces/subinterface[index = 0]
	if subIntf == nil || (subIntf.Ipv4 == nil && subIntf.Ipv6 == nil) {
		ipMap, _ := getIntfIpByName(d, tblName, ifName, true, true, "")
		for k, v := range ipMap {
			intfIpMap[k] = v
		}
	}

	// This handles the delete for a specific IPv4 address or a group of IPv4 addresses
	if subIntf != nil && subIntf.Ipv4 != nil && subIntf.Ipv4.Addresses != nil && len(subIntf.Ipv4.Addresses.Address) > 0 {
		for ip := range subIntf.Ipv4.Addresses.Address {
			ipMap, _ := getIntfIpByName(d, tblName, ifName, true, false, ip)
			for k, v := range ipMap {
				// Primary IPv4 delete
				ifIpMap, _ := getIntfIpByName(d, tblName, ifName, true, false, "")

				checkIPCfgred, _ := utlCheckAndRetrievePrimaryIPConfigured(ifIpMap)

				if checkIPCfgred {
					intfIpMap[k] = v
				}
			}
		}
	} else if subIntf != nil && subIntf.Ipv4 != nil {
		// Case when delete request is at IPv4 container level
		ipMap, _ := getIntfIpByName(d, tblName, ifName, true, false, "")
		for k, v := range ipMap {
			intfIpMap[k] = v
		}
	}

	// This handles the delete for a specific IPv6 address or a group of IPv6 addresses
	if subIntf != nil && subIntf.Ipv6 != nil && subIntf.Ipv6.Addresses != nil && len(subIntf.Ipv6.Addresses.Address) > 0 {
		for ip := range subIntf.Ipv6.Addresses.Address {
			ipMap, _ := getIntfIpByName(d, tblName, ifName, false, true, ip)
			for k, v := range ipMap {
				intfIpMap[k] = v
			}
		}
	} else if subIntf != nil && subIntf.Ipv6 != nil {
		// Case when the delete request is at IPv6 container level
		ipMap, _ := getIntfIpByName(d, tblName, ifName, false, true, "")
		for k, v := range ipMap {
			intfIpMap[k] = v
		}
	}
	if len(intfIpMap) > 0 {
		if _, ok := subIntfmap[tblName]; !ok {
			subIntfmap[tblName] = make(map[string]db.Value)
		}
		var data db.Value
		for k := range intfIpMap {
			ifKey := ifName + "|" + k
			subIntfmap[tblName][ifKey] = data
		}
		intfIpCnt := 0
		_ = interfaceIPcount(tblName, d, &ifName, &intfIpCnt)
		/* Delete interface from interface table if no other interface attributes/ip */
		ipCntAfterDeletion := intfIpCnt - len(intfIpMap)
		if check_if_delete_l3_intf_entry(d, tblName, ifName, ipCntAfterDeletion, nil) {
			if _, ok := subIntfmap[tblName]; !ok {
				subIntfmap[tblName] = make(map[string]db.Value)
			}
			subIntfmap[tblName][ifName] = data
		}
	}
	log.V(lvl.DEBUG).Info("Delete IP address list ", subIntfmap)
	return subIntfmap, nil
}

func interfaceIPcount(tblName string, d *db.DB, intfName *string, ipCnt *int) error {
	ipKeys, _ := d.GetKeysPattern(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{*intfName, "*"}})
	*ipCnt = len(ipKeys)
	return nil
}
func check_if_delete_l3_intf_entry(d *db.DB, tblName string, ifName string, ipCnt int, intfEntry *db.Value) bool {
	if intfEntry == nil {
		entry, err := d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{ifName}})
		if err != nil {
			// Failed to read entry from config DB
			return false
		}
		intfEntry = &entry
	}
	if ipCnt == 0 && intfEntry.IsPopulated() {
		intfEntryMap := intfEntry.Field
		_, nullValPresent := intfEntryMap["NULL"]
		/* Note: Unbinding shouldn't happen if VRF config is associated with interface.
		   Hence, we check for map length and only if NULL value is present */
		if len(intfEntryMap) == 1 && nullValPresent {
			return true
		}
	}
	return false
}

var Subscribe_intf_ip_addr_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	if log.V(3) {
		log.Info("Entering Subscribe_intf_ip_addr_xfmr")
	}
	var err error
	var result XfmrSubscOutParams

	pathInfo := NewPathInfo(inParams.uri)
	origTargetUriPath := pathInfo.YangPath

	log.Infof("Subscribe_intf_ip_addr_xfmr:- subscProc:%v URI: %s", inParams.subscProc, inParams.uri)
	log.Infof("Subscribe_intf_ip_addr_xfmr:- Target URI path: %s", origTargetUriPath)

	// When the subscribe subtree is invoked in the GET or CRUD context the inParams.subscProc is set to TRANSLATE_EXISTS
	if inParams.subscProc == TRANSLATE_EXISTS {
		// Defer the DB resource check done by infra by setting the virtual table to true.
		// Resource checks are now performed within the DbToYang or YangToDb subtree callback.
		result.isVirtualTbl = true
		return result, nil
	}
	if inParams.subscProc == TRANSLATE_SUBSCRIBE {

		ifBasePath := "/openconfig-interfaces:interfaces/interface"
		targetUriPath := origTargetUriPath[len(ifBasePath):]

		if strings.HasPrefix(targetUriPath, "/subinterfaces") {
			targetUriPath = targetUriPath[len("/subinterfaces/subinterface"):]
		}
		if strings.HasPrefix(targetUriPath, "/openconfig-if-ip:ipv4") {
			targetUriPath = targetUriPath[len("/openconfig-if-ip:ipv4/addresses"):]
		} else {
			targetUriPath = targetUriPath[len("/openconfig-if-ip:ipv6/addresses"):]
		}

		if targetUriPath == "" || targetUriPath == "/address" {
			result.isVirtualTbl = true
			log.V(lvl.DEBUG).Info("Subscribe_intf_ip_addr_xfmr:- result.isVirtualTbl: ", result.isVirtualTbl)
			return result, err
		}

		result.onChange = OnchangeEnable
		result.nOpts = &notificationOpts{}
		result.nOpts.pType = OnChange
		result.isVirtualTbl = false

		tableName := ""
		ipKey := ""
		ifKey := ""

		ifKey = pathInfo.StringVar("name", "*")

		addressConfigPath := "/address/config"
		addressStatePath := "/address/state"

		idx := pathInfo.Var("index")
		if ifKey != "" {
			if idx == "0" || idx == "*" {
				intfType, _, _ := getIntfTypeByName(ifKey)
				intTbl := IntfTypeTblMap[intfType]
				if targetUriPath == addressStatePath {
					tableName = intTbl.appStateDb.intfTN
				} else {
					tableName = intTbl.cfgDb.intfTN
				}
			} else {
				log.Infof("Subscribe_intf_ip_addr_xfmr: ifKey \"%s\" idx \"%s\"", ifKey, idx)
				err_str := "Subinterfaces not supported"
				return result, tlerr.NotSupported(err_str)
			}
		}

		ipKey = pathInfo.Var("ip")
		if ipKey == "" {
			ipKey = "*"
		}

		if ipKey != "*" {
			ipKey = ipKey + "/*"
		}

		log.Infof("path:%v ifKey:%v, ipKey:%v tbl:[%v]", origTargetUriPath, ifKey, ipKey, tableName)

		keyName := ""
		if targetUriPath == addressConfigPath {
			keyName = ifKey + "|" + ipKey
			if tableName != "" {
				result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {tableName: {keyName: {}}}}
			} else {
				result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {"INTERFACE": {keyName: {}},
					"PORTCHANNEL_INTERFACE": {keyName: {}}}}
			}
		} else if targetUriPath == addressStatePath {
			keyName = ifKey + ":" + ipKey
			if tableName != "" {
				result.dbDataMap = RedisDbSubscribeMap{db.ApplStateDB: {tableName: {keyName: {KEY_COMP_CNT: "2", DEL_AS_UPDATE: "true"}}}}
			} else {
				result.dbDataMap = RedisDbSubscribeMap{db.ApplStateDB: {"INTF_TABLE": {keyName: {KEY_COMP_CNT: "2", DEL_AS_UPDATE: "true"}}}}
			}
		}

		log.V(lvl.DEBUG).Info("Subscribe_intf_ip_addr_xfmr:- result dbDataMap: ", result.dbDataMap)
		log.V(lvl.DEBUG).Info("Subscribe_intf_ip_addr_xfmr:- result secDbDataMap: ", result.secDbDataMap)

		return result, err
	}
	/* Google: removing this code since EXIST and SUBSCRIBE are handled above and those are the only two cases
	result.isVirtualTbl = false
	result.dbDataMap = make(RedisDbSubscribeMap)
	uriIfName := pathInfo.Var("name")
	idx := pathInfo.Var("index")
	sonicIfName := &uriIfName
	keyName := *sonicIfName

	if keyName != "" {
		intfType, _, _ := getIntfTypeByName(keyName)
		intTbl := IntfTypeTblMap[intfType]
		tblName := intTbl.cfgDb.intfTN
		if idx != "" && idx != "0" {
			err_str := "Subinterfaces not supported"
			return result, tlerr.NotSupported(err_str)
		}
		result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {tblName: {keyName: {}}}}
	}
	log.Info("Returning Subscribe_intf_ip_addr_xfmr, result:", result)

	result.needCache = true
	result.nOpts = new(notificationOpts)
	result.nOpts.mInterval = 15
	result.nOpts.pType = OnChange
	log.Info("Returning Subscribe_intf_ip_addr_xfmr, result:", result)
	*/
	return result, err
}

var DbToYangPath_intf_ip_path_xfmr PathXfmrDbToYangFunc = func(params XfmrDbToYgPathParams) error {
	ifRoot := "/openconfig-interfaces:interfaces/interface"
	subIf := ifRoot + "/subinterfaces/subinterface"
	dbKey := ""

	log.V(lvl.DEBUG).Info("DbToYangPath_intf_ip_path_xfmr: params: ", params)

	uiName := &params.tblKeyComp[0]
	ifParts := strings.Split(*uiName, ".")

	params.ygPathKeys[ifRoot+"/name"] = ifParts[0]

	if params.tblName == "INTERFACE" || params.tblName == "INTF_TABLE" ||
		params.tblName == "PORTCHANNEL_INTERFACE" {

		addrPath := "/openconfig-if-ip:ipv4/addresses/address/ip"

		/* For APPL_DB IPv6 case, addr is split [fe80  56bf 64ff feba 3bc0/64] instead of
		   [fe80::56bf:64ff:feba:3bc0/64]
		   Handle this case
		*/
		dbKey = strings.Join(params.tblKeyComp[1:], ":")

		if len(params.tblKeyComp) > 2 || strings.Contains(dbKey, ":") {
			addrPath = "/openconfig-if-ip:ipv6/addresses/address/ip"
		}

		ipKey := strings.Split(dbKey, "/")

		if len(ifParts) > 1 {
			err_str := "Subinterfaces not supported"
			return tlerr.NotSupported(err_str)
		} else {
			params.ygPathKeys[subIf+"/index"] = "0"
		}
		params.ygPathKeys[subIf+addrPath] = ipKey[0]
	}

	log.Infof("DbToYangPath_intf_ip_path_xfmr:  tblName:%v dbKey:[%v] params.ygPathKeys: %v", params.tblName, dbKey, params.ygPathKeys)
	return nil
}

// YangToDb_subintf_ipv6_tbl_key_xfmr is a YangToDB Key transformer for IPv6 config.
var YangToDb_subintf_ipv6_tbl_key_xfmr KeyXfmrYangToDb = func(inParams XfmrParams) (string, error) {
	log.V(lvl.DEBUG).Info("Entering YangToDb_subintf_ipv6_tbl_key_xfmr")

	var err error
	var inst_key string
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	requestUriPath, err := getYangPathFromUri(inParams.requestUri)
	log.V(lvl.DEBUG).Info("inParams.requestUri: ", requestUriPath)
	idx := pathInfo.Var("index")
	var i32 uint32
	i32 = 0
	if idx != "" {
		i64, _ := strconv.ParseUint(idx, 10, 32)
		i32 = uint32(i64)
	}
	inst_key = ifName
	if i32 > 0 {
		inst_key = ifName + "." + idx
	}
	log.V(lvl.DEBUG).Infof("Exiting YangToDb_subintf_ipv6_tbl_key_xfmr, key %s", inst_key)
	return inst_key, err
}

// DbToYang_subintf_ipv6_tbl_key_xfmr is a DbToYang key transformer for IPv6 config.
var DbToYang_subintf_ipv6_tbl_key_xfmr KeyXfmrDbToYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	if log.V(3) {
		log.Info("Entering DbToYang_subintf_ipv6_tbl_key_xfmr")
	}

	rmap := make(map[string]interface{})
	return rmap, nil
}

// YangToDb_ipv6_enabled_xfmr is a YangToDB Field transformer for IPv6 config "enabled".
var YangToDb_ipv6_enabled_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	if log.V(3) {
		log.Info("Entering YangToDb_ipv6_enabled_xfmr")
	}
	var err error
	res_map := make(map[string]string)
	pathInfo := NewPathInfo(inParams.uri)
	ifUIName := pathInfo.Var("name")

	intfType, _, ierr := getIntfTypeByName(ifUIName)
	if ierr != nil || intfType == IntfTypeUnset {
		return res_map, errors.New("YangToDb_ipv6_enabled_xfmr, Error: Unsupported Interface: " + ifUIName)
	}

	if ifUIName == "" {
		errStr := "Interface KEY not present"
		log.V(lvl.DEBUG).Info("YangToDb_ipv6_enabled_xfmr: " + errStr)
		return res_map, errors.New(errStr)
	}

	if inParams.param == nil {
		return res_map, err
	}

	// Vlan Interface (routed-vlan) contains only one Key "ifname"
	// For all other interfaces (subinterfaces/subintfaces) will have 2 keys "ifname" & "subintf-index"
	if len(pathInfo.Vars) < 2 {
		return res_map, errors.New("YangToDb_ipv6_enabled_xfmr, Error: Invalid Key length")
	}

	if log.V(3) {
		log.Info("YangToDb_ipv6_enabled_xfmr, inParams.key: ", inParams.key)
	}

	ifName := &ifUIName

	intTbl := IntfTypeTblMap[intfType]
	tblName := intTbl.cfgDb.intfTN
	ipMap, _ := getIntfIpByName(inParams.d, tblName, *ifName, true, true, "")
	var enStr string
	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	subOpTblMap := make(map[string]map[string]db.Value)
	field_map := make(map[string]db.Value)
	res_values := db.Value{Field: map[string]string{}}
	IntfMap := make(map[string]string)

	enabled, _ := inParams.param.(*bool)
	if *enabled {
		enStr = "enable"
	} else {
		enStr = "disable"
	}

	IntfMapObj, err := inParams.d.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{*ifName}})
	if err == nil || IntfMapObj.IsPopulated() {
		IntfMap = IntfMapObj.Field
	}
	val, fieldExists := IntfMap["ipv6_use_link_local_only"]
	if fieldExists && val == enStr {
		// Check if already set to required value
		log.V(lvl.DEBUG).Info("IPv6 is already %s.", enStr)
		return nil, nil
	}

	res_map["ipv6_use_link_local_only"] = enStr
	if log.V(3) {
		log.Info("YangToDb_ipv6_enabled_xfmr, res_map: ", res_map)
	}

	if enStr == "disable" {

		if len(IntfMap) == 0 {
			return nil, nil
		}

		keys := make([]string, 0, len(IntfMap))
		for k := range IntfMap {
			keys = append(keys, k)
		}
		check_keys := []string{"NULL", "ipv6_use_link_local_only"}
		sort.Strings(keys)
		/* Delete interface from interface table if disabling IPv6 and no other interface attributes/ip
		   else remove ipv6_use_link_local_only field */
		if !((reflect.DeepEqual(keys, check_keys) || reflect.DeepEqual(keys, check_keys[1:])) && len(ipMap) == 0) {
			//Checking if field entry exists
			if !fieldExists {
				//Nothing to delete
				return nil, nil
			}
			log.V(lvl.DEBUG).Info("YangToDb_ipv6_enabled_xfmr, deleting ipv6_use_link_local_only field")
			//Delete field entry
			(&res_values).Set("ipv6_use_link_local_only", enStr)
		}
		field_map[*ifName] = res_values
		subOpTblMap[tblName] = field_map
		subOpMap[db.ConfigDB] = subOpTblMap
		inParams.subOpDataMap[DELETE] = &subOpMap
		if log.V(3) {
			log.Info("YangToDb_ipv6_enabled_xfmr, subOpMap: ", subOpMap)
		}
		return nil, nil
	}
	return res_map, nil
}

// DbToYang_ipv6_enabled_xfmr is a DbToYang Field transformer for IPv6 config "enabled". */
var DbToYang_ipv6_enabled_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("DbToYang_ipv6_enabled_xfmr, inParams.key ", inParams.key)

	intfType, _, _ := getIntfTypeByName(inParams.key)

	intTbl := IntfTypeTblMap[intfType]
	tblName, _ := getIntfTableNameByDBId(intTbl, inParams.curDb)

	data := (*inParams.dbDataMap)[inParams.curDb]

	res_map := make(map[string]interface{})
	res_map["enabled"] = false
	ipv6_status, ok := data[tblName][inParams.key].Field["ipv6_use_link_local_only"]

	if ok && ipv6_status == "enable" {
		res_map["enabled"] = true
	}
	return res_map, nil
}

func getMemTableNameByDBId(intftbl IntfTblData, curDb db.DBNum) (string, error) {

	var tblName string

	switch curDb {
	case db.ConfigDB:
		tblName = intftbl.cfgDb.memberTN
	case db.ApplDB:
		tblName = intftbl.appDb.memberTN
	case db.StateDB:
		tblName = intftbl.stateDb.memberTN
	case db.ApplStateDB:
		tblName = intftbl.appStateDb.memberTN
	default:
		tblName = intftbl.cfgDb.memberTN
	}

	return tblName, nil
}

func retrievePortChannelAssociatedWithIntf(inParams *XfmrParams, ifName *string) (*string, error) {
	var err error

	if strings.HasPrefix(*ifName, ETHERNET) {
		intTbl := IntfTypeTblMap[IntfTypePortChannel]
		tblName, _ := getMemTableNameByDBId(intTbl, inParams.curDb)
		var lagStr string

		lagKeys, err := inParams.d.GetKeysByPattern(&db.TableSpec{Name: tblName}, "*"+*ifName)
		/* Find the port-channel the given ifname is part of */
		if err != nil {
			return nil, err
		}
		var flag bool = false
		for i := range lagKeys {
			if *ifName == lagKeys[i].Get(1) {
				flag = true
				lagStr = lagKeys[i].Get(0)
				log.V(lvl.DEBUG).Info("Given interface part of PortChannel ", lagStr)
				break
			}
		}
		if !flag {
			log.V(lvl.DEBUG).Infof("Given Interface (%s) not part of any PortChannel", *ifName)
			return nil, err
		}
		return &lagStr, err
	}
	return nil, err
}

func getPCTablesForDeletion(cfgDB *db.DB, tblName string, keyLen int, keysToKeep map[string]bool) (map[string]db.Value, error) {
	deleteMap := make(map[string]db.Value)

	keys, err := cfgDB.GetKeys(&db.TableSpec{Name: tblName})
	if err != nil {
		return deleteMap, err
	}
	for _, key := range keys {
		if key.Len() < keyLen {
			continue
		}
		pcKey := key.Get(0)
		for i := 1; i < keyLen; i++ {
			pcKey += "|" + key.Get(i)
		}
		if _, ok := keysToKeep[pcKey]; !ok {
			deleteMap[pcKey] = db.Value{}
		}
	}
	return deleteMap, nil
}

func getTrunkQueueTablesForDeletion(cfgDB *db.DB, keysToKeep map[string]bool) (map[string]db.Value, error) {
	deleteMap := make(map[string]db.Value)
	keys, err := cfgDB.GetKeys(&db.TableSpec{Name: "UMF_TRUNK_QUEUE"})
	if err != nil {
		return deleteMap, err
	}
	for _, key := range keys {
		if key.Len() != 2 {
			continue
		}
		pcKey := key.Get(0)
		tqKey := strings.Join(key.Comp, "|")
		if _, ok := keysToKeep[pcKey]; !ok {
			deleteMap[tqKey] = db.Value{}
		}
	}
	return deleteMap, nil
}

// pcToMembersMap converts pcMembers (map[string]bool) to a map[string][]string
// that maps PortChannel (string) -> Members ([]string) and returns that map.
func pcToMembersMap() map[string][]string {
	pcToMembers := map[string][]string{}
	for pcKey, _ := range pcMembers {
		// pcKey is in the form PortChannel1|Ethernet1/1/1
		pcKeySplit := strings.Split(pcKey, "|")
		if len(pcKeySplit) != 2 {
			log.V(lvl.ERROR).Infof("Invalid pcKey: %v", pcKey)
			continue
		}
		pc := pcKeySplit[0]
		mem := pcKeySplit[1]

		if pcMems, ok := pcToMembers[pc]; ok {
			pcToMembers[pc] = append(pcMems, mem)
		} else {
			pcToMembers[pc] = []string{mem}
		}
	}
	return pcToMembers
}

func appendExistingDBAttr(cfgDB *db.DB, tblName, key string, attrList []string, retDbDataMap map[string]map[string]db.Value) {
	entry, err := cfgDB.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{key}})
	if err != nil {
		return
	}
	for _, attr := range attrList {
		if val := entry.Get(attr); val != "" {
			retDbDataMap[tblName][key].Field[attr] = val
		}
	}
}

var DbToYang_intf_hardware_port_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	index, err := getPortIndex(inParams, "DbToYang_intf_hardware_port_xfmr")
	if err != nil {
		return nil, err
	}
	result[HARDWARE_PORT] = "1/" + index
	return result, nil
}

var DbToYang_intf_transceiver_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	index, err := getPortIndex(inParams, "DbToYang_intf_transceiver_xfmr")
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"transceiver": "Ethernet" + index}, nil
}

func getDBValues(inParams XfmrParams, tblName string) (db.Value, error) {
	if tblName == "" {
		return db.Value{Field: map[string]string{}}, errors.New("Invalid inParams or invalid tableName")
	}
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	prtInst, dbErr := inParams.dbs[inParams.curDb].GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{ifName}})
	if dbErr != nil {
		return db.Value{Field: map[string]string{}}, dbErr
	}
	return prtInst, nil
}

func getPortIndex(inParams XfmrParams, funcName string) (string, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return "", errors.New("invalid interface type IntfTypeUnset. Err: " + err.Error())
	}
	if intfType != IntfTypeEthernet {
		return "", errors.New("interface type is not IntfTypeEthernet")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		log.V(lvl.ERROR).Infof("%s type not found : %v", funcName, intfType)
		return "", errors.New("interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		log.V(lvl.ERROR).Infof("%s table name not found", funcName)
		return "", errors.New("table name not found. Err: " + err.Error())
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return "", dbErr
	}
	index, ok := prtInst.Field[PORT_INDEX]
	if !ok {
		return "", errors.New(funcName + " index not found in DB")
	}
	return index, nil
}

func getUint64Field(p *db.Value, dbField string) (val uint64, err error) {
	valStr, ok := p.Field[dbField]
	if !ok || valStr == "" {
		return 0, tlerr.NotFound(dbField + " not found in DB")
	}
	if val, err = strconv.ParseUint(valStr, 10, 64); err == nil {
		return val, nil
	}
	return 0, err
}

var DbToYang_intf_physical_channel_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, errors.New("invalid interface type IntfTypeUnset. Err: " + err.Error())
	}
	if intfType != IntfTypeEthernet {
		return nil, errors.New("interface type is not IntfTypeEthernet")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("table name not found. Err: " + err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}
	lanes, ok := prtInst.Field["lanes"]
	if !ok {
		return nil, errors.New("DbToYang_intf_physical_channel_xfmr: lanes not found in DB")
	}

	index, err := getPortIndex(inParams, "DbToYang_intf_physical_channel_xfmr")
	if err != nil {
		return nil, err
	}
	xcvrName := "Ethernet" + index
	stateDB := inParams.dbs[db.StateDB]
	xcvrEntry, err := stateDB.GetEntry(&db.TableSpec{Name: "TRANSCEIVER_INFO"}, db.Key{Comp: []string{xcvrName}})
	if err != nil {
		return nil, err
	}
	xcvrType := xcvrEntry.Get("type")
	if xcvrType == "" {
		return nil, errors.New("DbToYang_intf_physical_channel_xfmr: empty transceiver type for physical-channel")
	}
	maxLanes, ok := sfpTypeToMaxLanesMap[xcvrType]
	if !ok {
		return nil, errors.New("DbToYang_intf_physical_channel_xfmr: could not find the max number of lanes for transceiver.")
	}
	offset, err := platform.ChannelOffset(ifName)
	if err != nil {
		return nil, errors.New("DbToYang_intf_physical_channel_xfmr: could not find the channel offset for " + ifName)
	}

	lanesSplit := strings.Split(lanes, ",")
	channels := make([]uint16, 0, len(lanesSplit))
	for _, str := range lanesSplit {
		val, err := strconv.ParseUint(str, 10, 16)
		if err != nil {
			return nil, errors.New("DbToYang_intf_physical_channel_xfmr: err in strconv")
		}
		channels = append(channels, (uint16(val)-offset)%uint16(maxLanes))
	}
	return map[string]interface{}{"physical-channel": channels}, nil
}

var DbToYang_intf_last_change_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, fmt.Errorf("DbToYang_intf_last_change_xfmr: Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, err)
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, errors.New("Interface type is not IntfTypeEthernet or IntfTypePortChannel")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_last_change_xfmr: interface type not found : " + strconv.Itoa(int(intfType)))
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_last_change_xfmr: table name not found.")
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	resMap := make(map[string]interface{})
	lcs, err := getUint64Field(&prtInst, "last-change-seconds")
	if err != nil {
		return nil, errors.New("DbToYang_intf_last_change_xfmr: " + err.Error())
	}
	lcns, err := getUint64Field(&prtInst, "last-change-nanoseconds")
	if err != nil {
		return nil, errors.New("DbToYang_intf_last_change_xfmr: " + err.Error())
	}
	lastChange := (lcs * 1000000000) + lcns
	resMap[PORT_LAST_CHANGE] = strconv.FormatUint(lastChange, 10)
	return resMap, nil
}

var YangToDb_intf_name_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	var err error

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if strings.HasPrefix(ifName, PORTCHANNEL) {
		pcs[ifName] = true
	}

	if strings.HasPrefix(ifName, LOOPBACK) || strings.HasPrefix(ifName, MGMT_BOND) || strings.HasPrefix(ifName, MGMT) || strings.HasPrefix(ifName, PORTCHANNEL) || strings.HasPrefix(ifName, BRIDGE) {
		res_map["NULL"] = "NULL"
	} else if strings.HasPrefix(ifName, ETHERNET) {
		if inParams.oper == UPDATE || inParams.oper == DELETE {
			intTbl, ok := IntfTypeTblMap[IntfTypeEthernet]
			if !ok {
				return nil, errors.New("YangToDb_intf_name_xfmr: interface type not found: IntfTypeEthernet")
			}
			// Check if physical interface exists, if not return error
			err = validateIntfExists(inParams.d, intTbl.cfgDb.portTN, ifName)
			if err != nil {
				errStr := "Interface " + ifName + " cannot be configured since it does not exist; err = " + err.Error()
				return res_map, tlerr.InvalidArgsError{Format: errStr}
			}
		}
	}
	log.V(lvl.DEBUG).Info("YangToDb_intf_name_xfmr: res_map:", res_map)
	return res_map, err
}

var DbToYang_intf_name_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	res_map := make(map[string]interface{})

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	log.V(lvl.DEBUG).Info("DbToYang_intf_name_xfmr: Interface Name = ", ifName)
	res_map["name"] = ifName
	return res_map, nil
}

func updateDefaultLoopbackMode(inParams *XfmrParams, ifName *string, ifType E_InterfaceType, resMap map[string]string) error {
	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	intfMap := make(map[string]map[string]db.Value)

	intTbl, ok := IntfTypeTblMap[ifType]
	if !ok {
		log.V(lvl.ERROR).Info("updateDefaultLoopbackMode interface type not found : ", ifType)
		return errors.New("interface type not found.")
	}
	resMap[PORT_LOOPBACK_MODE] = "none"

	intfMap[intTbl.cfgDb.portTN] = make(map[string]db.Value)
	intfMap[intTbl.cfgDb.portTN][*ifName] = db.Value{Field: resMap}

	subOpMap[db.ConfigDB] = intfMap
	inParams.subOpDataMap[UPDATE] = &subOpMap
	return nil
}

var YangToDb_intf_diag_profile_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || ifName == "" {
		return nil, tlerr.InvalidArgs("YangToDb_intf_diag_profile_xfmr intfsObj==nil or ifName missing")
	}

	log.V(lvl.DEBUG).Info("YangToDb_intf_diag_profile_xfmr - inParams.uri ", inParams.uri)

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		return nil, nil
	}
	if diagProfile, ok := inParams.param.(*string); ok {
		subOpMap := map[db.DBNum]map[string]map[string]db.Value{
			db.ConfigDB: map[string]map[string]db.Value{
				"BLACKHOLE_PORT_TO_PROFILE_MAP": map[string]db.Value{
					"GLOBAL": db.Value{
						Field: map[string]string{
							ifName: *diagProfile,
						},
					},
				},
			},
		}
		updateSubOpDataMap(subOpMap, UPDATE, inParams)
	}
	return nil, nil
}

var DbToYang_intf_diag_profile_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType != IntfTypeEthernet {
		return nil, fmt.Errorf("Invalid interface; err = %v", err)
	}

	log.V(lvl.DEBUG).Info("DbToYang_intf_diag_profile_xfmr - inParams.uri ", inParams.uri)

	curDb := inParams.dbs[inParams.curDb]
	if curDb == nil {
		return nil, tlerr.InvalidArgs("DbToYang_intf_diag_profile_xfmr curDb is nil")
	}
	entry, err := curDb.GetEntry(
		&db.TableSpec{Name: "BLACKHOLE_PORT_TO_PROFILE_MAP"}, db.Key{Comp: []string{"GLOBAL"}})
	if err == nil {
		if dp, ok := entry.Field[ifName]; ok {
			return map[string]interface{}{"diag-profile": dp}, nil
		}
	} else if !tlerr.IsTranslibRedisClientEntryNotExist(err) {
		return nil, err
	}

	return nil, nil
}

var YangToDb_intf_loopback_mode_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	pathInfo := NewPathInfo(inParams.uri)
	uriIfName := pathInfo.Var("name")
	ifName := uriIfName

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		return nil, errors.New("Interface type is not Ethernet")
	}
	resMap := make(map[string]string)
	if inParams.oper == DELETE {
		log.V(lvl.DEBUG).Infof("Updating the Interface: %s with default loopback-mode", ifName)
		err := updateDefaultLoopbackMode(&inParams, &ifName, intfType, resMap)
		if err != nil {
			log.V(lvl.ERROR).Infof("Updating Default loopback-mode for Interface: %s failed", ifName)
		}
		return resMap, err
	}
	mode, ok := inParams.param.(ocbinds.E_OpenconfigInterfaces_LoopbackModeType)
	if !ok {
		return nil, errors.New("YangToDb_intf_loopback_mode_xfmr, Error: Invalid parameter")
	}
	var enStr string
	switch mode {
	case ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_MAC_LOCAL:
		enStr = "mac_local"
	case ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_MAC_REMOTE:
		enStr = "mac_remote"
	case ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_PHY_LOCAL:
		enStr = "phy_local"
	case ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_PHY_REMOTE:
		enStr = "phy_remote"
	case ocbinds.OpenconfigInterfaces_LoopbackModeType_NONE:
		enStr = "none"
	default:
		return nil, tlerr.InvalidArgs("Loopback mode value not supported")
	}
	resMap[PORT_LOOPBACK_MODE] = enStr
	return resMap, nil
}

var DbToYang_intf_loopback_mode_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, fmt.Errorf("DbToYang_intf_loopback_mode_xfmr: Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, err)
	}
	if intfType != IntfTypeEthernet {
		return nil, errors.New("DbToYang_intf_loopback_mode_xfmr: Interface type is not Ethernet")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_loopback_mode_xfmr: interface type not found : " + strconv.Itoa(int(intfType)))
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_loopback_mode_xfmr: table name not found.")
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	if loopback, ok := prtInst.Field[PORT_LOOPBACK_MODE]; ok {
		var mode ocbinds.E_OpenconfigInterfaces_LoopbackModeType
		result := make(map[string]interface{})
		switch loopback {
		case "none":
			mode = ocbinds.OpenconfigInterfaces_LoopbackModeType_NONE
		case "mac_local":
			mode = ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_MAC_LOCAL
		case "mac_remote":
			mode = ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_MAC_REMOTE
		case "phy_local":
			mode = ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_PHY_LOCAL
		case "phy_remote":
			mode = ocbinds.OpenconfigInterfaces_LoopbackModeType_ASIC_PHY_REMOTE
		default:
			return nil, errors.New("Invalid loopback_mode value")
		}
		result[PORT_LOOPBACK_MODE] = ocbinds.E_OpenconfigInterfaces_LoopbackModeType.ΛMap(mode)["E_OpenconfigInterfaces_LoopbackModeType"][int64(mode)].Name
		return result, nil
	}
	return nil, errors.New("loopback_mode field not found in table.")
}

var YangToDb_intf_type_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	if inParams.oper == DELETE {
		return res_map, tlerr.NotSupported("Operation Not Supported")
	}
	if inParams.param == nil {
		return res_map, nil
	}
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return res_map, errors.New("YangToDb_intf_type_xfmr: Interface KEY not present")
	}

	intfType, _, ierr := getIntfTypeByName(ifName)
	if ierr != nil {
		return res_map, ierr
	}

	intfTypeVal, _ := inParams.param.(ocbinds.E_IETFInterfaces_InterfaceType)
	if val, ok := IF_TYPE_MAP[intfType]; ok {
		//Check if intfTypeVal valid for given interface
		if intfTypeVal == val {
			return res_map, nil
		}
	}
	return res_map, tlerr.InvalidArgsError{Format: "YangToDb_intf_type_xfmr: Invalid Interface type provided for ifname: " + ifName}
}

var DbToYang_intf_type_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	res_map := make(map[string]interface{})
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	errStr := "DbToYang_intf_type_xfmr: Interface type not found, ifname: " + ifName
	intfType, _, ierr := getIntfTypeByName(ifName)
	if ierr != nil {
		return res_map, errors.New(errStr)
	}
	if val, ok := IF_TYPE_MAP[intfType]; ok {
		intfTypeStr := ocbinds.E_IETFInterfaces_InterfaceType.ΛMap(val)["E_IETFInterfaces_InterfaceType"][int64(val)].Name
		log.V(lvl.DEBUG).Infof("DbToYang_intf_type_xfmr, Interface: %s type:%s.", ifName, intfTypeStr)
		res_map["type"] = intfTypeStr
		return res_map, nil
	}
	return res_map, errors.New(errStr)
}

var YangToDb_intf_bridge_id_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	resMap := make(map[string]string)
	if inParams.oper == DELETE {
		return resMap, tlerr.NotSupported("Operation Not Supported")
	}
	if inParams.param == nil {
		return resMap, nil
	}
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return resMap, tlerr.InvalidArgsError{Format: "YangToDb_intf_bridge_id_xfmr: Interface KEY not present"}
	}
	intfType, _, ierr := getIntfTypeByName(ifName)
	if ierr != nil {
		return resMap, tlerr.InvalidArgsError{Format: "YangToDb_intf_bridge_id_xfmr: Interface type not found, ifname: " + ifName}
	}
	if intfType != IntfTypeMgmt {
		return resMap, tlerr.InvalidArgsError{Format: "YangToDb_intf_bridge_id_xfmr: Interface bridge-id only supported on Mgmt Interfaces"}
	}

	intTbl := IntfTypeTblMap[IntfTypeBridge]
	bridge_id := inParams.param.(*string)

	subOpMap := map[db.DBNum]map[string]map[string]db.Value{
		db.ConfigDB: map[string]map[string]db.Value{
			intTbl.cfgDb.memberTN: map[string]db.Value{
				*bridge_id + intTbl.cfgDb.keySep + ifName: db.Value{
					Field: map[string]string{"NULL": "NULL"},
				},
			},
		},
	}
	updateSubOpDataMap(subOpMap, UPDATE, inParams)
	return resMap, nil
}

var DbToYang_intf_bridge_id_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	res_map := make(map[string]interface{})
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if ierr != nil {
		return nil, tlerr.InvalidArgsError{Format: "DbToYang_intf_bridge_id_xfmr: Interface type not found, ifname: " + ifName}
	}
	if intfType != IntfTypeMgmt {
		return nil, nil
	}
	intTbl := IntfTypeTblMap[IntfTypeBridge]
	tblName := intTbl.cfgDb.memberTN
	if inParams.curDb == db.ApplStateDB {
		tblName = intTbl.appStateDb.memberTN
	}

	bridge_id := ""
	bridgeKeys, err := inParams.d.GetKeysByPattern(&db.TableSpec{Name: tblName}, "*"+ifName)
	if err != nil || len(bridgeKeys) == 0 {
		return nil, err
	}
	// Keys will be in the form of <bridge-id>|<intf-name>
	for _, key := range bridgeKeys {
		if key.Len() != 2 {
			continue
		}
		if ifName == key.Get(1) {
			bridge_id = key.Get(0)
			break
		}
	}
	if bridge_id != "" {
		res_map["bridge-id"] = bridge_id
	}
	return res_map, nil
}

var DbToYang_intf_mgmt_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	res_map := make(map[string]interface{})
	pathInfo := NewPathInfo(inParams.uri)
	name := pathInfo.Var("name")
	if name == "" {
		errStr := "DbToYang_intf_mgmt_xfmr : Interface KEY not present"
		return res_map, errors.New(errStr)
	}
	res_map["management"] = strings.HasPrefix(name, MGMT) || strings.HasPrefix(name, MGMT_BOND)
	return res_map, nil
}

var DbToYang_intf_cpu_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	pathInfo := NewPathInfo(inParams.uri)
	name := pathInfo.Var("name")
	if name == "" {
		return nil, errors.New("DbToYang_intf_cpu_xfmr : Interface KEY not present")
	}
	return map[string]interface{}{"cpu": name == CPU}, nil
}

var YangToDb_intf_enabled_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || len(intfsObj.Interface) < 1 {
		return nil, errors.New("Interface object is nil")
	}
	enabled, _ := inParams.param.(*bool)
	var enStr string
	if *enabled {
		enStr = "up"
	} else {
		enStr = "down"
	}
	res_map := make(map[string]string)
	res_map[PORT_ADMIN_STATUS] = enStr

	return res_map, nil
}

var YangToDb_pins_if_health_indicator_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return nil, errors.New("YangToDb_pins_if_health_indicator_xfmr: Interface KEY not present")
	}

	errStr := "YangToDb_pins_if_health_indicator_xfmr: Interface type not found, ifname: " + ifName
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: errStr}
	}
	if intfType != IntfTypeEthernet {
		return nil, errors.New("YangToDb_pins_if_health_indicator_xfmr: Health indicator not supported for interface type: " + strconv.Itoa(int(intfType)))
	}

	inParamsIndicator, ok := inParams.param.(ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator)
	if !ok {
		return nil, errors.New("casting param to indicator not successful.")
	}
	log.V(lvl.DEBUG).Info("YangToDb_pins_if_health_indicator_xfmr : URI:", inParams.uri, " Health Indicator: ", inParamsIndicator)
	resMap := make(map[string]string)

	switch inParamsIndicator {
	case ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator_GOOD:
		resMap[PORT_HEALTH_INDICATOR] = "good"
	case ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator_BAD:
		resMap[PORT_HEALTH_INDICATOR] = "bad"
	default:
		return nil, tlerr.InvalidArgs("Health indicator value not supported")
	}
	return resMap, nil
}

var DbToYang_pins_if_health_indicator_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType != IntfTypeEthernet {
		log.V(lvl.DEBUG).Info("DbToYang_pins_if_health_indicator_xfmr - Invalid interface type: ", intfType)
		return nil, errors.New("DbToYang_pins_if_health_indicator_xfmr: interface type " + strconv.Itoa(int(intfType)) + " not supported for Health indicator ")
	}

	resMap := make(map[string]interface{})
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_pins_if_health_indicator_xfmr interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_pins_if_health_indicator_xfmr: table name not found. Err: " + err.Error())
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	var indctr ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator
	if hIndctr, ok := prtInst.Field[PORT_HEALTH_INDICATOR]; ok {
		switch hIndctr {
		case "good":
			indctr = ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator_GOOD
		case "bad":
			indctr = ocbinds.OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator_BAD
		}
		resMap["health-indicator"] = ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator.ΛMap(indctr)["E_OpenconfigInterfaces_Interfaces_Interface_Config_HealthIndicator"][int64(indctr)].Name
		return resMap, nil
	}
	log.V(lvl.DEBUG).Info("DbToYang_pins_if_health_indicator_xfmr: Health indicator field not found in DB.")
	return nil, errors.New("health indicator field not found in DB.")
}

var YangToDb_pins_if_id_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return nil, errors.New("YangToDb_pins_if_id_xfmr: Interface KEY not present")
	}

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType == IntfTypeUnset || (intfType != IntfTypeEthernet && intfType != IntfTypePortChannel && intfType != IntfTypeCpu) {
		return nil, errors.New("YangToDb_pins_if_id_xfmr: interface type " + strconv.Itoa(int(intfType)) + " not supported for Config Id.")
	}

	idVal, ok := inParams.param.(*uint32)
	if !ok {
		return nil, tlerr.InvalidArgsError{Format: "YangToDb_pins_if_id_xfmr: Config Id doesn't exist"}
	}
	log.V(lvl.DEBUG).Info("YangToDb_pins_if_id_xfmr : URI:", inParams.uri, " Id: ", idVal)
	resMap := make(map[string]string)

	resMap["id"] = strconv.FormatUint(uint64(*idVal), 10)
	return resMap, nil
}

var DbToYang_pins_if_id_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType == IntfTypeUnset || (intfType != IntfTypeEthernet && intfType != IntfTypePortChannel && intfType != IntfTypeCpu) {
		return nil, errors.New("DbToYang_pins_if_id_xfmr: interface type " + strconv.Itoa(int(intfType)) + " not supported for Config Id.")
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_pins_if_id_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}

	// By default we assume P4RT_PORT_ID_TABLE which is used when reading out state
	// for Ethernet and PortChannels.
	tblName := "P4RT_PORT_ID_TABLE"
	if inParams.curDb != db.ApplStateDB || (intfType != IntfTypeEthernet && intfType != IntfTypePortChannel && intfType != IntfTypeCpu) {
		tblName, err = getPortTableNameByDBId(intTbl, inParams.curDb)
		if err != nil {
			return nil, errors.New("DbToYang_pins_if_id_xfmr: Port table name not found.")
		}
	}

	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}

	resMap := make(map[string]interface{})
	if idStr, ok := prtInst.Field["id"]; ok && idStr != "" {
		if idVal, err := strconv.ParseUint(idStr, 10, 32); err == nil {
			resMap["id"] = uint32(idVal)
			return resMap, nil
		}
		return nil, err
	}
	log.V(lvl.DEBUG).Info("DbToYang_pins_if_id_xfmr: Config Id field not found in DB.")
	return nil, tlerr.NotFound("config id field not found in DB.")
}

var DbToYang_pins_ifindex_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}

	if intfType == IntfTypeUnset {
		return nil, errors.New("DbToYang_pins_ifindex_xfmr - interface type not supported for Config Id " + strconv.Itoa(int(intfType)))
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_pins_ifindex_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}

	// By default we assume P4RT_PORT_ID_TABLE which is used when reading out state
	// for Ethernet and PortChannels.
	tblName := "P4RT_PORT_ID_TABLE"
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel && intfType != IntfTypeCpu {
		tblName, err = getPortTableNameByDBId(intTbl, inParams.curDb)
		if err != nil {
			return nil, errors.New("DbToYang_pins_ifindex_xfmr: Port table name not found.")
		}
	}

	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}

	resMap := make(map[string]interface{})
	if idStr, ok := prtInst.Field["id"]; ok && idStr != "" {
		if idVal, err := strconv.ParseUint(idStr, 10, 32); err == nil {
			resMap["ifindex"] = uint32(idVal)
			return resMap, nil
		}
		return nil, err
	}
	log.V(lvl.DEBUG).Info("DbToYang_pins_ifindex_xfmr: State Id field not found in DB.")
	return nil, tlerr.NotFound("state ifindex field not found in DB.")
}

var YangToDb_intf_encoded_id_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	pathInfo := NewPathInfo(inParams.uri)
	uriIfName := pathInfo.Var("name")
	ifName := uriIfName
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	// Only singleton port is supported
	if intfType != IntfTypeEthernet {
		return nil, errors.New("Interface type is not Ethernet")
	}
	resMap := make(map[string]string)
	if inParams.oper == DELETE {
		return resMap, nil
	}

	encodedId, ok := inParams.param.(*uint32)
	if !ok {
		return nil, tlerr.InvalidArgsError{Format: "YangToDb_intf_encoded_id_xfmr: type case to *uint32 failed"}
	}
	log.V(lvl.DEBUG).Info("YangToDb_intf_encoded_id_xfmr : URI:", inParams.uri, "Encoded Id: ", encodedId)

	resMap["encoded_id"] = strconv.FormatUint(uint64(*encodedId), 10)
	return resMap, nil
}

var DbToYang_intf_encoded_id_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	// Only singleton port is supported
	if intfType != IntfTypeEthernet {
		return nil, errors.New("Interface type is not Ethernet")
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_encoded_id_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}

	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_encoded_id_xfmr: table name not found.")
	}

	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}

	if encodedIdStr, ok := prtInst.Field["encoded_id"]; ok && encodedIdStr != "" {
		encodedIdVal, err := strconv.ParseUint(encodedIdStr, 10, 32)
		if err == nil {
			return map[string]interface{}{"encoded-id": uint32(encodedIdVal)}, nil
		}
		return nil, err
	}
	log.V(lvl.DEBUG).Info("DbToYang_intf_encoded_id_xfmr: Encoded Id field not found in DB.")
	return nil, tlerr.NotFound("Encoded id field not found in DB.")
}

var DbToYang_intf_hw_vendor_id_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, errors.New("invalid interface type IntfTypeUnset. Err: " + err.Error())
	}
	if intfType != IntfTypeEthernet {
		return nil, errors.New("interface type is not IntfTypeEthernet")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("table name not found. Err: " + err.Error())
	}

	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	vendorId, ok := prtInst.Field[HARDWARE_VENDOR_ID]
	if !ok {
		return nil, errors.New("Hardware vendor port id is not found in DB")
	}

	vIdVal, err := strconv.ParseUint(vendorId, 10, 32)
	if err != nil {
		return nil, err
	}

	result["vendor-id"] = uint32(vIdVal)
	return result, nil
}

var YangToDb_intf_hold_time_config_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	memMap := make(map[string]map[string]db.Value)
	if inParams.oper == DELETE {
		return memMap, nil
	}
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return nil, errors.New("YangToDb_intf_hold_time_config_xfmr: interface KEY not present.")
	}

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType == IntfTypeUnset || (intfType != IntfTypeEthernet && intfType != IntfTypePortChannel) {
		return nil, errors.New("YangToDb_intf_hold_time_config_xfmr, Error: Invalid interface type " + ifName)
	}

	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || len(intfsObj.Interface) == 0 {
		return nil, errors.New("YangToDb_intf_hold_time_config_xfmr: IntfsObj/Interface is not specified.")
	}
	intfObj, ok := intfsObj.Interface[ifName]
	if !ok {
		errStr := "Interface entry not found in Ygot tree, ifname: " + ifName
		return nil, errors.New("YangToDb_intf_hold_time_config_xfmr : " + errStr)
	}

	resMap := make(map[string]string)
	value := db.Value{Field: resMap}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("YangToDb_intf_hold_time_config_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}

	var holdTimeDown, holdTimeUp *uint32

	if intfObj.HoldTime != nil {
		holdTimeDown = intfObj.HoldTime.Config.Down
		holdTimeUp = intfObj.HoldTime.Config.Up
	}

	switch {
	case strings.Contains(inParams.requestUri, "down"):
		// Config down val only
		if holdTimeDown == nil {
			return nil, tlerr.InvalidArgsError{Format: "HoldTime Config Down doesn't exist"}
		}
		resMap[PORT_HOLD_TIME_DOWN] = strconv.FormatUint(uint64(*holdTimeDown), 10)

	case strings.Contains(inParams.requestUri, "up"):
		// Config up val only
		if holdTimeUp == nil {
			return nil, tlerr.InvalidArgsError{Format: "HoldTime Config Up doesn't exist"}
		}
		resMap[PORT_HOLD_TIME_UP] = strconv.FormatUint(uint64(*holdTimeUp), 10)

	default:
		if holdTimeDown != nil {
			resMap[PORT_HOLD_TIME_DOWN] = strconv.FormatUint(uint64(*holdTimeDown), 10)
		}
		if holdTimeUp != nil {
			resMap[PORT_HOLD_TIME_UP] = strconv.FormatUint(uint64(*holdTimeUp), 10)
		}
	}

	if _, ok := memMap[intTbl.cfgDb.portTN]; !ok {
		memMap[intTbl.cfgDb.portTN] = make(map[string]db.Value)
	}
	memMap[intTbl.cfgDb.portTN][ifName] = value
	return memMap, nil
}

var DbToYang_intf_hold_time_config_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType == IntfTypeUnset || (intfType != IntfTypeEthernet && intfType != IntfTypePortChannel) {
		return errors.New("DbToYang_intf_hold_time_config_xfmr, Error: Invalid interface type " + ifName)
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return errors.New("DbToYang_intf_hold_time_config_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}
	tblName := intTbl.cfgDb.portTN
	configDb := inParams.dbs[db.ConfigDB]
	if configDb == nil {
		configDb, err = db.NewDB(getDBOptions(db.ConfigDB))
		if err != nil {
			return tlerr.InvalidArgsError{Format: err.Error()}
		}
		defer configDb.DeleteDB()
	}
	entry, err := configDb.GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{ifName}})
	if err != nil {
		return tlerr.InvalidArgsError{Format: err.Error()}
	}
	targetUriPath, err := getYangPathFromUri(inParams.uri)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/hold-time/config") {
		return errors.New("Invalid path prefix.")
	}

	get_cfg_obj := false
	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		intfObj, ok = intfsObj.Interface[ifName]
		if !ok {
			intfObj, err = intfsObj.NewInterface(ifName)
			if err != nil {
				return errors.New("Creation of new interface failed; err = " + err.Error())
			}
		}
		ygot.BuildEmptyTree(intfObj)
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, err = intfsObj.NewInterface(ifName)
		if err != nil {
			return errors.New("Creation of new interface failed; err = " + err.Error())
		}
		ygot.BuildEmptyTree(intfObj)
	}
	ygot.BuildEmptyTree(intfObj.HoldTime)
	ygot.BuildEmptyTree(intfObj.HoldTime.Config)

	if targetUriPath == "/openconfig-interfaces:interfaces/interface/hold-time/config" {
		get_cfg_obj = true
	}
	errStr := "Attribute not set"
	if entry.IsPopulated() {
		errStr = ""
		if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/hold-time/config/down" {
			hTDownStr, ok := entry.Field[PORT_HOLD_TIME_DOWN]
			if ok && hTDownStr != "" {
				hTDownVal, err := strconv.ParseUint(hTDownStr, 10, 32)
				if err != nil {
					return errors.New("DbToYang_intf_hold_time_config_xfmr error in converting string to uint32; err = " + err.Error())
				}
				hTDownValU32 := uint32(hTDownVal)
				intfObj.HoldTime.Config.Down = &hTDownValU32
			} else {
				errStr = "hold time down value not set"
			}
		}
		if get_cfg_obj || targetUriPath == "/openconfig-interfaces:interfaces/interface/hold-time/config/up" {
			hTUpStr, ok := entry.Field[PORT_HOLD_TIME_UP]
			if ok && hTUpStr != "" {
				hTUpVal, err := strconv.ParseUint(hTUpStr, 10, 32)
				if err != nil {
					return errors.New("DbToYang_intf_hold_time_config_xfmr error in converting string to uint32; err = " + err.Error())
				}
				hTUpValU32 := uint32(hTUpVal)
				intfObj.HoldTime.Config.Up = &hTUpValU32
			} else {
				errStr = "hold time up value not set"
			}
		}
	}
	if !get_cfg_obj && errStr != "" {
		err = tlerr.InvalidArgsError{Format: errStr}
	}
	return err
}

var DbToYang_intf_hold_time_down_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_hold_time_down_xfmr interface not supported for interface: ", inParams.key)
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, errors.New("DbToYang_intf_hold_time_down_xfmr: Invalid interface type " + strconv.Itoa(int(intfType)))
	}

	resMap := make(map[string]interface{})
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_hold_time_down_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}

	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_hold_time_down_xfmr: Port table name not found.")
	}
	pIntfKey, dBErr := getDBValues(inParams, tblName)
	if dBErr != nil {
		return nil, errors.New("DbToYang_intf_hold_time_down_xfmr: table not found : " + tblName)
	}

	if hTDownStr, ok := pIntfKey.Field[PORT_HOLD_TIME_DOWN]; ok && hTDownStr != "" {
		if hTDownVal, err := strconv.ParseUint(hTDownStr, 10, 32); err == nil {
			resMap["down"] = uint32(hTDownVal)
			return resMap, nil
		}
		return nil, err
	}
	log.V(lvl.DEBUG).Info("hold-time down field not found in DB")
	return nil, errors.New("hold-time down field not found in DB.")
}

var DbToYang_intf_hold_time_up_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_hold_time_up_xfmr interface not supported for interface: ", inParams.key)
		return nil, tlerr.InvalidArgsError{Format: err.Error()}
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, errors.New("DbToYang_intf_hold_time_up_xfmr: Invalid interface type " + strconv.Itoa(int(intfType)))
	}

	resMap := make(map[string]interface{})
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_hold_time_up_xfmr: interface type not found : " + strconv.Itoa(int(intfType)))
	}

	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_hold_time_up_xfmr: Port table name not found.")
	}
	pIntfKey, dBErr := getDBValues(inParams, tblName)
	if dBErr != nil {
		return nil, errors.New("DbToYang_intf_hold_time_up_xfmr: table not found : " + tblName)
	}
	if hTUpStr, ok := pIntfKey.Field[PORT_HOLD_TIME_UP]; ok && hTUpStr != "" {
		if hTUpVal, err := strconv.ParseUint(hTUpStr, 10, 32); err == nil {
			resMap["up"] = uint32(hTUpVal)
			return resMap, nil
		}
		return nil, err
	}
	log.V(lvl.DEBUG).Info("hold-time up field not found in DB")
	return nil, errors.New("hold-time up field not found in DB.")
}

var DbToYang_intf_oper_status_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_oper_status_xfmr - Invalid interface type IntfTypeUnset")
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	if intfType == IntfTypeCpu {
		return nil, errors.New("DbToYang_intf_oper_status_xfmr: Invalid interface type " + strconv.Itoa(int(intfType)))
	}
	var status ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_State_OperStatus
	result := make(map[string]interface{})
	if intfType == IntfTypeLoopback {
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_UP
		result["oper-status"] = status.String()
		return result, nil
	}
	intTbl := IntfTypeTblMap[intfType]
	dbName := db.ApplStateDB
	tblName := intTbl.appStateDb.portTN
	if intfType == IntfTypeMgmt || intfType == IntfTypeMgmtBond {
		dbName = db.StateDB
		tblName = intTbl.stateDb.portTN
	}
	entry, dbErr := inParams.dbs[dbName].GetEntry(&db.TableSpec{Name: tblName}, db.Key{Comp: []string{ifName}})
	if dbErr != nil {
		return nil, dbErr
	}

	// under_test attribute will be set to true when the link is under qualification
	// in that case, gNMI will export the oper_status as testing regardless of the
	// oper_status value
	if test, ok := entry.Field[PORT_UNDER_TEST]; ok && test == "1" {
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_TESTING
		result["oper-status"] = status.String()
		return result, nil
	}
	operStatus, ok := entry.Field[PORT_OPER_STATUS]
	if !ok {
		log.V(lvl.ERROR).Info("Oper status field not found in DB for interface " + ifName)
		return nil, errors.New("Oper status field not found in DB for interface " + ifName)
	}
	if operStatus == "up" {
		result["oper-status"] = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_UP.String()
		return result, nil
	}

	// If PORT_OPER_STATUS != up; use PORT_LOOPBACK_MODE and PORT_PRESENCE to derive if module is NOT_PRESENT
	if intfType == IntfTypeEthernet {
		loopback_mode, ok := entry.Field[PORT_LOOPBACK_MODE]
		if !ok {
			loopback_mode = "none"
		}
		presence, ok := entry.Field[PORT_PRESENCE]
		if !ok || presence != "1" {
			// ports configured with a "local" loopback-mode should ignore PORT_PRESENCE
			if !strings.Contains(loopback_mode, "local") {
				result["oper-status"] = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_NOT_PRESENT.String()
				return result, nil
			}
		}
	}
	switch operStatus {
	case "down":
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_DOWN
	case "testing":
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_TESTING
	case "dormant":
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_DORMANT
	case "lower_layer_down":
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_LOWER_LAYER_DOWN
	default:
		status = ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_OperStatus_UNKNOWN
	}
	result["oper-status"] = status.String()
	return result, nil
}

var DbToYang_intf_eth_duplex_mode_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return nil, fmt.Errorf("DbToYang_intf_eth_duplex_mode_xfmr: Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_eth_duplex_mode_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_duplex_mode_xfmr: table name not found " + tblName)
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	duplex, ok := prtInst.Field["duplex-mode"]
	if !ok {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_duplex_mode_xfmr duplex not found for interface: %v", inParams.key)
		return nil, errors.New("duplex field not found for interface " + inParams.key)
	}
	result := make(map[string]interface{})
	dup, err := getDbToYangDuplex(duplex)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_duplex_mode_xfmr: duplex-mode field not found in map; err = " + err.Error())
	}
	result["duplex-mode"] = ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode.ΛMap(dup)["E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode"][int64(dup)].Name
	return result, nil
}

// Helper function to get the dB table from inParams
func getDBTblForIntfEth(inParams XfmrParams, funcName string) (string, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		log.V(lvl.DEBUG).Infof("%s - Could not find interface type", funcName)
		return "", errors.New("Could not find interface type")
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return "", errors.New("Interface type is not IntfTypeEthernet or IntfTypePortChannel")
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		log.V(lvl.DEBUG).Infof("%s type not found : %v", funcName, intfType)
		return "", errors.New("interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		log.V(lvl.DEBUG).Infof("%s table name not found : %v", funcName, tblName)
		return "", errors.New("table name not found.")
	}
	return tblName, nil
}

var DbToYang_intf_eth_mac_address_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_mac_address_xfmr - Could not find interface type")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_eth_mac_address_xfmr: interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_mac_address_xfmr: table name not found.")
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, tlerr.New(err.Error())
	}

	resMap := make(map[string]interface{})
	if macAddr, ok := prtInst.Field[PORT_MAC_ADDR]; ok {
		resMap[PORT_MAC_ADDR] = macAddr
		return resMap, nil
	}

	/* According to the yang specification, if mac address is not specified
	for an interface, the corresponding operational state leaf is expected to
	show the system-assigned MAC address. This is returned here if the backend
	has not set this default value for the state path. */
	entry, err := inParams.dbs[db.ConfigDB].GetEntry(&db.TableSpec{Name: "DEVICE_METADATA"}, db.Key{Comp: []string{"localhost"}})
	if err != nil {
		return nil, fmt.Errorf("Unable to fetch DEVICE_METADATA|localhost entry from ConfigDB. Error: %w", err)
	}
	if entry.IsPopulated() && entry.Has("mac") {
		resMap[PORT_MAC_ADDR] = entry.Field["mac"]
		return resMap, nil
	}
	log.V(lvl.DEBUG).Info("mac-address field not found in DB")
	return nil, errors.New("mac-address field not found in DB")
}

var DbToYang_intf_eth_negotiated_port_speed_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	tblName, err := getDBTblForIntfEth(inParams, "DbToYang_intf_eth_negotiated_port_speed_xfmr")
	if err != nil {
		return nil, tlerr.New(err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, tlerr.New(err.Error())
	}

	resMap := make(map[string]interface{})
	npSpeed := ocbinds.OpenconfigIfEthernet_ETHERNET_SPEED_UNSET

	/* According to the yang specification, negotiated-port-speed is set when
	auto-negotiate is set to TRUE, and the interface has completed auto-negotiation
	with the remote peer. */
	if autoNeg, ok := prtInst.Field[PORT_AUTONEG]; !ok || autoNeg == "off" {
		return nil, errors.New("DbToYang_intf_eth_negotiated_port_speed_xfmr: negotiated_port_speed depends on auto-negotiate field to be set to TRUE")
	}

	if speed, ok := prtInst.Field[PORT_NEGOTIATED_SPEED]; ok {
		npSpeed, err = getDbToYangSpeed(speed)
		if err != nil {
			return nil, tlerr.New(err.Error())
		}
		resMap[PORT_NEGOTIATED_SPEED] = ocbinds.E_OpenconfigIfEthernet_ETHERNET_SPEED.ΛMap(npSpeed)["E_OpenconfigIfEthernet_ETHERNET_SPEED"][int64(npSpeed)].Name
		return resMap, nil
	}

	log.V(lvl.DEBUG).Info("negotiated_port_speed field not found in DB")
	return nil, errors.New("negotiated_port_speed field not found in DB")
}

var DbToYang_intf_eth_forwarding_viable_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	tblName, err := getDBTblForIntfEth(inParams, "DbToYang_intf_eth_forwarding_viable_xfmr")
	if err != nil {
		return nil, tlerr.New(err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, tlerr.New(err.Error())
	}

	resMap := make(map[string]interface{})
	// From the OC YANG definition, the default value for forwarding-viable is true.
	resMap[PORT_FWD_VIABLE] = true
	if fwdViable, ok := prtInst.Field[PORT_FWD_VIABLE]; ok {
		if fwdViable == "false" {
			resMap[PORT_FWD_VIABLE] = false
		}
	} else {
		log.V(lvl.DEBUG).Infof("forwarding-viable field not found in DB for interface %v, returning default value.", inParams.key)
	}
	return resMap, nil
}

var DbToYang_intf_eth_link_training_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	tblName, err := getDBTblForIntfEth(inParams, "DbToYang_intf_eth_link_training_xfmr")
	if err != nil {
		return nil, tlerr.New(err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, tlerr.New(err.Error())
	}
	linkTraining, ok := prtInst.Field["link_training"]
	if !ok {
		return nil, errors.New("link_training field not found in DB")
	}
	result := make(map[string]interface{})
	result[PORT_LINK_TRAINING] = linkTraining == "on"
	return result, nil
}

var DbToYang_intf_eth_xcvr_qualified_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	tblName, err := getDBTblForIntfEth(inParams, "DbToYang_intf_eth_xcvr_qualified_xfmr")
	if err != nil {
		return nil, tlerr.New(err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, tlerr.New(err.Error())
	}
	/* Adding default value of 'false' for transceiver-qualified attribute
	since writing the value in the DB is triggered by module insertion/speed
	change, and could potentially be missing when module has not been inserted.
	*/
	result := map[string]interface{}{"transceiver-qualified": false}
	xcvrQual, ok := prtInst.Field["xcvr_qualified"]
	if !ok {
		log.V(lvl.DEBUG).Info("DbToYang_intf_eth_xcvr_qualified_xfmr: xcvr_qualified field not found in DB")
		return result, nil
	}
	result["transceiver-qualified"] = (xcvrQual == "True" || xcvrQual == "true")
	return result, nil
}

var DbToYang_intf_eth_fec_mode_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType == IntfTypeUnset {
		log.V(lvl.DEBUG).Info("DbToYang_intf_fec_mode_xfmr - Invalid interface type IntfTypeUnset")
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, err)
	}
	if intfType != IntfTypeEthernet {
		log.V(lvl.DEBUG).Info("DbToYang_intf_fec_mode_xfmr - Invalid interface type not IntfTypeEthernet")
		return nil, errors.New("Invalid interface type not IntfTypeEthernet")
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_fec_mode_xfmr: Invalid interface type not found in table map")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_fec_mode_xfmr, table is not present in the Port Table")
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, errors.New("DbToYang_intf_eth_fec_mode_xfmr, field is not present in the table")
	}
	fec, ok := prtInst.Field[PORT_FEC]
	if !ok {
		log.V(lvl.DEBUG).Infof("fec-mode field not found in DB for interface %v", inParams.key)
		return nil, errors.New("fec-mode field not found in DB for interface " + inParams.key)
	}
	fecMode, ok := dbToYangFecModeMap[fec]
	if !ok {
		log.V(lvl.DEBUG).Info("fec-mode DB field not valid")
		return nil, errors.New("fec-mode DB field not valid")
	}

	result := make(map[string]interface{})
	result["fec-mode"] = ocbinds.E_OpenconfigIfEthernet_INTERFACE_FEC.ΛMap(fecMode)["E_OpenconfigIfEthernet_INTERFACE_FEC"][int64(fecMode)].Name

	return result, nil
}

func eth_delay_helper(inParams XfmrParams, fieldName, leafName string) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType != IntfTypeEthernet {
		return nil, nil
	}
	prtInst, err := getDBValues(inParams, "PORT_TABLE")
	if err != nil {
		return nil, fmt.Errorf("eth_delay_helper, field is not present in the table (intf=%v)", inParams.key)
	}
	delayStr, ok := prtInst.Field[fieldName]
	if !ok {
		return nil, fmt.Errorf("%s field not found in DB (intf=%v)", fieldName, inParams.key)
	}
	delay, err := float32StrTo4Bytes(delayStr)
	if err != nil {
		return nil, fmt.Errorf("Error converting %s=%s float32-str to binary: err=%w", fieldName, delayStr, err)
	}
	result := map[string]interface{}{
		leafName: delay,
	}
	return result, nil
}

var DbToYang_intf_eth_ingress_delay_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	return eth_delay_helper(inParams, PORT_INGRESS_DELAY, PORT_INGRESS_DELAY)
}
var DbToYang_intf_eth_egress_delay_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	return eth_delay_helper(inParams, PORT_EGRESS_DELAY, PORT_EGRESS_DELAY)
}
var DbToYang_intf_eth_ingress_delay_applied_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	return eth_delay_helper(inParams, PORT_INGRESS_DELAY, PORT_INGRESS_DELAY_APPLIED)
}
var DbToYang_intf_eth_egress_delay_applied_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	return eth_delay_helper(inParams, PORT_EGRESS_DELAY, PORT_EGRESS_DELAY_APPLIED)
}

var DbToYang_intf_eth_sfec_mode_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType == IntfTypeUnset {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_sfec_mode_xfmr - Invalid interface type IntfTypeUnset (intf=%v)", inParams.key)
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, err)
	}
	if intfType != IntfTypeEthernet {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_sfec_mode_xfmr - Invalid interface type not IntfTypeEthernet (intf=%v)", inParams.key)
		return nil, nil
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_fec_mode_xfmr: Invalid interface type not found in IntfTypeTblMap: " + inParams.key)
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, fmt.Errorf("DbToYang_intf_eth_sfec_mode_xfmr, %d table is not present in the Port Table (intf=%v)", inParams.curDb, inParams.key)
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, fmt.Errorf("DbToYang_intf_eth_sfec_mode_xfmr, field is not present in the table (intf=%v)", inParams.key)
	}
	sfec, ok := prtInst.Field[PORT_SFEC_MODE]
	if !ok {
		return nil, fmt.Errorf("%s field not found in DB (intf=%v)", PORT_SFEC_MODE, inParams.key)
	}
	sfecMode, ok := dbToYangSfecModeMap[sfec]
	if !ok {
		return nil, fmt.Errorf("%s DB field not valid(sfecMode=%v) (intf=%v)", PORT_SFEC_MODE, sfec, inParams.key)
	}

	result := map[string]interface{}{
		"sfec-mode": ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode.ΛMap(sfecMode)["E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_SfecMode"][int64(sfecMode)].Name,
	}

	return result, nil
}

var DbToYang_intf_eth_sfec_oper_mode_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType == IntfTypeUnset {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_sfec_oper_mode_xfmr - Invalid interface type IntfTypeUnset (intf=%v)", inParams.key)
		return nil, fmt.Errorf("Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, err)
	}
	if intfType != IntfTypeEthernet {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_eth_sfec_oper_mode_xfmr - Invalid interface type not IntfTypeEthernet (intf=%v)", inParams.key)
		return nil, nil
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_eth_sfec_oper_mode_xfmr: Invalid interface type not found in IntfTypeTblMap: " + inParams.key)
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, fmt.Errorf("DbToYang_intf_eth_sfec_oper_mode_xfmr, %d table is not present in the Port Table (intf=%v)", inParams.curDb, inParams.key)
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, fmt.Errorf("DbToYang_intf_eth_sfec_oper_mode_xfmr, field is not present in the table (intf=%v)", inParams.key)
	}
	sfecOper, ok := prtInst.Field[PORT_SFEC_OPER_MODE]
	if !ok {
		// If sfec oper mode is not provided, return unknown
		sfecOper = UNKNOWN
	}
	sfecOperMode, ok := dbToYangSfecOperModeMap[sfecOper]
	if !ok {
		return nil, fmt.Errorf("%s DB field not valid(sfecOper=%v) (intf=%v)", PORT_SFEC_OPER_MODE, sfecOper, inParams.key)
	}

	result := map[string]interface{}{
		"sfec-oper-mode": ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_SfecOperMode.ΛMap(sfecOperMode)["E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_State_SfecOperMode"][int64(sfecOperMode)].Name,
	}

	return result, nil
}

var DbToYang_intf_eth_state_pfc_enable_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("DbToYang_intf_eth_state_pfc_enable_xfmr: inParams: ", inParams)

	prtInst, err := retrieveDbEntryForSingletonInterface(inParams)
	if err != nil {
		return nil, err
	}
	pfcEnable, ok := prtInst.Field[PORT_PFC_ENABLE]
	if !ok {
		return nil, fmt.Errorf("%s field not found in DB (intf=%v)", PORT_PFC_ENABLE, inParams.key)
	}
	result := make(map[string]interface{})
	result["enable-pfc-rx"] = pfcEnable == "on"
	return result, nil
}

func getDbToYangDuplex(duplex string) (ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode, error) {
	dup := ocbinds.OpenconfigInterfaces_Interfaces_Interface_Ethernet_Config_DuplexMode_FULL
	err := errors.New("Unreliable duplex mode not found in db")
	if val, ok := dbToYangDuplexMap[duplex]; ok {
		dup = val
		err = nil
	}
	return dup, err
}

func deleteBridgeIntf(inParams *XfmrParams, ifName *string) error {
	if ifName == nil || inParams == nil {
		return tlerr.InvalidArgsError{Format: fmt.Sprintf("Invalid args passed in to deleteBridgeIntf: %v, %v", inParams, ifName)}
	}
	intTbl, _ := IntfTypeTblMap[IntfTypeBridge]
	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	resMap := make(map[string]map[string]db.Value)

	// Handle BRIDGE|<bridge> table
	bridgeKeys, err := inParams.d.GetKeysByPattern(&db.TableSpec{Name: intTbl.cfgDb.portTN}, *ifName)
	if err != nil || len(bridgeKeys) == 0 {
		return tlerr.InvalidArgsError{Format: fmt.Sprintf("Bridge table key not found for %v: %v", *ifName, err)}
	}
	ifMap := map[string]db.Value{*ifName: db.Value{Field: map[string]string{}}}
	resMap[intTbl.cfgDb.portTN] = ifMap

	// Handle BRIDGE_MEMBER|<bridge>|<interface> tables
	bridgeMemberKeys, err := inParams.d.GetKeysByPattern(&db.TableSpec{Name: intTbl.cfgDb.memberTN}, *ifName+intTbl.cfgDb.keySep+"*")
	if err != nil || len(bridgeMemberKeys) != 2 {
		log.V(lvl.DEBUG).Infof("Unexpected number of bridge member keys found: %v", bridgeMemberKeys)
		return tlerr.InvalidArgsError{Format: fmt.Sprintf("Bridge member table keys not found for %v: %v", *ifName, err)}
	}
	ifMemMap := map[string]db.Value{}
	for _, key := range bridgeMemberKeys {
		ifMemKey := strings.Join(key.Comp, inParams.d.Opts.KeySeparator)
		ifMemMap[ifMemKey] = db.Value{Field: map[string]string{}}
	}
	if len(ifMemMap) != 0 {
		resMap[intTbl.cfgDb.memberTN] = ifMemMap
	}

	subOpMap[db.ConfigDB] = resMap
	updateSubOpDataMap(subOpMap, DELETE, *inParams)
	return nil
}

var DbToYang_intf_eth_pfc_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return tlerr.InvalidArgs("DbToYang_intf_eth_pfc_xfmr no name available")
	}
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return errors.New("DbToYang_intf_eth_pfc_xfmr - err: " + err.Error())
	}
	if intfType != IntfTypeEthernet {
		return errors.New("DbToYang_intf_eth_pfc_xfmr unsupported interface: " + ifName)
	}

	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/google-pins-interfaces:pfc") &&
		!strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/openconfig-if-ethernet:ethernet/pfc") &&
		!strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/google-pins-interfaces:pfc") &&
		!strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/ethernet/pfc") {
		return nil
	}

	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		var ok bool = false
		if intfObj, ok = intfsObj.Interface[ifName]; !ok {
			intfObj, _ = intfsObj.NewInterface(ifName)
		}
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, _ = intfsObj.NewInterface(ifName)
	}
	ygot.BuildEmptyTree(intfObj)
	if intfObj.Ethernet == nil {
		ygot.BuildEmptyTree(intfObj.Ethernet)
	}
	if intfObj.Ethernet.Pfc == nil {
		ygot.BuildEmptyTree(intfObj.Ethernet.Pfc)
	}
	pfcObj := intfObj.Ethernet.Pfc

	pfcPrio := "0,1,2,3,4,5,6,7"
	// TODO(b/361822295): Uncomment and read from PORT_QOS_MAP after CVL changes
	/*
		cfgDB := inParams.dbs[db.ConfigDB]
		if cfgDB == nil {
			return errors.New("DbToYang_intf_eth_pfc_xfmr - ConfigDB is nil")
		}
		entry, err := cfgDB.GetEntry(&db.TableSpec{Name: "PORT_QOS_MAP"}, db.Key{Comp: []string{ifName}})
		if err != nil {
			return errors.New("DbToYang_intf_eth_pfc_xfmr - err: " + err.Error())
		}
		var pfcPrio string
		if pfcPrio = entry.Get("pfc_enable"); pfcPrio == "" {
			log.V(lvl.INFO).Infof("DbToYang_intf_eth_pfc_xfmr - pfc is disabled for interface %v", ifName)
			return nil
		}
	*/
	pfcPrioStrList := strings.Split(pfcPrio, ",")
	pfcPrioList := make([]uint8, len(pfcPrioStrList))
	for i, s := range pfcPrioStrList {
		val, _ := strconv.ParseUint(s, 10, 8)
		pfcPrioList[i] = uint8(val)
	}

	d := inParams.dbs[inParams.curDb]
	log.V(lvl.DEBUG).Info("DbToYang_intf_eth_pfc_xfmr : inParams.curDb : ", inParams.curDb, "D: ", inParams.d, "DB index : ", d)
	oid, oiderr := getIntfCountersTblKey(d, ifName)
	if oiderr != nil {
		return oiderr
	}
	countersEntry, err := d.GetEntry(&db.TableSpec{Name: "COUNTERS"}, db.Key{Comp: []string{oid}})
	if err != nil {
		return err
	}

	for prio := range pfcPrioList {
		prioVal := uint8(prio)
		pfc, ok := pfcObj.Priority[prioVal]
		if !ok || pfc == nil {
			pfc, err = pfcObj.NewPriority(prioVal)
			if err != nil {
				return fmt.Errorf("cannot create priority object: %w", err)
			}
		}
		ygot.BuildEmptyTree(pfc)
		ygot.BuildEmptyTree(pfc.State)
		ygot.BuildEmptyTree(pfc.State.Counters)
		pfc.State.Priority = &prioVal
		prioStr := strconv.Itoa(prio)
		fls := []fieldU64LeafPair{
			{fmt.Sprintf("SAI_PORT_STAT_PFC_%s_RX_PKTS", prioStr), &pfc.State.Counters.RxPackets},
			{fmt.Sprintf("SAI_PORT_STAT_PFC_%s_ON2OFF_RX_PKTS", prioStr), &pfc.State.Counters.On2OffRxPackets},
		}
		if readErr := readAndParseCounters(&countersEntry, fls); readErr != nil {
			return readErr
		}
	}
	return nil
}

var Subscribe_intf_eth_pfc_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	log.V(lvl.DEBUG).Info("Entering Subscribe_intf_eth_pfc_xfmr")

	result := XfmrSubscOutParams{
		isVirtualTbl: false,
		needCache:    true,
		onChange:     OnchangeDisable,
		dbDataMap:    make(RedisDbSubscribeMap),
		nOpts:        &notificationOpts{mInterval: 1, pType: Sample}, // Counters can only support Sample.
	}

	defer log.V(lvl.DEBUG).Info("Returning Subscribe_intf_eth_pfc_xfmr, result:", result)

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	oid, _ := getIntfCountersTblKey(inParams.dbs[db.CountersDB], ifName)

	if ifName != "*" {
		// for non wildcard path, tableName doesn't matter for Sample subscription.
		result.dbDataMap = RedisDbSubscribeMap{db.CountersDB: {"COUNTERS": {oid: {}}}}
		return result, nil
	}

	return result, nil
}

var DbToYang_intf_state_blackhole_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return tlerr.InvalidArgs("DbToYang_intf_state_blackhole_xfmr no name available")
	}
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return errors.New("DbToYang_intf_state_blackhole_xfmr - err: " + err.Error())
	}
	if intfType != IntfTypeEthernet {
		return errors.New("DbToYang_intf_state_blackhole_xfmr unsupported interface: " + ifName)
	}

	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil || !strings.HasPrefix(targetUriPath, "/openconfig-interfaces:interfaces/interface/state/google-pins-interfaces:blackhole") {
		return nil
	}

	var bhObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Blackhole
	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		var ok bool = false
		var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface
		if intfObj, ok = intfsObj.Interface[ifName]; !ok {
			intfObj, _ = intfsObj.NewInterface(ifName)
		}
		ygot.BuildEmptyTree(intfObj)
		if intfObj.State == nil || intfObj.State.Counters == nil {
			ygot.BuildEmptyTree(intfObj.State)
		}
		bhObj = intfObj.State.Blackhole
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, _ := intfsObj.NewInterface(ifName)
		ygot.BuildEmptyTree(intfObj)
		bhObj = intfObj.State.Blackhole
	}

	d := inParams.dbs[inParams.curDb]
	log.V(lvl.DEBUG).Info("DbToYang_intf_state_blackhole_xfmr : inParams.curDb : ", inParams.curDb, "D: ", inParams.d, "DB index : ", d)
	oid, oiderr := getIntfCountersTblKey(d, ifName)
	if oiderr != nil {
		log.V(lvl.ERROR).Info(oiderr)
		return oiderr
	}
	entry, err := d.GetEntry(
		&db.TableSpec{Name: "COUNTERS_BLACKHOLE_PORT"}, db.Key{Comp: []string{oid}})
	if err != nil {
		return err
	}

	fls := []fieldU64LeafPair{
		{"BLACKHOLE_PORT_IN_DISCARD_EVENTS", &bhObj.InDiscardEvents},
		{"BLACKHOLE_PORT_OUT_DISCARD_EVENTS", &bhObj.OutDiscardEvents},
		{"BLACKHOLE_PORT_IN_ERROR_EVENTS", &bhObj.InErrorEvents},
		{"BLACKHOLE_PORT_FEC_NOT_CORRECTABLE_EVENTS", &bhObj.FecNotCorrectableEvents},
	}
	if e := readAndParseCounters(&entry, fls); e != nil {
		return e
	}

	is_ts_set := false
	// Use counter specific timestamp if there is one.
	ts, ok := entry.Field["PORT_STAT_TIME_STAMP_USEC_last"]
	if ok && ts != "" {
		if usec, err := strconv.ParseInt(ts, 10, 64); err == nil {
			utils.UpdateYGSTimestamp(*inParams.ygRoot, bhObj, usec*1000)
			is_ts_set = true
		} else {
			log.V(lvl.ERROR).Infof("Invalid timestamp for port %s, %v", ifName, ts)
		}
	}

	bItvlTbl := "COUNTERS_PORT_BAD_INTERVALS"
	entry, err = d.GetEntry(
		&db.TableSpec{Name: bItvlTbl}, db.Key{Comp: []string{oid}})
	if err != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_state_blackhole_xfmr : key missed from COUNTERS_DB: err: ", err)
		return nil
	}
	if e := readAndParseCounter(&entry, "PORT_BAD_INTERVALS", &bhObj.BadIntervals); e != nil {
		switch e.(type) {
		case tlerr.NotFoundError:
		default:
			return e
		}
	}

	if !is_ts_set {
		// If bh timestamp is not available, use the timestamp in bi table is there is one.
		ts, ok = entry.Field["PORT_STAT_TIME_STAMP_USEC"]
		if ok && ts != "" {
			if usec, err := strconv.ParseInt(ts, 10, 64); err == nil {
				utils.UpdateYGSTimestamp(*inParams.ygRoot, bhObj, usec*1000)
				return nil
			} else {
				log.V(lvl.ERROR).Infof("Invalid timestamp for port %s, %v", ifName, ts)
			}
		}
	}

	return nil
}

var DbToYang_intf_state_hst_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if ifName == "" {
		return tlerr.InvalidArgs("DbToYang_intf_state_hst_xfmr no name available")
	}
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return errors.New("DbToYang_intf_state_hst_xfmr - err: " + err.Error())
	}
	if intfType != IntfTypeEthernet {
		return errors.New("DbToYang_intf_state_hst_xfmr unsupported interface: " + ifName)
	}
	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil ||
		(targetUriPath != "/openconfig-interfaces:interfaces/interface/state/google-pins-interfaces:hst" &&
			targetUriPath != "/openconfig-interfaces:interfaces/interface/state/hst") {
		return nil
	}

	var hObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Hst
	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		intfObj, ok := intfsObj.Interface[ifName]
		if !ok {
			if intfObj, err = intfsObj.NewInterface(ifName); err != nil {
				return err
			}
		}
		ygot.BuildEmptyTree(intfObj)
		hObj = intfObj.State.Hst
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, err := intfsObj.NewInterface(ifName)
		if err != nil {
			return err
		}
		ygot.BuildEmptyTree(intfObj)
		hObj = intfObj.State.Hst
	}

	d := inParams.dbs[db.CountersDB]
	log.V(lvl.DEBUG).Info("DbToYang_intf_state_hst_xfmr : inParams.curDb : ", inParams.curDb, "D: ", inParams.d, "DB index : ", d)
	oid, oiderr := getIntfCountersTblKey(d, ifName)
	if oiderr != nil {
		log.V(tlerr.ErrorSeverity(oiderr)).Infof("getIntfCountersTblKey lookup failed for ifName=%v: %v", ifName, oiderr)
		return oiderr
	}
	entry, err := d.GetEntry(
		&db.TableSpec{Name: "HST_STATS_TABLE"}, db.Key{Comp: []string{oid}})
	if err != nil {
		return err
	}

	fls := []fieldBinaryLeafPair{
		{"abwc_digest_0", &hObj.AbwcDigests_0},
		{"abwc_digest_cumulative_0", &hObj.AbwcDigestsCumulative_0},
		{"abwc_digest_1", &hObj.AbwcDigests_1},
		{"abwc_digest_cumulative_1", &hObj.AbwcDigestsCumulative_1},
		{"abwc_digest_2", &hObj.AbwcDigests_2},
		{"abwc_digest_cumulative_2", &hObj.AbwcDigestsCumulative_2},
		{"abwc_digest_3", &hObj.AbwcDigests_3},
		{"abwc_digest_cumulative_3", &hObj.AbwcDigestsCumulative_3},
		{"abwc_digest_4", &hObj.AbwcDigests_4},
		{"abwc_digest_cumulative_4", &hObj.AbwcDigestsCumulative_4},
		{"abwc_digest_5", &hObj.AbwcDigests_5},
		{"abwc_digest_cumulative_5", &hObj.AbwcDigestsCumulative_5},
		{"abwc_digest_6", &hObj.AbwcDigests_6},
		{"abwc_digest_cumulative_6", &hObj.AbwcDigestsCumulative_6},
		{"abwc_digest_7", &hObj.AbwcDigests_7},
	}
	for _, fl := range fls {
		*fl.leaf = extractFloat32Str(fl.field, &entry)
	}

	// Use counter specific timestamp if there is one.
	ts, ok := entry.Field["timestamp"]
	if ok && ts != "" {
		if usec, err := strconv.ParseInt(ts, 10, 64); err == nil {
			utils.UpdateYGSTimestamp(*inParams.ygRoot, hObj, usec*1000)
		} else {
			log.V(lvl.DEBUG).Infof("Invalid timestamp for port %s, %v", ifName, ts)
		}
	}

	return nil
}

func sumStateCounters(parent, member *ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters) {
	if member.CarrierTransitions != nil {
		CarrierTransitions := *member.CarrierTransitions
		if parent.CarrierTransitions != nil {
			CarrierTransitions += *parent.CarrierTransitions
		}
		parent.CarrierTransitions = &CarrierTransitions
	}
	if member.InBroadcastPkts != nil {
		InBroadcastPkts := *member.InBroadcastPkts
		if parent.InBroadcastPkts != nil {
			InBroadcastPkts += *parent.InBroadcastPkts
		}
		parent.InBroadcastPkts = &InBroadcastPkts
	}
	if member.InBufferDiscards != nil {
		InBufferDiscards := *member.InBufferDiscards
		if parent.InBufferDiscards != nil {
			InBufferDiscards += *parent.InBufferDiscards
		}
		parent.InBufferDiscards = &InBufferDiscards
	}
	if member.InDiscards != nil {
		InDiscards := *member.InDiscards
		if parent.InDiscards != nil {
			InDiscards += *parent.InDiscards
		}
		parent.InDiscards = &InDiscards
	}
	if member.InErrors != nil {
		InErrors := *member.InErrors
		if parent.InErrors != nil {
			InErrors += *parent.InErrors
		}
		parent.InErrors = &InErrors
	}
	if member.InFcsErrors != nil {
		InFcsErrors := *member.InFcsErrors
		if parent.InFcsErrors != nil {
			InFcsErrors += *parent.InFcsErrors
		}
		parent.InFcsErrors = &InFcsErrors
	}
	if member.InMulticastPkts != nil {
		InMulticastPkts := *member.InMulticastPkts
		if parent.InMulticastPkts != nil {
			InMulticastPkts += *parent.InMulticastPkts
		}
		parent.InMulticastPkts = &InMulticastPkts
	}
	if member.InUnicastPkts != nil {
		InUnicastPkts := *member.InUnicastPkts
		if parent.InUnicastPkts != nil {
			InUnicastPkts += *parent.InUnicastPkts
		}
		parent.InUnicastPkts = &InUnicastPkts
	}
	if member.InUnknownProtos != nil {
		InUnknownProtos := *member.InUnknownProtos
		if parent.InUnknownProtos != nil {
			InUnknownProtos += *parent.InUnknownProtos
		}
		parent.InUnknownProtos = &InUnknownProtos
	}
	if member.LastClear != nil {
		LastClear := *member.LastClear
		if parent.LastClear != nil {
			LastClear += *parent.LastClear
		}
		parent.LastClear = &LastClear
	}
	if member.OutBroadcastPkts != nil {
		OutBroadcastPkts := *member.OutBroadcastPkts
		if parent.OutBroadcastPkts != nil {
			OutBroadcastPkts += *parent.OutBroadcastPkts
		}
		parent.OutBroadcastPkts = &OutBroadcastPkts
	}
	if member.OutBufferDiscards != nil {
		OutBufferDiscards := *member.OutBufferDiscards
		if parent.OutBufferDiscards != nil {
			OutBufferDiscards += *parent.OutBufferDiscards
		}
		parent.OutBufferDiscards = &OutBufferDiscards
	}
	if member.OutDiscards != nil {
		OutDiscards := *member.OutDiscards
		if parent.OutDiscards != nil {
			OutDiscards += *parent.OutDiscards
		}
		parent.OutDiscards = &OutDiscards
	}
	if member.OutErrors != nil {
		OutErrors := *member.OutErrors
		if parent.OutErrors != nil {
			OutErrors += *parent.OutErrors
		}
		parent.OutErrors = &OutErrors
	}
	if member.OutMulticastPkts != nil {
		OutMulticastPkts := *member.OutMulticastPkts
		if parent.OutMulticastPkts != nil {
			OutMulticastPkts += *parent.OutMulticastPkts
		}
		parent.OutMulticastPkts = &OutMulticastPkts
	}
	if member.OutUnicastPkts != nil {
		OutUnicastPkts := *member.OutUnicastPkts
		if parent.OutUnicastPkts != nil {
			OutUnicastPkts += *parent.OutUnicastPkts
		}
		parent.OutUnicastPkts = &OutUnicastPkts
	}
	if member.InOctets != nil {
		InOctets := *member.InOctets
		if parent.InOctets != nil {
			InOctets += *parent.InOctets
		}
		parent.InOctets = &InOctets
	}
	if member.InPkts != nil {
		InPkts := *member.InPkts
		if parent.InPkts != nil {
			InPkts += *parent.InPkts
		}
		parent.InPkts = &InPkts
	}
	if member.OutOctets != nil {
		OutOctets := *member.OutOctets
		if parent.OutOctets != nil {
			OutOctets += *parent.OutOctets
		}
		parent.OutOctets = &OutOctets
	}
	if member.OutPkts != nil {
		OutPkts := *member.OutPkts
		if parent.OutPkts != nil {
			OutPkts += *parent.OutPkts
		}
		parent.OutPkts = &OutPkts
	}
	if member.OutEcnMarkedPkts != nil {
		OutEcnMarkedPkts := *member.OutEcnMarkedPkts
		if parent.OutEcnMarkedPkts != nil {
			OutEcnMarkedPkts += *parent.OutEcnMarkedPkts
		}
		parent.OutEcnMarkedPkts = &OutEcnMarkedPkts
	}
}

var mgmtCounterIndexMap = map[string]int{
	"in-octets":         1,
	"in-pkts":           2,
	"in-errors":         3,
	"in-discards":       4,
	"in-multicast-pkts": 8,
	"out-octets":        9,
	"out-pkts":          10,
	"out-errors":        11,
	"out-discards":      12,
}

func getMgmtCounters(val string, counter_val **uint64) error {

	var err error
	if len(val) > 0 {
		v, e := strconv.ParseUint(val, 10, 64)
		if err == nil {
			*counter_val = &v
			return nil
		}
		err = e
	}
	return err
}
func getMgmtSpecificCounterAttr(uri string, cnt_data []string, counter *ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters) error {

	var e error
	switch uri {
	case "/openconfig-interfaces:interfaces/interface/state/counters/in-octets":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["in-octets"]], &counter.InOctets)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/in-pkts":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["in-pkts"]], &counter.InPkts)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/in-errors":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["in-errors"]], &counter.InErrors)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/in-discards":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["in-discards"]], &counter.InDiscards)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/in-multicast-pkts":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["in-multicast-pkts"]], &counter.InMulticastPkts)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/out-octets":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["out-octets"]], &counter.OutOctets)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/out-pkts":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["out-pkts"]], &counter.OutPkts)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/out-errors":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["out-errors"]], &counter.OutErrors)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters/out-discards":
		e = getMgmtCounters(cnt_data[mgmtCounterIndexMap["out-discards"]], &counter.OutDiscards)
		return e
	case "/openconfig-interfaces:interfaces/interface/state/counters":
		for key := range mgmtCounterIndexMap {
			xuri := uri + "/" + key
			getMgmtSpecificCounterAttr(xuri, cnt_data, counter)
		}
		return nil
	}

	log.V(lvl.ERROR).Info("getMgmtSpecificCounterAttr - Invalid counters URI : ", uri)
	return errors.New("Invalid counters URI")

}

var populateMGMTPortCounters PopulateIntfCounters = func(inParams XfmrParams, intfName string, counter interface{}) error {
	pathInfo := NewPathInfo(inParams.uri)
	if intfName == "" {
		intfName = pathInfo.Var("name")
	}
	targetUriPath, err := getYangPathFromUri(pathInfo.Path)
	if err != nil {
		return err
	}

	fileName := "/proc/net/dev"
	file, err := os.Open(fileName)
	if err != nil {
		log.V(lvl.ERROR).Infof("failed opening file: %s", err)
		return err
	}

	counter_val := counter.(*ocbinds.OpenconfigInterfaces_Interfaces_Interface_State_Counters)

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var txtlines []string
	for scanner.Scan() {
		txtlines = append(txtlines, scanner.Text())
	}
	file.Close()
	var entry string
	for _, eachline := range txtlines {
		ln := strings.TrimSpace(eachline)
		if strings.HasPrefix(ln, intfName) {
			entry = ln
			log.V(lvl.DEBUG).Info(" Interface stats : ", entry)
			break
		}
	}

	if entry == "" {
		log.V(lvl.ERROR).Info("Counters not found for Interface " + intfName)
		return errors.New("Counters not found for Interface " + intfName)
	}

	stats := strings.Fields(entry)
	log.V(lvl.DEBUG).Info(" Interface filds: ", stats)

	ret := getMgmtSpecificCounterAttr(targetUriPath, stats, counter_val)
	log.V(lvl.DEBUG).Info(" getMgmtCounters : ", *counter_val)
	return ret
}

func sumV4Counters(parent, member *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv4_State_Counters) {
	if member.InDiscardedPkts != nil {
		InDiscardedPkts := *member.InDiscardedPkts
		if parent.InDiscardedPkts != nil {
			InDiscardedPkts += *parent.InDiscardedPkts
		}
		parent.InDiscardedPkts = &InDiscardedPkts
	}
	if member.InErrorPkts != nil {
		InErrorPkts := *member.InErrorPkts
		if parent.InErrorPkts != nil {
			InErrorPkts += *parent.InErrorPkts
		}
		parent.InErrorPkts = &InErrorPkts
	}
	if member.InForwardedOctets != nil {
		InForwardedOctets := *member.InForwardedOctets
		if parent.InForwardedOctets != nil {
			InForwardedOctets += *parent.InForwardedOctets
		}
		parent.InForwardedOctets = &InForwardedOctets
	}
	if member.InForwardedPkts != nil {
		InForwardedPkts := *member.InForwardedPkts
		if parent.InForwardedPkts != nil {
			InForwardedPkts += *parent.InForwardedPkts
		}
		parent.InForwardedPkts = &InForwardedPkts
	}
	if member.InOctets != nil {
		InOctets := *member.InOctets
		if parent.InOctets != nil {
			InOctets += *parent.InOctets
		}
		parent.InOctets = &InOctets
	}
	if member.InPkts != nil {
		InPkts := *member.InPkts
		if parent.InPkts != nil {
			InPkts += *parent.InPkts
		}
		parent.InPkts = &InPkts
	}
	if member.OutDiscardedPkts != nil {
		OutDiscardedPkts := *member.OutDiscardedPkts
		if parent.OutDiscardedPkts != nil {
			OutDiscardedPkts += *parent.OutDiscardedPkts
		}
		parent.OutDiscardedPkts = &OutDiscardedPkts
	}
	if member.OutErrorPkts != nil {
		OutErrorPkts := *member.OutErrorPkts
		if parent.OutErrorPkts != nil {
			OutErrorPkts += *parent.OutErrorPkts
		}
		parent.OutErrorPkts = &OutErrorPkts
	}
	if member.OutForwardedOctets != nil {
		OutForwardedOctets := *member.OutForwardedOctets
		if parent.OutForwardedOctets != nil {
			OutForwardedOctets += *parent.OutForwardedOctets
		}
		parent.OutForwardedOctets = &OutForwardedOctets
	}
	if member.OutForwardedPkts != nil {
		OutForwardedPkts := *member.OutForwardedPkts
		if parent.OutForwardedPkts != nil {
			OutForwardedPkts += *parent.OutForwardedPkts
		}
		parent.OutForwardedPkts = &OutForwardedPkts
	}
	if member.OutOctets != nil {
		OutOctets := *member.OutOctets
		if parent.OutOctets != nil {
			OutOctets += *parent.OutOctets
		}
		parent.OutOctets = &OutOctets
	}
	if member.OutPkts != nil {
		OutPkts := *member.OutPkts
		if parent.OutPkts != nil {
			OutPkts += *parent.OutPkts
		}
		parent.OutPkts = &OutPkts
	}
	if member.InMulticastPkts != nil {
		InMulticastPkts := *member.InMulticastPkts
		if parent.InMulticastPkts != nil {
			InMulticastPkts += *parent.InMulticastPkts
		}
		parent.InMulticastPkts = &InMulticastPkts
	}
	if member.OutMulticastPkts != nil {
		OutMulticastPkts := *member.OutMulticastPkts
		if parent.OutMulticastPkts != nil {
			OutMulticastPkts += *parent.OutMulticastPkts
		}
		parent.OutMulticastPkts = &OutMulticastPkts
	}
}

var DbToYang_intf_ipv4_counters_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, ierr := getIntfTypeByName(ifName)
	if ierr != nil {
		return errors.New("DbToYang_intf_ipv4_counters_xfmr - err: " + ierr.Error())
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_ipv4_counters_xfmr - Invalid interface type: intfType: %v, err: %v", intfType, ierr)
		return errors.New("DbToYang_intf_ipv4_counters_xfmr - Invalid interface type: " + strconv.Itoa(int(intfType)))
	}
	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface

	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		ok := false
		if intfObj, ok = intfsObj.Interface[ifName]; !ok {
			intfObj, _ = intfsObj.NewInterface(ifName)
		}
		ygot.BuildEmptyTree(intfObj)
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, _ = intfsObj.NewInterface(ifName)
		ygot.BuildEmptyTree(intfObj)
	}

	if _, ok := intfObj.Subinterfaces.Subinterface[uint32(0)]; !ok {
		_, err := intfObj.Subinterfaces.NewSubinterface(uint32(0))
		if err != nil {
			log.V(lvl.ERROR).Info("DbToYang_intf_ipv4_counters_xfmr: Creation of subinterface subtree failed!")
			return err
		}
	}
	subIntf := intfObj.Subinterfaces.Subinterface[uint32(0)]
	ygot.BuildEmptyTree(subIntf)
	v4_counters := subIntf.Ipv4.State.Counters

	members, err := getMembers(inParams.dbs[db.StateDB], ifName)
	if err != nil {
		return fmt.Errorf("%w; getMembers() for %s failed", err, ifName)
	}

	for _, member := range members {
		var mcounters ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv4_State_Counters
		populatePortCounters(inParams, member, &mcounters)
		sumV4Counters(v4_counters, &mcounters)
	}

	return nil
}

// Returns the members of intfName, or a slice of just intfName if it's a singleton
func getMembers(stateDb *db.DB, intfName string) ([]string, error) {
	if !strings.HasPrefix(intfName, PORTCHANNEL) {
		return []string{intfName}, nil
	}
	memKeys, err := stateDb.GetKeys(&db.TableSpec{Name: LAG_MEMBER_TABLE_TN + stateDb.Opts.KeySeparator + intfName})
	if err != nil {
		return []string{intfName}, fmt.Errorf("%w; Unable to retrieve member keys for %s", err, intfName)
	}
	var members []string
	for i := range memKeys {
		members = append(members, memKeys[i].Get(1))
	}
	return members, nil
}

func sumV6Counters(parent, member *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv6_State_Counters) {
	if member.InDiscardedPkts != nil {
		InDiscardedPkts := *member.InDiscardedPkts
		if parent.InDiscardedPkts != nil {
			InDiscardedPkts += *parent.InDiscardedPkts
		}
		parent.InDiscardedPkts = &InDiscardedPkts
	}
	if member.InErrorPkts != nil {
		InErrorPkts := *member.InErrorPkts
		if parent.InErrorPkts != nil {
			InErrorPkts += *parent.InErrorPkts
		}
		parent.InErrorPkts = &InErrorPkts
	}
	if member.InForwardedOctets != nil {
		InForwardedOctets := *member.InForwardedOctets
		if parent.InForwardedOctets != nil {
			InForwardedOctets += *parent.InForwardedOctets
		}
		parent.InForwardedOctets = &InForwardedOctets
	}
	if member.InForwardedPkts != nil {
		InForwardedPkts := *member.InForwardedPkts
		if parent.InForwardedPkts != nil {
			InForwardedPkts += *parent.InForwardedPkts
		}
		parent.InForwardedPkts = &InForwardedPkts
	}
	if member.InOctets != nil {
		InOctets := *member.InOctets
		if parent.InOctets != nil {
			InOctets += *parent.InOctets
		}
		parent.InOctets = &InOctets
	}
	if member.InPkts != nil {
		InPkts := *member.InPkts
		if parent.InPkts != nil {
			InPkts += *parent.InPkts
		}
		parent.InPkts = &InPkts
	}
	if member.OutDiscardedPkts != nil {
		OutDiscardedPkts := *member.OutDiscardedPkts
		if parent.OutDiscardedPkts != nil {
			OutDiscardedPkts += *parent.OutDiscardedPkts
		}
		parent.OutDiscardedPkts = &OutDiscardedPkts
	}
	if member.OutErrorPkts != nil {
		OutErrorPkts := *member.OutErrorPkts
		if parent.OutErrorPkts != nil {
			OutErrorPkts += *parent.OutErrorPkts
		}
		parent.OutErrorPkts = &OutErrorPkts
	}

	if member.OutForwardedOctets != nil {
		OutForwardedOctets := *member.OutForwardedOctets
		if parent.OutForwardedOctets != nil {
			OutForwardedOctets += *parent.OutForwardedOctets
		}
		parent.OutForwardedOctets = &OutForwardedOctets
	}
	if member.OutForwardedPkts != nil {
		OutForwardedPkts := *member.OutForwardedPkts
		if parent.OutForwardedPkts != nil {
			OutForwardedPkts += *parent.OutForwardedPkts
		}
		parent.OutForwardedPkts = &OutForwardedPkts
	}
	if member.OutOctets != nil {
		OutOctets := *member.OutOctets
		if parent.OutOctets != nil {
			OutOctets += *parent.OutOctets
		}
		parent.OutOctets = &OutOctets
	}
	if member.OutPkts != nil {
		OutPkts := *member.OutPkts
		if parent.OutPkts != nil {
			OutPkts += *parent.OutPkts
		}
		parent.OutPkts = &OutPkts
	}
	if member.OutMulticastPkts != nil {
		OutMulticastPkts := *member.OutMulticastPkts
		if parent.OutMulticastPkts != nil {
			OutMulticastPkts += *parent.OutMulticastPkts
		}
		parent.OutMulticastPkts = &OutMulticastPkts
	}
	if member.InMulticastPkts != nil {
		InMulticastPkts := *member.InMulticastPkts
		if parent.InMulticastPkts != nil {
			InMulticastPkts += *parent.InMulticastPkts
		}
		parent.InMulticastPkts = &InMulticastPkts
	}
}

var DbToYang_intf_ipv6_counters_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, ierr := getIntfTypeByName(ifName)
	if ierr != nil {
		return errors.New("DbToYang_intf_ipv6_counters_xfmr - err: " + ierr.Error())
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		log.V(lvl.DEBUG).Infof("DbToYang_intf_ipv6_counters_xfmr - Invalid interface type: intfType: %v, err: %v", intfType, ierr)
		return errors.New("DbToYang_intf_ipv6_counters_xfmr - Invalid interface type: " + strconv.Itoa(int(intfType)))
	}
	var intfObj *ocbinds.OpenconfigInterfaces_Interfaces_Interface

	if intfsObj != nil && intfsObj.Interface != nil && len(intfsObj.Interface) > 0 {
		ok := false
		if intfObj, ok = intfsObj.Interface[ifName]; !ok {
			intfObj, _ = intfsObj.NewInterface(ifName)
		}
		ygot.BuildEmptyTree(intfObj)
	} else {
		ygot.BuildEmptyTree(intfsObj)
		intfObj, _ = intfsObj.NewInterface(ifName)
		ygot.BuildEmptyTree(intfObj)
	}

	if _, ok := intfObj.Subinterfaces.Subinterface[uint32(0)]; !ok {
		_, err := intfObj.Subinterfaces.NewSubinterface(uint32(0))
		if err != nil {
			log.V(lvl.ERROR).Info("DbToYang_intf_ipv6_counters_xfmr: Creation of subinterface subtree failed!")
			return err
		}
	}
	subIntf := intfObj.Subinterfaces.Subinterface[uint32(0)]
	ygot.BuildEmptyTree(subIntf)
	v6_counters := subIntf.Ipv6.State.Counters

	members, err := getMembers(inParams.dbs[db.StateDB], ifName)
	if err != nil {
		return fmt.Errorf("%w; getMembers() for %s failed", err, ifName)
	}

	for _, member := range members {
		var mcounters ocbinds.OpenconfigInterfaces_Interfaces_Interface_Subinterfaces_Subinterface_Ipv6_State_Counters
		populatePortCounters(inParams, member, &mcounters)
		sumV6Counters(v6_counters, &mcounters)
	}

	return nil
}

/* Get default speed from valid speeds.  Max valid speed should be the default speed.*/
func validateSpeed(d *db.DB, ifName string, speed string) error {

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		errStr := "Invalid Interface"
		err = tlerr.InvalidArgsError{Format: errStr}
		return err
	}

	/* No validation possible for MGMT interface */
	if intfType == IntfTypeMgmt || intfType == IntfTypeMgmtBond {
		log.V(lvl.DEBUG).Info("Management port ", ifName, " skipped speed validation.")
		return nil
	}

	speeds, err := platform.ValidSpeedsForIf(ifName)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("Valid speeds for ", ifName, " is ", speeds, " SET ", speed)
	for _, vspeed := range speeds {
		if speed == strings.TrimSpace(vspeed) {
			log.V(lvl.DEBUG).Info(vspeed, " is valid.")
			return nil
		}
	}
	return tlerr.InvalidArgs("Unsupported speed %s for interface: %s", speed, ifName)
}

// YangToDb_subintf_ipv4_tbl_key_xfmr is a YangToDB Key transformer for IPv4 config.
var YangToDb_subintf_ipv4_tbl_key_xfmr KeyXfmrYangToDb = func(inParams XfmrParams) (string, error) {
	log.V(lvl.DEBUG).Info("Entering YangToDb_subintf_ipv4_tbl_key_xfmr")

	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	log.V(lvl.DEBUG).Info("Intf name: ", ifName)
	requestUriPath, err := getYangPathFromUri(inParams.requestUri)
	log.V(lvl.DEBUG).Info("inParams.requestUri: ", requestUriPath)
	log.V(lvl.DEBUG).Info("Exiting YangToDb_subintf_ipv4_tbl_key_xfmr")
	return ifName, err
}

// DbToYang_ipv4_enabled_xfmr is a DbToYang Field transformer for IPv4 config "enabled". */
var DbToYang_ipv4_enabled_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("Entering DbToYang_ipv4_enabled_xfmr inParams.key ", inParams.key)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_ipv4_enabled_xfmr, Error: key not found")
	}

	tblName, err := getIntfTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, err
	}

	data := (*inParams.dbDataMap)[inParams.curDb]
	resMap := make(map[string]interface{})
	resMap["enabled"] = false
	if ipv4Status, ok := data[tblName][inParams.key].Field["ipv4_enabled"]; ok && ipv4Status == "enable" {
		resMap["enabled"] = true
	}
	return resMap, nil
}

var DbToYang_intf_description_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, ierr := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || ierr != nil {
		return nil, fmt.Errorf("DbToYang_intf_description_xfmr: Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, ierr)
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_description_xfmr: interface type not found " + strconv.Itoa(int(intfType)))
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_description_xfmr table name not found : " + tblName)
	}
	prtInst, dbErr := getDBValues(inParams, tblName)
	if dbErr != nil {
		return nil, dbErr
	}
	result := make(map[string]interface{})
	if result["description"], ok = prtInst.Field["description"]; ok {
		return result, nil
	}
	return nil, errors.New("description field not found in DB.")
}

var DbToYang_intf_fqin_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if intfType == IntfTypeUnset || err != nil {
		return nil, fmt.Errorf("DbToYang_intf_fqin_xfmr: Invalid interface - Type Unset: %v; err = %v", intfType == IntfTypeUnset, err)
	}
	var dbFieldName string
	if intfType == IntfTypeEthernet {
		dbFieldName = PORT_FQIN
	} else if intfType == IntfTypePortChannel {
		dbFieldName = LAG_TABLE_ALIAS
	} else {
		return nil, errors.New("Interface type is not IntfTypeEthernet or IntfTypePortChannel")
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_intf_fqin_xfmr: interface type not found : " + strconv.Itoa(int(intfType)))
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_fqin_xfmr: table name not found : " + tblName)
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	if result[PORT_FQIN], ok = prtInst.Field[dbFieldName]; ok {
		return result, nil
	}
	return nil, errors.New("fully-qualified-interface-name field not found in DB.")
}

var YangToDb_intf_fqin_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	if inParams.oper == DELETE {
		return res_map, nil
	}
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	var dbFieldName string
	if intfType == IntfTypeEthernet {
		dbFieldName = PORT_FQIN
	} else if intfType == IntfTypePortChannel {
		dbFieldName = LAG_TABLE_ALIAS
	} else {
		return res_map, nil
	}
	var fqinValue *string
	var ok bool
	fqinValue, ok = inParams.param.(*string)
	if !ok {
		return nil, errors.New("YangToDb_intf_fqin_xfmr, Error: Invalid parameter")
	}
	res_map[dbFieldName] = *fqinValue
	return res_map, nil
}

var YangToDb_intf_ecmp_hash_offset_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	if inParams.oper == DELETE {
		return res_map, nil
	}
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		// only supported for singletons
		return nil, nil
	}

	offset, ok := inParams.param.(*uint64)
	if !ok {
		return nil, errors.New("YangToDb_intf_ecmp_hash_offset_xfmr, Error: Invalid parameter")
	}
	res_map["ecmp_hash_offset"] = strconv.FormatUint(*offset, 10)
	return res_map, nil
}

var DbToYang_intf_ecmp_hash_offset_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if intfType != IntfTypeEthernet || err != nil {
		// only supported for singletons
		return nil, nil
	}
	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		log.V(lvl.DEBUG).Info("DbToYang_intf_ecmp_hash_offset_xfmr type not found : ", intfType)
		return nil, errors.New("interface type not found.")
	}
	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		log.V(lvl.DEBUG).Info("DbToYang_intf_ecmp_hash_offset_xfmr table name not found : ", tblName)
		return nil, errors.New("table name not found.")
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	if result["ecmp-hash-offset"], ok = prtInst.Field["ecmp_hash_offset"]; ok {
		return result, nil
	}
	return nil, errors.New("ecmp-hash-offset field not found in DB.")
}

var YangToDb_intf_ecmp_hash_algorithm_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	if inParams.oper == DELETE {
		return res_map, nil
	}
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		// only supported for singletons
		return nil, nil
	}

	algoEnum, ok := inParams.param.(ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm)
	if !ok {
		return nil, errors.New("YangToDb_intf_ecmp_hash_algorithm_xfmr, Error: Invalid parameter")
	}
	algoStr, ok := yangToDbEcmpHashAlgorithmMap[algoEnum]
	if !ok {
		return nil, tlerr.InvalidArgs("Invalid ecmp-hash-algorithm %s", algoEnum)
	}

	res_map["ecmp_hash_algorithm"] = algoStr
	return res_map, nil
}

var DbToYang_intf_ecmp_hash_algorithm_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {

	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		// only supported for singletons
		return nil, nil
	}

	tblName, err := getPortTableNameByDBId(IntfTypeTblMap[IntfTypeEthernet], inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_ecmp_hash_algorithm_xfmr table name not found. Err: " + err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	algoStr, ok := prtInst.Field["ecmp_hash_algorithm"]
	if !ok {
		return nil, errors.New("ecmp_hash_algorithm field not found in DB.")
	}
	algoEnum, ok := dbToYangEcmpHashAlgorithmMap[algoStr]
	if !ok {
		return nil, errors.New("ecmp_hash_algorithm field read from DB not found in dbToYangEcmpHashAlgorithmMap.")
	}
	result["ecmp-hash-algorithm"] = ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm.ΛMap(algoEnum)["E_OpenconfigInterfaces_Interfaces_Interface_Config_EcmpHashAlgorithm"][int64(algoEnum)].Name
	return result, nil
}

var YangToDb_intf_port_direction_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	res_map := make(map[string]string)
	if inParams.oper == DELETE {
		return res_map, nil
	}
	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		// only supported for singletons
		log.V(lvl.DEBUG).Info("YangToDb_intf_port_direction_xfmr, Error: port-direction is only supported for singletons")
		return nil, nil
	}

	directionEnum, ok := inParams.param.(ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection)
	if !ok {
		log.V(lvl.DEBUG).Info("YangToDb_intf_port_direction_xfmr, Error: Invalid parameter")
		return nil, nil
	}
	directionStr, ok := yangToDbPortDirectionMap[directionEnum]
	if !ok {
		return nil, errors.New("YangToDb_intf_port_direction_xfmr, Error: Invalid port-direction")
	}

	res_map["port_direction"] = directionStr
	return res_map, nil
}

var DbToYang_intf_port_direction_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {

	ifName := NewPathInfo(inParams.uri).Var("name")
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, err
	}
	if intfType != IntfTypeEthernet {
		// only supported for singletons
		log.V(lvl.DEBUG).Info("DbToYang_intf_port_direction_xfmr, Error: port-direction is only supported for singletons")
		return nil, nil
	}

	tblName, err := getPortTableNameByDBId(IntfTypeTblMap[IntfTypeEthernet], inParams.curDb)
	if err != nil {
		return nil, errors.New("DbToYang_intf_port_direction_xfm table name not found. Err: " + err.Error())
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	directionStr, ok := prtInst.Field["port_direction"]
	if !ok {
		return nil, errors.New("port_direction field not found in DB.")
	}
	directionEnum, ok := dbToYangPortDirectionMap[directionStr]
	if !ok {
		return nil, errors.New("port_direction field read from DB not found in dbToYangPortDirectionMap.")
	}
	result["port-direction"] = ocbinds.E_OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection.ΛMap(directionEnum)["E_OpenconfigInterfaces_Interfaces_Interface_Config_PortDirection"][int64(directionEnum)].Name
	return result, nil
}

var DbToYangPath_intf_path_xfmr PathXfmrDbToYangFunc = func(inParams XfmrDbToYgPathParams) error {
	rootPath := "/openconfig-interfaces:interfaces/interface"

	log.V(lvl.DEBUG).Info("DbToYangPath_intf_path_xfmr: inParams: ", inParams)

	switch len(inParams.tblKeyComp) {
	case 1:
		inParams.ygPathKeys[rootPath+"/name"] = inParams.tblKeyComp[0]
	default:
		return fmt.Errorf("Invalid tblKeyCom for intf path xmfr:%v", inParams.tblKeyComp)
	}

	log.V(lvl.DEBUG).Info("DbToYangPath_intf_path_xfmr:- params.ygPathKeys: ", inParams.ygPathKeys)

	return nil
}

func retrieveDbEntryForSingletonInterface(inParams XfmrParams) (db.Value, error) {
	return retrieveDbEntry(inParams, []E_InterfaceType{IntfTypeEthernet})
}

func retrieveDbEntry(inParams XfmrParams, supportedTypes []E_InterfaceType) (db.Value, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	db_value := db.Value{Field: map[string]string{}}
	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return db_value, tlerr.InvalidArgsError{Format: err.Error()}
	}

	if !slices.Contains(supportedTypes, intfType) {
		return db_value, errors.New("Interface type not supported")
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return db_value, errors.New("interface type not found " + strconv.Itoa(int(intfType)))
	}

	tblName, err := getPortTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return db_value, errors.New("table name not found.")
	}

	return getDBValues(inParams, tblName)
}

var DbToYang_intf_eth_ingress_timestamp_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("DbToYang_intf_eth_ingress_timestamp_xfmr: inParams: ", inParams)

	prtInst, err := retrieveDbEntryForSingletonInterface(inParams)
	if err != nil {
		return nil, err
	}

	if ingressTimestamp, ok := prtInst.Field[PORT_INGRESS_TIMESTAMP]; ok && ingressTimestamp != "" {
		if ingressTimestampBool, err := strconv.ParseBool(ingressTimestamp); err != nil {
			return nil, err
		} else {
			return map[string]interface{}{"insert-ingress-timestamp": &ingressTimestampBool}, nil
		}
	}
	return nil, nil
}

var DbToYang_intf_eth_egress_timestamp_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	log.V(lvl.DEBUG).Info("DbToYang_intf_eth_egress_timestamp_xfmr: inParams: ", inParams)

	prtInst, err := retrieveDbEntryForSingletonInterface(inParams)
	if err != nil {
		return nil, err
	}

	if egressTimestamp, ok := prtInst.Field[PORT_EGRESS_TIMESTAMP]; ok && egressTimestamp != "" {
		if egressTimestampBool, err := strconv.ParseBool(egressTimestamp); err != nil {
			return nil, err
		} else {
			return map[string]interface{}{"insert-egress-timestamp": &egressTimestampBool}, nil
		}
	}
	return nil, nil
}

var YangToDb_intf_aied_link_damping_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType != IntfTypeEthernet {
		return nil, nil
	}
	intTbl, _ := IntfTypeTblMap[intfType]

	if inParams.oper == DELETE {
		if err := validateIntfExists(inParams.d, intTbl.cfgDb.portTN, ifName); err != nil {
			return nil, err
		}

		// Set link event damping algorithm to disabled.
		updateSubOpDataMap(map[db.DBNum]map[string]map[string]db.Value{
			db.ConfigDB: map[string]map[string]db.Value{
				intTbl.cfgDb.portTN: map[string]db.Value{
					ifName: db.Value{Field: map[string]string{
						"link_event_damping_algorithm": "disabled",
					}},
				},
			},
		}, UPDATE, inParams)

		// Delete the penalty-based-aied fields from the table.
		return map[string]map[string]db.Value{
			intTbl.cfgDb.portTN: map[string]db.Value{
				ifName: db.Value{Field: map[string]string{
					"max_suppress_time":  "",
					"decay_half_life":    "",
					"suppress_threshold": "",
					"reuse_threshold":    "",
					"flap_penalty":       "",
				}},
			},
		}, nil
	}

	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || len(intfsObj.Interface) == 0 {
		return nil, errors.New("YangToDb_intf_aied_link_damping_xfmr intfsObj/interface is not specified.")
	}
	intfObj, ok := intfsObj.Interface[ifName]
	if !ok {
		return nil, errors.New("YangToDb_intf_aied_link_damping_xfmr interface entry not found in ygot tree: " + ifName)
	}

	memMap := make(map[string]map[string]db.Value)
	resMap := make(map[string]string)
	value := db.Value{Field: resMap}

	if msp := intfObj.PenaltyBasedAied.Config.MaxSuppressTime; msp == nil {
		return nil, errors.New("Max suppress time missing from link damping config for " + ifName)
	} else {
		resMap["max_suppress_time"] = strconv.FormatUint(uint64(*msp), 10)
	}
	if dhl := intfObj.PenaltyBasedAied.Config.DecayHalfLife; dhl == nil {
		return nil, errors.New("Decay half life missing from link damping config for " + ifName)
	} else {
		resMap["decay_half_life"] = strconv.FormatUint(uint64(*dhl), 10)
	}
	if st := intfObj.PenaltyBasedAied.Config.SuppressThreshold; st == nil {
		return nil, errors.New("SuppressThreshold missing from link damping config for " + ifName)
	} else {
		resMap["suppress_threshold"] = strconv.FormatUint(uint64(*st), 10)
	}
	if rt := intfObj.PenaltyBasedAied.Config.ReuseThreshold; rt == nil {
		return nil, errors.New("Reuse threshold missing from link damping config for " + ifName)
	} else {
		resMap["reuse_threshold"] = strconv.FormatUint(uint64(*rt), 10)
	}
	if fp := intfObj.PenaltyBasedAied.Config.FlapPenalty; fp == nil {
		return nil, errors.New("Flap penalty missing from link damping config for " + ifName)
	} else {
		resMap["flap_penalty"] = strconv.FormatUint(uint64(*fp), 10)
	}
	resMap["link_event_damping_algorithm"] = "aied"

	if _, ok := memMap[intTbl.cfgDb.portTN]; !ok {
		memMap[intTbl.cfgDb.portTN] = make(map[string]db.Value)
	}
	memMap[intTbl.cfgDb.portTN][ifName] = value
	log.V(lvl.DEBUG).Infof("Setting AIED Link Damping Config: %v", memMap[intTbl.cfgDb.portTN])
	return memMap, nil
}

var DbToYang_intf_aied_link_damping_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	intfsObj := getIntfsRoot(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil || intfType != IntfTypeEthernet {
		return err
	}
	intTbl, _ := IntfTypeTblMap[intfType]

	ygot.BuildEmptyTree(intfsObj)
	intfObj, ok := intfsObj.Interface[ifName]
	if !ok {
		if intfObj, err = intfsObj.NewInterface(ifName); err != nil {
			return err
		}
	}
	ygot.BuildEmptyTree(intfObj)
	ygot.BuildEmptyTree(intfObj.PenaltyBasedAied)
	ygot.BuildEmptyTree(intfObj.PenaltyBasedAied.Config)
	ygot.BuildEmptyTree(intfObj.PenaltyBasedAied.State)

	configDb := inParams.dbs[db.ConfigDB]
	entry, err := configDb.GetEntry(&db.TableSpec{Name: intTbl.cfgDb.portTN}, db.Key{Comp: []string{ifName}})
	if err != nil {
		return err
	}
	if entry.Get("link_event_damping_algorithm") == "aied" {
		if mst := entry.Get("max_suppress_time"); mst != "" {
			mstVal, err := strconv.ParseUint(mst, 10, 32)
			if err != nil {
				return err
			}
			mst32 := uint32(mstVal)
			intfObj.PenaltyBasedAied.Config.MaxSuppressTime = &mst32
		}
		if dhl := entry.Get("decay_half_life"); dhl != "" {
			dhlVal, err := strconv.ParseUint(dhl, 10, 32)
			if err != nil {
				return err
			}
			dhl32 := uint32(dhlVal)
			intfObj.PenaltyBasedAied.Config.DecayHalfLife = &dhl32
		}
		if st := entry.Get("suppress_threshold"); st != "" {
			stVal, err := strconv.ParseUint(st, 10, 32)
			if err != nil {
				return err
			}
			st32 := uint32(stVal)
			intfObj.PenaltyBasedAied.Config.SuppressThreshold = &st32
		}
		if rt := entry.Get("reuse_threshold"); rt != "" {
			rtVal, err := strconv.ParseUint(rt, 10, 32)
			if err != nil {
				return err
			}
			rt32 := uint32(rtVal)
			intfObj.PenaltyBasedAied.Config.ReuseThreshold = &rt32
		}
		if fp := entry.Get("flap_penalty"); fp != "" {
			fpVal, err := strconv.ParseUint(fp, 10, 32)
			if err != nil {
				return err
			}
			fp32 := uint32(fpVal)
			intfObj.PenaltyBasedAied.Config.FlapPenalty = &fp32
		}
	}

	appStateDb := inParams.dbs[db.ApplStateDB]
	entry, err = appStateDb.GetEntry(&db.TableSpec{Name: intTbl.appStateDb.portTN}, db.Key{Comp: []string{ifName}})
	if err != nil {
		return err
	}
	if entry.Get("link_event_damping_algorithm") == "aied" {
		if mst := entry.Get("max_suppress_time"); mst != "" {
			mstVal, err := strconv.ParseUint(mst, 10, 32)
			if err != nil {
				return err
			}
			mst32 := uint32(mstVal)
			intfObj.PenaltyBasedAied.State.MaxSuppressTime = &mst32
		}
		if dhl := entry.Get("decay_half_life"); dhl != "" {
			dhlVal, err := strconv.ParseUint(dhl, 10, 32)
			if err != nil {
				return err
			}
			dhl32 := uint32(dhlVal)
			intfObj.PenaltyBasedAied.State.DecayHalfLife = &dhl32
		}
		if st := entry.Get("suppress_threshold"); st != "" {
			stVal, err := strconv.ParseUint(st, 10, 32)
			if err != nil {
				return err
			}
			st32 := uint32(stVal)
			intfObj.PenaltyBasedAied.State.SuppressThreshold = &st32
		}
		if rt := entry.Get("reuse_threshold"); rt != "" {
			rtVal, err := strconv.ParseUint(rt, 10, 32)
			if err != nil {
				return err
			}
			rt32 := uint32(rtVal)
			intfObj.PenaltyBasedAied.State.ReuseThreshold = &rt32
		}
		if fp := entry.Get("flap_penalty"); fp != "" {
			fpVal, err := strconv.ParseUint(fp, 10, 32)
			if err != nil {
				return err
			}
			fp32 := uint32(fpVal)
			intfObj.PenaltyBasedAied.State.FlapPenalty = &fp32
		}
	}
	return err
}

var Subscribe_intf_aied_link_damping_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	ifName := NewPathInfo(inParams.uri).Var("name")
	if ifName == "" {
		ifName = "*"
	}

	return XfmrSubscOutParams{
		isVirtualTbl: false,
		onChange:     OnchangeDisable,
		dbDataMap: RedisDbSubscribeMap{
			db.ConfigDB: {"PORT": {ifName: {}}},
		},
	}, nil
}
