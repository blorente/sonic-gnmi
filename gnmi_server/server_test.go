package gnmi

import (
	"container/ring"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/transformer"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/agiledragon/gomonkey/v2"
	linuxproc "github.com/c9s/goprocinfo/linux"
	"github.com/godbus/dbus/v5"
	"github.com/golang/protobuf/proto"
	"github.com/google/gnxi/utils/xpath"
	fuzz "github.com/google/gofuzz"
	"github.com/kylelemons/godebug/pretty"
	"github.com/openconfig/gnmi/client"
	cacheclient "github.com/openconfig/gnmi/client"
	gclient "github.com/openconfig/gnmi/client/gnmi"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	pb "github.com/openconfig/gnmi/proto/gnmi"
	ext_pb "github.com/openconfig/gnmi/proto/gnmi_ext"
	"github.com/openconfig/gnmi/value"
	gnoi_system_pb "github.com/openconfig/gnoi/system"
	"github.com/openconfig/ygot/ygot"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	jtest "github.com/sonic-net/sonic-gnmi/jsontest"
	spb "github.com/sonic-net/sonic-gnmi/proto/gnmi_sonic"
	sgpb "github.com/sonic-net/sonic-gnmi/proto/sonic_gnoi"
	spb_jwt "github.com/sonic-net/sonic-gnmi/proto/sonic_gnoi/jwt"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"
	"github.com/sonic-net/sonic-gnmi/swsscommon"
	"github.com/sonic-net/sonic-gnmi/test_utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
)

const (
	srvTestCertFile   = "../testdata/mtls/server_test_cert.pem"
	srvTestKeyFile    = "../testdata/mtls/server_test_key.pem"
	srvTestKeyLink    = "../testdata/mtls/server_key.lnk"
	srvTestCertLink   = "../testdata/mtls/server_cert.lnk"
	certzTestMetaFile = "../testdata/mtls/grpc-version.json"
)

const nsfNotificationSleep = 3 * time.Second

var clientTypes = []string{gclient.Type}

type testServerType int

const (
	testSrvType testServerType = iota
	readOnlySrvType
	rejectSrvType
	authSrvType
	keepAliveSrvType
)


var controllerPaths = []struct {
	path string
	mode pb.SubscriptionMode
}{
	{"/interfaces/interface[name=*]/state/admin-status", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/ethernet/state/mac-address", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/state/hardware-port", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/state/health-indicator", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/state/id", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/state/oper-status", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/ethernet/state/port-speed", pb.SubscriptionMode_ON_CHANGE},
	{"/components/component[name=*]/integrated-circuit/state/node-id", pb.SubscriptionMode_ON_CHANGE},
	{"/components/component[name=*]/state/parent", pb.SubscriptionMode_ON_CHANGE},
	{"/components/component[name=*]/state/oper-status", pb.SubscriptionMode_ON_CHANGE},
	{"/components/component[name=*]/state/software-version", pb.SubscriptionMode_ON_CHANGE},
	{"/components/component[name=*]/software-module/state/module-type", pb.SubscriptionMode_ON_CHANGE},
	{"/system/alarms/alarm[id=*]/state/severity", pb.SubscriptionMode_ON_CHANGE},
	{"/system/state/config-meta-data", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/ethernet/state/aggregate-id", pb.SubscriptionMode_SAMPLE},
	{"/interfaces/interface[name=*]/aggregation/state/lag-speed", pb.SubscriptionMode_ON_CHANGE},
	{"/lacp/interfaces/interface[name=*]/members/member[interface=*]/state/partner-id", pb.SubscriptionMode_ON_CHANGE},
	{"/lacp/interfaces/interface[name=*]/state/system-priority", pb.SubscriptionMode_ON_CHANGE},
	{"/interfaces/interface[name=*]/state/counters", pb.SubscriptionMode_TARGET_DEFINED},
	{"/qos/interfaces/interface[interface-id=*]/output/queues/queue[name=*]/state", pb.SubscriptionMode_TARGET_DEFINED},
}


var pictorPaths = []struct {
	path string
	mode pb.SubscriptionMode
}{
	{"/interfaces", pb.SubscriptionMode_SAMPLE},
	{"/lacp", pb.SubscriptionMode_SAMPLE},
	{"/components", pb.SubscriptionMode_SAMPLE},
	{"/qos", pb.SubscriptionMode_SAMPLE},
	{"/sampling", pb.SubscriptionMode_SAMPLE},
	{"/system", pb.SubscriptionMode_SAMPLE},
}

// Mock interface implementation that returns non-critical state
type mockSystemStateHelperSuccess struct{}

func (t mockSystemStateHelperSuccess) Close() {}

func (t mockSystemStateHelperSuccess) GetSystemState() common_utils.SystemState {
	return common_utils.SystemUp
}

func (t mockSystemStateHelperSuccess) IsSystemCritical() bool {
	return false
}

func (t mockSystemStateHelperSuccess) GetSystemCriticalReason() string {
	return "test"
}

func (t mockSystemStateHelperSuccess) AllComponentStates() map[common_utils.SystemComponent]common_utils.ComponentStateInfo {
	return map[common_utils.SystemComponent]common_utils.ComponentStateInfo{}
}

func onSonicSwitch() bool {
	fileInfo, err := os.Stat("/etc/sonic/sonic_version.yml")
	if err != nil {
		return false
	}
	if fileInfo.Size() == 0 {
		return false
	}
	return true
}

func prepareDbUtil(t *testing.T, db string, key string, files ...string) {
	for _, f := range files {
		err := jtest.LoadFileToDB(db, key, f)
		if err != nil {
			t.Fatalf("Failed to load config:%v", err)
		}
	}
}

func loadConfig(t *testing.T, key string, in []byte) map[string]interface{} {
	var fvp map[string]interface{}

	err := json.Unmarshal(in, &fvp)
	if err != nil {
		t.Errorf("Failed to Unmarshal %v err: %v", in, err)
	}
	if key != "" {
		kv := map[string]interface{}{}
		kv[key] = fvp
		return kv
	}
	return fvp
}

// assuming input data is in key field/value pair format
func loadDB(t *testing.T, rclient *redis.Client, mpi map[string]interface{}) {
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			_, err := rclient.HMSet(context.Background(), key, fv.(map[string]interface{})).Result()
			if err != nil {
				t.Errorf("Invalid data for db:  %v : %v %v", key, fv, err)
			}
		default:
			t.Errorf("Invalid data for db: %v : %v", key, fv)
		}
	}
}
func loadDBNotStrict(t *testing.T, rclient *redis.Client, mpi map[string]interface{}) {
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			rclient.HMSet(context.Background(), key, fv.(map[string]interface{})).Result()

		}
	}
}

func createClient(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()
	cred := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	conn, err := grpc.Dial(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(cred),
	)
	if err != nil {
		t.Fatalf("Dialing to :%d failed: %v", port, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// To avoid problems related to starting a new instance of the server on a port
// already being used by another instance every time a server is created it is
// started on a unique TCP port.
var testSrvPort int64 = 8000
var testSrvPortMu sync.Mutex

func getNewTestSrvPort() int64 {
	testSrvPortMu.Lock()
	defer testSrvPortMu.Unlock()
	testSrvPort = testSrvPort + 1
	if testSrvPort > 8888 {
		testSrvPort = 8000
	}
	return testSrvPort
}

func createServer(t *testing.T) *Server {
	t.Helper()
	s, err := NewServer(testServerConfig(testSrvType))
	if err != nil {
		t.Fatalf("Failed to create gNMI server: %v", err)
	}
	return s
}

func createCustomServer(t *testing.T, cfg *Config) *Server {
	t.Helper()
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create gNMI server: %v", err)
	}
	return s
}

func testServerConfig(srvType testServerType) *Config {
	oscfg := &OSConfig{
		ImgDir:          "/tmp",
		ProcessTrfReady: ProcessFakeTrfReady,
		ProcessTrfEnd:   ProcessFakeTrfEnd,
	}
	cfg := &Config{
		Port:                getNewTestSrvPort(),
		EnableTranslibWrite: true,
		StreamingThreshold:  100,
		UnaryThreshold:      100,
		LogLevel:            3,
		CaCertLnk:           "",
		CaCertFile:          "",
		SrvCertLnk:          srvTestCertLink,
		SrvCertFile:         srvTestCertFile,
		SrvKeyLnk:           srvTestKeyLink,
		SrvKeyFile:          srvTestKeyFile,
		OSCfg:               oscfg,
		GetOptions:          SrvTestConfig,
		CacheResponses:      true,
	}

	switch srvType {
	case readOnlySrvType:
		cfg.EnableTranslibWrite = false
	case rejectSrvType:
		cfg.StreamingThreshold = 2
		cfg.UnaryThreshold = 2
	case authSrvType:
		cfg.UserAuth = AuthTypes{"password": true, "cert": true, "jwt": true}
	case keepAliveSrvType:
		keepaliveMaxIdle = 1 * time.Second
		cfg.EnableNativeWrite = true
	case testSrvType:
		fallthrough
	default:
		cfg.EnableNativeWrite = true
	}

	return cfg
}

// runTestGet requests a path from the server by Get grpc call, and compares if
// the return code and response value are expected.
func runTestGet(t *testing.T, ctx context.Context, gClient pb.GNMIClient, pathTarget string,
	textPbPath string, getType pb.GetRequest_DataType, encType pb.Encoding, wantRetCode codes.Code, wantRespVal interface{}, valTest bool) {
	//var retCodeOk bool
	// Send request
	t.Helper()
	var pbPath pb.Path
	if err := proto.UnmarshalText(textPbPath, &pbPath); err != nil {
		t.Fatalf("error in unmarshaling path: %v %v", textPbPath, err)
	}
	prefix := pb.Path{Target: pathTarget}
	req := &pb.GetRequest{
		Prefix:   &prefix,
		Path:     []*pb.Path{&pbPath},
		Encoding: encType,
		Type:     getType,
	}

	resp, err := gClient.Get(ctx, req)
	// Check return code
	gotRetStatus, ok := status.FromError(err)
	if !ok {
		t.Fatal("got a non-grpc error from grpc call")
	}

	if gotRetStatus.Code() != wantRetCode {
		t.Log("err: ", err)
		t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), wantRetCode)
	}

	// Check response value
	if valTest {
		var gotVal interface{}
		if resp != nil {
			notifs := resp.GetNotification()
			if len(notifs) != 1 {
				t.Fatalf("got %d notifications, want 1", len(notifs))
			}
			if encType == gnmipb.Encoding_JSON || encType == gnmipb.Encoding_JSON_IETF {
				updates := notifs[0].GetUpdate()
				if len(updates) != 1 {
					t.Fatalf("got %d updates in the notification, want 1", len(updates))
				}
				val := updates[0].GetVal()
				if val.GetJsonIetfVal() == nil {
					gotVal, err = value.ToScalar(val)
					if err != nil {
						t.Errorf("got: %v, want a scalar value", gotVal)
					}
				} else {
					// Unmarshal json data to gotVal container for comparison
					if err := json.Unmarshal(val.GetJsonIetfVal(), &gotVal); err != nil {
						t.Fatalf("error in unmarshaling IETF JSON data to json container: %v", err)
					}
					var wantJSONStruct interface{}
					if v, ok := wantRespVal.(string); ok {
						wantRespVal = []byte(v)
					}
					if err := json.Unmarshal(wantRespVal.([]byte), &wantJSONStruct); err != nil {
						t.Fatalf("error in unmarshaling IETF JSON data to json container: %v", err)
					}
					wantRespVal = wantJSONStruct
				}
			} else if encType == gnmipb.Encoding_PROTO {
				updates := notifs[0].GetUpdate()
				if len(updates) == 1 {
					tVal := updates[0].GetVal()
					gotVal, err = value.ToScalar(tVal)
					if err != nil {
						t.Fatalf("Error \"%v\" converting proto encoded typed-value to scalar, TypedValue=%v", err, tVal)
					}
				} else if len(updates) == 0 && wantRespVal == nil {
					gotVal = nil
				} else {
					t.Fatalf("Expected 1 update, got %d (updates=%#v)", len(updates), updates)
				}
			} else {
				t.Fatalf("Unsupported encoding %v\n", encType)
			}
		}

		if !reflect.DeepEqual(gotVal, wantRespVal) {
			t.Errorf("got: %v (%T),\nwant %v (%T)", gotVal, gotVal, wantRespVal, wantRespVal)
		}
	}
}

func extractJSON(val string) []byte {
	jsonBytes, err := ioutil.ReadFile(val)
	if err == nil {
		return jsonBytes
	}
	return []byte(val)
}

type op_t int

const (
	Delete  op_t = 1
	Replace op_t = 2
	Update  op_t = 3
)

func runTestSet(t *testing.T, ctx context.Context, gClient pb.GNMIClient, pathTarget string,
	textPbPath string, wantRetCode codes.Code, wantRespVal interface{}, attributeData string, op op_t) {
	t.Helper()
	// Send request
	var pbPath pb.Path
	if err := proto.UnmarshalText(textPbPath, &pbPath); err != nil {
		t.Fatalf("error in unmarshaling path: %v %v", textPbPath, err)
	}
	req := &pb.SetRequest{}
	switch op {
	case Replace, Update:
		prefix := pb.Path{Target: pathTarget}
		var v *pb.TypedValue
		v = &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: extractJSON(attributeData)}}
		data := []*pb.Update{{Path: &pbPath, Val: v}}

		req = &pb.SetRequest{
			Prefix: &prefix,
		}
		if op == Replace {
			req.Replace = data
		} else {
			req.Update = data
		}
	case Delete:
		req = &pb.SetRequest{
			Delete: []*pb.Path{&pbPath},
		}
	}

	runTestSetRaw(t, ctx, gClient, req, wantRetCode)
}

func runTestSetRaw(t *testing.T, ctx context.Context, gClient pb.GNMIClient, req *pb.SetRequest,
	wantRetCode codes.Code) {
	t.Helper()

	_, err := gClient.Set(ctx, req)
	gotRetStatus, ok := status.FromError(err)
	if !ok {
		t.Fatal("got a non-grpc error from grpc call")
	}
	if gotRetStatus.Code() != wantRetCode {
		t.Log("err: ", err)
		t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), wantRetCode)
	} else {
	}
}

// pathToPb converts string representation of gnmi path to protobuf format
func pathToPb(s string) string {
	p, _ := ygot.StringToStructuredPath(s)
	return proto.MarshalTextString(p)
}

func removeModulePrefixFromPathPb(t *testing.T, s string) string {
	t.Helper()
	var p pb.Path
	if err := proto.UnmarshalText(s, &p); err != nil {
		t.Fatalf("error unmarshaling path: %v %v", s, err)
	}
	for _, ele := range p.Elem {
		if k := strings.IndexByte(ele.Name, ':'); k != -1 {
			ele.Name = ele.Name[k+1:]
		}
	}
	return proto.MarshalTextString(&p)
}

func runServer(t *testing.T, s *Server) {
	err := s.Serve() // blocks until close
	if err != nil {
		t.Logf("gRPC server err: %v", err)
	}
}

func getRedisClientN(t *testing.T, n int, namespace string) *redis.Client {
	addr, err := sdcfg.GetDbTcpAddr("COUNTERS_DB", namespace)
	if err != nil {
		t.Fatalf("failed to get addr %v", err)
	}
	rclient := db.TransactionalRedisClientWithOpts(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "", // no password set
		DB:          n,
		DialTimeout: 0,
	})
	_, err = rclient.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("failed to connect to redis server %v", err)
	}
	return rclient
}

func getRedisClient(t *testing.T, namespace string) *redis.Client {
	addr, err := sdcfg.GetDbTcpAddr("COUNTERS_DB", namespace)
	if err != nil {
		t.Fatalf("failed to get addr %v", err)
	}
	dbId, err := sdcfg.GetDbId("COUNTERS_DB", namespace)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	rclient := db.TransactionalRedisClientWithOpts(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "", // no password set
		DB:          dbId,
		DialTimeout: 0,
	})
	_, err = rclient.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("failed to connect to redis server %v", err)
	}
	return rclient
}

func getConfigDbClient(t *testing.T, namespace string) *redis.Client {
	addr, err := sdcfg.GetDbTcpAddr("CONFIG_DB", namespace)
	if err != nil {
		t.Fatalf("failed to get addr %v", err)
	}
	dbId, err := sdcfg.GetDbId("CONFIG_DB", namespace)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	rclient := db.TransactionalRedisClientWithOpts(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "", // no password set
		DB:          dbId,
		DialTimeout: 0,
	})
	_, err = rclient.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("failed to connect to redis server %v", err)
	}
	return rclient
}

func loadConfigDB(t *testing.T, rclient *redis.Client, mpi map[string]interface{}) {
	for key, fv := range mpi {
		switch fv.(type) {
		case map[string]interface{}:
			_, err := rclient.HMSet(context.Background(), key, fv.(map[string]interface{})).Result()
			if err != nil {
				t.Errorf("Invalid data for db: %v : %v %v", key, fv, err)
			}
		default:
			t.Errorf("Invalid data for db: %v : %v", key, fv)
		}
	}
}

func initFullConfigDb(t *testing.T, namespace string) {
	rclient := getConfigDbClient(t, namespace)
	defer db.CloseRedisClient(rclient)
	rclient.FlushDB(context.Background())

	fileName := "../testdata/CONFIG_DHCP_SERVER.txt"
	config, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	config_map := loadConfig(t, "", config)
	loadConfigDB(t, rclient, config_map)
}

func initFullCountersDb(t *testing.T, namespace string) {
	rclient := getRedisClient(t, namespace)
	defer db.CloseRedisClient(rclient)
	rclient.FlushDB(context.Background())

	fileName := "../testdata/COUNTERS_PORT_NAME_MAP.txt"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_name_map := loadConfig(t, "COUNTERS_PORT_NAME_MAP", countersPortNameMapByte)
	loadDB(t, rclient, mpi_name_map)

	fileName = "../testdata/COUNTERS_QUEUE_NAME_MAP.txt"
	countersQueueNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_qname_map := loadConfig(t, "COUNTERS_QUEUE_NAME_MAP", countersQueueNameMapByte)
	loadDB(t, rclient, mpi_qname_map)

	fileName = "../testdata/COUNTERS_Ethernet68.txt"
	countersEthernet68Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	// "Ethernet68": "oid:0x1000000000039",
	mpi_counter := loadConfig(t, "COUNTERS:oid:0x1000000000039", countersEthernet68Byte)
	loadDB(t, rclient, mpi_counter)

	fileName = "../testdata/COUNTERS_Ethernet1.txt"
	countersEthernet1Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	// "Ethernet1": "oid:0x1000000000003",
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1000000000003", countersEthernet1Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet64:0": "oid:0x1500000000092a"  : queue counter, to work as data noise
	fileName = "../testdata/COUNTERS_oid_0x1500000000092a.txt"
	counters92aByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000092a", counters92aByte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet68:1": "oid:0x1500000000091c"  : queue counter, for COUNTERS/Ethernet68/Queue vpath test
	fileName = "../testdata/COUNTERS_oid_0x1500000000091c.txt"
	countersEeth68_1Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000091c", countersEeth68_1Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet68:3": "oid:0x1500000000091e"  : lossless queue counter, for COUNTERS/Ethernet68/Pfcwd vpath test
	fileName = "../testdata/COUNTERS_oid_0x1500000000091e.txt"
	countersEeth68_3Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000091e", countersEeth68_3Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet68:4": "oid:0x1500000000091f"  : lossless queue counter, for COUNTERS/Ethernet68/Pfcwd vpath test
	fileName = "../testdata/COUNTERS_oid_0x1500000000091f.txt"
	countersEeth68_4Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000091f", countersEeth68_4Byte)
	loadDB(t, rclient, mpi_counter)
}

func prepareConfigDb(t *testing.T, namespace string) {
	rclient := getConfigDbClient(t, namespace)
	defer db.CloseRedisClient(rclient)
	rclient.FlushDB(context.Background())

	fileName := "../testdata/COUNTERS_PORT_ALIAS_MAP.txt"
	countersPortAliasMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_alias_map := loadConfig(t, "", countersPortAliasMapByte)
	loadConfigDB(t, rclient, mpi_alias_map)

	fileName = "../testdata/CONFIG_PFCWD_PORTS.txt"
	configPfcwdByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_pfcwd_map := loadConfig(t, "", configPfcwdByte)
	loadConfigDB(t, rclient, mpi_pfcwd_map)

	fileName = "../testdata/CONFIG_TELEMETRY_LOG_LEVEL.txt"
	configLogLevelByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_logLvl_map := loadConfig(t, "", configLogLevelByte)
	loadConfigDB(t, rclient, mpi_logLvl_map)
}
func prepareStateDb(t *testing.T, namespace string) {
	rclient := getRedisClientN(t, 6, namespace)
	defer db.CloseRedisClient(rclient)
	rclient.FlushDB(context.Background())
	rclient.HSet(context.Background(), "SWITCH_CAPABILITY|switch", "test_field", "test_value")
	fileName := "../testdata/NEIGH_STATE_TABLE.txt"
	neighStateTableByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_neigh := loadConfig(t, "", neighStateTableByte)
	loadDB(t, rclient, mpi_neigh)

}

func prepareDb(t *testing.T, namespace string) {
	rclient := getRedisClient(t, namespace)
	defer db.CloseRedisClient(rclient)
	rclient.FlushDB(context.Background())
	//Enable keysapce notification
	os.Setenv("PATH", "/usr/bin:/sbin:/bin:/usr/local/bin")
	cmd := exec.Command("redis-cli", "config", "set", "notify-keyspace-events", "KEA")
	_, err := cmd.Output()
	if err != nil {
		t.Fatal("failed to enable redis keyspace notification ", err)
	}

	fileName := "../testdata/COUNTERS_PORT_NAME_MAP.txt"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_name_map := loadConfig(t, "COUNTERS_PORT_NAME_MAP", countersPortNameMapByte)
	loadDB(t, rclient, mpi_name_map)

	fileName = "../testdata/COUNTERS_QUEUE_NAME_MAP.txt"
	countersQueueNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_qname_map := loadConfig(t, "COUNTERS_QUEUE_NAME_MAP", countersQueueNameMapByte)
	loadDB(t, rclient, mpi_qname_map)

	fileName = "../testdata/COUNTERS_Ethernet68.txt"
	countersEthernet68Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	// "Ethernet68": "oid:0x1000000000039",
	mpi_counter := loadConfig(t, "COUNTERS:oid:0x1000000000039", countersEthernet68Byte)
	loadDB(t, rclient, mpi_counter)

	fileName = "../testdata/COUNTERS_Ethernet1.txt"
	countersEthernet1Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	// "Ethernet1": "oid:0x1000000000003",
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1000000000003", countersEthernet1Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet64:0": "oid:0x1500000000092a"  : queue counter, to work as data noise
	fileName = "../testdata/COUNTERS_oid_0x1500000000092a.txt"
	counters92aByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000092a", counters92aByte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet68:1": "oid:0x1500000000091c"  : queue counter, for COUNTERS/Ethernet68/Queue vpath test
	fileName = "../testdata/COUNTERS_oid_0x1500000000091c.txt"
	countersEeth68_1Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000091c", countersEeth68_1Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet68:3": "oid:0x1500000000091e"  : lossless queue counter, for COUNTERS/Ethernet68/Pfcwd vpath test
	fileName = "../testdata/COUNTERS_oid_0x1500000000091e.txt"
	countersEeth68_3Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000091e", countersEeth68_3Byte)
	loadDB(t, rclient, mpi_counter)

	// "Ethernet68:4": "oid:0x1500000000091f"  : lossless queue counter, for COUNTERS/Ethernet68/Pfcwd vpath test
	fileName = "../testdata/COUNTERS_oid_0x1500000000091f.txt"
	countersEeth68_4Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	mpi_counter = loadConfig(t, "COUNTERS:oid:0x1500000000091f", countersEeth68_4Byte)
	loadDB(t, rclient, mpi_counter)

	// Load CONFIG_DB for alias translation
	prepareConfigDb(t, namespace)

	//Load STATE_DB to test non V2R dataset
	prepareStateDb(t, namespace)
}

func prepareDbTranslib(t *testing.T) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	rclient := getRedisClient(t, ns)
	rclient.FlushDB(context.Background())
	db.CloseRedisClient(rclient)

	//Enable keysapce notification
	os.Setenv("PATH", "/usr/bin:/sbin:/bin:/usr/local/bin")
	cmd := exec.Command("redis-cli", "config", "set", "notify-keyspace-events", "KEA")
	_, err := cmd.Output()
	if err != nil {
		t.Fatal("failed to enable redis keyspace notification ", err)
	}

	fileName := "../testdata/db_dump.json"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	var rj []map[string]interface{}
	json.Unmarshal(countersPortNameMapByte, &rj)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	for n, v := range rj {
		if n == 7 {
			// The data at the 7th index is the ApplStateDb data.
			n = 14
		}
		rclient := getRedisClientN(t, n, ns)
		loadDBNotStrict(t, rclient, v)
		db.CloseRedisClient(rclient)
	}
}

// subscriptionQuery represent the input to create an gnmi.Subscription instance.
type subscriptionQuery struct {
	Query          []string
	SubMode        pb.SubscriptionMode
	SampleInterval uint64
}

func pathToString(q client.Path) string {
	qq := make(client.Path, len(q))
	copy(qq, q)
	// Escape all slashes within a path element. ygot.StringToPath will handle
	// these escapes.
	for i, e := range qq {
		qq[i] = strings.Replace(e, "/", "\\/", -1)
	}
	return strings.Join(qq, "/")
}

// createQuery creates a client.Query with the given args. It assigns query.SubReq.
func createQuery(subListMode pb.SubscriptionList_Mode, target string, queries []subscriptionQuery, updatesOnly bool) (*client.Query, error) {
	s := &pb.SubscribeRequest_Subscribe{
		Subscribe: &pb.SubscriptionList{
			Mode:   subListMode,
			Prefix: &pb.Path{Target: target},
		},
	}
	if updatesOnly {
		s.Subscribe.UpdatesOnly = true
	}

	for _, qq := range queries {
		pp, err := ygot.StringToPath(pathToString(qq.Query), ygot.StructuredPath, ygot.StringSlicePath)
		if err != nil {
			return nil, fmt.Errorf("invalid query path %q: %v", qq, err)
		}
		s.Subscribe.Subscription = append(
			s.Subscribe.Subscription,
			&pb.Subscription{
				Path:           pp,
				Mode:           qq.SubMode,
				SampleInterval: qq.SampleInterval,
			})
	}

	subReq := &pb.SubscribeRequest{Request: s}
	query, err := client.NewQuery(subReq)
	query.TLS = &tls.Config{InsecureSkipVerify: true}
	return &query, err
}

// createQueryOrFail creates a query, in case of a failure it fails the test.
func createQueryOrFail(t *testing.T, subListMode pb.SubscriptionList_Mode, target string, queries []subscriptionQuery, updatesOnly bool) client.Query {
	q, err := createQuery(subListMode, target, queries, updatesOnly)
	if err != nil {
		t.Fatalf("failed to create query: %v", err)
	}

	return *q
}

// create query for subscribing to events.
func createEventsQuery(t *testing.T, paths ...string) client.Query {
	return createQueryOrFail(t,
		pb.SubscriptionList_STREAM,
		"EVENTS",
		[]subscriptionQuery{
			{
				Query:   paths,
				SubMode: pb.SubscriptionMode_ON_CHANGE,
			},
		},
		false)
}

func createStateDbQueryOnChangeMode(t *testing.T, paths ...string) client.Query {
	return createQueryOrFail(t,
		pb.SubscriptionList_STREAM,
		"STATE_DB",
		[]subscriptionQuery{
			{
				Query:   paths,
				SubMode: pb.SubscriptionMode_ON_CHANGE,
			},
		},
		false)
}

// createCountersDbQueryOnChangeMode creates a query with ON_CHANGE mode.
func createCountersDbQueryOnChangeMode(t *testing.T, paths ...string) client.Query {
	return createQueryOrFail(t,
		pb.SubscriptionList_STREAM,
		"COUNTERS_DB",
		[]subscriptionQuery{
			{
				Query:   paths,
				SubMode: pb.SubscriptionMode_ON_CHANGE,
			},
		},
		false)
}

// createCountersDbQuerySampleMode creates a query with SAMPLE mode.
func createCountersDbQuerySampleMode(t *testing.T, interval time.Duration, updateOnly bool, paths ...string) client.Query {
	return createQueryOrFail(t,
		pb.SubscriptionList_STREAM,
		"COUNTERS_DB",
		[]subscriptionQuery{
			{
				Query:          paths,
				SubMode:        pb.SubscriptionMode_SAMPLE,
				SampleInterval: uint64(interval.Nanoseconds()),
			},
		},
		updateOnly)
}

// createCountersTableSetUpdate creates a HSET request on the COUNTERS table.
func createCountersTableSetUpdate(tableKey string, fieldName string, fieldValue string) tablePathValue {
	return tablePathValue{
		dbName:    "COUNTERS_DB",
		tableName: "COUNTERS",
		tableKey:  tableKey,
		delimitor: ":",
		field:     fieldName,
		value:     fieldValue,
	}
}

// createCountersTableDeleteUpdate creates a DEL request on the COUNTERS table.
func createCountersTableDeleteUpdate(tableKey string, fieldName string) tablePathValue {
	return tablePathValue{
		dbName:    "COUNTERS_DB",
		tableName: "COUNTERS",
		tableKey:  tableKey,
		delimitor: ":",
		field:     fieldName,
		value:     "",
		op:        "hdel",
	}
}

// createIntervalTickerUpdate creates a request for triggering the interval clock.
func createIntervalTickerUpdate() tablePathValue {
	return tablePathValue{
		op: "intervaltick",
	}
}

// cloneObject clones a given object via JSON serialize/deserialize
func cloneObject(obj interface{}) interface{} {
	objData, err := json.Marshal(obj)
	if err != nil {
		panic(fmt.Errorf("marshal failed, %v", err))
	}

	var cloneObj interface{}
	err = json.Unmarshal(objData, &cloneObj)
	if err != nil {
		panic(fmt.Errorf("unmarshal failed, %v", err))
	}

	return cloneObj
}

// mergeStrMaps merges given maps where they are keyed with string.
func mergeStrMaps(sourceOrigin interface{}, updateOrigin interface{}) interface{} {
	// Clone the maps so that the originals are not changed during the merge.
	source := cloneObject(sourceOrigin)
	update := cloneObject(updateOrigin)

	// Check if both are string keyed maps
	sourceStrMap, okSrcMap := source.(map[string]interface{})
	updateStrMap, okUpdateMap := update.(map[string]interface{})
	if okSrcMap && okUpdateMap {
		for itemKey, updateItem := range updateStrMap {
			sourceItem, sourceItemOk := sourceStrMap[itemKey]
			if sourceItemOk {
				sourceStrMap[itemKey] = updateItem
			} else {
				sourceStrMap[itemKey] = mergeStrMaps(sourceItem, updateItem)
			}
		}
		return sourceStrMap
	}

	return update
}

func TestPanic(t *testing.T) {
	// Check if a stacktrace file is already present. None should.
	files, err := os.ReadDir(HostVarLogPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), StackTraceFileNamePrefix) && strings.HasSuffix(file.Name(), StackTraceFileNameSuffix) {
			t.Fatalf("Unexpected stack-trace file found: %v", file.Name())
		}
	}
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a subscription is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}
	// The line below makes the server to try an access through a `nil`.
	// This will not happen normally - here it is used to trigger a `panic()`.
	s.clients = nil
	pbPath, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/5]")
	// There is no point in checking the status returned by `stream.Send` as
	// during the processing of this request the server crashes.
	_ = stream.Send(&pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:     pb.SubscriptionList_STREAM,
				Encoding: pb.Encoding_PROTO,
				Subscription: []*pb.Subscription{
					{
						Path:           pbPath,
						Mode:           pb.SubscriptionMode_SAMPLE,
						SampleInterval: 10,
					},
				},
			},
		},
		Extension: []*ext_pb.Extension{
			{
				Ext: &ext_pb.Extension_MasterArbitration{
					MasterArbitration: &ext_pb.MasterArbitration{
						ElectionId: &ext_pb.Uint128{High: 1, Low: 1},
					},
				},
			},
		},
	})
	// Check if the connection has been closed by the server.
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("Expected an error but got nothing.")
	}
	// Check if the stacktrace file has been saved.
	files, err = os.ReadDir(HostVarLogPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), StackTraceFileNamePrefix) && strings.HasSuffix(file.Name(), StackTraceFileNameSuffix) {
			if err = os.Remove(fmt.Sprintf("%s/%s", HostVarLogPath, file.Name())); err != nil {
				t.Errorf("Cannot delete stack-trace file: %v", err)
			}
			return
		}
	}
	t.Errorf("Could not find `%v.*%v` file in `%v`.", StackTraceFileNamePrefix, StackTraceFileNameSuffix, HostVarLogPath)
}

func TestGnmiSet(t *testing.T) {
	if !ENABLE_TRANSLIB_WRITE {
		t.Skip("skipping test in read-only mode.")
	}
	s := createServer(t)
	go runServer(t, s)

	prepareDbTranslib(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tds := []struct {
		desc          string
		pathTarget    string
		textPbPath    string
		wantRetCode   codes.Code
		wantRespVal   interface{}
		attributeData string
		operation     op_t
		valTest       bool
	}{
		{
			desc:        "Invalid path",
			pathTarget:  "OC_YANG",
			textPbPath:  pathToPb("/openconfig-interfaces:interfaces/interface[name=Ethernet4]/unknown"),
			wantRetCode: codes.Aborted,
			operation:   Delete,
		},
		{
			desc:          "Set OC Interface MTU",
			pathTarget:    "OC_YANG",
			textPbPath:    pathToPb("openconfig-interfaces:interfaces/interface[name=Ethernet4]/config"),
			attributeData: "../testdata/set_interface_mtu.json",
			wantRetCode:   codes.OK,
			operation:     Update,
		},
		{
			desc:          "Set OC Interface IP",
			pathTarget:    "OC_YANG",
			textPbPath:    pathToPb("/openconfig-interfaces:interfaces/interface[name=Ethernet4]/subinterfaces/subinterface[index=0]/openconfig-if-ip:ipv4"),
			attributeData: "../testdata/set_interface_ipv4.json",
			wantRetCode:   codes.OK,
			operation:     Update,
		},
		// {
		//         desc:       "Check OC Interface values set",
		//         pathTarget: "OC_YANG",
		//         textPbPath: `
		//                 elem: <name: "openconfig-interfaces:interfaces" > elem: <name: "interface" key:<key:"name" value:"Ethernet4" > >
		//         `,
		//         wantRetCode: codes.OK,
		//         wantRespVal: interfaceData,
		//         valTest:true,
		// },
		{
			desc:       "Delete OC Interface IP",
			pathTarget: "OC_YANG",
			textPbPath: `
                    elem:<name:"openconfig-interfaces:interfaces" > elem:<name:"interface" key:<key:"name" value:"Ethernet4" > > elem:<name:"subinterfaces" > elem:<name:"subinterface" key:<key:"index" value:"0" > > elem:<name: "ipv4" > elem:<name: "addresses" > elem:<name:"address" key:<key:"ip" value:"9.9.9.9" > >
                `,
			attributeData: "",
			wantRetCode:   codes.OK,
			operation:     Delete,
			valTest:       false,
		},
		{
			desc:          "Set OC Interface IPv6 (unprefixed path)",
			pathTarget:    "OC_YANG",
			textPbPath:    pathToPb("/interfaces/interface[name=Ethernet0]/subinterfaces/subinterface[index=0]/ipv6/addresses/address"),
			attributeData: `{"address": [{"ip": "150::1","config": {"ip": "150::1","prefix-length": 80}}]}`,
			wantRetCode:   codes.OK,
			operation:     Update,
		},
		{
			desc:        "Delete OC Interface IPv6 (unprefixed path)",
			pathTarget:  "OC_YANG",
			textPbPath:  pathToPb("/interfaces/interface[name=Ethernet0]/subinterfaces/subinterface[index=0]/ipv6/addresses/address[ip=150::1]"),
			wantRetCode: codes.OK,
			operation:   Delete,
		},
		{
			desc:       "Create ACL (unprefixed path)",
			pathTarget: "OC_YANG",
			textPbPath: pathToPb("/acl/acl-sets/acl-set"),
			attributeData: `{"acl-set": [{"name": "A001", "type": "ACL_IPV4",
							"config": {"name": "A001", "type": "ACL_IPV4", "description": "hello, world!"}}]}`,
			wantRetCode: codes.OK,
			operation:   Update,
		},
		{
			desc:        "Verify Create ACL",
			pathTarget:  "OC_YANG",
			textPbPath:  pathToPb("/openconfig-acl:acl/acl-sets/acl-set[name=A001][type=ACL_IPV4]/config/description"),
			wantRespVal: `{"openconfig-acl:description": "hello, world!"}`,
			wantRetCode: codes.OK,
			valTest:     true,
		},
		{
			desc:          "Replace ACL Description (unprefixed path)",
			pathTarget:    "OC_YANG",
			textPbPath:    pathToPb("/acl/acl-sets/acl-set[name=A001][type=ACL_IPV4]/config/description"),
			attributeData: `{"description": "dummy"}`,
			wantRetCode:   codes.OK,
			operation:     Replace,
		},
		{
			desc:        "Verify Replace ACL Description",
			pathTarget:  "OC_YANG",
			textPbPath:  pathToPb("/openconfig-acl:acl/acl-sets/acl-set[name=A001][type=ACL_IPV4]/config/description"),
			wantRespVal: `{"openconfig-acl:description": "dummy"}`,
			wantRetCode: codes.OK,
			valTest:     true,
		},
		{
			desc:        "Delete ACL",
			pathTarget:  "OC_YANG",
			textPbPath:  pathToPb("/openconfig-acl:acl/acl-sets/acl-set[name=A001][type=ACL_IPV4]"),
			wantRetCode: codes.OK,
			operation:   Delete,
		},
		{
			desc:        "Verify Delete ACL",
			pathTarget:  "OC_YANG",
			textPbPath:  pathToPb("/openconfig-acl:acl/acl-sets/acl-set[name=A001][type=ACL_IPV4]"),
			wantRetCode: codes.NotFound,
			valTest:     true,
		},
	}

	for _, td := range tds {
		if td.valTest == true {
			t.Run(td.desc, func(t *testing.T) {
				runTestGet(t, ctx, gClient, td.pathTarget, td.textPbPath, pb.GetRequest_ALL, pb.Encoding_JSON_IETF, td.wantRetCode, td.wantRespVal, td.valTest)
			})
			t.Run(td.desc+" (unprefixed path)", func(t *testing.T) {
				p := removeModulePrefixFromPathPb(t, td.textPbPath)
				runTestGet(t, ctx, gClient, td.pathTarget, p, pb.GetRequest_ALL, pb.Encoding_JSON_IETF, td.wantRetCode, td.wantRespVal, td.valTest)
			})
		} else {
			t.Run(td.desc, func(t *testing.T) {
				runTestSet(t, ctx, gClient, td.pathTarget, td.textPbPath, td.wantRetCode, td.wantRespVal, td.attributeData, td.operation)
			})
		}
	}
	s.Stop()
}

func TestGnmiSetReadOnly(t *testing.T) {
	s := createCustomServer(t, testServerConfig(readOnlySrvType))
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.SetRequest{}
	_, err = gClient.Set(ctx, req)
	gotRetStatus, ok := status.FromError(err)
	if !ok {
		t.Fatal("got a non-grpc error from grpc call")
	}
	wantRetCode := codes.Unimplemented
	if gotRetStatus.Code() != wantRetCode {
		t.Log("err: ", err)
		t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), wantRetCode)
	}
}

func TestGnmiSetAuthFail(t *testing.T) {
	s := createCustomServer(t, testServerConfig(authSrvType))
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.SetRequest{}
	_, err = gClient.Set(ctx, req)
	gotRetStatus, ok := status.FromError(err)
	if !ok {
		t.Fatal("got a non-grpc error from grpc call")
	}
	wantRetCode := codes.Unauthenticated
	if gotRetStatus.Code() != wantRetCode {
		t.Log("err: ", err)
		t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), wantRetCode)
	}
}

func TestGnmiGetAuthFail(t *testing.T) {
	s := createCustomServer(t, testServerConfig(authSrvType))
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.GetRequest{}
	_, err = gClient.Get(ctx, req)
	gotRetStatus, ok := status.FromError(err)
	if !ok {
		t.Fatal("got a non-grpc error from grpc call")
	}
	wantRetCode := codes.Unauthenticated
	if gotRetStatus.Code() != wantRetCode {
		t.Log("err: ", err)
		t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), wantRetCode)
	}
}

func runGnmiTestGet(t *testing.T, port int64, namespace string) {
	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileName := "../testdata/COUNTERS_PORT_NAME_MAP.txt"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	fileName = "../testdata/COUNTERS_Ethernet68.txt"
	countersEthernet68Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	fileName = "../testdata/COUNTERS_Ethernet68_Pfcwd.txt"
	countersEthernet68PfcwdByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	fileName = "../testdata/COUNTERS_Ethernet68_Pfcwd_alias.txt"
	countersEthernet68PfcwdAliasByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	fileName = "../testdata/COUNTERS_Ethernet_wildcard_alias.txt"
	countersEthernetWildcardByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	fileName = "../testdata/COUNTERS_Ethernet_wildcard_PFC_7_RX_alias.txt"
	countersEthernetWildcardPfcByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	fileName = "../testdata/COUNTERS_Ethernet_wildcard_Pfcwd_alias.txt"
	countersEthernetWildcardPfcwdByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	stateDBPath := "STATE_DB"

	ns, _ := sdcfg.GetDbDefaultNamespace()
	if namespace != ns {
		stateDBPath = "STATE_DB" + "/" + namespace
	}

	type testCase struct {
		desc        string
		pathTarget  string
		textPbPath  string
		wantRetCode codes.Code
		wantRespVal interface{}
		valTest     bool
		testInit    func()
	}

	// A helper function create test cases for 'osversion/build' queries.
	createBuildVersionTestCase := func(desc string, wantedVersion string, versionFileContent string, fileReadErr error) testCase {
		return testCase{
			desc:       desc,
			pathTarget: "OTHERS",
			textPbPath: `
						elem: <name: "osversion" >
						elem: <name: "build" >
					`,
			wantRetCode: codes.OK,
			valTest:     true,
			wantRespVal: []byte(wantedVersion),
			testInit: func() {
				// Override file read function to mock file content.
				sdc.ImplIoutilReadFile = func(filePath string) ([]byte, error) {
					if filePath == sdc.SonicVersionFilePath {
						if fileReadErr != nil {
							return nil, fileReadErr
						}
						return []byte(versionFileContent), nil
					}
					return ioutil.ReadFile(filePath)
				}

				// Reset the cache so that the content gets loaded again.
				sdc.InvalidateVersionFileStash()
			},
		}
	}

	tds := []testCase{{
		desc:       "Test non-existing path Target",
		pathTarget: "MY_DB",
		textPbPath: `
			elem: <name: "MyCounters" >
		`,
		wantRetCode: codes.NotFound,
	}, {
		desc:       "Test passing asic in path for V2R Dataset Target",
		pathTarget: "COUNTER_DB" + "/" + namespace,
		textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68" >
				`,
		wantRetCode: codes.NotFound,
	},
		{
			desc:       "Get valid but non-existing node",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
			elem: <name: "MyCounters" >
		`,
			wantRetCode: codes.NotFound,
		}, {
			desc:       "Get COUNTERS_PORT_NAME_MAP",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
			elem: <name: "COUNTERS_PORT_NAME_MAP" >
		`,
			wantRetCode: codes.OK,
			wantRespVal: countersPortNameMapByte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS:Ethernet68",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernet68Byte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS:Ethernet68 SAI_PORT_STAT_PFC_7_RX_PKTS",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68" >
					elem: <name: "SAI_PORT_STAT_PFC_7_RX_PKTS" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: "2",
			valTest:     true,
		}, {
			desc:       "get COUNTERS:Ethernet68 Pfcwd",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68" >
					elem: <name: "Pfcwd" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernet68PfcwdByte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS (use vendor alias):Ethernet68/1",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68/1" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernet68Byte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS (use vendor alias):Ethernet68/1 SAI_PORT_STAT_PFC_7_RX_PKTS",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68/1" >
					elem: <name: "SAI_PORT_STAT_PFC_7_RX_PKTS" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: "2",
			valTest:     true,
		}, {
			desc:       "get COUNTERS (use vendor alias):Ethernet68/1 Pfcwd",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet68/1" >
					elem: <name: "Pfcwd" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernet68PfcwdAliasByte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS:Ethernet*",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet*" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernetWildcardByte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS:Ethernet* SAI_PORT_STAT_PFC_7_RX_PKTS",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet*" >
					elem: <name: "SAI_PORT_STAT_PFC_7_RX_PKTS" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernetWildcardPfcByte,
			valTest:     true,
		}, {
			desc:       "get COUNTERS:Ethernet* Pfcwd",
			pathTarget: "COUNTERS_DB",
			textPbPath: `
					elem: <name: "COUNTERS" >
					elem: <name: "Ethernet*" >
					elem: <name: "Pfcwd" >
				`,
			wantRetCode: codes.OK,
			wantRespVal: countersEthernetWildcardPfcwdByte,
			valTest:     true,
		}, {
			desc:       "get State DB Data for SWITCH_CAPABILITY switch",
			pathTarget: stateDBPath,
			textPbPath: `
					elem: <name: "SWITCH_CAPABILITY" >
					elem: <name: "switch" >
				`,
			valTest:     true,
			wantRetCode: codes.OK,
			wantRespVal: []byte(`{"test_field": "test_value"}`),
		}, {
			desc:        "Invalid DBKey of length 1",
			pathTarget:  stateDBPath,
			textPbPath:  ``,
			valTest:     true,
			wantRetCode: codes.NotFound,
		},

		// Happy path
		createBuildVersionTestCase(
			"get osversion/build",                                  // query path
			`{"build_version": "SONiC.12345678.90", "error":""}`,   // expected response
			"build_version: '12345678.90'\ndebian_version: '9.13'", // YAML file content
			nil), // mock file reading error

		// File reading error
		createBuildVersionTestCase(
			"get osversion/build file load error",
			`{"build_version": "sonic.NA", "error":"Cannot access '/etc/sonic/sonic_version.yml'"}`,
			"",
			fmt.Errorf("Cannot access '%v'", sdc.SonicVersionFilePath)),

		// File content is not valid YAML
		createBuildVersionTestCase(
			"get osversion/build file parse error",
			`{"build_version": "sonic.NA", "error":"yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `+"`not a v...`"+` into client.SonicVersionInfo"}`,
			"not a valid YAML content",
			nil),

		// Happy path with different value
		createBuildVersionTestCase(
			"get osversion/build different value",
			`{"build_version": "SONiC.23456789.01", "error":""}`,
			"build_version: '23456789.01'\ndebian_version: '9.15'",
			nil),
	}

	for _, td := range tds {
		if td.testInit != nil {
			td.testInit()
		}

		t.Run(td.desc, func(t *testing.T) {
			runTestGet(t, ctx, gClient, td.pathTarget, td.textPbPath, pb.GetRequest_ALL, pb.Encoding_JSON_IETF, td.wantRetCode, td.wantRespVal, td.valTest)
		})
	}

}

func TestGnmiGet(t *testing.T) {
	//t.Log("Start server")
	s := createServer(t)
	go runServer(t, s)

	ns, _ := sdcfg.GetDbDefaultNamespace()
	prepareDb(t, ns)

	runGnmiTestGet(t, s.config.Port, ns)

	s.Stop()
}
func TestGnmiGetMultiNs(t *testing.T) {
	sdcfg.Init()
	err := test_utils.SetupMultiNamespace()
	if err != nil {
		t.Fatalf("error Setting up MultiNamespace files with err %T", err)
	}

	/* https://www.gopherguides.com/articles/test-cleanup-in-go-1-14*/
	t.Cleanup(func() {
		if err := test_utils.CleanUpMultiNamespace(); err != nil {
			t.Fatalf("error Cleaning up MultiNamespace files with err %T", err)

		}
	})

	//t.Log("Start server")
	s := createServer(t)
	go runServer(t, s)

	prepareDb(t, test_utils.GetMultiNsNamespace())

	runGnmiTestGet(t, s.config.Port, test_utils.GetMultiNsNamespace())

	s.Stop()
}

func TestGnmiGetTranslib(t *testing.T) {
	//t.Log("Start server")
	s := createServer(t)
	go runServer(t, s)

	prepareDbTranslib(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var emptyRespVal interface{}
	tds := []struct {
		desc        string
		pathTarget  string
		textPbPath  string
		wantRetCode codes.Code
		wantRespVal interface{}
		valTest     bool
	}{

		//These tests only work on the real switch platform, since they rely on files in the /proc and another running service
		// 	{
		// 	desc:       "Get OC Platform",
		// 	pathTarget: "OC_YANG",
		// 	textPbPath: `
		//                        elem: <name: "openconfig-platform:components" >
		//                `,
		// 	wantRetCode: codes.OK,
		// 	wantRespVal: emptyRespVal,
		// 	valTest:     false,
		// },
		// 	{
		// 		desc:       "Get OC System State",
		// 		pathTarget: "OC_YANG",
		// 		textPbPath: `
		//                        elem: <name: "openconfig-system:system" > elem: <name: "state" >
		//                `,
		// 		wantRetCode: codes.OK,
		// 		wantRespVal: emptyRespVal,
		// 		valTest:     false,
		// 	},
		// 	{
		// 		desc:       "Get OC System CPU",
		// 		pathTarget: "OC_YANG",
		// 		textPbPath: `
		//                        elem: <name: "openconfig-system:system" > elem: <name: "cpus" >
		//                `,
		// 		wantRetCode: codes.OK,
		// 		wantRespVal: emptyRespVal,
		// 		valTest:     false,
		// 	},
		// 	{
		// 		desc:       "Get OC System memory",
		// 		pathTarget: "OC_YANG",
		// 		textPbPath: `
		//                        elem: <name: "openconfig-system:system" > elem: <name: "memory" >
		//                `,
		// 		wantRetCode: codes.OK,
		// 		wantRespVal: emptyRespVal,
		// 		valTest:     false,
		// 	},
		// 	{
		// 		desc:       "Get OC System processes",
		// 		pathTarget: "OC_YANG",
		// 		textPbPath: `
		//                        elem: <name: "openconfig-system:system" > elem: <name: "processes" >
		//                `,
		// 		wantRetCode: codes.OK,
		// 		wantRespVal: emptyRespVal,
		// 		valTest:     false,
		// 	},
		{
			desc:       "Get OC Interfaces",
			pathTarget: "OC_YANG",
			textPbPath: `
                        elem: <name: "openconfig-interfaces:interfaces" >
                `,
			wantRetCode: codes.OK,
			wantRespVal: emptyRespVal,
			valTest:     false,
		},
		{
			desc:       "Get OC Interface",
			pathTarget: "OC_YANG",
			textPbPath: `
                        elem: <name: "openconfig-interfaces:interfaces" > elem: <name: "interface" key:<key:"name" value:"Ethernet4" > >
                `,
			wantRetCode: codes.OK,
			wantRespVal: emptyRespVal,
			valTest:     false,
		},
		{
			desc:       "Get OC Interface admin-status",
			pathTarget: "OC_YANG",
			textPbPath: `
                        elem: <name: "openconfig-interfaces:interfaces" > elem: <name: "interface" key:<key:"name" value:"Ethernet4" > > elem: <name: "state" > elem: <name: "admin-status" >
                `,
			wantRetCode: codes.OK,
			wantRespVal: emptyRespVal,
			valTest:     false,
		},
		{
			desc:       "Get OC Interface ifindex",
			pathTarget: "OC_YANG",
			textPbPath: `
                        elem: <name: "openconfig-interfaces:interfaces" > elem: <name: "interface" key:<key:"name" value:"Ethernet4" > > elem: <name: "state" > elem: <name: "ifindex" >
                `,
			wantRetCode: codes.OK,
			wantRespVal: emptyRespVal,
			valTest:     false,
		},
		{
			desc:       "Get OC Interface mtu",
			pathTarget: "OC_YANG",
			textPbPath: `
                        elem: <name: "openconfig-interfaces:interfaces" > elem: <name: "interface" key:<key:"name" value:"Ethernet4" > > elem: <name: "state" > elem: <name: "mtu" >
                `,
			wantRetCode: codes.OK,
			wantRespVal: emptyRespVal,
			valTest:     false,
		},
	}

	for _, td := range tds {
		t.Run(td.desc, func(t *testing.T) {
			runTestGet(t, ctx, gClient, td.pathTarget, td.textPbPath, pb.GetRequest_ALL, pb.Encoding_JSON_IETF, td.wantRetCode, td.wantRespVal, td.valTest)
		})
	}
	s.Stop()
}

type tablePathValue struct {
	dbName    string
	tableName string
	tableKey  string
	delimitor string
	field     string
	value     string
	op        string
}

// runTestSubscribe subscribe DB path in stream mode or poll mode.
// The return code and response value are compared with expected code and value.
func runTestSubscribe(t *testing.T, port int64, namespace string) {
	fileName := "../testdata/COUNTERS_PORT_NAME_MAP.txt"
	countersPortNameMapByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersPortNameMapJson interface{}
	json.Unmarshal(countersPortNameMapByte, &countersPortNameMapJson)
	var tmp interface{}
	json.Unmarshal(countersPortNameMapByte, &tmp)
	countersPortNameMapJsonUpdate := tmp.(map[string]interface{})
	countersPortNameMapJsonUpdate["test_field"] = "test_value"

	// for table key subscription
	fileName = "../testdata/COUNTERS_Ethernet68.txt"
	countersEthernet68Byte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernet68Json interface{}
	json.Unmarshal(countersEthernet68Byte, &countersEthernet68Json)

	var tmp2 interface{}
	json.Unmarshal(countersEthernet68Byte, &tmp2)
	countersEthernet68JsonUpdate := tmp2.(map[string]interface{})
	countersEthernet68JsonUpdate["test_field"] = "test_value"

	var tmp3 interface{}
	json.Unmarshal(countersEthernet68Byte, &tmp3)
	countersEthernet68JsonPfcUpdate := tmp3.(map[string]interface{})
	// field SAI_PORT_STAT_PFC_7_RX_PKTS has new value of 4
	countersEthernet68JsonPfcUpdate["SAI_PORT_STAT_PFC_7_RX_PKTS"] = "4"

	// for Ethernet68/Pfcwd subscription
	fileName = "../testdata/COUNTERS_Ethernet68_Pfcwd.txt"
	countersEthernet68PfcwdByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernet68PfcwdJson interface{}
	json.Unmarshal(countersEthernet68PfcwdByte, &countersEthernet68PfcwdJson)

	var tmp4 interface{}
	json.Unmarshal(countersEthernet68PfcwdByte, &tmp4)
	countersEthernet68PfcwdJsonUpdate := map[string]interface{}{}
	countersEthernet68PfcwdJsonUpdate["Ethernet68:3"] = tmp4.(map[string]interface{})["Ethernet68:3"]
	countersEthernet68PfcwdJsonUpdate["Ethernet68:3"].(map[string]interface{})["PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED"] = "1"

	tmp4.(map[string]interface{})["Ethernet68:3"].(map[string]interface{})["PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED"] = "1"
	countersEthernet68PfcwdPollUpdate := tmp4

	// (use vendor alias) for Ethernet68/1 Pfcwd subscription
	fileName = "../testdata/COUNTERS_Ethernet68_Pfcwd_alias.txt"
	countersEthernet68PfcwdAliasByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernet68PfcwdAliasJson interface{}
	json.Unmarshal(countersEthernet68PfcwdAliasByte, &countersEthernet68PfcwdAliasJson)

	var tmp5 interface{}
	json.Unmarshal(countersEthernet68PfcwdAliasByte, &tmp5)
	countersEthernet68PfcwdAliasJsonUpdate := map[string]interface{}{}
	countersEthernet68PfcwdAliasJsonUpdate["Ethernet68/1:3"] = tmp5.(map[string]interface{})["Ethernet68/1:3"]
	countersEthernet68PfcwdAliasJsonUpdate["Ethernet68/1:3"].(map[string]interface{})["PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED"] = "1"

	tmp5.(map[string]interface{})["Ethernet68/1:3"].(map[string]interface{})["PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED"] = "1"
	countersEthernet68PfcwdAliasPollUpdate := tmp5

	fileName = "../testdata/COUNTERS_Ethernet_wildcard_alias.txt"
	countersEthernetWildcardByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernetWildcardJson interface{}
	json.Unmarshal(countersEthernetWildcardByte, &countersEthernetWildcardJson)
	// Will have "test_field" : "test_value" in Ethernet68,
	countersEtherneWildcardJsonUpdate := map[string]interface{}{"Ethernet68/1": countersEthernet68JsonUpdate}

	// all counters on all ports with change on one field of one port
	var countersFieldUpdate map[string]interface{}
	json.Unmarshal(countersEthernetWildcardByte, &countersFieldUpdate)
	countersFieldUpdate["Ethernet68/1"] = countersEthernet68JsonPfcUpdate

	fileName = "../testdata/COUNTERS_Ethernet_wildcard_PFC_7_RX_alias.txt"
	countersEthernetWildcardPfcByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernetWildcardPfcJson interface{}
	json.Unmarshal(countersEthernetWildcardPfcByte, &countersEthernetWildcardPfcJson)
	//The update with new value of 4 (original value is 2)
	pfc7Map := map[string]interface{}{"SAI_PORT_STAT_PFC_7_RX_PKTS": "4"}
	singlePortPfcJsonUpdate := make(map[string]interface{})
	singlePortPfcJsonUpdate["Ethernet68/1"] = pfc7Map

	allPortPfcJsonUpdate := make(map[string]interface{})
	json.Unmarshal(countersEthernetWildcardPfcByte, &allPortPfcJsonUpdate)
	//allPortPfcJsonUpdate := countersEthernetWildcardPfcJson.(map[string]interface{})
	allPortPfcJsonUpdate["Ethernet68/1"] = pfc7Map

	// for Ethernet*/Pfcwd subscription
	fileName = "../testdata/COUNTERS_Ethernet_wildcard_Pfcwd_alias.txt"
	countersEthernetWildPfcwdByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	var countersEthernetWildPfcwdJson interface{}
	json.Unmarshal(countersEthernetWildPfcwdByte, &countersEthernetWildPfcwdJson)

	var tmp6 interface{}
	json.Unmarshal(countersEthernetWildPfcwdByte, &tmp6)
	tmp6.(map[string]interface{})["Ethernet68/1:3"].(map[string]interface{})["PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED"] = "1"
	countersEthernetWildPfcwdUpdate := tmp6

	fileName = "../testdata/COUNTERS_Ethernet_wildcard_Queues_alias.txt"
	countersEthernetWildQueuesByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernetWildQueuesJson interface{}
	json.Unmarshal(countersEthernetWildQueuesByte, &countersEthernetWildQueuesJson)

	fileName = "../testdata/COUNTERS_Ethernet68_Queues.txt"
	countersEthernet68QueuesByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernet68QueuesJson interface{}
	json.Unmarshal(countersEthernet68QueuesByte, &countersEthernet68QueuesJson)

	countersEthernet68QueuesJsonUpdate := make(map[string]interface{})
	json.Unmarshal(countersEthernet68QueuesByte, &countersEthernet68QueuesJsonUpdate)
	eth68_1 := map[string]interface{}{
		"SAI_QUEUE_STAT_BYTES":           "0",
		"SAI_QUEUE_STAT_DROPPED_BYTES":   "0",
		"SAI_QUEUE_STAT_DROPPED_PACKETS": "4",
		"SAI_QUEUE_STAT_PACKETS":         "0",
	}
	countersEthernet68QueuesJsonUpdate["Ethernet68:1"] = eth68_1

	// Alias translation for query Ethernet68/1:Queues
	fileName = "../testdata/COUNTERS_Ethernet68_Queues_alias.txt"
	countersEthernet68QueuesAliasByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var countersEthernet68QueuesAliasJson interface{}
	json.Unmarshal(countersEthernet68QueuesAliasByte, &countersEthernet68QueuesAliasJson)

	countersEthernet68QueuesAliasJsonUpdate := make(map[string]interface{})
	json.Unmarshal(countersEthernet68QueuesAliasByte, &countersEthernet68QueuesAliasJsonUpdate)
	countersEthernet68QueuesAliasJsonUpdate["Ethernet68/1:1"] = eth68_1

	type TestExec struct {
		desc       string
		q          client.Query
		prepares   []tablePathValue
		updates    []tablePathValue
		wantErr    bool
		wantNoti   []client.Notification
		wantSubErr error

		poll        int
		wantPollErr string

		generateIntervals bool
	}
	tests := []TestExec{
		{
			desc: "stream query for table COUNTERS_PORT_NAME_MAP with new test_field field",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS_PORT_NAME_MAP"),
			updates: []tablePathValue{{
				dbName:    "COUNTERS_DB",
				tableName: "COUNTERS_PORT_NAME_MAP",
				field:     "test_field",
				value:     "test_value",
			}},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJsonUpdate},
			},
		},
		{
			desc: "stream query for table key Ethernet68 with new test_field field",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet68"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "test_field",
					value:     "test_value",
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "test_field",
					value:     "test_value",
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68"}, TS: time.Unix(0, 200), Val: countersEthernet68Json},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68"}, TS: time.Unix(0, 200), Val: countersEthernet68JsonUpdate},
			},
		},
		{
			desc: "(use vendor alias) stream query for table key Ethernet68/1 with new test_field field",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet68/1"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "test_field",
					value:     "test_value",
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "test_field",
					value:     "test_value",
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1"}, TS: time.Unix(0, 200), Val: countersEthernet68Json},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1"}, TS: time.Unix(0, 200), Val: countersEthernet68JsonUpdate},
			},
		},
		{
			desc: "stream query for COUNTERS/Ethernet68/SAI_PORT_STAT_PFC_7_RX_PKTS with update of field value",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "3", // be changed to 3 from 2
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "3", // be changed to 3 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "2"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "3"},
			},
		},
		{
			desc: "(use vendor alias) stream query for COUNTERS/[Ethernet68/1]/SAI_PORT_STAT_PFC_7_RX_PKTS with update of field value",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "3", // be changed to 3 from 2
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "3", // be changed to 3 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "2"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "3"},
			},
		},
		{
			desc: "stream query for COUNTERS/Ethernet68/Pfcwd with update of field value",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet68", "Pfcwd"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e",
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // be changed to 1 from 0
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e"
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // be changed to 1 from 1
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdJsonUpdate},
			},
		},
		{
			desc: "(use vendor alias) stream query for COUNTERS/[Ethernet68/1]/Pfcwd with update of field value",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet68/1", "Pfcwd"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e",
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // be changed to 1 from 0
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e"
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // be changed to 1 from 1
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJsonUpdate},
			},
		},
		{
			desc: "stream query for table key Ethernet* with new test_field field on Ethernet68",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet*"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "test_field",
					value:     "test_value",
				},
				{ //Same value set should not trigger multiple updates
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "test_field",
					value:     "test_value",
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEtherneWildcardJsonUpdate},
			},
		},
		{
			desc: "stream query for table key Ethernet*/SAI_PORT_STAT_PFC_7_RX_PKTS with field value update",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "4", // being changed to 4 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardPfcJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: singlePortPfcJsonUpdate},
			},
		},
		{
			desc: "stream query for table key Ethernet*/Pfcwd with field value update",
			q:    createCountersDbQueryOnChangeMode(t, "COUNTERS", "Ethernet*", "Pfcwd"),
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e",
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // being changed to 1 from 0
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJsonUpdate},
			},
		},
		{
			desc: "poll query for table COUNTERS_PORT_NAME_MAP with new field test_field",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS_PORT_NAME_MAP"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{{
				dbName:    "COUNTERS_DB",
				tableName: "COUNTERS_PORT_NAME_MAP",
				field:     "test_field",
				value:     "test_value",
			}},
			wantNoti: []client.Notification{
				client.Connected{},
				// We are starting from the result data of "stream query for table with update of new field",
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJsonUpdate},
				client.Sync{},
			},
		},
		{
			desc: "poll query for table COUNTERS_PORT_NAME_MAP with test_field delete",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS_PORT_NAME_MAP"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			prepares: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS_PORT_NAME_MAP",
					field:     "test_field",
					value:     "test_value",
				},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS_PORT_NAME_MAP",
					field:     "test_field",
					op:        "hdel",
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				// We are starting from the result data of "stream query for table with update of new field",
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS_PORT_NAME_MAP"}, TS: time.Unix(0, 200), Val: countersPortNameMapJson},
				client.Sync{},
			},
		},
		{
			desc: "poll query for COUNTERS/Ethernet68/SAI_PORT_STAT_PFC_7_RX_PKTS with field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "4", // being changed to 4 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "2"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "4"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "4"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "4"},
				client.Sync{},
			},
		},
		{
			desc: "(use vendor alias) poll query for COUNTERS/[Ethernet68/1]/SAI_PORT_STAT_PFC_7_RX_PKTS with field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "4", // being changed to 4 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "2"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "4"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "4"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "4"},
				client.Sync{},
			},
		},
		{
			desc: "poll query for COUNTERS/Ethernet68/Pfcwd with field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68", "Pfcwd"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e",
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // be changed to 1 from 0
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdPollUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdPollUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdPollUpdate},
				client.Sync{},
			},
		},
		{
			desc: "(use vendor alias) poll query for COUNTERS/[Ethernet68/1]/Pfcwd with field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68/1", "Pfcwd"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e",
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // be changed to 1 from 0
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasPollUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasPollUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasPollUpdate},
				client.Sync{},
			},
		},
		{
			desc: "poll query for table key Ethernet* with Ethernet68/SAI_PORT_STAT_PFC_7_RX_PKTS field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "4", // being changed to 4 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersFieldUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersFieldUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersFieldUpdate},
				client.Sync{},
			},
		},
		{
			desc: "poll query for table key field Ethernet*/SAI_PORT_STAT_PFC_7_RX_PKTS with Ethernet68/SAI_PORT_STAT_PFC_7_RX_PKTS field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1000000000039", // "Ethernet68": "oid:0x1000000000039",
					delimitor: ":",
					field:     "SAI_PORT_STAT_PFC_7_RX_PKTS",
					value:     "4", // being changed to 4 from 2
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardPfcJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: allPortPfcJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: allPortPfcJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: allPortPfcJsonUpdate},
				client.Sync{},
			},
		},
		{
			desc: "poll query for table key field Etherenet*/Pfcwd with Ethernet68:3/PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*", "Pfcwd"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091e", // "Ethernet68:3": "oid:0x1500000000091e",
					delimitor: ":",
					field:     "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED",
					value:     "1", // being changed to 1 from 0
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdUpdate},
				client.Sync{},
			},
		},
		{
			desc: "poll query for COUNTERS/Ethernet*/Queues",
			poll: 1,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*", "Queues"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernetWildQueuesJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernetWildQueuesJson},
				client.Sync{},
			},
		},
		{
			desc: "poll query for COUNTERS/Ethernet68/Queues with field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68", "Queues"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091c", // "Ethernet68:1": "oid:0x1500000000091c",
					delimitor: ":",
					field:     "SAI_QUEUE_STAT_DROPPED_PACKETS",
					value:     "4", // being changed to 0 from 4
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesJsonUpdate},
				client.Sync{},
			},
		},
		{
			desc: "(use vendor alias) poll query for COUNTERS/Ethernet68/Queues with field value change",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68/1", "Queues"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			updates: []tablePathValue{
				{
					dbName:    "COUNTERS_DB",
					tableName: "COUNTERS",
					tableKey:  "oid:0x1500000000091c", // "Ethernet68:1": "oid:0x1500000000091c",
					delimitor: ":",
					field:     "SAI_QUEUE_STAT_DROPPED_PACKETS",
					value:     "4", // being changed to 0 from 4
				},
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesAliasJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesAliasJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesAliasJsonUpdate},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Queues"}, TS: time.Unix(0, 200), Val: countersEthernet68QueuesAliasJsonUpdate},
				client.Sync{},
			},
		},
		{
			desc:       "use invalid sample interval",
			q:          createCountersDbQuerySampleMode(t, 10*time.Millisecond, false, "COUNTERS", "Ethernet1"),
			updates:    []tablePathValue{},
			wantSubErr: fmt.Errorf("rpc error: code = InvalidArgument desc = invalid interval: 10ms. It cannot be less than %v", sdc.MinSampleInterval),
			wantNoti:   []client.Notification{},
		},
		{
			desc:              "sample stream query for table key Ethernet68 with new test_field field",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet68"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1000000000039", "test_field", "test_value"),
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68"}, TS: time.Unix(0, 200), Val: countersEthernet68Json},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68"}, TS: time.Unix(0, 200), Val: countersEthernet68JsonUpdate},
			},
		},
		{
			desc:              "sample stream query for COUNTERS/Ethernet68/SAI_PORT_STAT_PFC_7_RX_PKTS with 2 updates",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1000000000039", "SAI_PORT_STAT_PFC_7_RX_PKTS", "3"), // be changed to 3 from 2
				createIntervalTickerUpdate(), // no value change but imitate interval ticker
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "2"},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "3"},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: "3"},
			},
		},
		{
			desc:              "(use vendor alias) sample stream query for table key Ethernet68/1 with new test_field field",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet68/1"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1000000000039", "test_field", "test_value"),
				createIntervalTickerUpdate(), // no value change but imitate interval ticker
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1"}, TS: time.Unix(0, 200), Val: countersEthernet68Json},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1"}, TS: time.Unix(0, 200), Val: countersEthernet68JsonUpdate},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1"}, TS: time.Unix(0, 200), Val: countersEthernet68JsonUpdate},
			},
		},
		{
			desc:              "sample stream query for COUNTERS/Ethernet68/Pfcwd with update of field value",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet68", "Pfcwd"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1500000000091e", "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED", "1"),
				createIntervalTickerUpdate(), // no value change but imitate interval ticker
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernet68PfcwdJson, countersEthernet68PfcwdJsonUpdate)},
				client.Update{Path: []string{"COUNTERS", "Ethernet68", "Pfcwd"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernet68PfcwdJson, countersEthernet68PfcwdJsonUpdate)},
			},
		},
		{
			desc:              "(use vendor alias) sample stream query for COUNTERS/[Ethernet68/1]/Pfcwd with update of field value",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet68/1", "Pfcwd"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1500000000091e", "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED", "1"),
				createCountersTableSetUpdate("oid:0x1500000000091e", "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED", "0"), // change back to 0
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernet68PfcwdAliasJson, countersEthernet68PfcwdAliasJsonUpdate)},
				client.Update{Path: []string{"COUNTERS", "Ethernet68/1", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJson},
			},
		},
		{
			desc:              "sample stream query for table key Ethernet* with new test_field field on Ethernet68",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet*"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1000000000039", "test_field", "test_value"),
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernetWildcardJson, countersEtherneWildcardJsonUpdate)},
			},
		},
		{
			desc:              "(updates only) sample stream query for table key Ethernet* with new test_field field on Ethernet68",
			q:                 createCountersDbQuerySampleMode(t, 0, true, "COUNTERS", "Ethernet*"),
			generateIntervals: true,
			updates: []tablePathValue{
				createIntervalTickerUpdate(), // no value change but imitate interval ticker
				createCountersTableSetUpdate("oid:0x1000000000039", "test_field", "test_value"),
				createIntervalTickerUpdate(),
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: map[string]interface{}{}}, //empty update
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEtherneWildcardJsonUpdate},
				client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: map[string]interface{}{}}, //empty update
			},
		},
		/*
			// deletion of field from table is not supported. It'd keep sending the last value before the deletion.
				{
					desc:              "sample stream query for table key Ethernet* with new test_field field deleted from Ethernet68",
					q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet*"),
					generateIntervals: true,
					updates: []tablePathValue{
						createCountersTableSetUpdate("oid:0x1000000000039", "test_field", "test_value"),
						createCountersTableDeleteUpdate("oid:0x1000000000039", "test_field"),
					},
					wantNoti: []client.Notification{
						client.Connected{},
						client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardJson},
						client.Sync{},
						client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernetWildcardJson, countersEtherneWildcardJsonUpdate)},
						client.Update{Path: []string{"COUNTERS", "Ethernet*"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardJson}, //go back to original after deletion of test_field
					},
				},
		*/
		{
			desc:              "sample stream query for table key Ethernet*/SAI_PORT_STAT_PFC_7_RX_PKTS with field value update",
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"),
			generateIntervals: true,
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1000000000039", "SAI_PORT_STAT_PFC_7_RX_PKTS", "4"),
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: countersEthernetWildcardPfcJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "SAI_PORT_STAT_PFC_7_RX_PKTS"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernetWildcardPfcJson, singlePortPfcJsonUpdate)},
			},
		},
		{
			desc:              "sample stream query for table key Ethernet*/Pfcwd with field value update",
			generateIntervals: true,
			q:                 createCountersDbQuerySampleMode(t, 0, false, "COUNTERS", "Ethernet*", "Pfcwd"),
			updates: []tablePathValue{
				createCountersTableSetUpdate("oid:0x1500000000091e", "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED", "1"),
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: mergeStrMaps(countersEthernetWildPfcwdJson, countersEthernet68PfcwdAliasJsonUpdate)},
			},
		},
		{
			desc:              "(update only) sample stream query for table key Ethernet*/Pfcwd with field value update",
			generateIntervals: true,
			q:                 createCountersDbQuerySampleMode(t, 0, true, "COUNTERS", "Ethernet*", "Pfcwd"),
			updates: []tablePathValue{
				createIntervalTickerUpdate(),
				createCountersTableSetUpdate("oid:0x1500000000091e", "PFC_WD_QUEUE_STATS_DEADLOCK_DETECTED", "1"),
				createIntervalTickerUpdate(),
			},
			wantNoti: []client.Notification{
				client.Connected{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernetWildPfcwdJson},
				client.Sync{},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: map[string]interface{}{}}, //empty update
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: countersEthernet68PfcwdAliasJsonUpdate},
				client.Update{Path: []string{"COUNTERS", "Ethernet*", "Pfcwd"}, TS: time.Unix(0, 200), Val: map[string]interface{}{}}, //empty update
			},
		},
	}

	sdc.NeedMock = true
	rclient := getRedisClient(t, namespace)
	defer db.CloseRedisClient(rclient)
	var wg sync.WaitGroup
	for _, tt := range tests {
		wg.Add(1)
		prepareDb(t, namespace)
		// Extra db preparation for this test case
		for _, prepare := range tt.prepares {
			switch prepare.op {
			case "hdel":
				rclient.HDel(context.Background(), prepare.tableName+prepare.delimitor+prepare.tableKey, prepare.field)
			default:
				rclient.HSet(context.Background(), prepare.tableName+prepare.delimitor+prepare.tableKey, prepare.field, prepare.value)
			}
		}

		sdcIntervalTicker := sdc.IntervalTicker
		intervalTickerChan := make(chan time.Time)
		if tt.generateIntervals {
			sdc.IntervalTicker = func(interval time.Duration) <-chan time.Time {
				return intervalTickerChan
			}
		}

		time.Sleep(time.Millisecond * 1000)
		t.Run(tt.desc, func(t *testing.T) {
			q := tt.q
			q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", port)}
			c := client.New()
			defer c.Close()
			var gotNoti []client.Notification
			var mutexGotNoti sync.Mutex
			q.NotificationHandler = func(n client.Notification) error {
				mutexGotNoti.Lock()
				if nn, ok := n.(client.Update); ok {
					nn.TS = time.Unix(0, 200)
					gotNoti = append(gotNoti, nn)
				} else {
					gotNoti = append(gotNoti, n)
				}
				mutexGotNoti.Unlock()
				return nil
			}
			go func(t2 TestExec) {
				defer wg.Done()
				err := c.Subscribe(context.Background(), q)
				if t2.wantSubErr != nil && t2.wantSubErr.Error() != err.Error() {
					t.Errorf("c.Subscribe expected %v, got %v", t2.wantSubErr, err)
				}
				/*
					err := c.Subscribe(context.Background(), q)
					t.Log("c.Subscribe err:", err)
					switch {
					case tt.wantErr && err != nil:
						return
					case tt.wantErr && err == nil:
						t.Fatalf("c.Subscribe(): got nil error, expected non-nil")
					case !tt.wantErr && err != nil:
						t.Fatalf("c.Subscribe(): got error %v, expected nil", err)
					}
				*/
			}(tt)
			// wait for half second for subscribeRequest to sync
			time.Sleep(time.Millisecond * 500)
			for _, update := range tt.updates {
				switch update.op {
				case "hdel":
					rclient.HDel(context.Background(), update.tableName+update.delimitor+update.tableKey, update.field)
				case "intervaltick":
					// This is not a DB update but a request to trigger sample interval
				default:
					rclient.HSet(context.Background(), update.tableName+update.delimitor+update.tableKey, update.field, update.value)
				}

				time.Sleep(time.Millisecond * 1000)

				if tt.generateIntervals {
					intervalTickerChan <- time.Now()
				}
			}
			// wait for half second for change to sync
			time.Sleep(time.Millisecond * 500)

			for i := 0; i < tt.poll; i++ {
				err := c.Poll()
				switch {
				case err == nil && tt.wantPollErr != "":
					t.Errorf("c.Poll(): got nil error, expected non-nil %v", tt.wantPollErr)
				case err != nil && tt.wantPollErr == "":
					t.Errorf("c.Poll(): got error %v, expected nil", err)
				case err != nil && err.Error() != tt.wantPollErr:
					t.Errorf("c.Poll(): got error %v, expected error %v", err, tt.wantPollErr)
				}
			}
			// t.Log("\n Want: \n", tt.wantNoti)
			// t.Log("\n Got : \n", gotNoti)
			mutexGotNoti.Lock()
			defer mutexGotNoti.Unlock()
			if diff := pretty.Compare(tt.wantNoti, gotNoti); diff != "" {
				t.Log("\n Want: \n", tt.wantNoti)
				t.Log("\n Got : \n", gotNoti)
				t.Errorf("unexpected updates:\n%s", diff)
			}
		})
		if tt.generateIntervals {
			sdc.SetIntervalTicker(sdcIntervalTicker)
		}
	}
	sdc.NeedMock = false
	wg.Wait()
}

func TestGnmiSubscribe(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)

	ns, _ := sdcfg.GetDbDefaultNamespace()
	runTestSubscribe(t, s.config.Port, ns)

	s.Stop()
}
func TestGnmiSubscribeMultiNS(t *testing.T) {
	sdcfg.Init()
	err := test_utils.SetupMultiNamespace()
	if err != nil {
		t.Fatalf("error Setting up MultiNamespace files with err %T", err)
	}

	/* https://www.gopherguides.com/articles/test-cleanup-in-go-1-14*/
	t.Cleanup(func() {
		if err := test_utils.CleanUpMultiNamespace(); err != nil {
			t.Fatalf("error Cleaning up MultiNamespace files with err %T", err)

		}
	})

	s := createServer(t)
	go runServer(t, s)

	runTestSubscribe(t, s.config.Port, test_utils.GetMultiNsNamespace())

	s.Stop()
}

func TestCapabilities(t *testing.T) {
	//t.Log("Start server")
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// prepareDb(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := pb.CapabilityRequest{
		Extension: []*ext_pb.Extension{
			{Ext: &ext_pb.Extension_MasterArbitration{}},
		},
	}
	resp, err := gClient.Capabilities(ctx, &req)
	if err != nil {
		t.Fatalf("Failed to get Capabilities")
	}
	if len(resp.SupportedModels) == 0 {
		t.Fatalf("No Supported Models found!")
	}

}

func TestGetWithMasterArbitration(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := pb.GetRequest{
		Extension: []*ext_pb.Extension{
			{Ext: &ext_pb.Extension_MasterArbitration{}},
		},
	}
	_, err = gClient.Get(ctx, &req)
	if err != nil {
		t.Fatalf("Failed to perform Get()")
	}
}

func TestGNOI(t *testing.T) {
	if !ENABLE_TRANSLIB_WRITE {
		t.Skip("skipping test in read-only mode.")
	}
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// prepareDb(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	t.Run("SystemTime", func(t *testing.T) {
		sc := gnoi_system_pb.NewSystemClient(conn)
		resp, err := sc.Time(ctx, new(gnoi_system_pb.TimeRequest))
		if err != nil {
			t.Fatal(err.Error())
		}
		ctime := uint64(time.Now().UnixNano())
		if ctime-resp.Time < 0 || ctime-resp.Time > 1e9 {
			t.Fatalf("Invalid System Time %d", resp.Time)
		}
	})

	t.Run("SonicShowTechsupport", func(t *testing.T) {
		t.Skip("Not supported yet")
		sc := sgpb.NewSonicServiceClient(conn)
		rtime := time.Now().AddDate(0, -1, 0)
		req := &sgpb.TechsupportRequest{
			Input: &sgpb.TechsupportRequest_Input{
				Date: rtime.Format("20060102_150405"),
			},
		}
		resp, err := sc.ShowTechsupport(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}

		if len(resp.Output.OutputFilename) == 0 {
			t.Fatalf("Invalid Output Filename: %s", resp.Output.OutputFilename)
		}
	})

	type configData struct {
		source      string
		destination string
		overwrite   bool
		status      int32
	}

	var cfg_data = []configData{
		configData{"running-configuration", "startup-configuration", false, 0},
		configData{"running-configuration", "file://etc/sonic/config_db_test.json", false, 0},
		configData{"file://etc/sonic/config_db_test.json", "running-configuration", false, 0},
		configData{"startup-configuration", "running-configuration", false, 0},
		configData{"file://etc/sonic/config_db_3.json", "running-configuration", false, 1}}

	for _, v := range cfg_data {

		t.Run("SonicCopyConfig", func(t *testing.T) {
			t.Skip("Not supported yet")
			sc := sgpb.NewSonicServiceClient(conn)
			req := &sgpb.CopyConfigRequest{
				Input: &sgpb.CopyConfigRequest_Input{
					Source:      v.source,
					Destination: v.destination,
					Overwrite:   v.overwrite,
				},
			}
			t.Logf("source: %s dest: %s overwrite: %t", v.source, v.destination, v.overwrite)
			resp, err := sc.CopyConfig(ctx, req)
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.Output.Status != v.status {
				t.Fatalf("Copy Failed: status %d,  %s", resp.Output.Status, resp.Output.StatusDetail)
			}
		})
	}
}

func TestBundleVersion(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// prepareDb(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Run("Invalid Bundle Version Format", func(t *testing.T) {
		var pbPath *pb.Path
		pbPath, err := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet0]/config")
		prefix := pb.Path{Target: "OC-YANG"}
		if err != nil {
			t.Fatalf("error in unmarshaling path: %v", err)
		}
		bundleVersion := "50.0.0"
		bv, err := proto.Marshal(&spb.BundleVersion{
			Version: bundleVersion,
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		req := &pb.GetRequest{
			Path:     []*pb.Path{pbPath},
			Prefix:   &prefix,
			Encoding: pb.Encoding_JSON_IETF,
		}
		req.Extension = append(req.Extension, &ext_pb.Extension{
			Ext: &ext_pb.Extension_RegisteredExt{
				RegisteredExt: &ext_pb.RegisteredExtension{
					Id:  spb.BUNDLE_VERSION_EXT,
					Msg: bv,
				}}})

		_, err = gClient.Get(ctx, req)
		gotRetStatus, ok := status.FromError(err)
		if !ok {
			t.Fatal("got a non-grpc error from grpc call")
		}
		if gotRetStatus.Code() != codes.NotFound {
			t.Log("err: ", err)
			t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), codes.OK)
		}
	})
}

func TestBulkSet(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	prepareDbTranslib(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("Set Multiple mtu", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
				newPbUpdate("interface[name=Ethernet4]/config/mtu", `{"mtu": 9105}`),
			}}
		runTestSetRaw(t, ctx, gClient, req, codes.OK)
	})

	t.Run("Update and Replace", func(t *testing.T) {
		aclKeys := `"name": "A002", "type": "ACL_IPV4"`
		req := &pb.SetRequest{
			Replace: []*pb.Update{
				newPbUpdate(
					"openconfig-acl:acl/acl-sets/acl-set",
					`{"acl-set": [{`+aclKeys+`, "config":{`+aclKeys+`}}]}`),
			},
			Update: []*pb.Update{
				newPbUpdate(
					"interfaces/interface[name=Ethernet0]/config/description",
					`{"description": "Bulk update 1"}`),
				newPbUpdate(
					"openconfig-interfaces:interfaces/interface[name=Ethernet4]/config/description",
					`{"description": "Bulk update 2"}`),
			}}
		runTestSetRaw(t, ctx, gClient, req, codes.OK)
	})

	aclPath1, _ := ygot.StringToStructuredPath("/acl/acl-sets")
	aclPath2, _ := ygot.StringToStructuredPath("/openconfig-acl:acl/acl-sets")

	t.Run("Multiple deletes", func(t *testing.T) {
		req := &pb.SetRequest{
			Delete: []*pb.Path{aclPath1, aclPath2},
		}
		runTestSetRaw(t, ctx, gClient, req, codes.OK)
	})

	t.Run("Invalid Update Path", func(t *testing.T) {
		req := &pb.SetRequest{
			Delete: []*pb.Path{aclPath1, aclPath2},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			}}
		runTestSetRaw(t, ctx, gClient, req, codes.Aborted)
	})

	t.Run("Invalid Replace Path", func(t *testing.T) {
		req := &pb.SetRequest{
			Delete: []*pb.Path{aclPath1, aclPath2},
			Replace: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			}}
		runTestSetRaw(t, ctx, gClient, req, codes.Aborted)
	})

	t.Run("Invalid Delete Path", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Delete: []*pb.Path{aclPath1, aclPath2},
		}
		runTestSetRaw(t, ctx, gClient, req, codes.Aborted)
	})

	t.Run("Update Multiple mtu with same path but different value, expect Set Aborted", func(t *testing.T) {
		pbPath1, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu")
		v := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update1 := &pb.Update{
			Path: pbPath1,
			Val:  v,
		}
		pbPath2, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu")
		v2 := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9105}")}}
		update2 := &pb.Update{
			Path: pbPath2,
			Val:  v2,
		}

		req := &pb.SetRequest{
			Extension: []*ext_pb.Extension{
				{Ext: &ext_pb.Extension_MasterArbitration{}},
			},
			Update: []*pb.Update{update1, update2},
		}

		_, err = gClient.Set(ctx, req)
		e, ok := status.FromError(err)
		if !ok {
			t.Fatal("got a non-grpc error from grpc call")
		}
		if e.Code() != codes.Aborted {
			t.Fatal("Expected Error with Aborted return code")
		}
	})

	t.Run("Update Multiple mtu on state path, expect Set Aborted", func(t *testing.T) {
		pbPath1, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu")
		v := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update1 := &pb.Update{
			Path: pbPath1,
			Val:  v,
		}
		pbPath2, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/state/mtu")
		v2 := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update2 := &pb.Update{
			Path: pbPath2,
			Val:  v2,
		}

		req := &pb.SetRequest{
			Extension: []*ext_pb.Extension{
				{Ext: &ext_pb.Extension_MasterArbitration{}},
			},
			Update: []*pb.Update{update1, update2},
		}

		_, err = gClient.Set(ctx, req)
		e, ok := status.FromError(err)
		if !ok {
			t.Fatal("got a non-grpc error from grpc call")
		}
		if e.Code() != codes.Aborted {
			t.Fatal("Expected Error with Aborted return code")
		}
	})

	t.Run("Replace Multiple mtu with same path but different value, expect Set Aborted", func(t *testing.T) {
		pbPath1, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu")
		v := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update1 := &pb.Update{
			Path: pbPath1,
			Val:  v,
		}
		pbPath2, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu")
		v2 := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9105}")}}
		update2 := &pb.Update{
			Path: pbPath2,
			Val:  v2,
		}

		req := &pb.SetRequest{
			Extension: []*ext_pb.Extension{
				{Ext: &ext_pb.Extension_MasterArbitration{}},
			},
			Replace: []*pb.Update{update1, update2},
		}

		_, err = gClient.Set(ctx, req)
		e, ok := status.FromError(err)
		if !ok {
			t.Fatal("got a non-grpc error from grpc call")
		}
		if e.Code() != codes.Aborted {
			t.Fatal("Expected Error with Aborted return code")
		}
	})

	t.Run("Replace Multiple mtu on state path, expect Set Aborted", func(t *testing.T) {
		pbPath1, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu")
		v := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update1 := &pb.Update{
			Path: pbPath1,
			Val:  v,
		}
		pbPath2, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/state/mtu")
		v2 := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update2 := &pb.Update{
			Path: pbPath2,
			Val:  v2,
		}

		req := &pb.SetRequest{
			Extension: []*ext_pb.Extension{
				{Ext: &ext_pb.Extension_MasterArbitration{}},
			},
			Replace: []*pb.Update{update1, update2},
		}

		_, err = gClient.Set(ctx, req)
		e, ok := status.FromError(err)
		if !ok {
			t.Fatal("got a non-grpc error from grpc call")
		}
		if e.Code() != codes.Aborted {
			t.Fatal("Expected Error with Aborted return code")
		}
	})

	t.Run("Replace mtu on state path, expect Set Aborted", func(t *testing.T) {
		pbPath2, _ := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/state/mtu")
		v2 := &pb.TypedValue{
			Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"mtu\": 9104}")}}
		update2 := &pb.Update{
			Path: pbPath2,
			Val:  v2,
		}

		req := &pb.SetRequest{
			Extension: []*ext_pb.Extension{
				{Ext: &ext_pb.Extension_MasterArbitration{}},
			},
			Replace: []*pb.Update{update2},
		}

		_, err = gClient.Set(ctx, req)
		e, ok := status.FromError(err)
		if !ok {
			t.Fatal("got a non-grpc error from grpc call")
		}
		if e.Code() != codes.Aborted {
			t.Fatal("Expected Error with Aborted return code")
		}
	})

}

func newPbUpdate(path, value string) *pb.Update {
	p, _ := ygot.StringToStructuredPath(path)
	v := &pb.TypedValue_JsonIetfVal{JsonIetfVal: extractJSON(value)}
	return &pb.Update{
		Path: p,
		Val:  &pb.TypedValue{Value: v},
	}
}

type loginCreds struct {
	Username, Password string
}

func (c *loginCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}

func (c *loginCreds) RequireTransportSecurity() bool {
	return true
}

func TestAuthCapabilities(t *testing.T) {
	mock1 := gomonkey.ApplyFunc(UserPwAuth, func(username string, passwd string) (bool, error) {
		return true, nil
	})
	defer mock1.Reset()

	s := createCustomServer(t, testServerConfig(authSrvType))
	go runServer(t, s)
	defer s.Stop()

	currentUser, _ := user.Current()
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	cred := &loginCreds{Username: currentUser.Username, Password: "dummy"}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithPerRPCCredentials(cred)}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var req pb.CapabilityRequest
	resp, err := gClient.Capabilities(ctx, &req)
	if err != nil {
		t.Fatalf("Failed to get Capabilities: %v", err)
	}
	if len(resp.SupportedModels) == 0 {
		t.Fatalf("No Supported Models found!")
	}
}

func TestTableKeyOnDeletion(t *testing.T) {
	s := createCustomServer(t, testServerConfig(keepAliveSrvType))
	go runServer(t, s)
	defer s.Stop()

	fileName := "../testdata/NEIGH_STATE_TABLE_MAP.txt"
	neighStateTableByte, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var neighStateTableJson interface{}
	json.Unmarshal(neighStateTableByte, &neighStateTableJson)

	fileName = "../testdata/NEIGH_STATE_TABLE_key_deletion_57.txt"
	neighStateTableDeletedByte57, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var neighStateTableDeletedJson57 interface{}
	json.Unmarshal(neighStateTableDeletedByte57, &neighStateTableDeletedJson57)

	fileName = "../testdata/NEIGH_STATE_TABLE_MAP_2.txt"
	neighStateTableByteTwo, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var neighStateTableJsonTwo interface{}
	json.Unmarshal(neighStateTableByteTwo, &neighStateTableJsonTwo)

	fileName = "../testdata/NEIGH_STATE_TABLE_key_deletion_59.txt"
	neighStateTableDeletedByte59, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var neighStateTableDeletedJson59 interface{}
	json.Unmarshal(neighStateTableDeletedByte59, &neighStateTableDeletedJson59)

	fileName = "../testdata/NEIGH_STATE_TABLE_key_deletion_61.txt"
	neighStateTableDeletedByte61, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}
	var neighStateTableDeletedJson61 interface{}
	json.Unmarshal(neighStateTableDeletedByte61, &neighStateTableDeletedJson61)

	namespace, _ := sdcfg.GetDbDefaultNamespace()
	rclient := getRedisClientN(t, 6, namespace)
	defer db.CloseRedisClient(rclient)
	prepareStateDb(t, namespace)

	tests := []struct {
		desc     string
		q        client.Query
		wantNoti []client.Notification
		paths    []string
	}{
		{
			desc: "Testing deletion of NEIGH_STATE_TABLE:10.0.0.57",
			q:    createStateDbQueryOnChangeMode(t, "NEIGH_STATE_TABLE"),
			wantNoti: []client.Notification{
				client.Update{Path: []string{"NEIGH_STATE_TABLE"}, TS: time.Unix(0, 200), Val: neighStateTableJson},
				client.Update{Path: []string{"NEIGH_STATE_TABLE"}, TS: time.Unix(0, 200), Val: neighStateTableDeletedJson57},
			},
			paths: []string{
				"NEIGH_STATE_TABLE|10.0.0.57",
			},
		},
		{
			desc: "Testing deletion of NEIGH_STATE_TABLE:10.0.0.59 and NEIGH_STATE_TABLE 10.0.0.61",
			q:    createStateDbQueryOnChangeMode(t, "NEIGH_STATE_TABLE"),
			wantNoti: []client.Notification{
				client.Update{Path: []string{"NEIGH_STATE_TABLE"}, TS: time.Unix(0, 200), Val: neighStateTableJsonTwo},
				client.Update{Path: []string{"NEIGH_STATE_TABLE"}, TS: time.Unix(0, 200), Val: neighStateTableDeletedJson59},
				client.Update{Path: []string{"NEIGH_STATE_TABLE"}, TS: time.Unix(0, 200), Val: neighStateTableDeletedJson61},
			},
			paths: []string{
				"NEIGH_STATE_TABLE|10.0.0.59",
				"NEIGH_STATE_TABLE|10.0.0.61",
			},
		},
	}

	var mutexNoti sync.RWMutex
	var mutexPaths sync.Mutex
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			q := tt.q
			q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}
			c := client.New()
			defer c.Close()
			var gotNoti []client.Notification
			q.NotificationHandler = func(n client.Notification) error {
				if nn, ok := n.(client.Update); ok {
					nn.TS = time.Unix(0, 200)
					mutexNoti.Lock()
					currentNoti := gotNoti
					mutexNoti.Unlock()

					mutexNoti.RLock()
					gotNoti = append(currentNoti, nn)
					mutexNoti.RUnlock()
				}
				return nil
			}

			go func() {
				c.Subscribe(context.Background(), q)
			}()

			time.Sleep(time.Millisecond * 500) // half a second for subscribe request to sync

			mutexPaths.Lock()
			paths := tt.paths
			mutexPaths.Unlock()

			rclient.Del(context.Background(), paths...)

			time.Sleep(time.Millisecond * 1500)

			mutexNoti.Lock()
			if diff := pretty.Compare(tt.wantNoti, gotNoti); diff != "" {
				t.Log("\n Want: \n", tt.wantNoti)
				t.Log("\n Got : \n", gotNoti)
				t.Errorf("unexpected updates:\n%s", diff)
			}
			mutexNoti.Unlock()
		})
	}
}

func TestCPUUtilization(t *testing.T) {
	mock := gomonkey.ApplyFunc(sdc.PollStats, func() {
		var i uint64
		for i = 0; i < 3000; i++ {
			sdc.WriteStatsToBuffer(&linuxproc.Stat{})
		}
	})

	defer mock.Reset()
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	tests := []struct {
		desc string
		q    client.Query
		want []client.Notification
		poll int
	}{
		{
			desc: "poll query for CPU Utilization",
			poll: 10,
			q: client.Query{
				Target:  "OTHERS",
				Type:    client.Poll,
				Queries: []client.Path{{"platform", "cpu"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			want: []client.Notification{
				client.Connected{},
				client.Sync{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			q := tt.q
			q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}
			c := client.New()
			var gotNoti []client.Notification
			q.NotificationHandler = func(n client.Notification) error {
				if nn, ok := n.(client.Update); ok {
					nn.TS = time.Unix(0, 200)
					gotNoti = append(gotNoti, nn)
				} else {
					gotNoti = append(gotNoti, n)
				}
				return nil
			}

			wg := new(sync.WaitGroup)
			wg.Add(1)

			go func() {
				defer wg.Done()
				if err := c.Subscribe(context.Background(), q); err != nil {
					t.Errorf("c.Subscribe(): got error %v, expected nil", err)
				}
			}()

			wg.Wait()

			for i := 0; i < tt.poll; i++ {
				if err := c.Poll(); err != nil {
					t.Errorf("c.Poll(): got error %v, expected nil", err)
				}
			}

			if len(gotNoti) == 0 {
				t.Errorf("expected non zero notifications")
			}

			c.Close()
		})
	}
}

func TestClientConnections(t *testing.T) {
	s := createCustomServer(t, testServerConfig(rejectSrvType))
	go runServer(t, s)
	defer s.Stop()

	tests := []struct {
		desc string
		q    client.Query
		want []client.Notification
		poll int
	}{
		{
			desc: "Reject OTHERS/proc/uptime",
			poll: 10,
			q: client.Query{
				Target:  "OTHERS",
				Type:    client.Poll,
				Queries: []client.Path{{"proc", "uptime"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			want: []client.Notification{
				client.Connected{},
				client.Sync{},
			},
		},
		{
			desc: "Reject COUNTERS/Ethernet*",
			poll: 10,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			want: []client.Notification{
				client.Connected{},
				client.Sync{},
			},
		},
		{
			desc: "Reject COUNTERS/Ethernet68",
			poll: 10,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet68"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			want: []client.Notification{
				client.Connected{},
				client.Sync{},
			},
		},
	}

	var clients []*cacheclient.CacheClient

	for i, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			q := tt.q
			q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}
			var gotNoti []client.Notification
			q.NotificationHandler = func(n client.Notification) error {
				if nn, ok := n.(client.Update); ok {
					nn.TS = time.Unix(0, 200)
					gotNoti = append(gotNoti, nn)
				} else {
					gotNoti = append(gotNoti, n)
				}
				return nil
			}

			wg := new(sync.WaitGroup)
			wg.Add(1)

			go func() {
				defer wg.Done()
				c := client.New()
				clients = append(clients, c)
				err := c.Subscribe(context.Background(), q)
				if err == nil && i == len(tests)-1 { // reject third
					t.Errorf("Expecting rejection message as no connections are allowed")
				}
				if err != nil && i < len(tests)-1 { // accept first two
					t.Errorf("Expecting accepts for first two connections")
				}
			}()

			wg.Wait()
		})
	}

	for _, cacheClient := range clients {
		cacheClient.Close()
	}
}

func TestStreamingRPCLimit(t *testing.T) {
	s := createCustomServer(t, testServerConfig(rejectSrvType))
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()
	wg := sync.WaitGroup{}

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)

	req := &pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:     pb.SubscriptionList_STREAM,
				Encoding: pb.Encoding_PROTO,
				Subscription: []*pb.Subscription{{
					Mode:              pb.SubscriptionMode_SAMPLE,
					SampleInterval:    30000000000,
					SuppressRedundant: true,
				}},
			},
		},
	}

	// The first two subscriptions should succeed, the last one should be rejected.
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Errorf("Failed to create subscribe client: %v", err)
				return
			}
			defer stream.CloseSend()
			if err = stream.Send(req); err != nil {
				t.Errorf("Failed to send subscription: %v", err)
				return
			}
			for {
				resp, err := stream.Recv()
				if i < 2 && err != nil {
					t.Errorf("Unexpected error returned to client %d: %v", i, err)
					return
				} else if i == 2 {
					testErr(err, codes.Unavailable, "Server connections are at capacity.", t)
					return
				}
				if resp.GetSyncResponse() {
					return
				}
			}
		}(i)
		// Sleep to give the client time to send their request before the next one.
		time.Sleep(100 * time.Millisecond)
	}
	wg.Wait()

	// Another subscription should succeed.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Errorf("Failed to create subscribe client: %v", err)
		return
	}
	defer stream.CloseSend()
	if err = stream.Send(req); err != nil {
		t.Errorf("Failed to send subscription: %v", err)
		return
	}
	_, err = stream.Recv()
	if err != nil {
		t.Errorf("Unexpected error returned to client: %v", err)
	}
}

func TestUnaryRPCLimit(t *testing.T) {
	s := createCustomServer(t, testServerConfig(rejectSrvType))
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()
	wg := sync.WaitGroup{}

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &pb.GetRequest{
		Prefix: &pb.Path{
			Elem: []*pb.PathElem{
				&pb.PathElem{
					Name: "openconfig",
				}},
		},
		Encoding: pb.Encoding_JSON_IETF,
	}

	// The first two GETs should succeed, the last one should be rejected.
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := gClient.Get(ctx, req)
			if i < 2 && err != nil {
				t.Errorf("Unexpected error returned to client %d: %v", i, err)
			} else if i == 2 {
				testErr(err, codes.Unavailable, "Server connections are at capacity.", t)
			}
		}(i)
		// Sleep to give the client time to send their request before the next one.
		time.Sleep(100 * time.Millisecond)
	}
	wg.Wait()

	// Another GET should succeed.
	_, err = gClient.Get(ctx, req)
	if err != nil {
		t.Errorf("Unexpected error returned to client: %v", err)
	}
}

func TestConnectionDataSet(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	tests := []struct {
		desc string
		q    client.Query
		want []client.Notification
		poll int
	}{
		{
			desc: "poll query for COUNTERS/Ethernet*",
			poll: 10,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			want: []client.Notification{
				client.Connected{},
				client.Sync{},
			},
		},
	}
	namespace, _ := sdcfg.GetDbDefaultNamespace()
	rclient := getRedisClientN(t, 6, namespace)
	defer db.CloseRedisClient(rclient)

	for _, tt := range tests {
		prepareStateDb(t, namespace)
		t.Run(tt.desc, func(t *testing.T) {
			q := tt.q
			q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}
			c := client.New()

			wg := new(sync.WaitGroup)
			wg.Add(1)

			go func() {
				defer wg.Done()
				if err := c.Subscribe(context.Background(), q); err != nil {
					t.Errorf("c.Subscribe(): got error %v, expected nil", err)
				}
			}()

			wg.Wait()

			resultMap, err := rclient.HGetAll(context.Background(), "TELEMETRY_CONNECTIONS").Result()

			if resultMap == nil {
				t.Errorf("result Map is nil, expected non nil, err: %v", err)
			}
			if len(resultMap) != 1 {
				t.Errorf("result for TELEMETRY_CONNECTIONS should be 1")
			}

			for key, _ := range resultMap {
				if !strings.Contains(key, "COUNTERS_DB|COUNTERS|Ethernet*") {
					t.Errorf("key is expected to contain correct query, received: %s", key)
				}
			}

			c.Close()
		})
	}
}

func TestConnectionsKeepAlive(t *testing.T) {
	s := createCustomServer(t, testServerConfig(keepAliveSrvType))
	go runServer(t, s)
	defer s.Stop()

	tests := []struct {
		desc string
		q    client.Query
		want []client.Notification
		poll int
	}{
		{
			desc: "Testing KeepAlive with goroutine count",
			poll: 3,
			q: client.Query{
				Target:  "COUNTERS_DB",
				Type:    client.Poll,
				Queries: []client.Path{{"COUNTERS", "Ethernet*"}},
				TLS:     &tls.Config{InsecureSkipVerify: true},
			},
			want: []client.Notification{
				client.Connected{},
				client.Sync{},
			},
		},
	}
	for _, tt := range tests {
		for i := 0; i < 5; i++ {
			t.Run(tt.desc, func(t *testing.T) {
				q := tt.q
				q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}
				c := client.New()
				wg := new(sync.WaitGroup)
				wg.Add(1)

				go func() {
					defer wg.Done()
					if err := c.Subscribe(context.Background(), q); err != nil {
						t.Errorf("c.Subscribe(): got error %v, expected nil", err)
					}
				}()

				wg.Wait()
				after_subscribe := runtime.NumGoroutine()
				t.Logf("Num go routines after client subscribe: %d", after_subscribe)
				time.Sleep(10 * time.Second)
				after_sleep := runtime.NumGoroutine()
				t.Logf("Num go routines after sleep, should be less, as keepalive should close idle connections: %d", after_sleep)
				if after_sleep > after_subscribe {
					t.Errorf("Expecting goroutine after sleep to be less than or equal to after subscribe, after_subscribe: %d, after_sleep: %d", after_subscribe, after_sleep)
				}
			})
		}
	}
}

func TestClient(t *testing.T) {
	var mutexDeInit sync.RWMutex
	var mutexHB sync.RWMutex
	var mutexIdx sync.RWMutex

	// sonic-host:device-test-event is a test event.
	// Events client will drop it on floor.
	events := []sdc.Evt_rcvd{
		{"test0", 7, 777},
		{"test1", 6, 677},
		{"{\"sonic-host:device-test-event\"", 5, 577},
		{"test2", 5, 577},
		{"test3", 4, 477},
	}

	HEARTBEAT_SET := 5
	heartbeat := 0
	event_index := 0
	rcv_timeout := sdc.SUBSCRIBER_TIMEOUT
	deinit_done := false

	mock1 := gomonkey.ApplyFunc(sdc.C_init_subs, func(use_cache bool) unsafe.Pointer {
		return nil
	})
	defer mock1.Reset()

	mock2 := gomonkey.ApplyFunc(sdc.C_recv_evt, func(h unsafe.Pointer) (int, sdc.Evt_rcvd) {
		rc := (int)(0)
		var evt sdc.Evt_rcvd
		mutexIdx.Lock()
		current_index := event_index
		mutexIdx.Unlock()
		if current_index < len(events) {
			evt = events[current_index]
			mutexIdx.RLock()
			event_index = current_index + 1
			mutexIdx.RUnlock()
		} else {
			time.Sleep(time.Millisecond * time.Duration(rcv_timeout))
			rc = -1
		}
		return rc, evt
	})
	defer mock2.Reset()

	mock3 := gomonkey.ApplyFunc(sdc.Set_heartbeat, func(val int) {
		mutexHB.RLock()
		heartbeat = val
		mutexHB.RUnlock()
	})
	defer mock3.Reset()

	mock4 := gomonkey.ApplyFunc(sdc.C_deinit_subs, func(h unsafe.Pointer) {
		mutexDeInit.RLock()
		deinit_done = true
		mutexDeInit.RUnlock()
	})
	defer mock4.Reset()

	mock5 := gomonkey.ApplyMethod(reflect.TypeOf(&queue.PriorityQueue{}), "Put", func(pq *queue.PriorityQueue, item ...queue.Item) error {
		return fmt.Errorf("Queue error")
	})
	defer mock5.Reset()

	mock6 := gomonkey.ApplyMethod(reflect.TypeOf(&queue.PriorityQueue{}), "Len", func(pq *queue.PriorityQueue) int {
		return 150000 // Max size for pending events in PQ is 102400
	})
	defer mock6.Reset()

	s := createServer(t)
	go runServer(t, s)

	qstr := fmt.Sprintf("all[heartbeat=%d]", HEARTBEAT_SET)
	q := createEventsQuery(t, qstr)
	q.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}

	tests := []struct {
		desc     string
		pub_data []string
		wantErr  bool
		wantNoti []client.Notification
		pause    int
		poll     int
	}{
		{
			desc: "dropped event",
			poll: 3,
		},
		{
			desc: "queue error",
			poll: 3,
		},
		{
			desc: "base client create",
			poll: 3,
		},
	}

	sdc.C_init_subs(true)

	var mutexNoti sync.RWMutex

	for testNum, tt := range tests {
		mutexHB.RLock()
		heartbeat = 0
		mutexHB.RUnlock()

		mutexIdx.RLock()
		event_index = 0
		mutexIdx.RUnlock()

		mutexDeInit.RLock()
		deinit_done = false
		mutexDeInit.RUnlock()

		t.Run(tt.desc, func(t *testing.T) {
			c := client.New()
			defer c.Close()

			var gotNoti []string
			q.NotificationHandler = func(n client.Notification) error {
				if nn, ok := n.(client.Update); ok {
					nn.TS = time.Unix(0, 200)
					str := fmt.Sprintf("%v", nn.Val)

					mutexNoti.Lock()
					currentNoti := gotNoti
					mutexNoti.Unlock()

					mutexNoti.RLock()
					gotNoti = append(currentNoti, str)
					mutexNoti.RUnlock()
				}
				return nil
			}

			go func() {
				c.Subscribe(context.Background(), q)
			}()

			// wait for half second for subscribeRequest to sync
			// and to receive events via notification handler.

			time.Sleep(time.Millisecond * 2000)

			if testNum > 1 {
				mutexNoti.Lock()
				// -1 to discount test event, which receiver would drop.
				if (len(events) - 1) != len(gotNoti) {
					t.Errorf("noti[%d] != events[%d]", len(gotNoti), len(events)-1)
				}

				mutexHB.Lock()
				if heartbeat != HEARTBEAT_SET {
					t.Errorf("Heartbeat is not set %d != expected:%d", heartbeat, HEARTBEAT_SET)
				}
				mutexHB.Unlock()

				fmt.Printf("DONE: Expect events:%d - 1 gotNoti=%d\n", len(events), len(gotNoti))
				mutexNoti.Unlock()
			}
		})

		if testNum == 0 {
			mock6.Reset()
		}

		if testNum == 1 {
			mock5.Reset()
		}
		time.Sleep(time.Millisecond * 1000)

		mutexDeInit.Lock()
		if deinit_done == false {
			t.Errorf("Events client deinit *NOT* called.")
		}
		mutexDeInit.Unlock()
		// t.Log("END of a TEST")
	}

	s.Stop()
}

func TestOnChangeSubWithDBChange(t *testing.T) {
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a subscription is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatalf("Failed to create subscribe client: %v", err)
	}
	path, err := xpath.ToGNMIPath("/interfaces/interface[name=Ethernet1/2/1]/state/admin-status")
	if err != nil {
		t.Fatalf("Failed to translate path: %v", err)
	}
	subscriptions := pb.SubscriptionList{
		Prefix: &pb.Path{Target: "OC_YANG", Origin: "openconfig"},
		Subscription: []*pb.Subscription{
			{
				Path: path,
				Mode: gnmipb.SubscriptionMode_ON_CHANGE,
			},
		},
		Mode: pb.SubscriptionList_STREAM,
	}
	req := &gnmipb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &subscriptions,
		},
	}
	cfgDb := db.RedisClient(db.ConfigDB)
	defer db.CloseRedisClient(cfgDb)
	applStateDb := db.RedisClient(db.ApplStateDB)
	defer db.CloseRedisClient(applStateDb)

	cfgDb.HSet(context.Background(), "PORT|Ethernet1/2/1", "admin_status", "up")
	applStateDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2/1", "admin_status", "up")

	err = stream.Send(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	var changeNo int
	var subRespNo int

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for end := time.Now().Add(time.Second * 6); time.Now().Before(end); {
			resp, err := stream.Recv()
			// Check On_Change. Skip SyncResponse.
			if err == nil && !resp.GetSyncResponse() {
				subRespNo++
				responseVal := resp.GetUpdate().Update[0].GetVal().String()
				verifyRes, _ := applStateDb.HGet(context.Background(), "PORT_TABLE:Ethernet1/2/1", "admin_status").Result()

				if !strings.Contains(responseVal, strings.ToUpper(verifyRes)) {
					t.Fatalf("Check DB value and subscribe response failed, want %v, get %v", verifyRes, responseVal)
				}

				if changeNo != subRespNo {
					t.Logf("Missing on_change subscribe notification")
				}

				t.Logf("Check DB value and subscribe response, want %v, get %v", verifyRes, responseVal)
			}
		}
	}()

	setup := []string{"up", "down"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for end := time.Now().Add(time.Second * 5); time.Now().Before(end); {
			for i := 0; i < 2; i++ {
				time.Sleep(time.Second * 1)
				applStateDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2/1", "admin_status", setup[i%2])
				changeNo++
			}
		}

	}()

	wg.Wait()
	if changeNo != subRespNo {
		t.Fatalf("changeNo(%v) != subRespNo(%v)", changeNo, subRespNo)
	}
}

func TestOnChangeScenariosDuringSync(t *testing.T) {
	ctxDuration := 15 * time.Second
	appDb := db.RedisClient(db.ApplStateDB)
	defer db.CloseRedisClient(appDb)
	cfgDb := db.RedisClient(db.ConfigDB)
	defer db.CloseRedisClient(cfgDb)

	tests := []struct {
		name              string
		port              string
		setupFunc         func(t *testing.T)
		dbChangeFunc      func(t *testing.T) // Function that changes the DB to test race conditions.
		expectedInSync    string             // If the only update is during the SyncResponse, this is the expected value.
		expectedAfterSync string             // If an update is received after the SyncResponse, this is the expected value.
	}{
		{
			// This test changes oper-status for a port while the CONTROLLER sync response is being processed
			// and expects an oper-status update on that port.
			name: "FieldChangeDuringSync",
			port: "Ethernet1/2220/1",
			setupFunc: func(t *testing.T) {
				t.Helper()
				cfgDb.HSet(context.Background(), "PORT|Ethernet1/2220/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2220/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
			},
			dbChangeFunc: func(t *testing.T) {
				t.Helper()
				// This sleep attempts to delay a PORT_TABLE notification until after SendInitialResonse has begun, but before it has completed.
				sleepTime := 120 * time.Millisecond
				time.Sleep(sleepTime)

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2220/1", "oper_status", "down")
				t.Logf("Wrote down for port PORT_TABLE:Ethernet1/2220/1 to ApplStateDB at %v", time.Now())
			},
			expectedInSync:    "DOWN",
			expectedAfterSync: "DOWN",
		},
		{
			// This test flaps oper-status for a port while the CONTROLLER sync response is being processed
			// and expects an oper-status update on that port.
			name: "LinkFlapDuringSync",
			port: "Ethernet1/2221/1",
			setupFunc: func(t *testing.T) {
				t.Helper()
				cfgDb.HSet(context.Background(), "PORT|Ethernet1/2221/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2221/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
			},
			dbChangeFunc: func(t *testing.T) {
				t.Helper()
				// This sleep attempts to delay a PORT_TABLE notification until after SendInitialResonse has begun, but before it has completed.
				sleepTime := 60 * time.Millisecond
				time.Sleep(sleepTime)

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2221/1", "oper_status", "down")
				t.Logf("Wrote down for port PORT_TABLE:Ethernet1/2221/1 to ApplStateDB at %v", time.Now())

				time.Sleep(sleepTime)

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2221/1", "oper_status", "up")
				t.Logf("Wrote up for port PORT_TABLE:Ethernet1/2221/1 to ApplStateDB at %v", time.Now())
			},
			expectedInSync:    "UP",
			expectedAfterSync: "UP",
		},
		{
			// This test adds a new interface to the ApplStateDB while the CONTROLLER sync response is being processed
			// and expects a hardware-port update on that interface.
			name: "PortAddDuringSync",
			port: "Ethernet1/2222/1",
			setupFunc: func(t *testing.T) {
				t.Helper()
				cfgDb.HSet(context.Background(), "PORT|Ethernet1/2222/1", map[string]string{
					"index":        "2222",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
				})
			},
			dbChangeFunc: func(t *testing.T) {
				t.Helper()
				// This sleep attempts to delay a PORT_TABLE notification until after SendInitialResonse has begun, but before it has completed.
				sleepTime := 60 * time.Millisecond
				time.Sleep(sleepTime)

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2222/1", map[string]string{
					"index":        "2222",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
				})
				t.Logf("Wrote new port to ApplStateDB at %v", time.Now())
			},
			expectedInSync:    "UP",
			expectedAfterSync: "UP",
		},
		{
			// This test removes then adds back an interface to the ApplStateDB while the CONTROLLER sync response
			// is being processed and expects oper-status to be correctly reported.
			name: "PortDeleteAndAddDuringSync",
			port: "Ethernet1/2223/1",
			setupFunc: func(t *testing.T) {
				t.Helper()
				cfgDb.HSet(context.Background(), "PORT|Ethernet1/2223/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2223/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
			},
			dbChangeFunc: func(t *testing.T) {
				t.Helper()
				// This sleep attempts to delay a PORT_TABLE notification until after SendInitialResonse has begun, but before it has completed.
				sleepTime := 60 * time.Millisecond
				time.Sleep(sleepTime)

				appDb.Del(context.Background(), "PORT_TABLE:Ethernet1/2223/1")

				t.Logf("Removed port from ApplStateDB at %v", time.Now())

				time.Sleep(sleepTime)

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2223/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
				t.Logf("Wrote port to ApplStateDB at %v", time.Now())
			},
			expectedInSync:    "UP",
			expectedAfterSync: "UP",
		},
		{
			// This deletes a port while the CONTROLLER sync response is being processed
			// and expects a delete or no update during sync response for that port.
			name: "PortDeleteDuringSync",
			port: "Ethernet1/2224/1",
			setupFunc: func(t *testing.T) {
				t.Helper()
				cfgDb.HSet(context.Background(), "PORT|Ethernet1/2224/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})

				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2224/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
			},
			dbChangeFunc: func(t *testing.T) {
				t.Helper()
				// This sleep attempts to delay a PORT_TABLE notification until after SendInitialResonse has begun, but before it has completed.
				sleepTime := 120 * time.Millisecond
				time.Sleep(sleepTime)

				appDb.Del(context.Background(), "PORT_TABLE:Ethernet1/2224/1")

				t.Logf("Removed port from ApplStateDB at %v", time.Now())
			},
			expectedInSync:    "",
			expectedAfterSync: "delete",
		},
		{
			// This adds and deletes a port while the CONTROLLER sync response is being processed
			// and expects a delete or no update during sync response for that port.
			name: "PortAddAndDeleteDuringSync",
			port: "Ethernet1/2225/1",
			setupFunc: func(t *testing.T) {
				t.Helper()
				cfgDb.HSet(context.Background(), "PORT|Ethernet1/2225/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
			},
			dbChangeFunc: func(t *testing.T) {
				t.Helper()
				// This sleep attempts to delay a PORT_TABLE notification until after SendInitialResonse has begun, but before it has completed.
				sleepTime := 60 * time.Millisecond
				time.Sleep(sleepTime)
				appDb.HSet(context.Background(), "PORT_TABLE:Ethernet1/2225/1", map[string]string{
					"index":        "2223",
					"oper_status":  "up",
					"id":           "1324",
					"admin_status": "up",
					"presence":     "1",
				})
				t.Logf("Wrote port to ApplStateDB at %v", time.Now())

				time.Sleep(sleepTime)
				appDb.Del(context.Background(), "PORT_TABLE:Ethernet1/2225/1")
				t.Logf("Removed port from ApplStateDB at %v", time.Now())
			},
			expectedInSync:    "",
			expectedAfterSync: "delete",
		},
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Disable DEBUG logs if enabled. Log spam changes the timing of events.
	if v := flag.Lookup("v"); v != nil {
		defer flag.Set("v", v.Value.String())
	}
	flag.Set("v", "2")

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	// Construct the subscription
	subs := []*pb.Subscription{}
	for _, controllerSub := range controllerPaths {
		path, err := xpath.ToGNMIPath(controllerSub.path)
		if err != nil {
			t.Fatalf("Failed to convert string to GNMI path: %v", err)
		}
		subs = append(subs, &pb.Subscription{
			Path: path,
			Mode: controllerSub.mode,
		})
	}
	subscription := &gnmipb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:       &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:         gnmipb.SubscriptionList_STREAM,
				Encoding:     gnmipb.Encoding_PROTO,
				Subscription: subs,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receivedInSync := ""
			receivedAfterSync := ""
			ctx, cancel := context.WithTimeout(context.Background(), ctxDuration)
			defer cancel()

			stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			defer stream.CloseSend()

			test.setupFunc(t)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Send the subscription and wait for sync response
				if err = stream.Send(subscription); err != nil {
					t.Fatalf("Failed to send subscription: %v", err)
				}
				for {
					resp, err := stream.Recv()
					if err != nil {
						t.Fatalf("stream.Recv() returned an error before the SyncResponse was received: %v", err)
					}
					if strings.Contains(resp.String(), "oper-status") && strings.Contains(resp.String(), test.port) {
						t.Logf("Received during SyncResponse: %v", resp)
						if len(resp.GetUpdate().GetUpdate()) > 0 {
							receivedInSync = resp.GetUpdate().GetUpdate()[0].GetVal().GetStringVal()
						} else if len(resp.GetUpdate().GetDelete()) > 0 {
							receivedInSync = "delete"
						}
					}
					if resp.GetSyncResponse() {
						break
					}
				}
			}()

			test.dbChangeFunc(t)

			wg.Wait()
			for {
				resp, err := stream.Recv()
				if err != nil {
					t.Logf("stream.Recv() returned an error, which is okay if the correct value was received already: %v", err)
					break
				}

				if strings.Contains(resp.String(), "oper-status") && strings.Contains(resp.String(), test.port) {
					t.Logf("Received after SyncResponse: %v", resp)
					if len(resp.GetUpdate().GetUpdate()) > 0 {
						receivedAfterSync = resp.GetUpdate().GetUpdate()[0].GetVal().GetStringVal()
					} else if len(resp.GetUpdate().GetDelete()) > 0 {
						receivedAfterSync = "delete"
					}
				}
			}

			if receivedInSync == "" && receivedAfterSync == "" && test.expectedInSync != "" {
				t.Fatalf("Never received oper-status update for %v", test.port)
			}
			if receivedInSync != "" && receivedInSync != test.expectedInSync && receivedAfterSync == "" {
				t.Fatalf("Received incorrect value during sync: %v", receivedInSync)
			}
			if receivedAfterSync != "" && receivedAfterSync != test.expectedAfterSync {
				t.Fatalf("Received incorrect value after sync: %v", receivedAfterSync)
			}
		})
	}
}

func TestMultipleSubscribers(t *testing.T) {
	tests := []struct {
		name          string
		delay         time.Duration // The delay between sending the subscriptions.
		duration      time.Duration // The duration of the subscriptions.
		sampleUpdates uint          // The number of Sample updates expected based on the duration and sample interval.
	}{
		{
			name:          "TestSubscribeWithNoDelay",
			delay:         0 * time.Second,
			duration:      5 * time.Second,
			sampleUpdates: 4,
		},
		{
			name:          "TestSubscribeWithPartialDelay",
			delay:         2 * time.Second,
			duration:      5 * time.Second,
			sampleUpdates: 4,
		},
		{
			name:          "TestSubscribeWithFullDelay",
			delay:         5 * time.Second,
			duration:      5 * time.Second,
			sampleUpdates: 4,
		},
	}

	// Load DB snapshots
	prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/json_tests/appl_state_db.txt")
	prepareDbUtil(t, "CONFIG_DB", "", "../testdata/json_tests/config_db.txt")

	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	samplePath, _ := xpath.ToGNMIPath("/interfaces/interface[name=Ethernet1/1/1]/state/name")
	onChangePath, _ := xpath.ToGNMIPath("/system/state/config-meta-data")
	subReq := &pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:     pb.SubscriptionList_STREAM,
				Encoding: pb.Encoding_PROTO,
				Subscription: []*pb.Subscription{
					{
						Path:           samplePath,
						Mode:           pb.SubscriptionMode_SAMPLE,
						SampleInterval: 1000000000, // 1 second
					},
					{
						Path: onChangePath,
						Mode: pb.SubscriptionMode_ON_CHANGE,
					},
				},
			},
		},
	}

	var subTest = func(t *testing.T, duration time.Duration, expectedSamples uint, wg *sync.WaitGroup) {
		defer wg.Done()
		t.Helper()
		numSampleUpdates := uint(0)
		numOnChangeUpdates := uint(0)
		rclient := db.RedisClient(db.ConfigDB)
		defer db.CloseRedisClient(rclient)
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		stream, err := gClient.Subscribe(ctx, grpc.MaxCallRecvMsgSize(6000000))
		if err != nil {
			t.Error(err.Error())
			return
		}
		// Send the subscription and wait for sync response
		if err = stream.Send(subReq); err != nil {
			t.Errorf("Failed to send subscription: %v", err)
			return
		}
		go func() {
			time.Sleep(duration)

		}()
		syncReceived := false
		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}
			if resp.GetSyncResponse() {
				syncReceived = true

				// Trigger an OnChange update.
				if err = rclient.HSet(context.Background(), "DEVICE_METADATA|localhost", "config-meta-data", time.Now().String()).Err(); err != nil {
					t.Logf("Failed to set config-meta-data field: %v", err)
				}

				continue
			}
			if syncReceived {
				if strings.Contains(resp.GetUpdate().GetUpdate()[0].GetPath().String(), "\"name\"") {
					numSampleUpdates += 1
				} else if strings.Contains(resp.GetUpdate().GetUpdate()[0].GetPath().String(), "\"config-meta-data\"") {
					numOnChangeUpdates += 1
				}
			}
		}
		if numSampleUpdates < expectedSamples {
			t.Errorf("Unexepcted number of Sample updates after sync response: got=%v, want at least %v", numSampleUpdates, expectedSamples)
		}
		if numOnChangeUpdates < 1 {
			t.Errorf("Unexepcted number of OnChange updates after sync response: got=%v, want at least %v", numOnChangeUpdates, 1)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wg := &sync.WaitGroup{}

			wg.Add(1)
			go subTest(t, test.duration, test.sampleUpdates, wg)

			time.Sleep(test.delay)

			wg.Add(1)
			go subTest(t, test.duration, test.sampleUpdates, wg)

			wg.Wait()
		})
	}
}

func TestSubscriptionDeduplication(t *testing.T) {
	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	rclient := db.RedisClient(db.ConfigDB)
	defer db.CloseRedisClient(rclient)

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	samplePath, _ := xpath.ToGNMIPath("/interfaces/interface[name=Ethernet1/1/1]/config/description")
	subReq := &pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:     pb.SubscriptionList_STREAM,
				Encoding: pb.Encoding_PROTO,
				Subscription: []*pb.Subscription{
					{
						Path:           samplePath,
						Mode:           pb.SubscriptionMode_SAMPLE,
						SampleInterval: 1000000000, // 1 second
					},
				},
			},
		},
	}

	updates := make([][]*pb.SubscribeResponse, 2)
	wg := sync.WaitGroup{}

	// Start a goroutine to change the description of the port every 100ms.
	wg.Add(1)
	go func() {
		defer wg.Done()
		end := time.Now().Add(5 * time.Second)
		for time.Now().Before(end) {
			if err := rclient.HSet(context.Background(), "PORT|Ethernet1/1/1", "description", time.Now().String()).Err(); err != nil {
				t.Errorf("Failed to set description: %v", err)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Start two Sample subscriptions and collect their responses.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subUpdates := []*pb.SubscribeResponse{}
			ctx, cancel := context.WithTimeout(context.Background(), 5500*time.Millisecond)
			defer cancel()

			stream, err := gClient.Subscribe(ctx, grpc.MaxCallRecvMsgSize(6000000))
			if err != nil {
				t.Error(err.Error())
				return
			}
			if err = stream.Send(subReq); err != nil {
				t.Errorf("Failed to send subscription: %v", err)
				return
			}

			syncReceived := false
			for {
				resp, err := stream.Recv()
				if err != nil {
					break
				}
				if resp.GetSyncResponse() {
					syncReceived = true
					continue
				}
				if syncReceived {
					subUpdates = append(subUpdates, resp)
				}
			}
			updates[i] = subUpdates
		}(i)
		time.Sleep(300 * time.Millisecond)
	}
	wg.Wait()

	if !reflect.DeepEqual(updates[0], updates[1]) {
		t.Errorf("Updates received do not match for identical subscriptions!\nFirst Client's Updates:%v\nSeconds Client's Updates:%v", updates[0], updates[1])
	}
}

func TestDynamicPortBreakoutWithInProgressWait(t *testing.T) {
	tests := []struct {
		name            string
		inProgressDelay time.Duration
		expectFailure   bool
	}{
		{
			name:            "InProgressWaitFail",
			inProgressDelay: 6 * time.Second,
			expectFailure:   true,
		},
		{
			name:            "InProgressWaitSucceed",
			inProgressDelay: 3 * time.Second,
			expectFailure:   false,
		},
	}
	// Load DB snapshots
	prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/json_tests/appl_state_db.txt")
	prepareDbUtil(t, "CONFIG_DB", "", "../testdata/json_tests/config_db.txt")
	prepareDbUtil(t, "STATE_DB", "", "../testdata/json_tests/state_db.txt")

	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()
	s.SsHelper = mockSystemStateHelperSuccess{}

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &pb.SetRequest{
		Replace: []*pb.Update{
			{
				Path: &pb.Path{
					Target: "OC_YANG",
					Elem: []*pb.PathElem{
						{
							Name: "openconfig",
						},
					},
				},
				Val: &pb.TypedValue{
					Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"openconfig-interfaces:interfaces\":{\"interface\":[{\"name\":\"Ethernet1/1/1\"}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}}]}}}}]}}")},
				},
			},
		},
	}

	applStateDB := db.RedisClient(db.ApplStateDB)
	stateDB := db.RedisClient(db.StateDB)

	initPorts := []string{"Ethernet1/4/1", "Ethernet1/4/5"}
	for _, existPort := range initPorts {
		if _, err := applStateDB.HSet(context.Background(), "PORT_STATE:"+existPort, "phase", "pending_delete").Result(); err != nil {
			t.Fatalf("Failed to set pending delete for PORT_STATE: %v: %v", existPort, err)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := stateDB.HSet(context.Background(), "PORT_BREAKOUT|Ethernet1/4/1", "status", "InProgress").Result(); err != nil {
				t.Fatalf("Failed to set InProgress for PORT_BREAKOUT|Ethernet1/4/1: %v", err)
			}

			// CVL will wait up to 5 seconds for a port to finish DPB.
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(test.inProgressDelay)
				if _, err := stateDB.HSet(context.Background(), "PORT_BREAKOUT|Ethernet1/4/1", "status", "").Result(); err != nil {
					t.Errorf("Failed to set InProgress for PORT_BREAKOUT|Ethernet1/4/1: %v", err)
				}
			}()

			_, err = gClient.Set(ctx, req)
			if (err != nil) != test.expectFailure {
				t.Fatalf("SetRequest: expectedFailure=%v, got=%v", test.expectFailure, err)
			}
			wg.Wait()
		})
	}
}

func TestSubscribeFuzzing(t *testing.T) {
	t.Skipf("b/354040122: Test will fail because the server currently removes unsupported paths from the subscription")
	runtime := 15 * time.Second
	numClients := 10

	// Set up the fuzzer
	customFuzzer := func(sub *pb.SubscriptionList, c fuzz.Continue) {
		sub.Prefix = &pb.Path{Origin: c.RandString(), Target: c.RandString()}
		sub.Mode = gnmipb.SubscriptionList_Mode(c.Intn(3))
		sub.Encoding = gnmipb.Encoding(c.Intn(5))

		// Insert updates-only ~10% of the time
		if c.Intn(10) < 1 {
			sub.UpdatesOnly = true
		}

		sub.Subscription = []*pb.Subscription{
			{
				Path: &pb.Path{
					Elem: []*pb.PathElem{
						{Name: c.RandString()},
						{Name: c.RandString()},
						{Name: c.RandString()},
					},
				},
			},
		}

		// Insert a subscription mode ~70% of the time
		if c.Intn(10) < 7 {
			sub.Subscription[0].Mode = gnmipb.SubscriptionMode(c.Intn(3))
		}

		// Insert a sample interval ~30% of the time
		if c.Intn(10) < 3 {
			sub.Subscription[0].SampleInterval = c.RandUint64()
		}

		// Insert suppress-redundant ~30% of the time
		if c.Intn(10) < 3 {
			sub.Subscription[0].SuppressRedundant = true
		}

		// Insert a hearbeat interval ~30% of the time
		if c.Intn(10) < 3 {
			sub.Subscription[0].HeartbeatInterval = c.RandUint64()
		}
	}

	s := createServer(t)
	// s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Turn off all server logs for the duration of the test.
	if n := flag.Lookup("logfirstn"); n != nil {
		origN := n.Value.String()
		n.Value.Set("-1")
		defer n.Value.Set(origN)
	}

	// The server is ready - now a subscription is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	t.Logf("Fuzz testing in progress...")

	var mu sync.Mutex
	var wg sync.WaitGroup
	iterations := uint64(0)
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := fuzz.New().NilChance(0.05).Funcs(customFuzzer)
			for end := time.Now().Add(runtime); time.Now().Before(end); {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
				if err != nil {
					t.Fatalf("Failed to create subscribe client: %v", err)
				}
				var subscription pb.SubscriptionList
				f.Fuzz(&subscription)
				req := &gnmipb.SubscribeRequest{
					Request: &pb.SubscribeRequest_Subscribe{
						Subscribe: &subscription,
					},
				}
				err = stream.Send(req)
				if err != nil {
					t.Fatalf("Failed to send request: %v", err)
				}
				resp, err := stream.Recv()
				if subscription.UpdatesOnly {
					// When UpdatesOnly is set, the error is sent after the sync-response
					resp, err = stream.Recv()
				}
				if err == nil {
					t.Fatalf("Expected error but got %v", resp)
				}
				cancel()

				// Check for stack trace files.
				files, err := os.ReadDir(HostVarLogPath)
				if err != nil {
					t.Fatalf("Failed to read directory: %v", err)
				}
				for _, file := range files {
					if strings.HasPrefix(file.Name(), StackTraceFileNamePrefix) && strings.HasSuffix(file.Name(), StackTraceFileNameSuffix) {
						trace, rErr := os.ReadFile(filepath.Join(HostVarLogPath, file.Name()))
						if rErr == nil {
							t.Log(string(trace))
						}
						if err = os.Remove(filepath.Join(HostVarLogPath, file.Name())); err != nil {
							t.Logf("Cannot delete stack-trace file: %v", err)
						}
						t.Fatalf("Found stack trace file with request: %v", req)
					}
				}
				mu.Lock()
				iterations++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("Number of iterations: %v", iterations)
}

func TestGetFuzzing(t *testing.T) {
	runtime := 15 * time.Second
	numClients := 10

	// Set up the fuzzer
	customFuzzer := func(req *pb.GetRequest, c fuzz.Continue) {
		req.Prefix = &pb.Path{Origin: c.RandString(), Target: c.RandString()}
		req.Path = []*pb.Path{{
			Elem: []*pb.PathElem{
				{Name: c.RandString()},
				{Name: c.RandString()},
				{Name: c.RandString()},
			}}}
		req.Type = pb.GetRequest_DataType(c.Intn(4))
		req.Encoding = gnmipb.Encoding(c.Intn(5))
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Turn off all server logs for the duration of the test.
	if n := flag.Lookup("logfirstn"); n != nil {
		origN := n.Value.String()
		n.Value.Set("-1")
		defer n.Value.Set(origN)
	}

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	t.Logf("Fuzz testing in progress...")

	var mu sync.Mutex
	var wg sync.WaitGroup
	iterations := uint64(0)
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := fuzz.New().NilChance(0.05).Funcs(customFuzzer)
			for end := time.Now().Add(runtime); time.Now().Before(end); {
				var getReq pb.GetRequest
				f.Fuzz(&getReq)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				resp, err := gClient.Get(ctx, &getReq)
				if err == nil {
					t.Fatalf("Expected error but got: %v - err: %v", resp, err)
				}
				cancel()

				// Check for stack trace files.
				files, err := os.ReadDir(HostVarLogPath)
				if err != nil {
					t.Fatalf("Failed to read directory: %v", err)
				}
				for _, file := range files {
					if strings.HasPrefix(file.Name(), StackTraceFileNamePrefix) && strings.HasSuffix(file.Name(), StackTraceFileNameSuffix) {
						trace, rErr := os.ReadFile(filepath.Join(HostVarLogPath, file.Name()))
						if rErr == nil {
							t.Log(string(trace))
						}
						if err = os.Remove(filepath.Join(HostVarLogPath, file.Name())); err != nil {
							t.Logf("Cannot delete stack-trace file: %v", err)
						}
						t.Fatalf("Found stack trace file with request: %v", getReq)
					}
				}
				mu.Lock()
				iterations++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("Number of iterations: %v", iterations)
}

func TestSetFuzzing(t *testing.T) {
	runtime := 15 * time.Second
	numClients := 10

	// Set up the fuzzer
	customFuzzer := func(req *pb.SetRequest, c fuzz.Continue) {
		req.Prefix = &pb.Path{Origin: c.RandString(), Target: c.RandString()}
		req.Delete = []*pb.Path{{Elem: []*pb.PathElem{{Name: c.RandString()}}}}
		req.Replace = []*pb.Update{{
			Path: &pb.Path{Elem: []*pb.PathElem{{Name: c.RandString()}}},
			Val:  &pb.TypedValue{Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(c.RandString())}},
		}}
		req.Update = []*pb.Update{{
			Path: &pb.Path{Elem: []*pb.PathElem{{Name: c.RandString()}}},
			Val:  &pb.TypedValue{Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(c.RandString())}},
		}}
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Turn off all server logs for the duration of the test.
	if n := flag.Lookup("logfirstn"); n != nil {
		origN := n.Value.String()
		n.Value.Set("-1")
		defer n.Value.Set(origN)
	}

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	t.Logf("Fuzz testing in progress...")

	var mu sync.Mutex
	var wg sync.WaitGroup
	iterations := uint64(0)
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := fuzz.New().NilChance(0.05).Funcs(customFuzzer)
			for end := time.Now().Add(runtime); time.Now().Before(end); {
				var setReq pb.SetRequest
				f.Fuzz(&setReq)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				resp, err := gClient.Set(ctx, &setReq)
				if err == nil {
					t.Fatalf("Expected error but got: %v - err: %v", resp, err)
				}
				cancel()

				// Check for stack trace files.
				files, err := os.ReadDir(HostVarLogPath)
				if err != nil {
					t.Fatalf("Failed to read directory: %v", err)
				}
				for _, file := range files {
					if strings.HasPrefix(file.Name(), StackTraceFileNamePrefix) && strings.HasSuffix(file.Name(), StackTraceFileNameSuffix) {
						trace, rErr := os.ReadFile(filepath.Join(HostVarLogPath, file.Name()))
						if rErr == nil {
							t.Log(string(trace))
						}
						if err = os.Remove(filepath.Join(HostVarLogPath, file.Name())); err != nil {
							t.Logf("Cannot delete stack-trace file: %v", err)
						}
						t.Fatalf("Found stack trace file with request: %v", setReq)
					}
				}
				mu.Lock()
				iterations++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("Number of iterations: %v", iterations)
}

func TestXfmr(t *testing.T) {
	tests := []struct {
		name string
		f    func(t *testing.T)
	}{
		{
			name: "DbToYang_sys_state_xfmr with a getter",
			f: func(t *testing.T) {
				var device ygot.GoStruct = &ocbinds.Device{}
				ygot.BuildEmptyTree(device)
				params := transformer.XfmrParams{}
				*params.YgRoot() = &device
				if err := transformer.DbToYang_sys_state_xfmr(params); err != nil {
					t.Logf("Calling transformer.DbToYang_sys_state_xfmr failed: %#v", err)
				}
			},
		},
		{
			name: "DbToYang_sys_state_xfmr with PublicXfmrParams",
			f: func(t *testing.T) {
				var device ygot.GoStruct = &ocbinds.Device{System: &ocbinds.OpenconfigSystem_System{}}
				params := transformer.NewXfmrParams(transformer.PublicXfmrParams{YgRoot: &device})
				if err := transformer.DbToYang_sys_state_xfmr(*params); err != nil {
					t.Logf("Calling transformer.DbToYang_sys_state_xfmr failed: %#v", err)
				}
			},
		},
		{
			name: "PatternGenerator negative test of invalid params",
			f: func(t *testing.T) {
				invalidParam := []string{"str1"}
				if expectEmpty := transformer.PatternGenerator(invalidParam, "xpath"); expectEmpty != "" {
					t.Errorf("Expect \"\", Got %v", expectEmpty)
				}
			},
		},
		{
			name: "PatternGenerator negative test of invalid operation",
			f: func(t *testing.T) {
				invalidParam := []string{"str1", "str2"}
				if expectEmpty := transformer.PatternGenerator(invalidParam, "xpath"); expectEmpty != "" {
					t.Errorf("Expect \"\", Got %v", expectEmpty)
				}
			},
		},
	}
	for _, c := range tests {
		t.Run(c.name, c.f)
	}
}

func TestTableData2MsiUseKey(t *testing.T) {
	tblPath := sdc.CreateTablePath("STATE_DB", "NEIGH_STATE_TABLE", "|", "10.0.0.57")
	newMsi := make(map[string]interface{})
	sdc.TableData2Msi(&tblPath, true, nil, &newMsi)
	newMsiData, _ := json.MarshalIndent(newMsi, "", "  ")
	t.Logf(string(newMsiData))
	expectedMsi := map[string]interface{}{
		"10.0.0.57": map[string]interface{}{
			"peerType": "e-BGP",
			"state":    "Established",
		},
	}
	expectedMsiData, _ := json.MarshalIndent(expectedMsi, "", "  ")
	t.Logf(string(expectedMsiData))

	if !reflect.DeepEqual(newMsi, expectedMsi) {
		t.Errorf("Msi data does not match for use key = true")
	}
}

func TestRecoverFromJSONSerializationPanic(t *testing.T) {
	panicMarshal := func(v interface{}) ([]byte, error) {
		panic("json.Marshal panics and is unable to serialize JSON")
	}
	mock := gomonkey.ApplyFunc(json.Marshal, panicMarshal)
	defer mock.Reset()

	tblPath := sdc.CreateTablePath("STATE_DB", "NEIGH_STATE_TABLE", "|", "10.0.0.57")
	msi := make(map[string]interface{})
	sdc.TableData2Msi(&tblPath, true, nil, &msi)

	typedValue, err := sdc.Msi2TypedValue(msi)
	if typedValue != nil && err != nil {
		t.Errorf("Test should recover from panic and have nil TypedValue/Error after attempting JSON serialization")
	}

}

func TestGnmiSetBatch(t *testing.T) {
	mockCode :=
		`
print('No Yang validation for test mode...')
print('%s')
`
	mock1 := gomonkey.ApplyGlobalVar(&sdc.PyCodeForYang, mockCode)
	defer mock1.Reset()

	sdcfg.Init()
	s := createServer(t)
	go runServer(t, s)

	prepareDbTranslib(t)

	//t.Log("Start gNMI client")
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var emptyRespVal interface{}

	tds := []struct {
		desc          string
		pathTarget    string
		textPbPath    string
		wantRetCode   codes.Code
		wantRespVal   interface{}
		attributeData string
		operation     op_t
		valTest       bool
	}{
		{
			desc:       "Set APPL_DB in batch",
			pathTarget: "",
			textPbPath: `
						origin: "sonic-db",
                        elem: <name: "APPL_DB" > elem: <name: "localhost" > elem:<name:"DASH_QOS" >
                `,
			attributeData: "../testdata/batch.txt",
			wantRetCode:   codes.OK,
			wantRespVal:   emptyRespVal,
			operation:     Replace,
			valTest:       false,
		},
	}

	for _, td := range tds {
		if td.valTest == true {
			// wait for 2 seconds for change to sync
			time.Sleep(2 * time.Second)
			t.Run(td.desc, func(t *testing.T) {
				runTestGet(t, ctx, gClient, td.pathTarget, td.textPbPath, pb.GetRequest_ALL, pb.Encoding_JSON_IETF, td.wantRetCode, td.wantRespVal, td.valTest)
			})
		} else {
			t.Run(td.desc, func(t *testing.T) {
				runTestSet(t, ctx, gClient, td.pathTarget, td.textPbPath, td.wantRetCode, td.wantRespVal, td.attributeData, td.operation)
			})
		}
	}
	s.Stop()
}

func TestGNMINative(t *testing.T) {
	mock1 := gomonkey.ApplyFunc(dbus.SystemBus, func() (conn *dbus.Conn, err error) {
		return &dbus.Conn{}, nil
	})
	defer mock1.Reset()
	mock2 := gomonkey.ApplyMethod(reflect.TypeOf(&dbus.Object{}), "Go", func(obj *dbus.Object, method string, flags dbus.Flags, ch chan *dbus.Call, args ...interface{}) *dbus.Call {
		ret := &dbus.Call{}
		ret.Err = nil
		ret.Body = make([]interface{}, 2)
		ret.Body[0] = int32(0)
		ch <- ret
		return &dbus.Call{}
	})
	defer mock2.Reset()
	mock3 := gomonkey.ApplyFunc(sdc.RunPyCode, func(text string) error { return nil })
	defer mock3.Reset()

	sdcfg.Init()
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()
	ns, _ := sdcfg.GetDbDefaultNamespace()
	initFullConfigDb(t, ns)
	initFullCountersDb(t, ns)

	path, _ := os.Getwd()
	path = filepath.Dir(path)

	// This test is used for single database configuration
	// Run tests not marked with multidb
	cmd := exec.Command("bash", "-c", "cd "+path+" && "+"pytest -m 'not multidb'")
	if result, err := cmd.Output(); err != nil {
		fmt.Println(string(result))
		t.Errorf("Fail to execute pytest: %v", err)
	} else {
		fmt.Println(string(result))
	}

	var counters [int(common_utils.COUNTER_SIZE)]uint64
	err := common_utils.GetMemCounters(&counters)
	if err != nil {
		t.Errorf("Error: Fail to read counters, %v", err)
	}
	for i := 0; i < int(common_utils.COUNTER_SIZE); i++ {
		cnt := common_utils.CounterType(i)
		counterName := cnt.String()
		if counterName == "GNMI set" && counters[i] == 0 {
			t.Errorf("GNMI set counter should not be 0")
		}
		if counterName == "GNMI get" && counters[i] == 0 {
			t.Errorf("GNMI get counter should not be 0")
		}
	}
	s.Stop()
}

// Test configuration with multiple databases
func TestGNMINativeMultiDB(t *testing.T) {
	sdcfg.Init()
	err := test_utils.SetupMultiInstance()
	if err != nil {
		t.Fatalf("error Setting up MultiInstance files with err %T", err)
	}

	/* https://www.gopherguides.com/articles/test-cleanup-in-go-1-14*/
	t.Cleanup(func() {
		if err := test_utils.CleanUpMultiInstance(); err != nil {
			t.Fatalf("error Cleaning up MultiInstance files with err %T", err)

		}
	})

	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	path, _ := os.Getwd()
	path = filepath.Dir(path)

	// This test is used for multiple database configuration
	// Run tests marked with multidb
	cmd := exec.Command("bash", "-c", "cd "+path+" && "+"pytest -m 'multidb'")
	if result, err := cmd.Output(); err != nil {
		fmt.Println(string(result))
		t.Errorf("Fail to execute pytest: %v", err)
	} else {
		fmt.Println(string(result))
	}
}

func TestServerPort(t *testing.T) {
	cfg := testServerConfig(testSrvType)
	cfg.Port = -8080
	s := createCustomServer(t, cfg)
	port := s.Port()
	if port != 0 {
		t.Errorf("Invalid port: %d", port)
	}
	s.Stop()
}

func TestNilServerStop(t *testing.T) {
	// Create a server with nil grpc server, such that s.Stop is called with nil value
	t.Log("Expecting s.Stop to log error as server is nil")
	s := &Server{}
	s.Stop()
}

func TestNilClientDbWriters(t *testing.T) {
	for _, f := range []func(rc *redis.Client){recordSuccessfulSet, recordFailedSet} {
		f(nil)
	}

	if disablePortCyclingErr := disablePortCycling(nil); disablePortCyclingErr == nil {
		t.Error("disablePortCycling(nil) did not return an error")
	}
}

func TestInvalidServer(t *testing.T) {
	s, err := NewServer(nil)
	if s != nil {
		t.Errorf("Should not create invalid server: %v", err)
	}
}

func TestParseOrigin(t *testing.T) {
	var test_paths []*gnmipb.Path
	var err error

	_, err = ParseOrigin(test_paths)
	if err != nil {
		t.Errorf("ParseOrigin failed for empty path: %v", err)
	}

	test_origin := "sonic-test"
	path, err := xpath.ToGNMIPath(test_origin + ":CONFIG_DB/VLAN")
	test_paths = append(test_paths, path)
	origin, err := ParseOrigin(test_paths)
	if err != nil {
		t.Errorf("ParseOrigin failed to get origin: %v", err)
	}
	if origin != test_origin {
		t.Errorf("ParseOrigin return wrong origin: %v", origin)
	}
	test_origin = "sonic-invalid"
	path, err = xpath.ToGNMIPath(test_origin + ":CONFIG_DB/PORT")
	test_paths = append(test_paths, path)
	origin, err = ParseOrigin(test_paths)
	if err == nil {
		t.Errorf("ParseOrigin should fail for conflict")
	}
}

func TestMasterArbitration(t *testing.T) {
	s := createServer(t)
	// Turn on Master Arbitration
	s.ReqFromMaster = ReqFromMasterEnabledMA
	go runServer(t, s)
	defer s.Stop()

	prepareDbTranslib(t)

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("[::1]:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maExt0 := &ext_pb.Extension{
		Ext: &ext_pb.Extension_MasterArbitration{
			MasterArbitration: &ext_pb.MasterArbitration{
				ElectionId: &ext_pb.Uint128{High: 0, Low: 0},
			},
		},
	}
	maExt1 := &ext_pb.Extension{
		Ext: &ext_pb.Extension_MasterArbitration{
			MasterArbitration: &ext_pb.MasterArbitration{
				ElectionId: &ext_pb.Uint128{High: 0, Low: 1},
			},
		},
	}
	maExt1H0L := &ext_pb.Extension{
		Ext: &ext_pb.Extension_MasterArbitration{
			MasterArbitration: &ext_pb.MasterArbitration{
				ElectionId: &ext_pb.Uint128{High: 1, Low: 0},
			},
		},
	}
	regExt := &ext_pb.Extension{
		Ext: &ext_pb.Extension_RegisteredExt{
			RegisteredExt: &ext_pb.RegisteredExtension{},
		},
	}

	// By default ElectionID starts from 0 so this test does not change it.
	t.Run("MasterArbitrationOnElectionIdZero", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{maExt0},
		}
		_, err = gClient.Set(ctx, req)
		if err != nil {
			t.Fatal("Did not expected an error: " + err.Error())
		}
		if _, ok := status.FromError(err); !ok {
			t.Fatal("Got a non-grpc error from grpc call")
		}
		reqEid0 := maExt0.GetMasterArbitration().GetElectionId()
		expectedEID0 := uint128{High: reqEid0.GetHigh(), Low: reqEid0.GetLow()}
		if s.masterEID.Compare(&expectedEID0) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID0, s.masterEID)
		}
	})
	// After this test ElectionID is one.
	t.Run("MasterArbitrationOnElectionIdZeroThenOne", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{maExt0},
		}
		if _, err = gClient.Set(ctx, req); err != nil {
			t.Fatal("Did not expected an error: " + err.Error())
		}
		reqEid0 := maExt0.GetMasterArbitration().GetElectionId()
		expectedEID0 := uint128{High: reqEid0.GetHigh(), Low: reqEid0.GetLow()}
		if s.masterEID.Compare(&expectedEID0) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID0, s.masterEID)
		}
		req = &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{maExt1},
		}
		if _, err = gClient.Set(ctx, req); err != nil {
			t.Fatal("Set gRPC failed")
		}
		reqEid1 := maExt1.GetMasterArbitration().GetElectionId()
		expectedEID1 := uint128{High: reqEid1.GetHigh(), Low: reqEid1.GetLow()}
		if s.masterEID.Compare(&expectedEID1) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID1, s.masterEID)
		}
	})
	// Multiple ElectionIDs with the last being one.
	t.Run("MasterArbitrationOnElectionIdMultipleIdsZeroThenOne", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{maExt0, maExt1, regExt},
		}
		_, err = gClient.Set(ctx, req)
		if err != nil {
			t.Fatal("Did not expected an error: " + err.Error())
		}
		if _, ok := status.FromError(err); !ok {
			t.Fatal("Got a non-grpc error from grpc call")
		}
		reqEid1 := maExt1.GetMasterArbitration().GetElectionId()
		expectedEID1 := uint128{High: reqEid1.GetHigh(), Low: reqEid1.GetLow()}
		if s.masterEID.Compare(&expectedEID1) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID1, s.masterEID)
		}
	})
	// ElectionIDs with the high word set to 1 and low word to 0.
	t.Run("MasterArbitrationOnElectionIdHighOne", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{maExt1H0L},
		}
		_, err = gClient.Set(ctx, req)
		if err != nil {
			t.Fatal("Did not expected an error: " + err.Error())
		}
		if _, ok := status.FromError(err); !ok {
			t.Fatal("Got a non-grpc error from grpc call")
		}
		reqEid10 := maExt1H0L.GetMasterArbitration().GetElectionId()
		expectedEID10 := uint128{High: reqEid10.GetHigh(), Low: reqEid10.GetLow()}
		if s.masterEID.Compare(&expectedEID10) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID10, s.masterEID)
		}
	})
	// As the ElectionID is one, a request with ElectionID==0 will fail.
	// Also a request without Election ID will fail.
	t.Run("MasterArbitrationOnElectionIdZeroThenNone", func(t *testing.T) {
		req := &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{maExt0},
		}
		_, err = gClient.Set(ctx, req)
		if err == nil {
			t.Fatal("Expected a PermissionDenied error")
		}
		ret, ok := status.FromError(err)
		if !ok {
			t.Fatal("Got a non-grpc error from grpc call")
		}
		if ret.Code() != codes.PermissionDenied {
			t.Fatalf("Expected PermissionDenied. Got %v", ret.Code())
		}
		reqEid10 := maExt1H0L.GetMasterArbitration().GetElectionId()
		expectedEID10 := uint128{High: reqEid10.GetHigh(), Low: reqEid10.GetLow()}
		if s.masterEID.Compare(&expectedEID10) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID10, s.masterEID)
		}
		req = &pb.SetRequest{
			Prefix: &pb.Path{Elem: []*pb.PathElem{{Name: "interfaces"}}},
			Update: []*pb.Update{
				newPbUpdate("interface[name=Ethernet0]/config/mtu", `{"mtu": 9104}`),
			},
			Extension: []*ext_pb.Extension{},
		}
		_, err = gClient.Set(ctx, req)
		if err != nil {
			t.Fatal("Expected a successful set call.")
		}
		if s.masterEID.Compare(&expectedEID10) != 0 {
			t.Fatalf("Master EID update failed. Want %v, got %v", expectedEID10, s.masterEID)
		}
	})
}

func TestSetTOSToAF4(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	//ctx = peer.NewContext(ctx, nil)
	if setTOSToAF4(ctx) == nil {
		t.Fatal("Expected error with missing peer")
	}
	p := peer.Peer{}
	ctx = peer.NewContext(ctx, &p)
	if setTOSToAF4(ctx) == nil {
		t.Fatal("Expected error with empty peer, nil address")
	}
	peerAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}
	p = peer.Peer{Addr: peerAddr}
	ctx = peer.NewContext(ctx, &p)
	if setTOSToAF4(ctx) != nil {
		t.Fatal("Connection miss threw an unexpected error")
	}
}

func TestHasMasterEID(t *testing.T) {
	ext := []*ext_pb.Extension{}
	if hasMasterEID(ext) {
		t.Fatalf("Expected extension to be missing!")
	}
	maExt := &ext_pb.Extension{
		Ext: &ext_pb.Extension_MasterArbitration{
			MasterArbitration: &ext_pb.MasterArbitration{
				ElectionId: &ext_pb.Uint128{High: 0, Low: 0},
			},
		},
	}
	ext = append(ext, maExt)
	if !hasMasterEID(ext) {
		t.Fatalf("Expected extension to be present!")
	}

}

func TestCleanupIPToConn(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	cleanupIPToConn(ctx)
	p := peer.Peer{}
	ctx = peer.NewContext(ctx, &p)
	cleanupIPToConn(ctx)
}

func TestSaveOnSet(t *testing.T) {
	// Fail client creation
	dbusCaller = nil
	if err := SaveOnSetEnabled(); err == nil {
		t.Error("Expected Client Failure")
	}

	// Fail Dbus call
	dbusCaller = &ssc.FailDbusCaller{}
	if err := SaveOnSetEnabled(); err == nil {
		t.Error("Expected DBUS failure")
	}

	// Successful Dbus call
	dbusCaller = &ssc.FakeDbusCaller{}
	if err := SaveOnSetEnabled(); err != nil {
		t.Error("Unexpected DBUS failure")
	}
}

func TestPopulateAuthStructByCommonName(t *testing.T) {
	// check auth with nil cert name
	err := PopulateAuthStructByCommonName("certname1", nil, "")
	if err == nil {
		t.Errorf("PopulateAuthStructByCommonName with empty config table should failed: %v", err)
	}
}

func CreateAuthorizationCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cert := x509.Certificate{
		Subject: pkix.Name{
			CommonName: "certname1",
		},
	}
	verifiedCerts := make([][]*x509.Certificate, 1)
	verifiedCerts[0] = make([]*x509.Certificate, 1)
	verifiedCerts[0][0] = &cert
	p := peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				VerifiedChains: verifiedCerts,
			},
		},
	}
	ctx = peer.NewContext(ctx, &p)
	return ctx, cancel
}

func TestClientCertAuthenAndAuthor(t *testing.T) {
	if !swsscommon.SonicDBConfigIsInit() {
		swsscommon.SonicDBConfigInitialize()
	}

	var configDb = swsscommon.NewDBConnector("CONFIG_DB", uint(0), true)
	var gnmiTable = swsscommon.NewTable(configDb, "GNMI_CLIENT_CERT")
	configDb.Flushdb()

	// initialize err variable
	err := status.Error(codes.Unauthenticated, "")

	// when config table is empty, will authorize with PopulateAuthStruct
	mockpopulate := gomonkey.ApplyFunc(PopulateAuthStruct, func(username string, auth *common_utils.AuthInfo, r []string) error {
		return nil
	})
	defer mockpopulate.Reset()

	// check auth with nil cert name
	ctx, cancel := CreateAuthorizationCtx()
	ctx, err = ClientCertAuthenAndAuthor(ctx, "")
	if err != nil {
		t.Errorf("CommonNameMatch with empty config table should success: %v", err)
	}

	cancel()

	// check get 1 cert name
	ctx, cancel = CreateAuthorizationCtx()
	configDb.Flushdb()
	gnmiTable.Hset("certname1", "role", "role1")
	ctx, err = ClientCertAuthenAndAuthor(ctx, "GNMI_CLIENT_CERT")
	if err != nil {
		t.Errorf("CommonNameMatch with correct cert name should success: %v", err)
	}

	cancel()

	// check get multiple cert names
	ctx, cancel = CreateAuthorizationCtx()
	configDb.Flushdb()
	gnmiTable.Hset("certname1", "role", "role1")
	gnmiTable.Hset("certname2", "role", "role2")
	ctx, err = ClientCertAuthenAndAuthor(ctx, "GNMI_CLIENT_CERT")
	if err != nil {
		t.Errorf("CommonNameMatch with correct cert name should success: %v", err)
	}

	cancel()

	// check a invalid cert cname
	ctx, cancel = CreateAuthorizationCtx()
	configDb.Flushdb()
	gnmiTable.Hset("certname2", "role", "role2")
	ctx, err = ClientCertAuthenAndAuthor(ctx, "GNMI_CLIENT_CERT")
	if err == nil {
		t.Errorf("CommonNameMatch with invalid cert name should fail: %v", err)
	}

	cancel()

	swsscommon.DeleteTable(gnmiTable)
	swsscommon.DeleteDBConnector(configDb)
}

type MockServerStream struct {
	grpc.ServerStream
}

func (x *MockServerStream) Context() context.Context {
	return context.Background()
}

type MockPingServer struct {
	MockServerStream
}

func (x *MockPingServer) Send(m *gnoi_system_pb.PingResponse) error {
	return nil
}

type MockTracerouteServer struct {
	MockServerStream
}

func (x *MockTracerouteServer) Send(m *gnoi_system_pb.TracerouteResponse) error {
	return nil
}

type MockSetPackageServer struct {
	MockServerStream
}

func (x *MockSetPackageServer) Send(m *gnoi_system_pb.SetPackageResponse) error {
	return nil
}

func (x *MockSetPackageServer) SendAndClose(m *gnoi_system_pb.SetPackageResponse) error {
	return nil
}

func (x *MockSetPackageServer) Recv() (*gnoi_system_pb.SetPackageRequest, error) {
	return nil, nil
}

func TestGnoiAuthorization(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	mockAuthenticate := gomonkey.ApplyFunc(s.Authenticate, func(ctx context.Context, req *spb_jwt.AuthenticateRequest) (*spb_jwt.AuthenticateResponse, error) {
		return nil, nil
	})
	defer mockAuthenticate.Reset()

	err := s.Ping(new(gnoi_system_pb.PingRequest), new(MockPingServer))
	if err == nil {
		t.Errorf("Ping should failed, because not implement.")
	}

	s.Traceroute(new(gnoi_system_pb.TracerouteRequest), new(MockTracerouteServer))
	if err == nil {
		t.Errorf("Traceroute should failed, because not implement.")
	}

	s.SetPackage(new(MockSetPackageServer))
	if err == nil {
		t.Errorf("SetPackage should failed, because not implement.")
	}

	peerAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: peerAddr})
	s.SwitchControlProcessor(ctx, new(gnoi_system_pb.SwitchControlProcessorRequest))
	if err == nil {
		t.Errorf("SwitchControlProcessor should failed, because not implement.")
	}

	s.Refresh(ctx, new(spb_jwt.RefreshRequest))
	if err == nil {
		t.Errorf("Refresh should failed, because not implement.")
	}

	s.ClearNeighbors(ctx, new(sgpb.ClearNeighborsRequest))
	if err == nil {
		t.Errorf("ClearNeighbors should failed, because not implement.")
	}

	s.CopyConfig(ctx, new(sgpb.CopyConfigRequest))
	if err == nil {
		t.Errorf("CopyConfig should failed, because not implement.")
	}

	s.Stop()
}

func TestGnmiGenerated(t *testing.T) {
	var hook jtest.JSONFilter

	// Initialize config_db if not running tests on a sonic switch.
	// Flushes all existing data in config_db.
	if !onSonicSwitch() {
		prepareDbUtil(t, "CONFIG_DB", "", "../testdata/json_tests/config_db.txt")
		prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/json_tests/appl_state_db.txt")
		prepareDbUtil(t, "COUNTERS_DB", "", "../testdata/json_tests/counters_db.txt")
		prepareDbUtil(t, "STATE_DB", "", "../testdata/json_tests/state_db.txt")
		prepareDbUtil(t, "ASIC_DB", "", "../testdata/json_tests/asic_db.txt")
		transformer.TestingQosClearState()
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	ctx = context.WithValue(ctx, jtest.CtxIDGChannel, conn)
	defer cancel()

	savedSsHelper := s.SsHelper
	s.SsHelper = mockSystemStateHelperSuccess{}

	var tds []jtest.UnitTest
	files, err := filepath.Glob("../testdata/json_tests/generated/*.json")
	if err != nil {
		t.Fatal("Error opening generated test files")
	}
	for _, f := range files {
		tst, err := jtest.UnitTestFromFile(t, ctx, f, hook)
		if err != nil {
			t.Fatal(err)
		}
		tds = append(tds, tst)
	}

	for _, td := range tds {
		jtest.RunTest(t, ctx, &td)
		time.Sleep(50 * time.Millisecond) // Give time for change to register.
	}
	s.SsHelper.Close()
	s.SsHelper = savedSsHelper
}

func TestGnmiGetSet(t *testing.T) {
	var hook jtest.JSONFilter

	dbInit := func(t *testing.T) {
		if !onSonicSwitch() {
			dbInits := []struct {
				name string
				file string
			}{
				{"CONFIG_DB", "../testdata/json_tests/config_db.txt"},
				{"APPL_STATE_DB", "../testdata/json_tests/appl_state_db.txt"},
				{"COUNTERS_DB", "../testdata/json_tests/counters_db.txt"},
				{"STATE_DB", "../testdata/json_tests/state_db.txt"},
				{"ASIC_DB", "../testdata/json_tests/asic_db.txt"},
			}
			ns, _ := sdcfg.GetDbDefaultNamespace()
			for _, dbI := range dbInits {
				dbId, err := sdcfg.GetDbId(dbI.name, ns)
				if err != nil {
					t.Fatalf("failed to get DB Id for %s, %v", dbI.name, err)
				}
				rclient := getRedisClientN(t, dbId, ns)
				defer db.CloseRedisClient(rclient)
				// Clear all existing entries from the DB.
				rclient.FlushDB(context.Background())
				// Load the entries specified in the test's input file.
				prepareDbUtil(t, dbI.name, "", dbI.file)
			}
			transformer.TestingQosClearState()
		}
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	ctx = context.WithValue(ctx, jtest.CtxIDGChannel, conn)
	defer cancel()

	savedSsHelper := s.SsHelper
	s.SsHelper = mockSystemStateHelperSuccess{}

	fileFilter := os.Getenv("JSON_TGT")
	if fileFilter == "" {
		fileFilter = "*.json"
	}
	var tds []jtest.UnitTest
	files, err := filepath.Glob("../testdata/json_tests/getset/" + fileFilter)
	if err != nil {
		t.Fatal("Error opening test files")
	}
	for _, f := range files {
		tst, err := jtest.UnitTestFromFile(t, ctx, f, hook)
		if err != nil {
			t.Fatalf("File: %s: %v", f, err)
		}
		tds = append(tds, tst)
	}

	for _, td := range tds {
		dbInit(t) // Reset the contents of the DBs before each test
		jtest.RunTest(t, ctx, &td)
		time.Sleep(250 * time.Millisecond) // Give time for change to register.
	}
	s.SsHelper.Close()
	s.SsHelper = savedSsHelper
}

func TestGnmiSubscribeJSON(t *testing.T) {
	var hook jtest.JSONFilter

	// Initialize config_db if not running tests on a sonic switch.
	dbInit := func(t *testing.T) {
		if !onSonicSwitch() {
			dbInits := []struct {
				name string
				file string
			}{
				{"CONFIG_DB", "../testdata/json_tests/subscribe/config_db.txt"},
				{"APPL_STATE_DB", "../testdata/json_tests/subscribe/appl_state_db.txt"},
				{"COUNTERS_DB", "../testdata/json_tests/subscribe/counters_db.txt"},
				{"STATE_DB", "../testdata/json_tests/subscribe/state_db.txt"},
				{"ASIC_DB", "../testdata/json_tests/subscribe/asic_db.txt"},
			}
			ns, _ := sdcfg.GetDbDefaultNamespace()
			for _, dbI := range dbInits {
				dbId, err := sdcfg.GetDbId(dbI.name, ns)
				if err != nil {
					t.Fatalf("failed to get DB Id for %s, %v", dbI.name, err)
				}
				rclient := getRedisClientN(t, dbId, ns)
				defer db.CloseRedisClient(rclient)
				// Clear all existing entries from the DB.
				rclient.FlushDB(context.Background())
				// Load the entries specified in the test's input file.
				prepareDbUtil(t, dbI.name, "", dbI.file)
			}
			transformer.TestingQosClearState()
		}
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	ctx = context.WithValue(ctx, jtest.CtxIDGChannel, conn)

	defer cancel()

	fileFilter := os.Getenv("JSON_TGT")
	if fileFilter == "" {
		fileFilter = "*.json"
	}
	var tds []jtest.UnitTest
	files, err := filepath.Glob("../testdata/json_tests/subscribe/" + fileFilter)
	if err != nil {
		t.Fatal("Error opening test files")
	}
	for _, f := range files {
		tst, err := jtest.UnitTestFromFile(t, ctx, f, hook)
		if err != nil {
			t.Fatal(err)
		}
		tds = append(tds, tst)
	}

	for _, td := range tds {
		dbInit(t) // Reset the contents of the DBs before each test
		jtest.RunTest(t, ctx, &td)
		time.Sleep(250 * time.Millisecond) // Give time for change to register.
	}
}

// TestGetScalar performs a gNMI Get with a proto encoding (as opposed to the
// default JSON IETF encoding) and expects to receive a scalar value in the
// response instead of a json encoded value.
// This is done for each type of Get (Get-All, Get-State, etc.) and also covers
// a case where no data is matched (Get-Config to a state path).
func TestGetScalar(t *testing.T) {
	const clientTimeout = 5 * time.Minute
	const key_sep = "|"
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Clear the DBs so we are starting in a known state
	ns, _ := sdcfg.GetDbDefaultNamespace()
	cfgDbId, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	sttDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	for _, dbNum := range [2]int{cfgDbId, sttDbId} {
		rclient := getRedisClientN(t, dbNum, ns)
		defer db.CloseRedisClient(rclient)
		rclient.FlushDB(context.Background())
	}

	// Enable key space notifications; TODO use an API rather than redis-cli
	os.Setenv("PATH", "/usr/bin:/sbin:/bin:/usr/local/bin")
	cmd := exec.Command("redis-cli", "config", "set", "notify-keyspace-events", "KEA")
	_, err = cmd.Output()
	if err != nil {
		t.Fatal("failed to enable redis keyspace notification ", err)
	}

	// Connect our client
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	// Populate the State DB
	{
		rclient := getRedisClientN(t, sttDbId, ns)
		defer db.CloseRedisClient(rclient)
		key := "BOOT_INFO" + key_sep + "system"
		data := map[string]interface{}{"boot-type": "warm_boot",
			"warmboot-count":          "3",
			"last-coldboot-timestamp": "02/12/2024 03:18:18 +0000 UTC",
			"last-coldboot-version":   "gpins_release_20240202_17_prod_RC00"}
		rclient.HMSet(context.Background(), key, data)
		key = "CHASSIS_INFO" + key_sep + "chassis"
		data = map[string]interface{}{"boot-time": "10/18/2023 03:18:18 +0000 UTC"}
		rclient.HMSet(context.Background(), key, data)
	}
	// Populate the Config DB
	{
		rclient := getRedisClientN(t, cfgDbId, ns)
		defer db.CloseRedisClient(rclient)
		key := "DEVICE_METADATA" + key_sep + "localhost"
		data := map[string]interface{}{"hostname": "sonic",
			"platform": "test",
			"mac":      "00:01:02:03:04:05"}
		rclient.HMSet(context.Background(), key, data)
	}

	for _, reqDataType := range []pb.GetRequest_DataType{pb.GetRequest_STATE, pb.GetRequest_ALL, pb.GetRequest_OPERATIONAL, pb.GetRequest_CONFIG} {
		gStart := time.Now()
		desc := fmt.Sprintf("PROTO Encoded Get-%s", reqDataType)
		pathTgt := "OC_YANG"
		pbPath := pathToPb("/openconfig-system:system/state/boot-time")
		expRetCode := codes.OK
		var expResp interface{}
		expResp = uint64(1697599098000000000)
		if reqDataType == pb.GetRequest_CONFIG {
			expResp = nil
		}

		t.Run(desc, func(t *testing.T) {
			runTestGet(t, ctx, gClient, pathTgt, pbPath, reqDataType, pb.Encoding_PROTO, expRetCode, expResp, true)
		})
		gTime := time.Since(gStart)
		t.Logf("TestGetSysBootTime: Get-%s: took %s", reqDataType, gTime)
	}
}

// TestGetEmptyTree tests a gNMI Get request which returns no data.
// The request is made to the enclosing container of a list and the
// DBs are cleared prior to the get to ensure no keys will be present.
// The Get operation is performed with both JSON and PROTO encodings.
func TestGetEmptyTree(t *testing.T) {
	const clientTimeout = 5 * time.Minute
	const key_sep = "|"
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Clear the DBs so we are starting in a known state
	ns, _ := sdcfg.GetDbDefaultNamespace()
	cfgDbId, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	sttDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	for _, dbNum := range [2]int{cfgDbId, sttDbId} {
		rclient := getRedisClientN(t, dbNum, ns)
		defer db.CloseRedisClient(rclient)
		rclient.FlushDB(context.Background())
	}

	// Enable key space notifications; TODO use an API rather than redis-cli
	os.Setenv("PATH", "/usr/bin:/sbin:/bin:/usr/local/bin")
	cmd := exec.Command("redis-cli", "config", "set", "notify-keyspace-events", "KEA")
	_, err = cmd.Output()
	if err != nil {
		t.Fatal("failed to enable redis keyspace notification ", err)
	}

	// Connect our client
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	// Clear the State DB
	{
		rclient := getRedisClientN(t, sttDbId, ns)
		defer db.CloseRedisClient(rclient)
		rclient.FlushDB(context.Background())
	}
	// Clear the Config DB
	{
		rclient := getRedisClientN(t, cfgDbId, ns)
		defer db.CloseRedisClient(rclient)
		rclient.FlushDB(context.Background())
	}

	for _, encoding := range []pb.Encoding{pb.Encoding_PROTO, pb.Encoding_JSON, pb.Encoding_JSON_IETF} {
		gStart := time.Now()
		desc := fmt.Sprintf("%s Encoded Get to empty target", encoding)
		pathTgt := "OC_YANG"
		pbPath := pathToPb("/openconfig-interfaces:interfaces")
		expRetCode := codes.OK
		var expResp interface{}
		if encoding == pb.Encoding_PROTO {
			expResp = nil
		} else {
			expResp = "{}"
		}

		t.Run(desc, func(t *testing.T) {
			runTestGet(t, ctx, gClient, pathTgt, pbPath, pb.GetRequest_ALL, encoding, expRetCode, expResp, true)
		})
		gTime := time.Since(gStart)
		t.Logf("Empty Target Path Get: took %s", gTime)
	}
}

// TestGnmiGetType tests a gNMI Get request which specifies the type of data to get.
// Requests are made with either Get-Config, Get-Operational, or Get-State to a
// path of each type (config, state, operational).  In the event of a type mismatch
// the Get should be successful but return no data.
func TestGnmiGetType(t *testing.T) {
	const clientTimeout = 5 * time.Minute
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()
	prepareDbTranslib(t)

	// Connect our client
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.NewClient(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	cfgPath := "/openconfig-interfaces:interfaces/interface[name=Ethernet4]/config/mtu"
	oprPath := "/openconfig-interfaces:interfaces/interface[name=Ethernet4]/state/oper-status"
	sttPath := "/openconfig-interfaces:interfaces/interface[name=Ethernet4]/state/mtu"
	pathInfos := []struct {
		path      string
		pathType  string
		respJson  string
		respProto interface{}
	}{
		{
			path:      cfgPath,
			pathType:  "config",
			respJson:  "{\"openconfig-interfaces:mtu\": 9122}",
			respProto: uint64(9122),
		},
		{
			path:      oprPath,
			pathType:  "operational",
			respJson:  "{\"openconfig-interfaces:oper-status\": \"DOWN\"}",
			respProto: "DOWN",
		},
		{
			path:      sttPath,
			pathType:  "state",
			respJson:  "{\"openconfig-interfaces:mtu\": 9122}",
			respProto: uint64(9122),
		},
	}
	for _, encoding := range []pb.Encoding{pb.Encoding_PROTO, pb.Encoding_JSON, pb.Encoding_JSON_IETF} {
		for _, reqDataType := range []pb.GetRequest_DataType{pb.GetRequest_STATE, pb.GetRequest_OPERATIONAL, pb.GetRequest_CONFIG} {
			for _, pathInfo := range pathInfos {
				// If the Get-Type doesn't match the path's type an empty response is expected.
				// Note that Get-State will match both state and operational paths (i.e. non-config)
				mismatch := reqDataType == pb.GetRequest_CONFIG && pathInfo.pathType != "config" ||
					reqDataType == pb.GetRequest_OPERATIONAL && pathInfo.pathType != "operational" ||
					reqDataType == pb.GetRequest_STATE && pathInfo.pathType == "config"
				var expResp interface{}
				if encoding == pb.Encoding_PROTO {
					expResp = nil
					if !mismatch {
						expResp = pathInfo.respProto
					}
				} else {
					expResp = "{}"
					if !mismatch {
						expResp = pathInfo.respJson
					}
				}
				tStart := time.Now()
				desc := fmt.Sprintf("%s Encoded Get-%s to %s path", encoding, reqDataType, pathInfo.pathType)
				t.Run(desc, func(t *testing.T) {
					runTestGet(t, ctx, gClient, "OC_YANG", pathToPb(pathInfo.path), reqDataType, encoding, codes.OK, expResp, true)
				})
				tTaken := time.Since(tStart)
				t.Logf("Get took %s", tTaken)
			}
		}
	}
}

func TestGnmiRootGetTypes(t *testing.T) {
	const clientTimeout = 5 * time.Minute

	// Clear all existing entries from the DB.
	ns, _ := sdcfg.GetDbDefaultNamespace()
	for _, dbName := range []string{"CONFIG_DB", "APPL_STATE_DB", "COUNTERS_DB", "STATE_DB", "ASIC_DB"} {
		dbId, err := sdcfg.GetDbId(dbName, ns)
		if err != nil {
			t.Fatalf("failed to get DB Id for %s, %v", dbName, err)
		}
		rclient := getRedisClientN(t, dbId, ns)
		defer db.CloseRedisClient(rclient)
		rclient.FlushDB(context.Background())
	}

	s := createServer(t)
	s.config.EnableTranslation = true
	s.config.CacheResponses = false
	go runServer(t, s)
	defer s.Stop()
	prepareDbTranslib(t)

	// Connect our client
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.NewClient(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	for _, reqDataType := range []pb.GetRequest_DataType{pb.GetRequest_ALL, pb.GetRequest_CONFIG, pb.GetRequest_STATE, pb.GetRequest_OPERATIONAL} {
		desc := fmt.Sprintf("Get-%s", reqDataType)
		t.Run(desc, func(t *testing.T) {
			prefix := pb.Path{Origin: "openconfig", Target: "ThatTarget"}
			req := &pb.GetRequest{
				Prefix:   &prefix,
				Path:     nil,
				Encoding: pb.Encoding_JSON_IETF,
				Type:     reqDataType,
			}

			resp, err := gClient.Get(ctx, req)

			gotRetStatus, ok := status.FromError(err)
			if !ok {
				t.Fatal("got a non-grpc error (%v) from grpc call", ok)
			}
			if gotRetStatus.Code() != codes.OK {
				t.Log("err: ", err)
				t.Fatalf("got return code %v, want %v", gotRetStatus.Code(), codes.OK)
			}
			if resp == nil {
				t.Fatalf("got nil response")
			}

			notifs := resp.GetNotification()
			if len(notifs) != 1 {
				t.Fatalf("got %d notifications, expected one", len(notifs))
			}
			updates := notifs[0].GetUpdate()
			if len(updates) != 1 {
				t.Fatalf("got %d updates, expected one", len(updates))
			}
			update := updates[0].GetVal()
			jsonVal := update.GetJsonIetfVal()
			if jsonVal == nil {
				t.Fatalf("got nil json val")
			}
			strVal := string(jsonVal)

			// Sanity check that we received something by ensuring we have a couple of top-level
			// module names in the response.  This is not an exhaustive check.
			if !strings.Contains(strVal, "interfaces") || !strings.Contains(strVal, "components") {
				t.Fatalf("Did not receive expected get response data: %s", strVal)
			}
			var wantCfg, wantStt, wantOpr bool
			switch reqDataType {
			case pb.GetRequest_ALL:
				wantCfg = true
				wantStt = true
				wantOpr = true
			case pb.GetRequest_CONFIG:
				wantCfg = true
			case pb.GetRequest_STATE:
				wantStt = true
				wantOpr = true
			case pb.GetRequest_OPERATIONAL:
				// Ideally "want state" would be false since we expect read-only leaves without
				// corresponding config leaves.  However, the check for "state" is very coarse
				// and only looks for paths under a "state" container which may be a mix of
				// both state and operational.
				wantStt = true
				wantOpr = true
			}

			hasCfg := strings.Contains(strVal, "\"config\"")
			hasStt := strings.Contains(strVal, "\"state\"")
			// There is no single string which identifies an operational path.  Therefore we
			// are doing a partial check by looking for a string corresponding to a specific
			// leaf that we know is operational.
			hasOpr := strings.Contains(strVal, "\"oper-status\"")

			if wantCfg != hasCfg || wantStt != hasStt || wantOpr != hasOpr {
				t.Fatalf("wantCfg %v, hasCfg %v, wantStt %v, hasStt %v, wantOpr %v, hasOpr %v", wantCfg, hasCfg, wantStt, hasStt, wantOpr, hasOpr)
			}
		})
	}
}

func TestOpticalSwitch(t *testing.T) {
	const clientTimeout = 15 * time.Minute
	const entryCnt = 300
	const tbl_cfg = "OCS_XCONNECTS"
	const tbl_stt = "OCS_XCONNECTS"
	const key_sep = "|"
	const key_prefix_cfg = tbl_cfg + key_sep
	const key_prefix_stt = tbl_stt + key_sep

	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// Clear the DBs so we are starting in a known state
	ns, _ := sdcfg.GetDbDefaultNamespace()
	cfgDbId, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	sttDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	for _, dbNum := range [2]int{cfgDbId, sttDbId} {
		rclient := getRedisClientN(t, dbNum, ns)
		defer db.CloseRedisClient(rclient)
		rclient.FlushDB(context.Background())
	}
	// Enable key space notifications; TODO use an API rather than redis-cli
	os.Setenv("PATH", "/usr/bin:/sbin:/bin:/usr/local/bin")
	cmd := exec.Command("redis-cli", "config", "set", "notify-keyspace-events", "KEA")
	_, err = cmd.Output()
	if err != nil {
		t.Fatal("failed to enable redis keyspace notification ", err)
	}

	// Connect our client
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	// A type and a few helpers to work with the data stored in the yang tree.
	type EntryFlds struct {
		PeerSlotNum int
		PeerPortNum int
	}
	type Entry struct {
		SlotNum   int
		PortNum   int
		Config    EntryFlds
		State     EntryFlds
		ConfigVal bool
		StateVal  bool
	}
	newRandEntries := func(rng *rand.Rand, cnt int) []Entry {
		rv := make([]Entry, cnt)
		// For a two-key list, generate two lists of values, a list for the first
		// key and a list for the second key.  The cross product of these lists will
		// then be used to generate our entries.  This ensures that for a given
		// value of one key there are multiple values for the other, i.e. k1|* and
		// *|k2 would cover more than one entry.
		limit := int(math.Sqrt(float64(cnt)))
		var k1 []int
		var k2 []int
		seen := map[int]bool{}
		for len(k1) < limit {
			x := rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32
			y := rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32
			if _, inMap := seen[x]; inMap {
				continue
			}
			if _, inMap := seen[y]; inMap {
				continue
			}
			seen[x] = true
			seen[y] = true
			k1 = append(k1, x)
			k2 = append(k2, y)
		}
		added := 0
		for _, x := range k1 {
			for _, y := range k2 {
				rv[added] = Entry{
					SlotNum: x,
					PortNum: y,
					Config: EntryFlds{
						PeerSlotNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32,
						PeerPortNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32},
					State: EntryFlds{
						PeerSlotNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32,
						PeerPortNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32},
					ConfigVal: true,
					StateVal:  true,
				}
				added++
			}
		}
		// We may be a few entries short of our target number of entries due to the
		// sqrt value used above.  Just add random keys to reach our target.  This
		// step also ensures we have some unique key values (i.e. k1|* and *|k2
		// match a single key).
		for added < cap(rv) {
			x := rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32
			y := rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32
			if _, inMap := seen[x]; inMap {
				continue
			}
			if _, inMap := seen[y]; inMap {
				continue
			}
			seen[x] = true
			seen[y] = true
			rv[added] = Entry{
				SlotNum: x,
				PortNum: y,
				Config: EntryFlds{
					PeerSlotNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32,
					PeerPortNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32},
				State: EntryFlds{
					PeerSlotNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32,
					PeerPortNum: rng.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32},
				ConfigVal: true,
				StateVal:  true,
			}
			added++
		}
		return rv
	}
	entryCntrToJSONStr := func(e *Entry, flds *EntryFlds) string {
		return fmt.Sprintf("{\"slot-number\": %d, \"port-number\": %d, \"peer-slot-number\": %d, \"peer-port-number\": %d}",
			e.SlotNum, e.PortNum, flds.PeerSlotNum, flds.PeerPortNum)
	}
	entryToJSONStr := func(e *Entry, config, state bool) string {
		rv := fmt.Sprintf("{\"slot-number\": %d, \"port-number\": %d", e.SlotNum, e.PortNum)
		suffix := "}"
		if config {
			rv = rv + ", \"config\": " + entryCntrToJSONStr(e, &e.Config)
		}
		if state {
			rv = rv + ", \"state\": " + entryCntrToJSONStr(e, &e.State)
		}
		rv = rv + suffix
		return rv
	}
	entriesToJSONList := func(entries []Entry, config, state bool) string {
		rv := "["
		for _, e := range entries {
			rv = rv + entryToJSONStr(&e, config, state) + ", "
		}
		// Cut the trailing comma and space (", ") if it exists
		if len(rv) > 0 {
			rv = rv[:len(rv)-2]
		}
		rv = rv + "]"
		return rv
	}
	entryEqual := func(a, b *Entry) bool {
		if a.SlotNum != b.SlotNum || a.PortNum != b.PortNum {
			return false
		}
		checked := false
		if a.ConfigVal && b.ConfigVal {
			if a.Config.PeerSlotNum != b.Config.PeerSlotNum || a.Config.PeerPortNum != b.Config.PeerPortNum {
				return false
			}
			checked = true
		}
		if a.StateVal && b.StateVal {
			if a.State.PeerSlotNum != b.State.PeerSlotNum || a.State.PeerPortNum != b.State.PeerPortNum {
				return false
			}
			checked = true
		}
		return checked
	}
	// Funky "less than" function to align with the json compare used by runTestGet
	entryListSort := func(eList []Entry) {
		sort.Slice(eList[:], func(i, j int) bool {
			var a, b int
			if eList[i].SlotNum == eList[j].SlotNum {
				a = eList[i].PortNum
				b = eList[j].PortNum
			} else {
				a = eList[i].SlotNum
				b = eList[j].SlotNum
			}

			if a < 0 && b >= 0 {
				return true
			} else if a >= 0 && b < 0 {
				return false
			}
			astr := strconv.Itoa(a)
			bstr := strconv.Itoa(b)
			if len(astr) == len(bstr) {
				return astr < bstr
			}
			return len(astr) < len(bstr)

		})
	}

	// Create a dataset to work with.  We use random data but fix the seed for
	// reproducibility.
	dsStart := time.Now()
	rng := rand.New(rand.NewSource(1027)) // Arbitrary seed value
	valList := newRandEntries(rng, entryCnt)

	// Sort them for ease of use.
	entryListSort(valList)
	dsTime := time.Since(dsStart)
	fmt.Printf("TestOpticalSwitch: Dataset creation: %s\n", dsTime)

	// State side is read-only to gNMI, populate the dataset directly to the DB
	// with redis APIs.
	{
		rclient := getRedisClientN(t, sttDbId, ns)
		defer db.CloseRedisClient(rclient)
		for _, v := range valList {
			key := tbl_stt + key_sep + strconv.Itoa(v.SlotNum) + key_sep + strconv.Itoa(v.PortNum)
			data := map[string]interface{}{"slot-number": strconv.Itoa(v.SlotNum),
				"port-number":      strconv.Itoa(v.PortNum),
				"peer-slot-number": strconv.Itoa(v.State.PeerSlotNum),
				"peer-port-number": strconv.Itoa(v.State.PeerPortNum)}
			rclient.HMSet(context.Background(), key, data)
		}
	}

	// Perform a bulk set to write the config side
	{
		bsStart := time.Now()
		desc := "Load config"
		pathTgt := "OC_YANG"
		pbPath := pathToPb("/openconfig-optical-switch:optical-switch/port-connections")
		expRetCode := codes.OK
		cfgItems := "\"port-connection\": " + entriesToJSONList(valList, true, false)
		setData := fmt.Sprintf("{\"openconfig-optical-switch:port-connections\": { %s }}", cfgItems)
		op := Replace
		t.Run(desc, func(t *testing.T) {
			runTestSet(t, ctx, gClient, pathTgt, pbPath, expRetCode, nil, setData, op)
		})
		bsTime := time.Since(bsStart)
		fmt.Printf("TestOpticalSwitch: Bulk Set: %s\n", bsTime)
	}

	// Read the databases (config and state) with redis APIs to ensure the
	// values are present and correct.
	for _, dbId := range []int{cfgDbId, sttDbId} {
		isCfg := dbId == cfgDbId
		isStt := !isCfg
		rclient := getRedisClientN(t, dbId, ns)
		defer db.CloseRedisClient(rclient)
		key_prefix := tbl_cfg
		if dbId == sttDbId {
			key_prefix = tbl_stt
		}
		keys, err := rclient.Keys(context.Background(), key_prefix+"*").Result()
		if err != nil {
			t.Fatalf("Key verification error %v", err)
		}
		if len(keys) != len(valList) {
			t.Fatalf("Unexpected number of keys in db %d, got %d, expected %d", dbId, len(keys), len(valList))
		}
		seen := make([]Entry, len(keys))
		for i, k := range keys {
			ks := strings.Split(k, key_sep)
			if len(ks) != 3 {
				t.Fatalf("Unexpected database key \"%s\", expected \"%s%s<num>%s<num>\"", k, key_prefix, key_sep, key_sep)
			}
			// Key must have the expected format
			if ks[0] != key_prefix {
				t.Fatalf("Unexpected database key \"%s\", expected prefix \"%s\"", k, key_prefix)
			}
			entMap, err := rclient.HGetAll(context.Background(), k).Result()
			if err != nil {
				t.Fatalf("Error (%v) reading key \"%s\" via redis API", err, k)
			}
			f1, _ := strconv.Atoi(ks[1])
			f2, _ := strconv.Atoi(ks[2])
			f3, _ := strconv.Atoi(entMap["peer-slot-number"])
			f4, _ := strconv.Atoi(entMap["peer-port-number"])
			seen[i] = Entry{SlotNum: f1, PortNum: f2}
			if isCfg {
				seen[i].ConfigVal = true
				seen[i].Config = EntryFlds{
					PeerSlotNum: f3,
					PeerPortNum: f4}
			} else {
				seen[i].StateVal = true
				seen[i].State = EntryFlds{
					PeerSlotNum: f3,
					PeerPortNum: f4}
			}
		}
		entryListSort(seen)
		for i := 0; i < len(seen); i++ {
			if !entryEqual(&seen[i], &valList[i]) {
				for j := 0; j < len(seen); j++ {
					fmt.Printf("Gotten[%d] %s\n", j, entryToJSONStr(&seen[j], isCfg, isStt))
				}
				for j := 0; j < len(valList); j++ {
					fmt.Printf("Expect[%d] %s\n", j, entryToJSONStr(&valList[j], isCfg, isStt))
				}
				t.Fatalf("Mismatched entry in db=%d at index %d, found %s, expected %s", dbId, i, entryToJSONStr(&seen[i], isCfg, isStt), entryToJSONStr(&valList[i], isCfg, isStt))
			}
		}
	}

	// Read with individual Get operations and verify the data
	for _, tgt := range []string{"config", "state"} {
		gStart := time.Now()
		isCfg := tgt == "config"
		for _, val := range valList {
			k1 := val.SlotNum
			k2 := val.PortNum
			desc := fmt.Sprintf("Get port-connection[slot-number=%d][port-number=%d]/%s", k1, k2, tgt)
			pathTgt := "OC_YANG"
			pbPath := pathToPb(fmt.Sprintf("/openconfig-optical-switch:optical-switch/port-connections/port-connection[slot-number=%d][port-number=%d]/%s", k1, k2, tgt))
			expRetCode := codes.OK
			expResp := "{\"openconfig-optical-switch:" + tgt + "\": "
			if isCfg {
				expResp = expResp + entryCntrToJSONStr(&val, &val.Config) + "}"
			} else {
				expResp = expResp + entryCntrToJSONStr(&val, &val.State) + "}"
			}
			t.Run(desc, func(t *testing.T) {
				runTestGet(t, ctx, gClient, pathTgt, pbPath, pb.GetRequest_ALL, pb.Encoding_JSON_IETF, expRetCode, expResp, true)
			})
		}
		gTime := time.Since(gStart)
		fmt.Printf("TestOpticalSwitch: Get %s individual: %s\n", tgt, gTime)
	}

	// Perform individual writes to the config side
	{
		bsStart := time.Now()
		desc := "Load config"
		pathTgt := "OC_YANG"
		pbPath := pathToPb("/openconfig-optical-switch:optical-switch/port-connections")
		expRetCode := codes.OK
		op := Update
		for _, val := range valList {
			cfgItems := "\"port-connection\": " + entriesToJSONList([]Entry{val}, true, false)
			setData := fmt.Sprintf("{\"openconfig-optical-switch:port-connections\": { %s }}", cfgItems)
			t.Run(desc, func(t *testing.T) {
				runTestSet(t, ctx, gClient, pathTgt, pbPath, expRetCode, nil, setData, op)
			})
		}

		bsTime := time.Since(bsStart)
		fmt.Printf("TestOpticalSwitch: Individual Set: %s\n", bsTime)
	}

	// Read with a Bulk get of state, config, and all
	for _, tgt := range []pb.GetRequest_DataType{pb.GetRequest_CONFIG, pb.GetRequest_STATE, pb.GetRequest_OPERATIONAL, pb.GetRequest_ALL} {
		gStart := time.Now()
		desc := fmt.Sprintf("Bulk Get %s", tgt)
		pathTgt := "OC_YANG"
		pbPath := pathToPb("/openconfig-optical-switch:optical-switch/port-connections/")
		expRetCode := codes.OK
		var expEnts string
		if tgt == pb.GetRequest_CONFIG {
			expEnts = entriesToJSONList(valList, true, false)
		} else if tgt == pb.GetRequest_STATE {
			expEnts = entriesToJSONList(valList, false, true)
		} else if tgt == pb.GetRequest_OPERATIONAL {
			expEnts = ""
		} else {
			expEnts = entriesToJSONList(valList, true, true)
		}
		expResp := fmt.Sprintf("{\"openconfig-optical-switch:port-connections\": { \"port-connection\": %s } }", expEnts)
		if expEnts == "" {
			expResp = "{}"
		}

		t.Run(desc, func(t *testing.T) {
			runTestGet(t, ctx, gClient, pathTgt, pbPath, tgt, pb.Encoding_JSON_IETF, expRetCode, expResp, true)
		})
		gTime := time.Since(gStart)
		fmt.Printf("TestOpticalSwitch: Get %s all: %s\n", tgt, gTime)
	}

	// Read with a Subscribe-Once subscription.  Include filtered reads as well.
	f1Fltr := valList[0].SlotNum
	f2Fltr := valList[0].PortNum
	var f1FltrData []Entry
	var f2FltrData []Entry
	for _, e := range valList {
		if e.SlotNum == f1Fltr {
			f1FltrData = append(f1FltrData, e)
		}
		if e.PortNum == f2Fltr {
			f2FltrData = append(f2FltrData, e)
		}
	}
	fldName2VarName := map[string]string{"slot-number": "SlotNum",
		"port-number":      "PortNum",
		"peer-slot-number": "PeerSlotNum",
		"peer-port-number": "PeerPortNum"}
	type subscriptionKey struct {
		pathIndex    int
		extractIndex int
		keyName      string
		keyVal       string
	}
	subs := []struct {
		description string
		path        []string
		key         []subscriptionKey
		expData     []Entry
	}{
		{
			description: "SubOnce All Config",
			path:        []string{"optical-switch", "port-connections", "port-connection", "config"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: "*"},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: "*"}},
			expData: valList,
		},
		{
			description: "All State",
			path:        []string{"optical-switch", "port-connections", "port-connection", "state"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: "*"},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: "*"}},
			expData: valList,
		},
		{
			description: "All Config, implicit wildcard",
			path:        []string{"optical-switch", "port-connections", "port-connection", "config"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: ""},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: ""}},
			expData: valList,
		},
		{
			description: "All State, implicit wildcard",
			path:        []string{"optical-switch", "port-connections", "port-connection", "state"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: ""},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: ""}},
			expData: valList,
		},
		{
			description: "Config Filter slot-number= " + strconv.Itoa(f1Fltr) + "]",
			path:        []string{"optical-switch", "port-connections", "port-connection", "config"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: strconv.Itoa(f1Fltr)},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: "*"}},
			expData: f1FltrData,
		},
		{
			description: "Config Filter port-number=" + strconv.Itoa(f2Fltr) + "]",
			path:        []string{"optical-switch", "port-connections", "port-connection", "config"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: "*"},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: strconv.Itoa(f2Fltr)}},
			expData: f2FltrData,
		},
		{
			description: "State Filter slot-number= " + strconv.Itoa(f1Fltr) + "]",
			path:        []string{"optical-switch", "port-connections", "port-connection", "state"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: strconv.Itoa(f1Fltr)},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: "*"}},
			expData: f1FltrData,
		},
		{
			description: "State Filter port-number=" + strconv.Itoa(f2Fltr) + "]",
			path:        []string{"optical-switch", "port-connections", "port-connection", "state"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: "*"},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: strconv.Itoa(f2Fltr)}},
			expData: f2FltrData,
		},
		{
			description: "All Entries",
			path:        []string{"optical-switch", "port-connections"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: ""},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: ""}},
			expData: valList,
		},
		{
			description: "All Filter slot-number= " + strconv.Itoa(f1Fltr) + "]",
			path:        []string{"optical-switch", "port-connections", "port-connection"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: strconv.Itoa(f1Fltr)},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: "*"}},
			expData: f1FltrData,
		},
		{
			description: "All Filter port-number= " + strconv.Itoa(f2Fltr) + "]",
			path:        []string{"optical-switch", "port-connections", "port-connection"},
			key: []subscriptionKey{
				{pathIndex: 2, extractIndex: 4, keyName: "slot-number", keyVal: "*"},
				{pathIndex: 2, extractIndex: 3, keyName: "port-number", keyVal: strconv.Itoa(f2Fltr)}},
			expData: f2FltrData,
		},
	}
	for _, sub := range subs {
		gStart := time.Now()
		t.Run(sub.description, func(t *testing.T) {
			// Set up a subscription, the notification handler will do minor
			// checks and push the notifications to the main routine
			var elem []*pb.PathElem
			for _, p := range sub.path {
				elem = append(elem, &pb.PathElem{Name: p})
			}
			for _, k := range sub.key {
				if k.keyVal == "" {
					continue
				}
				if elem[k.pathIndex].Key == nil {
					elem[k.pathIndex].Key = make(map[string]string)
				}
				elem[k.pathIndex].Key[k.keyName] = k.keyVal
			}
			sr := &pb.SubscribeRequest_Subscribe{
				Subscribe: &pb.SubscriptionList{
					Mode:   pb.SubscriptionList_ONCE,
					Prefix: &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{Elem: elem},
							Mode: gnmipb.SubscriptionMode_ON_CHANGE,
						},
					},
				},
			}
			query, err := client.NewQuery(&pb.SubscribeRequest{Request: sr})
			if err != nil {
				t.Fatalf("Failed to create query, err \"%s\"", err)
			}
			query.UpdatesOnly = false
			query.TLS = &tls.Config{InsecureSkipVerify: true}
			query.Addrs = []string{fmt.Sprintf("127.0.0.1:%d", s.config.Port)}
			c := client.New()
			defer c.Close()
			notifCnt, notifCntCon, notifCntUpd, notifCntSyn := 0, 0, 0, 0
			ch := make(chan client.Notification)
			// Basic handler to push notifications over the "ch" channel.
			query.NotificationHandler = func(n client.Notification) error {
				notifCnt++

				if _, ok := n.(cacheclient.Connected); ok {
					// First message must be a Connected notification
					if notifCnt != 1 {
						t.Fatalf("Received Connected as non-first notification (%d)", notifCnt)
					}
					notifCntCon++
				} else if nn, ok := n.(cacheclient.Update); ok {
					notifCntUpd++
					ch <- nn
				} else if _, ok := n.(cacheclient.Sync); ok {
					if notifCntCon != 1 {
						t.Fatal("Received Sync before Connected")
					}
					if notifCntSyn != 0 {
						t.Fatal("Received multiple Sync messages")
					}
					notifCntSyn++
					close(ch)
				}
				return nil
			}
			go func() {
				err = c.Subscribe(context.Background(), query)
				if err != nil {
					t.Fatalf("Subscribe returned error %v", err)
				}
			}()

			gottenMap := map[string]Entry{}
			gottenCnt := 0
			for receiving := true; receiving; {
				select {
				case <-time.After(clientTimeout / 4):
					t.Fatalf("Timeout waiting for Update messages on subscription")
				case notif, ok := <-ch:
					if !ok {
						receiving = false
					} else {
						updateNotif, _ := notif.(cacheclient.Update)
						if len(updateNotif.Path) < (len(sub.path) + len(sub.key)) {
							t.Fatalf("Unexpected path in notification, got \"%v\" for \"%v\"", updateNotif.Path, sub.path)
						}
						//t.Logf("Got Update: %v\n", updateNotif)

						// Extract keys from update and find or make an Entry for this update
						var entKey string
						for _, k := range sub.key {
							kv := updateNotif.Path[k.extractIndex]
							if entKey != "" {
								entKey = entKey + "|"
							}
							entKey = entKey + kv
						}
						e, ok := gottenMap[entKey]
						if !ok {
							e = Entry{}
							//t.Logf("\t New Entry: key %s\n", entKey)
							for i, k := range sub.key {
								er := reflect.ValueOf(&e).Elem()
								f := er.FieldByName(fldName2VarName[k.keyName])
								xStr := strings.Split(entKey, "|")[i]
								x, _ := strconv.Atoi(xStr)
								//t.Logf("\t Setting %s to %d\n", fldName2VarName[k.keyName], x)
								f.SetInt(int64(x))
							}
							gottenCnt++
						}
						// Populate the Entry with data from the update
						val := updateNotif.Val.(int64)
						var fldsr reflect.Value
						if updateNotif.Path[len(updateNotif.Path)-1] == "slot-number" {
							if val != int64(e.SlotNum) {
								t.Fatalf("Inconsistent update: entry %v, field %s val %d\n", e, updateNotif.Path[6], val)
							}
						} else if updateNotif.Path[len(updateNotif.Path)-1] == "port-number" {
							if val != int64(e.PortNum) {
								t.Fatalf("Inconsistent update: entry %v, field %s val %d\n", e, updateNotif.Path[6], val)
							}
						} else if updateNotif.Path[5] == "config" {
							e.ConfigVal = true
							fldsr = reflect.ValueOf(&e.Config).Elem()
							fldr := fldsr.FieldByName(fldName2VarName[updateNotif.Path[6]])
							//t.Logf("\t Trying to set field %s (%s) to %v\n", fldName2VarName[updateNotif.Path[6]], updateNotif.Path[5], val)
							fldr.SetInt(val)
						} else if updateNotif.Path[5] == "state" {
							e.StateVal = true
							fldsr = reflect.ValueOf(&e.State).Elem()
							fldr := fldsr.FieldByName(fldName2VarName[updateNotif.Path[6]])
							//t.Logf("\t Trying to set field %s (%s) to %v\n", fldName2VarName[updateNotif.Path[6]], updateNotif.Path[5], val)
							fldr.SetInt(val)
						} else {
							t.Fatalf("Unexpected notification path (not config or state) %v", updateNotif.Path)
						}
						gottenMap[entKey] = e
					}
				}
			}
			gotten := make([]Entry, gottenCnt)
			gottenI := 0
			for _, v := range gottenMap {
				gotten[gottenI] = v
				gottenI++
			}
			entryListSort(gotten)
			if len(gotten) != len(sub.expData) {
				t.Fatalf("Got notifications for %d entries, expected %d", len(gotten), len(sub.expData))
			}
			for i := 0; i < len(gotten); i++ {
				if !entryEqual(&gotten[i], &sub.expData[i]) {
					t.Fatalf("Received incorrect notification: gotten[%d]=%v, expected[%d]=%v", i, gotten[i], i, sub.expData[i])
				}
			}
		})

		gTime := time.Since(gStart)
		fmt.Printf("Subscribe-Once: %s\n", gTime)
	}
}

func TestNSFRegistration(t *testing.T) {
	// Start Telemetry Server
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// Create redis Client
	ns, _ := sdcfg.GetDbDefaultNamespace()
	stateDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	rclient := getRedisClientN(t, stateDbId, ns)
	defer db.CloseRedisClient(rclient)

	// Check telemetry NSF table
	resp, err := rclient.HGetAll(context.Background(), "WARM_RESTART_REGISTRATION_TABLE|telemetry|telemetry").Result()
	if err != nil {
		t.Fatalf("WARM_RESTART_REGISTRATION_TABLE for telemetry not found, err = %v", err)
	}
	if freeze, ok := resp["freeze"]; !ok || freeze != "true" {
		t.Fatalf("Failed to set freeze field: got=%v, want=true", freeze)
	}
	if checkpoint, ok := resp["checkpoint"]; !ok || checkpoint != "false" {
		t.Fatalf("Failed to set checkpoint field: got=%v, want=false", checkpoint)
	}
	if reconciliation, ok := resp["reconciliation"]; !ok || reconciliation != "true" {
		t.Fatalf("Failed to set checkpoint field: got=%v, want=true", reconciliation)
	}
	t.Log("NSF Registration Success!")
}

func TestNSFInitAndReconciliation(t *testing.T) {
	// Set the Warmboot flag
	ns, _ := sdcfg.GetDbDefaultNamespace()
	stateDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	rclient := getRedisClientN(t, stateDbId, ns)
	defer db.CloseRedisClient(rclient)
	if _, err := rclient.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "true").Result(); err != nil {
		t.Fatalf("Failed to set warmboot flag: %v", err)
	}
	defer rclient.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "false")
	if _, err := rclient.HSet(context.Background(), "WARM_RESTART_TABLE|telemetry", "restore_count", "0").Result(); err != nil {
		t.Fatalf("Failed to set restore_count: %v", err)
	}

	// Start Telemetry Server
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// Check telemetry NSF table
	res, err := rclient.HGetAll(context.Background(), "WARM_RESTART_TABLE|telemetry").Result()
	if err != nil {
		t.Fatalf("WARM_RESTART_REGISTRATION_TABLE for telemetry not found, err = %v", err)
	}
	if state, ok := res["state"]; !ok || state != "reconciled" {
		t.Fatalf("Server failed to initialize and reconcile, table contents: %v", res)
	}
	if res["restore_count"] != "1" {
		t.Fatalf("Server failed to update restore count: %v", res)
	}
}

func TestNSFUnfreezeWithSV(t *testing.T) {
	// Set the Warmboot flag and state verification
	ns, _ := sdcfg.GetDbDefaultNamespace()
	stateDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	stateDb := getRedisClientN(t, stateDbId, ns)
	defer db.CloseRedisClient(stateDb)
	configDbId, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	configDb := getRedisClientN(t, configDbId, ns)
	defer db.CloseRedisClient(configDb)
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "true").Result(); err != nil {
		t.Fatalf("Failed to set warmboot flag: %v", err)
	}
	defer stateDb.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "false")
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_TABLE|telemetry", "restore_count", "0").Result(); err != nil {
		t.Fatalf("Failed to set restore_count: %v", err)
	}
	if _, err := configDb.HSet(context.Background(), "WARM_RESTART|system", "state_verification_bootup", "true").Result(); err != nil {
		t.Fatalf("Failed to set state verification: %v", err)
	}

	// Start Telemetry Server
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// Check that the server was started in freeze mode
	if s.WarmRestartHelper.FetchFreezeStatus() != true {
		t.Fatal("Server was not started in freeze mode!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.RECONCILED {
		t.Fatalf("Server not in reconciled state: %v", state)
	}

	// Send an unfreeze notification
	producer, err := common_utils.NewNotificationProducer("NSF_MANAGER_COMMON_NOTIFICATION_CHANNEL")
	if err != nil {
		t.Fatalf("Failed to create Notification Producer: %v", err)
	}
	defer producer.Close()
	if err := producer.Send("unfreeze", "", map[string]string{}); err != nil {
		t.Fatalf("Failed to publish to request channel: %v", err)
	}
	time.Sleep(nsfNotificationSleep)

	// Check that the server is unfrozen
	if s.WarmRestartHelper.FetchFreezeStatus() != false {
		t.Fatal("Server still in freeze mode!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.COMPLETED {
		t.Fatalf("Server not in completed state: %v", state)
	}

}

func TestNSFUnfreezeWithoutSV(t *testing.T) {
	// Set the Warmboot flag and state verification
	ns, _ := sdcfg.GetDbDefaultNamespace()
	stateDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	stateDb := getRedisClientN(t, stateDbId, ns)
	defer db.CloseRedisClient(stateDb)
	configDbId, err := sdcfg.GetDbId("CONFIG_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	configDb := getRedisClientN(t, configDbId, ns)
	defer db.CloseRedisClient(configDb)
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "true").Result(); err != nil {
		t.Fatalf("Failed to set warmboot flag: %v", err)
	}
	// Disable the NSF flag after the test.
	defer func() {
		if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "false").Result(); err != nil {
			t.Errorf("Failed to set warmboot flag: %v", err)
		}
	}()
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_TABLE|telemetry", "restore_count", "0").Result(); err != nil {
		t.Fatalf("Failed to set restore_count: %v", err)
	}
	if _, err := configDb.HSet(context.Background(), "WARM_RESTART|system", "state_verification_bootup", "false").Result(); err != nil {
		t.Fatalf("Failed to set state verification: %v", err)
	}

	// Start Telemetry Server
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// Check that the server was started in freeze mode
	if s.WarmRestartHelper.FetchFreezeStatus() != true {
		t.Fatal("Server was not started in freeze mode!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.RECONCILED {
		t.Fatalf("Server not in reconciled state: %v", state)
	}

	// Set some apps to reconciled and make sure the server is still in freeze mode
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_TABLE|teammgrd", "state", "reconciled").Result(); err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_TABLE|p4rt", "state", "reconciled").Result(); err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	time.Sleep(4 * time.Second)
	if s.WarmRestartHelper.FetchFreezeStatus() != true {
		t.Fatal("Server was not started in freeze mode!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.RECONCILED {
		t.Fatalf("Server not in reconciled state: %v", state)
	}

	// Set the last app to reconciled and make sure the server is unfrozen
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_TABLE|orchagent", "state", "reconciled").Result(); err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	time.Sleep(4 * time.Second)
	if s.WarmRestartHelper.FetchFreezeStatus() != false {
		t.Fatal("Server did not unfreeze!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.RECONCILED {
		t.Fatalf("Server not in reconciled state: %v", state)
	}
}

func TestHandleNSFStateNotifications(t *testing.T) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	stateDbId, err := sdcfg.GetDbId("STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	stateDb := getRedisClientN(t, stateDbId, ns)
	defer db.CloseRedisClient(stateDb)
	if _, err := stateDb.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "true").Result(); err != nil {
		t.Fatalf("Failed to set warmboot flag: %v", err)
	}
	defer stateDb.HSet(context.Background(), "WARM_RESTART_ENABLE_TABLE|system", "enable", "false")

	// Start Telemetry Server
	s := createServer(t)
	defer s.Stop()
	s.WarmRestartHelper.CheckWarmStart(false)
	// The server starts in freeze mode and checks for reconcilliation of other containers before unfreezing.
	// Sleep to give the server time to do this check and come out of freeze mode.
	time.Sleep(nsfNotificationSleep)

	producer, err := common_utils.NewNotificationProducer("NSF_MANAGER_COMMON_NOTIFICATION_CHANNEL")
	if err != nil {
		t.Fatalf("Failed to create Notification Producer: %v", err)
	}
	defer producer.Close()

	// Test freeze notification.
	if err := producer.Send("freeze", "", map[string]string{}); err != nil {
		t.Fatalf("Failed to publish to request channel: %v", err)
	}
	time.Sleep(nsfNotificationSleep)
	if freezeMode := s.WarmRestartHelper.FetchFreezeStatus(); freezeMode != true {
		t.Fatal("The server is not in freeze mode!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.QUIESCENT {
		t.Fatalf("Expected telemetry to be QUIESCENT, got %v", state)
	}
	performanceTable, err := stateDb.HGetAll(context.Background(), "WARM_RESTART_PERFORMANCE_TABLE|freeze|telemetry").Result()
	if err != nil {
		t.Fatalf("Failed to get performance table: %v", err)
	}
	if start := performanceTable["start-timestamp"]; start == "" {
		t.Fatal("Start timestamp was not updated in the performance table!")
	}
	if end := performanceTable["finish-timestamp"]; end == "" {
		t.Fatal("Finish timestamp was not updated in the performance table!")
	}

	// Test unfreeze notification.
	if err := producer.Send("unfreeze", "", map[string]string{}); err != nil {
		t.Fatalf("Failed to publish to request channel: %v", err)
	}
	time.Sleep(nsfNotificationSleep)
	if freezeMode := s.WarmRestartHelper.FetchFreezeStatus(); freezeMode != false {
		t.Fatal("The server is in freeze mode!")
	}
	if state := s.WarmRestartHelper.GetWarmStartState("telemetry"); state != common_utils.COMPLETED {
		t.Fatalf("Expected telemetry to be QUIESCENT, got %v", state)
	}
	performanceTable, err = stateDb.HGetAll(context.Background(), "WARM_RESTART_PERFORMANCE_TABLE|unfreeze|telemetry").Result()
	if err != nil {
		t.Fatalf("Failed to get performance table: %v", err)
	}
	if start := performanceTable["start-timestamp"]; start == "" {
		t.Fatal("Start timestamp was not updated in the performance table!")
	}
	if end := performanceTable["finish-timestamp"]; end == "" {
		t.Fatal("Finish timestamp was not updated in the performance table!")
	}

	// Test checkpoint notification.
	if err := producer.Send("checkpoint", "", map[string]string{}); err != nil {
		t.Fatalf("Failed to publish to request channel: %v", err)
	}
	// Checkpoint is a no-op.

	// Test reconciliation notification.
	if err := producer.Send("reconciliation", "", map[string]string{}); err != nil {
		t.Fatalf("Failed to publish to request channel: %v", err)
	}
	// Reconciliation is a no-op.

	// Test invalid notification.
	if err := producer.Send("test", "", map[string]string{}); err != nil {
		t.Fatalf("Failed to publish to request channel: %v", err)
	}
	// Invalid notification is a no-op.
}

func TestNSFCloseExistingClientsOnFreeze(t *testing.T) {
	// Start Telemetry Server
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()
	numClients := 3

	for i := 0; i < numClients; i++ {
		client := NewClient(&net.TCPAddr{IP: []byte("1.1.1.1"), Port: i}, s.gnsiPathz.pathzProcessor, s.recorder, s.ConnectionManager)
		client.subscribe = &pb.SubscriptionList{Mode: pb.SubscriptionList_STREAM}
		s.clients[client.String()] = client
	}

	if err := s.CloseExistingClientsOnFreeze(); err != nil {
		t.Fatalf("CloseExistingClientsOnFreeze returned an error: %v", err)
	}
	if len(s.clients) != 0 {
		t.Fatalf("Server clients map is not empty: %v", s.clients)
	}
}

func TestGnmiRPCsDuringFreezeMode(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	s.WarmRestartHelper.SetFreezeStatus(true)
	defer s.WarmRestartHelper.SetFreezeStatus(false)

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	_, err = gClient.Get(context.Background(), &pb.GetRequest{})
	testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)

	_, err = gClient.Set(context.Background(), &pb.SetRequest{})
	testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)

	_, err = gClient.Capabilities(context.Background(), &pb.CapabilityRequest{})
	testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)

	stream, err := gClient.Subscribe(context.Background(), grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := stream.Send(&pb.SubscribeRequest{}); err != nil {
		t.Fatalf("Failed to send subscription: %v", err)
	}
	_, err = stream.Recv()
	testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
}

func TestSubscribeCloseSend(t *testing.T) {
	// Start Telemetry Server
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}

	subscription := &pb.SubscriptionList{
		Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
		Mode:     pb.SubscriptionList_ONCE,
		Encoding: pb.Encoding_PROTO,
		Subscription: []*pb.Subscription{
			{
				Path: &pb.Path{
					Elem: []*pb.PathElem{
						{Name: "interfaces"},
					},
				},
			},
		},
	}

	_ = stream.Send(&pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: subscription,
		},
	})
	stream.CloseSend() // This closes the sending channel only

	_, err = stream.Recv()
	if err != nil {
		t.Fatalf("Error receiving response: %v", err)
	}
}

func TestStressStreamPath(t *testing.T) {
	// Stressed the grpc queues by writing the entire range of MTU values
	// and then making sure no out of order values are read back in updates.
	prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/json_tests/appl_state_db.txt")
	prepareDbUtil(t, "CONFIG_DB", "", "../testdata/json_tests/config_db.txt")
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}

	err = stream.Send(&pb.SubscribeRequest{
		Extension: []*ext_pb.Extension{
			{Ext: &ext_pb.Extension_MasterArbitration{}},
		},
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:     pb.SubscriptionList_STREAM,
				Encoding: pb.Encoding_PROTO,
				Subscription: []*pb.Subscription{
					{
						Path: &pb.Path{
							Elem: []*pb.PathElem{
								{Name: "interfaces"},
								{Name: "interface[name=Ethernet1/1/1]"},
								{Name: "state"},
								{Name: "mtu"},
							},
						},
						Mode: pb.SubscriptionMode_ON_CHANGE,
					},
				},
			},
		}},
	)
	if err != nil {
		t.Fatal(err.Error())
	}

	ns, _ := sdcfg.GetDbDefaultNamespace()
	applStateDb, err := sdcfg.GetDbId("APPL_STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	rclient := getRedisClientN(t, applStateDb, ns)
	defer db.CloseRedisClient(rclient)

	for i := 0; i < 65534; i++ {
		rclient.HSet(context.Background(), "PORT_TABLE:Ethernet1/1/1", "mtu", i)
	}

	last := uint64(0)
	for end := time.Now().Add(time.Second * 3); time.Now().Before(end); {
		resp, err := stream.Recv()
		if err == nil && !resp.GetSyncResponse() {
			val := resp.GetUpdate().Update[0].GetVal().GetUintVal()
			if last > val {
				t.Fatalf("Updates were not increasing, current %v, last %v", val, last)
			}
			last = val
		}
	}
}

func TestDeleteSample(t *testing.T) {
	// Panics on failure
	prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/json_tests/appl_state_db.txt")
	prepareDbUtil(t, "CONFIG_DB", "", "../testdata/json_tests/config_db.txt")
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}

	err = stream.Send(&pb.SubscribeRequest{
		Extension: []*ext_pb.Extension{
			{Ext: &ext_pb.Extension_MasterArbitration{}},
		},
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix: &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:   pb.SubscriptionList_STREAM,
				Subscription: []*pb.Subscription{
					{
						Path: &pb.Path{
							Elem: []*pb.PathElem{
								{Name: "interfaces"},
								{
									Name: "interface",
									Key:  map[string]string{"name": "*"},
								},
								{Name: "state"},
								{Name: "mtu"},
							},
						},
						Mode:           pb.SubscriptionMode_SAMPLE,
						SampleInterval: 1000000000,
					},
				},
			},
		}},
	)
	if err != nil {
		t.Fatal(err.Error())
	}

	ns, _ := sdcfg.GetDbDefaultNamespace()
	applStateDb, err := sdcfg.GetDbId("APPL_STATE_DB", ns)
	if err != nil {
		t.Fatalf("failed to get db %v", err)
	}
	rclient := getRedisClientN(t, applStateDb, ns)
	defer db.CloseRedisClient(rclient)

	for end := time.Now().Add(time.Second * 5); time.Now().Before(end); {
		if resp, err := stream.Recv(); err == nil {
			if resp.GetSyncResponse() {
				rclient.Del(context.Background(), "PORT_TABLE:Ethernet1/1/1")
				t.Logf("Deleted PORT_TABLE:Ethernet1/1/1")
			}
		} else {
			t.Errorf("Recieved error in response: %v", err)
		}
	}
}

func TestChangeLogLevel(t *testing.T) {
	s := createServer(t)
	defer s.Stop()

	// Default flag
	originalFlag := "server=3"

	// Send updates in database
	ns, _ := sdcfg.GetDbDefaultNamespace()
	rclient := getConfigDbClient(t, ns)
	defer db.CloseRedisClient(rclient)

	fileName := "../testdata/CONFIG_TELEMETRY_LOG_LEVEL.txt"
	telemetryLogLevelJson, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatalf("read file %v err: %v", fileName, err)
	}

	var telemetryLogLevel map[string]interface{}
	err = json.Unmarshal(telemetryLogLevelJson, &telemetryLogLevel)
	if err != nil {
		t.Fatalf("Failed to Unmarshal %v err: %v", telemetryLogLevelJson, err)
	}

	for key, fv := range telemetryLogLevel {
		switch fv.(type) {
		case map[string]interface{}:
			_, err := rclient.HMSet(context.Background(), key, fv.(map[string]interface{})).Result()
			if err != nil {
				t.Fatal("Invalid data for db: ", key, fv, err)
			}
		default:
			t.Fatalf("Invalid data for db: %v : %v", key, fv)
		}
	}

	type logLevelTest struct {
		desc          string
		parameters    []string
		oper          string
		validDBUpdate bool
		expectedFlag  string
	}

	tests := []logLevelTest{
		{
			desc:          "VerfifyLogLevelChangeWhenNoUpdates",
			parameters:    []string{},
			oper:          "hset",
			validDBUpdate: false,
			expectedFlag:  "",
		},
		{
			desc:          "VerfifyLogLevelChangeWhenTableUpdates",
			parameters:    []string{"TELEMETRY|gnmi", "vmodule", "server=1,gnoi=1"},
			oper:          "hset",
			validDBUpdate: true,
			expectedFlag:  "server=1,gnoi=1",
		},
		{
			desc:          "VerfifyNoLogLevelChangeWhenOtherTableUpdates",
			parameters:    []string{"DEVICE_METADATA|localhost", "hostname", "gpins"},
			oper:          "hset",
			validDBUpdate: false,
			expectedFlag:  "server=1,gnoi=1",
		},
		{
			desc:          "VerfifyNoLogLevelChangeWhenOtherTableKeyUpdates",
			parameters:    []string{"TELEMETRY|testTableKey", "testField", "testValue"},
			oper:          "hset",
			validDBUpdate: false,
			expectedFlag:  "server=1,gnoi=1",
		},
		{
			desc:          "VerfifyLogLevelChangeAfterSeveralTableUpdates",
			parameters:    []string{"TELEMETRY|gnmi", "vmodule", "server=3,gnsi=1"},
			oper:          "hset",
			validDBUpdate: true,
			expectedFlag:  "server=3,gnsi=1",
		},
		{
			desc:          "InvalidFlagFormetFlagSetFail",
			parameters:    []string{"TELEMETRY|gnmi", "vmodule", "server=2 gnsi=1"},
			oper:          "hset",
			validDBUpdate: false,
			expectedFlag:  "server=3,gnsi=1",
		},
		{
			desc:          "DeleteFlagFromDB",
			parameters:    []string{"TELEMETRY|gnmi"},
			oper:          "del",
			validDBUpdate: false,
			expectedFlag:  "server=3,gnsi=1",
		},
		{
			desc:          "ResetFlagInDB",
			parameters:    []string{"TELEMETRY|gnmi", "vmodule", "server=2,gnoi=2,gnsi=1"},
			oper:          "hset",
			validDBUpdate: true,
			expectedFlag:  "server=2,gnoi=2,gnsi=1",
		},
	}

	var updateAndVerify = func(t *testing.T, test logLevelTest) {
		switch test.oper {
		case "del":
			if len(test.parameters) < 1 {
				t.Log("Invalid test case, no DB deletion operation")
			} else {
				if _, err := rclient.Del(context.Background(), test.parameters[0]).Result(); err != nil {
					t.Fatal(err)
				}
			}
		case "hset":
			if len(test.parameters) < 3 {
				t.Log("Invalid test case, no DB updates")
			} else {
				if _, err := rclient.HSet(context.Background(), test.parameters[0], test.parameters[1], test.parameters[2]).Result(); err != nil {
					t.Fatal(err)
				}
			}
		default:
			t.Errorf("Invalid test case, %s DB operation is not supported.", test.oper)
		}
		time.Sleep(time.Millisecond * 500)

		// Read updated value from flag and database, compare if they are as expected.
		if test.validDBUpdate {
			updatedFlag, err := rclient.HGet(context.Background(), "TELEMETRY|gnmi", "vmodule").Result()
			if err != nil {
				t.Fatal("TELEMETRY|gnmi not found.")
			}
			if diff := pretty.Compare(test.expectedFlag, updatedFlag); diff != "" {
				t.Log("Expect: ", test.expectedFlag)
				t.Log("Got : ", updatedFlag)
				t.Errorf("Unexpected updates: %s", diff)
			}
		}
		if vm := flag.Lookup("vmodule"); vm != nil {
			s.cMu.Lock()
			updatedFlag := vm.Value.String()
			s.cMu.Unlock()

			if diff := pretty.Compare(test.expectedFlag, updatedFlag); diff != "" {
				t.Log("Expect: ", test.expectedFlag)
				t.Log("Got : ", updatedFlag)
				t.Errorf("Unexpected updates: %s", diff)
			}
		} else {
			// Handle error if vmodule flag is nil
			t.Error("No vmodule flag")
		}
	}

	// This channel is used to request the goroutine to stop its infinite loop
	done := make(chan bool, 1)
	stopped := make(chan bool, 1)
	defer func() {
		done <- true
		<-stopped
	}()
	s.ChangeLogLevel(done, stopped)

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			updateAndVerify(t, test)
			time.Sleep(time.Millisecond * 1)
		})
	}

	// Make sure reset is executed at the very end of the test
	defer func() {
		if vm := flag.Lookup("vmodule"); vm != nil {
			s.cMu.Lock()
			vm.Value.Set(originalFlag)
			s.cMu.Unlock()
		} else {
			// Handle error if vmodule flag is nil
			t.Error("No vmodule flag")
		}
	}()
}

func TestGetConfigCacheHit(t *testing.T) {
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	// Construct GetConfig request.
	req := &pb.GetRequest{
		Prefix: &pb.Path{
			Elem: []*pb.PathElem{
				&pb.PathElem{
					Name: "openconfig",
				}},
		},
		Type:     pb.GetRequest_CONFIG,
		Encoding: pb.Encoding_JSON_IETF,
	}

	// Send the first GetConfig request.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp1, err := gClient.Get(ctx, req)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	// The cache should be valid now.
	if resp := s.GetConfigCache.GetResponse(); resp == nil {
		t.Fatalf("Expected the GetResponse cache to be valid, but got: %v", resp)
	}

	// Send the second GetConfig request.
	resp2, err := gClient.Get(ctx, req)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	// The second GetConfig response should be exactly the same as the first.
	if !proto.Equal(resp1, resp2) {
		t.Fatalf("Expected identical responses, got different responses:\nresp1=%v\nresp2=%v", resp1, resp2)
	}
}

func TestGetConfigCacheUpdate(t *testing.T) {
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	// Construct GetConfig request.
	req := &pb.GetRequest{
		Prefix: &pb.Path{
			Elem: []*pb.PathElem{
				&pb.PathElem{
					Name: "openconfig",
				}},
		},
		Type:     pb.GetRequest_CONFIG,
		Encoding: pb.Encoding_JSON_IETF,
	}

	// Send the first GetConfig request.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp1, err := gClient.Get(ctx, req)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	// The cache should be valid now.
	if resp := s.GetConfigCache.GetResponse(); resp == nil {
		t.Fatalf("Expected the GetResponse cache to be valid, but got: %v", resp)
	}

	// Change a field in ConfigDB and sleep to give the cache time to receive the notification.
	ns, _ := sdcfg.GetDbDefaultNamespace()
	rclient := getConfigDbClient(t, ns)
	defer db.CloseRedisClient(rclient)
	rclient.HSet(context.Background(), "PORTCHANNEL|PortChannel1", "system-priority", 512)
	time.Sleep(100 * time.Millisecond)

	// The cache should be invalid now.
	if resp := s.GetConfigCache.GetResponse(); resp != nil {
		t.Fatalf("Expected the GetResponse cache to be invalid, but got: %v", resp)
	}

	// Send the second GetConfigRequest.
	resp2, err := gClient.Get(ctx, req)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	// The second GetConfig response should be different.
	if proto.Equal(resp1, resp2) {
		t.Fatalf("Expected different responses, got identical responses:\nresp1=%v\nresp2=%v", resp1, resp2)
	}
}

func TestGetConfigCacheBypassed(t *testing.T) {
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	// Construct GetConfig request.
	req := &pb.GetRequest{
		Prefix: &pb.Path{
			Elem: []*pb.PathElem{
				&pb.PathElem{
					Name: "openconfig",
				}},
		},
		Type:     pb.GetRequest_CONFIG,
		Encoding: pb.Encoding_JSON_IETF,
	}

	// Send the GetConfig request.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp1, err := gClient.Get(ctx, req)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	// Send a different request and make sure the cache was not used.
	req = &pb.GetRequest{
		Prefix: &pb.Path{
			Elem: []*pb.PathElem{
				&pb.PathElem{
					Name: "openconfig",
				}},
		},
		Type:     pb.GetRequest_STATE,
		Encoding: pb.Encoding_JSON_IETF,
	}
	resp2, err := gClient.Get(ctx, req)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}

	if proto.Equal(resp1, resp2) {
		t.Fatalf("Expected different responses, got identical responses:\nresp1=%v\nresp2=%v", resp1, resp2)
	}
}

func TestPictorSubscription(t *testing.T) {
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}

	// Construct the subscription
	subs := []*pb.Subscription{}
	for _, p := range pictorPaths {
		path, err := xpath.ToGNMIPath(p.path)
		if err != nil {
			t.Fatalf("Failed to convert string to GNMI path: %v", err)
		}
		subs = append(subs, &pb.Subscription{
			Path:              path,
			Mode:              p.mode,
			SampleInterval:    30000000000,
			SuppressRedundant: true,
		})
	}
	subscription := &pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:       &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:         pb.SubscriptionList_STREAM,
				Encoding:     pb.Encoding_PROTO,
				Subscription: subs,
			},
		},
	}

	// Send the subscription and wait for sync response
	if err = stream.Send(subscription); err != nil {
		t.Fatalf("Failed to send subscription: %v", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Failed to receive response: %v", err)
		}
		if resp.GetSyncResponse() {
			break
		}
	}
}

func TestCONTROLLERSubscription(t *testing.T) {
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := gClient.Subscribe(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Fatal(err.Error())
	}

	// Construct the subscription
	subs := []*pb.Subscription{}
	for _, controllerSub := range controllerPaths {
		path, err := xpath.ToGNMIPath(controllerSub.path)
		if err != nil {
			t.Fatalf("Failed to convert string to GNMI path: %v", err)
		}
		subs = append(subs, &pb.Subscription{
			Path: path,
			Mode: controllerSub.mode,
		})
	}
	subscription := &pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_Subscribe{
			Subscribe: &pb.SubscriptionList{
				Prefix:       &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
				Mode:         pb.SubscriptionList_STREAM,
				Encoding:     pb.Encoding_PROTO,
				Subscription: subs,
			},
		},
	}

	// Send the subscription and wait for sync response
	if err = stream.Send(subscription); err != nil {
		t.Fatalf("Failed to send subscription: %v", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Failed to receive response: %v", err)
		}
		if resp.GetSyncResponse() {
			break
		}
	}
}

func TestDumpDebugData(t *testing.T) {
	responseTimeout := 3 * time.Second

	tests := []struct {
		desc          string
		component     string
		dir           string
		payload       map[string]string
		channel       chan bool
		expectErr     bool
		expectTimeout bool
	}{
		{
			desc:          "DumpDebugDataSucceeds",
			component:     "telemetry",
			dir:           HostVarLogPath,
			payload:       map[string]string{"level": "all"},
			channel:       make(chan bool),
			expectErr:     false,
			expectTimeout: false,
		},
		{
			desc:          "DumpDebugDataOtherComponent",
			component:     "xyz",
			dir:           HostVarLogPath,
			payload:       map[string]string{"level": "all"},
			channel:       make(chan bool),
			expectErr:     false,
			expectTimeout: true,
		},
		{
			desc:          "DumpDebugDataInvalidDir",
			component:     "telemetry",
			dir:           "INVALIDDIR",
			payload:       map[string]string{"level": "all"},
			channel:       make(chan bool),
			expectErr:     true,
			expectTimeout: false,
		},
		{
			desc:          "DumpDebugDataInvalidLevel",
			component:     "telemetry",
			dir:           HostVarLogPath,
			payload:       map[string]string{"level": "xyz"},
			channel:       make(chan bool),
			expectErr:     true,
			expectTimeout: false,
		},
		{
			desc:          "DumpDebugDataMissingLevel",
			component:     "telemetry",
			dir:           HostVarLogPath,
			payload:       map[string]string{"xyz": "all"},
			channel:       make(chan bool),
			expectErr:     false,
			expectTimeout: false,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			s := createServer(t)
			go runServer(t, s)
			defer s.Stop()

			// Create new consumer with custom callback function
			callback := func(msg *redis.Message) {
				component, dir, payload, err := processMsgPayload(msg.Payload)
				if err != nil {
					test.channel <- false
					return
				}
				if component != "telemetry" || dir != HostVarLogPath {
					test.channel <- false
					return
				}
				if status, ok := payload["status"]; !ok || status != "success" {
					test.channel <- false
					return
				}
				test.channel <- true
			}
			consumer, err := common_utils.NewNotificationConsumer("DEBUG_DATA_RESP_CHANNEL", callback)
			if err != nil {
				t.Fatalf("Failed to create Notification Consumer: %v", err)
			}
			defer consumer.Close()

			// Create a producer to send debug data requests to the server
			producer, err := common_utils.NewNotificationProducer("DEBUG_DATA_REQ_CHANNEL")
			if err != nil {
				t.Fatalf("Failed to create Notification Producer: %v", err)
			}
			defer producer.Close()

			if err := producer.Send(test.component, test.dir, test.payload); err != nil {
				t.Fatalf("Failed to publish to request channel: %v", err)
			}

			select {
			case valid := <-test.channel:
				if !valid {
					if test.expectErr {
						return
					}
					t.Fatal("Failed to successfully write debug data")
				}
				if test.expectErr {
					t.Fatal("Expected error but got valid response")
				}
			case <-time.After(responseTimeout):
				if test.expectTimeout {
					return
				}
				t.Fatalf("Timed out after %v seconds waiting for response on DEBUG_DATA_RESP_CHANNEL", responseTimeout)
			}
			getDebugInfo(t)
		})
		time.Sleep(2 * time.Second)
	}
}

func TestDumpSubscriptionInfo(t *testing.T) {
	ip := "1.1.1.1"
	port := uint64(1234)
	tests := []struct {
		desc      string
		mode      spb.GnmiSubscriptionClientInfo_Mode
		subMode   spb.GnmiSubscriptionClientInfo_SubMode
		startTime time.Time
		syncTime  time.Time
		curDepth  uint64
		maxDepth  uint64
		client    *Client
	}{
		{
			desc:      "StreamSampleOnly",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_STREAM,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_SAMPLE_ONLY,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_STREAM,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
							Mode:           pb.SubscriptionMode_SAMPLE,
							SampleInterval: 100000000,
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "StreamOnChange",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_STREAM,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_ON_CHANGE,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_STREAM,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
							Mode: pb.SubscriptionMode_ON_CHANGE,
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "StreamTargetDefined",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_STREAM,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_STREAM,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
							Mode: pb.SubscriptionMode_TARGET_DEFINED,
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "Once",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_ONCE,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_ONCE,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "Poll",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_POLL,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_POLL,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "UDPAddress",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_STREAM,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_SAMPLE_ONLY,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.UDPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_STREAM,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
							Mode:           pb.SubscriptionMode_SAMPLE,
							SampleInterval: 100000000,
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "NilClient",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_UNKNOWN,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Time{},
			syncTime:  time.Time{},
			curDepth:  0,
			maxDepth:  0,
			client:    nil,
		},
		{
			desc:      "NilAddr",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_UNKNOWN,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Time{},
			syncTime:  time.Time{},
			curDepth:  0,
			maxDepth:  0,
			client: &Client{
				addr: nil,
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_STREAM,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
							Mode:           pb.SubscriptionMode_SAMPLE,
							SampleInterval: 100000000,
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "NilSubscribe",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_UNKNOWN,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: nil,
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "NilSubscriptionList",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_STREAM,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:       &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:         pb.SubscriptionList_STREAM,
					Encoding:     pb.Encoding_PROTO,
					Subscription: nil,
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "NilSubscription",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_STREAM,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_STREAM,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						nil,
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
				qDepthMax: 10,
			},
		},
		{
			desc:      "UnsetMaxQueueDepth",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_POLL,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  8,
			maxDepth:  0,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_POLL,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthCur: 8,
			},
		},
		{
			desc:      "UnsetMaxQueueDepth",
			mode:      spb.GnmiSubscriptionClientInfo_MODE_POLL,
			subMode:   spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
			startTime: time.Unix(10000, 900000),
			syncTime:  time.Unix(904915413, 1325412),
			curDepth:  0,
			maxDepth:  10,
			client: &Client{
				addr: &net.TCPAddr{
					IP:   net.ParseIP("1.1.1.1"),
					Port: 1234,
				},
				subscribe: &pb.SubscriptionList{
					Prefix:   &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
					Mode:     pb.SubscriptionList_POLL,
					Encoding: pb.Encoding_PROTO,
					Subscription: []*pb.Subscription{
						{
							Path: &pb.Path{
								Elem: []*pb.PathElem{
									{Name: "interfaces"},
								},
							},
						},
					},
				},
				startTime: time.Unix(10000, 900000),
				syncTime:  time.Unix(904915413, 1325412),
				qDepthMax: 10,
			},
		},
	}

	s := new(Server)
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			s.cMu.Lock()
			s.clients = map[string]*Client{}
			s.clients["1.1.1.1:1234"] = test.client
			s.cMu.Unlock()

			s.WriteDebugData(HostVarLogPath, "all")
			debugInfo := getDebugInfo(t)
			t.Logf("debugInfo: %v", debugInfo)
			if len(debugInfo.SubscriptionClientInfo) != 1 {
				t.Fatalf("Invalid debugInfo: %v", debugInfo)
			}
			subscriptionInfo := debugInfo.SubscriptionClientInfo[0]
			if subscriptionInfo.IpAddress != ip && test.client != nil && test.client.addr != nil {
				t.Fatalf("Invalid ip: %v", subscriptionInfo.IpAddress)
			}
			if subscriptionInfo.TcpSourcePort != port && test.client != nil && test.client.addr != nil {
				t.Fatalf("Invalid port: %v", subscriptionInfo.TcpSourcePort)
			}
			if subscriptionInfo.SubscriptionMode != test.mode {
				t.Fatalf("Invalid mode: %v", subscriptionInfo.SubscriptionMode)
			}
			if subscriptionInfo.SubscriptionSubMode != test.subMode {
				t.Fatalf("Invalid subMode: %v", subscriptionInfo.SubscriptionSubMode)
			}
			if subscriptionInfo.SubscriptionStart.AsTime().String() != test.startTime.String() {
				t.Fatalf("Invalid startTime: %v vs %v", subscriptionInfo.SubscriptionStart.AsTime(), test.startTime)
			}
			if subscriptionInfo.SyncResponseTime.AsTime().String() != test.syncTime.String() {
				t.Fatalf("Invalid syncTime: %v vs %v", subscriptionInfo.SyncResponseTime.AsTime(), test.syncTime)
			}
			if subscriptionInfo.CurrentQueueDepth != test.curDepth {
				t.Fatalf("Invalid CurrentQueueDepth: %v vs %v", subscriptionInfo.CurrentQueueDepth, test.curDepth)
			}
			if subscriptionInfo.MaxQueueDepth != test.maxDepth {
				t.Fatalf("Invalid maxDepth: %v vs %v", subscriptionInfo.MaxQueueDepth, test.maxDepth)
			}
		})
	}
}

func TestDumpReqTimingInfo(t *testing.T) {
	numRequests := 10
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	savedSsHelper := s.SsHelper
	s.SsHelper = mockSystemStateHelperSuccess{}

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)

	// Send some Get and Set requests to add some data
	pbPath, err := xpath.ToGNMIPath("openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/description")
	if err != nil {
		t.Fatalf("error in unmarshaling path: %v", err)
	}
	getReq := &pb.GetRequest{
		Path:     []*pb.Path{pbPath},
		Encoding: pb.Encoding_JSON_IETF,
	}
	setReq := &pb.SetRequest{
		Replace: []*pb.Update{
			{
				Path: pbPath,
				Val: &pb.TypedValue{
					Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte("{\"description\": \"test\"}")},
				},
			},
		},
	}
	for i := 1; i <= numRequests; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = gClient.Get(ctx, getReq)
		if err != nil {
			t.Fatalf("GetRequest failed: %v", err)
		}
		_, err = gClient.Set(ctx, setReq)
		if err != nil {
			t.Fatalf("SetRequest failed: %v", err)
		}
		cancel()
	}

	s.WriteDebugData(HostVarLogPath, "all")
	debugInfo := getDebugInfo(t)
	t.Logf("debugInfo: %v", debugInfo)
	if len(debugInfo.GetRequestTimingInfo) != numRequests {
		t.Fatalf("Invalid number of Get requests returned in debug data: %v", len(debugInfo.GetRequestTimingInfo))
	}
	for i := 0; i < numRequests; i++ {
		info := debugInfo.GetRequestTimingInfo[i]
		if info.IpAddress == "" {
			t.Fatalf("Empty IpAddress")
		}
		if info.TcpSourcePort == 0 {
			t.Fatalf("Invalid TcpSourcePort")
		}
		if info.StartTime == nil {
			t.Fatalf("Nil StartTime")
		}
		if info.EndTime == nil {
			t.Fatalf("Nil EndTime")
		}
	}
	if len(debugInfo.SetRequestTimingInfo) != numRequests {
		t.Fatalf("Invalid number of Set requests returned in debug data: %v", len(debugInfo.SetRequestTimingInfo))
	}
	for i := 0; i < numRequests; i++ {
		info := debugInfo.SetRequestTimingInfo[i]
		if info.IpAddress == "" {
			t.Fatalf("Empty IpAddress")
		}
		if info.TcpSourcePort == 0 {
			t.Fatalf("Invalid TcpSourcePort")
		}
		if info.StartTime == nil {
			t.Fatalf("Nil StartTime")
		}
		if info.EndTime == nil {
			t.Fatalf("Nil EndTime")
		}
	}

	// Send one more Get and Set request to make sure the server is still working correctly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = gClient.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}
	_, err = gClient.Set(ctx, setReq)
	if err != nil {
		t.Fatalf("SetRequest failed: %v", err)
	}
	s.SsHelper.Close()
	s.SsHelper = savedSsHelper
}

func TestRecordReqTimes(t *testing.T) {
	ip := "1.1.1.1"
	port := 1234
	tests := []struct {
		desc      string
		addr      net.Addr
		startTime time.Time
	}{
		{
			desc: "TcpAddress",
			addr: &net.TCPAddr{
				IP:   net.ParseIP(ip),
				Port: port,
			},
			startTime: time.Unix(10000, 900000),
		},
		{
			desc: "UdpAddress",
			addr: &net.UDPAddr{
				IP:   net.ParseIP(ip),
				Port: port,
			},
			startTime: time.Unix(10000, 900000),
		},
		{
			desc:      "InvalidAddress",
			addr:      nil,
			startTime: time.Unix(10000, 900000),
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			rt := &reqTimes{
				mu:    sync.Mutex{},
				ringL: ring.New(1),
			}
			rt.recordReqTime(&peer.Peer{Addr: test.addr}, test.startTime)

			// Verify contents of ring list
			reqTimes := rt.readReqTimes()
			if len(reqTimes) != 1 {
				t.Fatalf("Failed to read any valid reqTimes: %v", reqTimes)
			}

			info := reqTimes[0]
			if info.IpAddress != ip && test.addr != nil {
				t.Fatalf("Invalid ip address: %v", info.IpAddress)
			}
			if info.TcpSourcePort != uint64(port) && test.addr != nil {
				t.Fatalf("Invalid port: %v", info.TcpSourcePort)
			}
			if info.StartTime.AsTime().String() != test.startTime.String() {
				t.Fatalf("Invalid StartTime: %v", info.StartTime)
			}
		})
	}
}

func getDebugInfo(t *testing.T) *spb.UmfDebugInfo {
	files, err := os.ReadDir(HostVarLogPath)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}
	fileName := ""
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "debug_data") {
			fileName = file.Name()
			break
		}
	}
	if fileName == "" {
		t.Fatalf("Failed to find debug file in %v", HostVarLogPath)
	}
	t.Logf("Found file %v", fileName)
	defer func() {
		err = os.Remove(HostVarLogPath + "/" + fileName)
		if err != nil {
			t.Errorf("Failed to remove file: %v", fileName)
		}
	}()
	body, err := os.ReadFile(HostVarLogPath + "/" + fileName)
	if err != nil {
		t.Fatalf("Failed to open debug file: %v", err)
	}

	debugInfo := new(spb.UmfDebugInfo)
	err = prototext.Unmarshal(body, debugInfo)
	if err != nil {
		t.Fatalf("Failed to unmarshal file contents: %v", err)
	}
	return debugInfo
}

func TestConfigDbJournal(t *testing.T) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	rclient := getConfigDbClient(t, ns)
	defer db.CloseRedisClient(rclient)
	tests := []struct {
		desc          string
		cmd           func()
		expectedEntry string
	}{
		{
			desc: "HSetNew",
			cmd: func() {
				rclient.HSet(context.Background(), "DB_JOURNAL|Test", "new", "test")
			},
			expectedEntry: "hset DB_JOURNAL|Test +new:test",
		},
		{
			desc: "HSetExisting",
			cmd: func() {
				rclient.HSet(context.Background(), "DB_JOURNAL|Test", "new", "already exists")
			},
			expectedEntry: "hset DB_JOURNAL|Test new=already exists",
		},
		{
			desc: "HDel",
			cmd: func() {
				rclient.HDel(context.Background(), "DB_JOURNAL|Test", "new")
			},
			expectedEntry: "hdel DB_JOURNAL|Test -new",
		},
		{
			desc: "Set",
			cmd: func() {
				rclient.Set(context.Background(), "NEW_DBJOURNAL_TABLE", "TEST", 0)
			},
			expectedEntry: "set NEW_DBJOURNAL_TABLE",
		},
		{
			desc: "Del",
			cmd: func() {
				rclient.Del(context.Background(), "NEW_DBJOURNAL_TABLE")
			},
			expectedEntry: "del NEW_DBJOURNAL_TABLE",
		},
	}

	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	// Ensure the keys used in this test are not already in the DB.
	rclient.Del(context.Background(), "DB_JOURNAL|Test")
	rclient.Del(context.Background(), "NEW_DBJOURNAL_TABLE")
	time.Sleep(500 * time.Millisecond)

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Clear the DbJournal file
			err := os.Remove(HostVarLogPath + "/config_db.txt")
			if err != nil {
				t.Fatalf("Failed to remove journal file: %v", err)
			}

			// Trigger a redis event
			test.cmd()

			time.Sleep(500 * time.Millisecond)

			// Verify the contents of the file
			file, err := os.Open(HostVarLogPath + "/config_db.txt")
			if err != nil {
				t.Fatalf("Failed to open file: %v", err)
			}
			defer file.Close()
			data, err := io.ReadAll(file)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}
			if !strings.Contains(string(data), test.expectedEntry) {
				t.Fatalf("Incorrect file contents: %s", data)
			}
		})
	}
}

func TestDynamicPortBreakoutWithSkipLane(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		expectedPorts []struct {
			name    string
			laneSet string
		}
		expectedSkippedPorts []string
	}{
		{
			name:    "[-Ethernet1/4/1,-Ethernet1/4/5] 2X200G",
			payload: "{\"openconfig-interfaces:interfaces\":{\"interface\":[{\"name\":\"Ethernet1/1/1\"}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{},
			expectedSkippedPorts: []string{"Ethernet1/4/1", "Ethernet1/4/5"},
		},
		{
			name:    "[-Ethernet1/4/1,-Ethernet1/4/5,-Ethernet1/4/7] 1X200G+2X100G",
			payload: "{\"openconfig-interfaces:interfaces\":{\"interface\":[{\"name\":\"Ethernet1/1/1\"}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_200GB\",\"index\":0,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":1,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{},
			expectedSkippedPorts: []string{"Ethernet1/4/1", "Ethernet1/4/5", "Ethernet1/4/7"},
		},
		{
			name:    "[-Ethernet1/4/1,-Ethernet1/4/3,-Ethernet1/4/5] 2X100G+1X200G",
			payload: "{\"openconfig-interfaces:interfaces\":{\"interface\":[{\"name\":\"Ethernet1/1/1\"}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":0,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_200GB\",\"index\":1,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{},
			expectedSkippedPorts: []string{"Ethernet1/4/1", "Ethernet1/4/3", "Ethernet1/4/5"},
		},
		{
			name:    "[-Ethernet1/4/1,-Ethernet1/4/3,-Ethernet1/4/5,-Ethernet1/4/7] 4X100G",
			payload: "{\"openconfig-interfaces:interfaces\":{\"interface\":[{\"name\":\"Ethernet1/1/1\"}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":4,\"breakout-speed\":\"SPEED_100GB\",\"num-physical-channels\":2}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{},
			expectedSkippedPorts: []string{"Ethernet1/4/1", "Ethernet1/4/3", "Ethernet1/4/5", "Ethernet1/4/7"},
		},
		{
			name:    "[Ethernet1/4/1,-Ethernet1/4/5] 2X200G",
switch:Ethernet1/4/1\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/1\",\"openconfig-p4rt:id\":4,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/1\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_200GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18,19,20",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/5"},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/5,-Ethernet1/4/7] 1X200G+2X100G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":1,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}},{\"index\":1,\"config\":{\"index\":1,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_100GB\",\"num-physical-channels\":2}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18,19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/7"},
		},
		{
			name:    "[Ethernet1/4/1,-Ethernet1/4/3,Ethernet1/4/5] 2X100G+1X200G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_200GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_100GB\",\"num-physical-channels\":2}},{\"index\":1,\"config\":{\"index\":1,\"num-breakouts\":1,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22,23,24",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/3"},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/3,-Ethernet1/4/5,Ethernet1/4/7] 4X100G",
switch:Ethernet1/4/7\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/7\",\"openconfig-p4rt:id\":1540,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/7\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":0,\"num-breakouts\":4,\"num-physical-channels\":2},\"index\":0}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/3",
					laneSet: "19,20",
				},
				{
					name:    "Ethernet1/4/7",
					laneSet: "23,24",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/5"},
		},
		{
			name:    "[Ethernet1/4/1,-Ethernet1/4/5,-Ethernet1/4/7] 1X200G+2X100G",
switch:Ethernet1/4/1\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/1\",\"openconfig-p4rt:id\":4,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/1\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_200GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":1,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}},{\"index\":1,\"config\":{\"index\":1,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_100GB\",\"num-physical-channels\":2}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18,19,20",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/5", "Ethernet1/4/7"},
		},
		{
			name:    "[Ethernet1/4/1,-Ethernet1/4/3,-Ethernet1/4/5] 2X100G+1X200G",
switch:Ethernet1/4/1\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/1\",\"openconfig-p4rt:id\":4,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/1\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"port-id\":4},\"breakout-mode\":{\"groups\":{\"group\":[{\"index\":0,\"config\":{\"index\":0,\"num-breakouts\":2,\"breakout-speed\":\"SPEED_100GB\",\"num-physical-channels\":2}},{\"index\":1,\"config\":{\"index\":1,\"num-breakouts\":1,\"breakout-speed\":\"SPEED_200GB\",\"num-physical-channels\":4}}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/3", "Ethernet1/4/5"},
		},
		{
			name:    "[Ethernet1/4/1,-Ethernet1/4/3,-Ethernet1/4/5,Ethernet1/4/7] 4X100G",
switch:Ethernet1/4/7\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/7\",\"openconfig-p4rt:id\":1540,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/7\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":0,\"num-breakouts\":4,\"num-physical-channels\":2},\"index\":0}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/7",
					laneSet: "23,24",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/3", "Ethernet1/4/5"},
		},
		{
			name:    "[Ethernet1/4/1,-Ethernet1/4/3,Ethernet1/4/5,-Ethernet1/4/7] 4X100G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":0,\"num-breakouts\":4,\"num-physical-channels\":2},\"index\":0}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/3", "Ethernet1/4/7"},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/3,Ethernet1/4/5,Ethernet1/4/7] 4X100G",
switch:Ethernet1/4/7\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/7\",\"openconfig-p4rt:id\":1540,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/7\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":0,\"num-breakouts\":4,\"num-physical-channels\":2},\"index\":0}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/3",
					laneSet: "19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
				{
					name:    "Ethernet1/4/7",
					laneSet: "23,24",
				},
			},
			expectedSkippedPorts: []string{},
		},
		{
			name:    "[-Ethernet1/4/1,Ethernet1/4/3,Ethernet1/4/5,Ethernet1/4/7] 4X100G",
switch:Ethernet1/4/7\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/7\",\"openconfig-p4rt:id\":1540,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/7\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_100GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_100GB\",\"index\":0,\"num-breakouts\":4,\"num-physical-channels\":2},\"index\":0}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/3",
					laneSet: "19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
				{
					name:    "Ethernet1/4/7",
					laneSet: "23,24",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/1"},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/3,Ethernet1/4/5,Ethernet1/4/7] 4X50G",
switch:Ethernet1/4/7\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/7\",\"openconfig-p4rt:id\":1540,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/7\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_50GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_50GB\",\"index\":0,\"num-breakouts\":4,\"num-physical-channels\":2},\"index\":0}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/3",
					laneSet: "19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
				{
					name:    "Ethernet1/4/7",
					laneSet: "23,24",
				},
			},
			expectedSkippedPorts: []string{},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/5,-Ethernet1/4/7] 1X400G+2X200G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_200GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_400GB\",\"index\":0,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_200GB\",\"index\":1,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18,19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/7"},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/5,-Ethernet1/4/7] 1X200G+2X50G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_50GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_200GB\",\"index\":0,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_50GB\",\"index\":1,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18,19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/7"},
		},
		{
			name:    "[-Ethernet1/4/1,Ethernet1/4/3,Ethernet1/4/5] 2X50G+1X200G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_200GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_50GB\",\"index\":0,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_200GB\",\"index\":1,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/3",
					laneSet: "19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22,23,24",
				},
			},
			expectedSkippedPorts: []string{"Ethernet1/4/1"},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/5,Ethernet1/4/7] 1X400G+2X50G",
switch:Ethernet1/4/7\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/7\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/7\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_50GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_400GB\",\"index\":0,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_50GB\",\"index\":1,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18,19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22",
				},
				{
					name:    "Ethernet1/4/7",
					laneSet: "23,24",
				},
			},
			expectedSkippedPorts: []string{},
		},
		{
			name:    "[Ethernet1/4/1,Ethernet1/4/3,Ethernet1/4/5] 2X50G+1X400G",
switch:Ethernet1/4/5\",\"google-pins-interfaces:port-direction\":\"FABRIC_FACING\",\"loopback-mode\":\"NONE\",\"mtu\":9216,\"name\":\"Ethernet1/4/5\",\"openconfig-p4rt:id\":1028,\"openconfig-pins-interfaces:health-indicator\":\"GOOD\",\"type\":\"iana-if-type:ethernetCsmacd\"},\"hold-time\":{\"config\":{\"down\":0,\"up\":8000}},\"name\":\"Ethernet1/4/5\",\"openconfig-if-ethernet:ethernet\":{\"config\":{\"fec-mode\":\"openconfig-if-ethernet:FEC_DISABLED\",\"port-speed\":\"openconfig-if-ethernet:SPEED_400GB\"}},\"subinterfaces\":{\"subinterface\":[{\"config\":{\"index\":0},\"index\":0,\"openconfig-if-ip:ipv6\":{\"unnumbered\":{\"config\":{\"enabled\":true}}}}]}}]},\"openconfig-platform:components\":{\"component\":[{\"config\":{\"name\":\"1/4\"},\"name\":\"1/4\",\"port\":{\"config\":{\"openconfig-pins-platform-port:port-id\":4},\"openconfig-platform-port:breakout-mode\":{\"groups\":{\"group\":[{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_50GB\",\"index\":0,\"num-breakouts\":2,\"num-physical-channels\":2},\"index\":0},{\"config\":{\"breakout-speed\":\"openconfig-if-ethernet:SPEED_400GB\",\"index\":1,\"num-breakouts\":1,\"num-physical-channels\":4},\"index\":1}]}}}}]}}",
			expectedPorts: []struct {
				name    string
				laneSet string
			}{
				{
					name:    "Ethernet1/4/1",
					laneSet: "17,18",
				},
				{
					name:    "Ethernet1/4/3",
					laneSet: "19,20",
				},
				{
					name:    "Ethernet1/4/5",
					laneSet: "21,22,23,24",
				},
			},
			expectedSkippedPorts: []string{},
		},
	}
	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()
	s.SsHelper = mockSystemStateHelperSuccess{}

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ns, _ := sdcfg.GetDbDefaultNamespace()
	cfgDbId, _ := sdcfg.GetDbId("CONFIG_DB", ns)
	configDB := getRedisClientN(t, cfgDbId, ns)
	defer db.CloseRedisClient(configDB)
	appStateDbId, _ := sdcfg.GetDbId("APPL_STATE_DB", ns)
	applStateDB := getRedisClientN(t, appStateDbId, ns)
	defer db.CloseRedisClient(applStateDB)
	stateDbId, _ := sdcfg.GetDbId("STATE_DB", ns)
	stateDB := getRedisClientN(t, stateDbId, ns)
	defer db.CloseRedisClient(stateDB)

	// Bootstrap the first test mode
	initPorts := []string{"Ethernet1/4/1", "Ethernet1/4/5"}
	for _, existPort := range initPorts {
		if _, err := applStateDB.HSet(context.Background(), "PORT_STATE:"+existPort, "phase", "pending_delete").Result(); err != nil {
			t.Fatalf("Failed to set pending delete for PORT_STATE: %v: %v", existPort, err)
		}
		if _, err := applStateDB.HSet(context.Background(), "PORT_TABLE:"+existPort, "index", "4").Result(); err != nil {
			t.Fatalf("Failed to set index for PORT_STATE: %v: %v", existPort, err)
		}
		if _, err := applStateDB.HSet(context.Background(), "INTF_TABLE:"+existPort, "unnumbered_enabled", "true").Result(); err != nil {
			t.Fatalf("Failed to set unnumbered_enabled for PORT_STATE: %v: %v", existPort, err)
		}
		// In STATE_DB, add PORT_TABLE
		if _, err := stateDB.HSet(context.Background(), "PORT_TABLE|"+existPort, "ref_count", "1").Result(); err != nil {
			t.Fatalf("Failed to set unnumbered_enabled for PORT_STATE: %v: %v", existPort, err)
		}
	}

	req := &pb.SetRequest{
		Replace: []*pb.Update{
			{
				Path: &pb.Path{
					Target: "OC_YANG",
					Elem: []*pb.PathElem{
						{
							Name: "openconfig",
						},
					},
				},
				Val: &pb.TypedValue{
					Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(tests[0].payload)},
				},
			},
		},
	}

	_, err = gClient.Set(ctx, req)
	if err != nil {
		t.Fatalf("SetRequest failed: %v", err)
	}

	for _, removePort := range tests[0].expectedSkippedPorts {
		// In APPL_STATE_DB, delete PORT_TABLE, INTF_TABLE, and BUFFER_QUEUE_TABLEs
		if _, err := applStateDB.Del(context.Background(), "PORT_TABLE:"+removePort).Result(); err != nil {
			t.Fatalf("Failed to delete PORT_TABLE:%v: %v", removePort, err)
		}
		if _, err := applStateDB.Del(context.Background(), "INTF_TABLE:"+removePort).Result(); err != nil {
			t.Fatalf("Failed to delete INTF_TABLE:%v: %v", removePort, err)
		}
		for i := 0; i < 8; i++ {
			qID := strconv.Itoa(i)
			if _, err := applStateDB.Del(context.Background(), "BUFFER_QUEUE_TABLE:"+removePort+":"+qID).Result(); err != nil {
				t.Fatalf("Failed to delete BUFFER_QUEUE_TABLE:%v: %v", removePort, err)
			}
		}
		// In STATE_DB, delete PORT_TABLE
		if _, err := stateDB.Del(context.Background(), "PORT_TABLE|"+removePort).Result(); err != nil {
			t.Fatalf("Failed to delete PORT_TABLE|%v: %v", removePort, err)
		}
	}

	var transtionList [][]int
	for i := 0; i < len(tests)-1; i++ {
		transtionList = append(transtionList, []int{i, i})
		transtionList = append(transtionList, []int{i, i + 1})
	}

	transtionList = append(transtionList, []int{len(tests) - 1, len(tests) - 1})
	for i := 1; i < len(tests); i++ {
		for j := 0; j < len(tests)-i; j++ {
			transtionList = append(transtionList, []int{len(tests) - i, j})
			if len(tests)-i-1 == j {
				continue
			}
			transtionList = append(transtionList, []int{j, len(tests) - i})
		}
	}

	for _, transtion := range transtionList {
		from_status := tests[transtion[0]]
		to_status := tests[transtion[1]]
		testName := from_status.name + "-->" + to_status.name
		t.Run(testName, func(t *testing.T) {
			// Set pending_delete for the ports existed in test[i].
			// These ports will undergo the DBP. Skipped port in test[i]
			// was not created, no need to set pending delete
			for _, port := range from_status.expectedPorts {
				if _, err := applStateDB.HSet(context.Background(), "PORT_STATE:"+port.name, "phase", "pending_delete").Result(); err != nil {
					t.Fatalf("%v - Failed to set pending delete for PORT_STATE: %v: %v", testName, port.name, err)
				}
				// In APPL_STATE_DB, add PORT_TABLE and INTF_TABLE
				if _, err := applStateDB.HSet(context.Background(), "PORT_TABLE:"+port.name, "index", "4").Result(); err != nil {
					t.Fatalf("%v - Failed to set index for PORT_TABLE:%v: %v", testName, port.name, err)
				}
				if _, err := applStateDB.HSet(context.Background(), "INTF_TABLE:"+port.name, "unnumbered_enabled", "true").Result(); err != nil {
					t.Fatalf("%v - Failed to set unnumbered_enabled for INTF_TABLE:%v: %v", testName, port.name, err)
				}
				// In STATE_DB, add PORT_TABLE
				if _, err := stateDB.HSet(context.Background(), "PORT_TABLE|"+port.name, "ref_count", "1").Result(); err != nil {
					t.Fatalf("%v - Failed to set ref_count for PORT_TABLE|%v: %v", testName, port.name, err)
				}
			}

			req := &pb.SetRequest{
				Replace: []*pb.Update{
					{
						Path: &pb.Path{
							Target: "OC_YANG",
							Elem: []*pb.PathElem{
								{
									Name: "openconfig",
								},
							},
						},
						Val: &pb.TypedValue{
							Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(to_status.payload)},
						},
					},
				},
			}

			_, err := gClient.Set(ctx, req)
			if err != nil {
				t.Fatalf("SetRequest failed: %v", err)
			}

			// Verify expected ports exist
			for _, port := range to_status.expectedPorts {
				laneField, err := configDB.HGet(context.Background(), "PORT|"+port.name, "lanes").Result()
				if err != nil {
					t.Fatalf("%v - Failed to read lanes from PORT|%v in config db.", testName, port.name)
				}
				if laneField != port.laneSet {
					t.Fatalf("%v - Expect PORT %v with %v, got %v", testName, port.name, port.laneSet, laneField)
				}
			}

			// Verify skipped ports are missing
			for _, skipPort := range to_status.expectedSkippedPorts {
				val, err := configDB.HKeys(context.Background(), "PORT|"+skipPort).Result()
				if err != nil || len(val) > 0 {
					t.Fatalf("%v - Expect no PORT table %v in config db. Got: %v with error %v", testName, skipPort, val, err)
				}
			}
			// If ports removed because of breakin or skip, remove it from APPL_STATE_DB and STATE_DB
			var breakinPorts []string
			for _, from_port := range from_status.expectedPorts {
				if !slices.Contains(to_status.expectedSkippedPorts, from_port.name) {
					var exist bool
					for _, to_port := range to_status.expectedPorts {
						if to_port.name == from_port.name {
							exist = true
							break
						}
					}
					if !exist {
						breakinPorts = append(breakinPorts, from_port.name)
					}
				}
			}
			for _, removePort := range append(breakinPorts, to_status.expectedSkippedPorts...) {
				// In APPL_STATE_DB, delete PORT_TABLE and INTF_TABLE
				if _, err := applStateDB.Del(context.Background(), "PORT_TABLE:"+removePort).Result(); err != nil {
					t.Fatalf("%v - Failed to delete PORT_TABLE:%v: %v", testName, removePort, err)
				}
				if _, err := applStateDB.Del(context.Background(), "INTF_TABLE:"+removePort).Result(); err != nil {
					t.Fatalf("%v - Failed to delete INTF_TABLE:%v: %v", testName, removePort, err)
				}
				for i := 0; i < 8; i++ {
					qID := strconv.Itoa(i)
					if _, err := applStateDB.Del(context.Background(), "BUFFER_QUEUE_TABLE:"+removePort+":"+qID).Result(); err != nil {
						t.Fatalf("Failed to delete BUFFER_QUEUE_TABLE:%v: %v", removePort, err)
					}
				}
				// In STATE_DB, delete PORT_TABLE
				if _, err := stateDB.Del(context.Background(), "PORT_TABLE|"+removePort).Result(); err != nil {
					t.Fatalf("%v - Failed to delete PORT_TABLE|%v: %v", testName, removePort, err)
				}
			}
		})
	}
}

func TestGetEncoding(t *testing.T) {
	tests := []struct {
		name string
		itrs int
		req  *pb.GetRequest
	}{
		{
			name: "GetConfigPROTO",
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_CONFIG,
				Encoding: pb.Encoding_PROTO,
			},
		},
		{
			name: "GetConfigJSON",
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_CONFIG,
				Encoding: pb.Encoding_JSON_IETF,
			},
		},
		{
			name: "GetConfigPROTOReset",
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_CONFIG,
				Encoding: pb.Encoding_PROTO,
			},
		},
	}

	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gClient := pb.NewGNMIClient(conn)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			res, err := gClient.Get(ctx, test.req)
			if err != nil {
				t.Fatalf("GetRequest failed: %v", err)
			}
			notifs := res.GetNotification()

			for _, notif := range notifs {
				updates := notif.Update
				for _, update := range updates {
					if test.req.Encoding == pb.Encoding_JSON_IETF && (update.Val.GetJsonIetfVal() == nil || len(update.Val.GetJsonIetfVal()) == 0) {
						t.Fatalf("GetRequest failed with incorrect encoding. Wanted %v", pb.Encoding_JSON_IETF)
					}
					if test.req.Encoding == pb.Encoding_PROTO && (update.Val.GetJsonIetfVal() != nil && len(update.Val.GetJsonIetfVal()) > 0) {
						t.Fatalf("GetRequest failed with incorrect encoding. Wanted %v", pb.Encoding_PROTO)
					}
				}
			}
		})
	}
}

func TestGetBenchmark(t *testing.T) {
	tests := []struct {
		name string
		itrs int
		req  *pb.GetRequest
	}{
		{
			name: "GetConfig",
			itrs: 1,
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_CONFIG,
				Encoding: pb.Encoding_JSON_IETF,
			},
		},
		{
			name: "GetState",
			itrs: 1,
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_STATE,
				Encoding: pb.Encoding_JSON_IETF,
			},
		},
		{
			name: "GetAll",
			itrs: 1,
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Encoding: pb.Encoding_JSON_IETF,
			},
		},
		{
			name: "GetSinglePath",
			itrs: 1,
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Path: []*pb.Path{
					&pb.Path{
						Elem: []*pb.PathElem{
							{
								Name: "system",
							},
							{
								Name: "state",
							},
							{
								Name: "config-meta-data",
							},
						},
					},
				},
				Encoding: pb.Encoding_JSON_IETF,
			},
		},
	}

	// Load DB snapshots
	prepareDbUtil(t, "CONFIG_DB", "", "../testdata/db_snapshots/rtor/config_db.json")
	prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/db_snapshots/rtor/appl_state_db.json")
	prepareDbUtil(t, "COUNTERS_DB", "", "../testdata/db_snapshots/rtor/counters_db.json")
	prepareDbUtil(t, "STATE_DB", "", "../testdata/db_snapshots/rtor/state_db.json")

	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	s.config.CacheResponses = false
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Disable DEBUG logs if enabled
	if v := flag.Lookup("v"); v != nil {
		defer flag.Set("v", v.Value.String())
	}
	flag.Set("v", "2")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Uncomment to enable CPU profiling -- results in sonic-gnmi/artifacts/
			/* cpuprof, profErr := os.OpenFile(fmt.Sprintf("/mnt/artifacts/%s_cpu.prof", test.name), os.O_CREATE|os.O_WRONLY, 0755)
			if profErr != nil {
				t.Fatalf("Failed to open CPU profile file: %v", profErr)
			}
			defer cpuprof.Close()
			if err := pprof.StartCPUProfile(cpuprof); err != nil {
				t.Fatalf("Failed to start CPU profiling: %v", err)
			}
			defer pprof.StopCPUProfile() */

			// Run the benchmark test
			res := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < test.itrs; i++ {
					_, err := gClient.Get(ctx, test.req)
					if err != nil {
						t.Fatalf("GetRequest failed: %v", err)
					}
				}
			})
			t.Logf("BenchmarkResults:\nItrs=%v\nTime=%v\nMemAllocs=%v\nMemBytes=%v", test.itrs, time.Duration(res.T.Nanoseconds()/int64(test.itrs)), res.MemAllocs/uint64(test.itrs), res.MemBytes/uint64(test.itrs))
		})
	}
}

func TestSetBenchmark(t *testing.T) {
	tests := []struct {
		name         string
		itrs         int
		file         string
		db_files_dir string
	}{
		{
			name: "RToR",
			itrs: 1,
			// file:         "../testdata/benchmark/rtor_config.json",
			file:         "../testdata/benchmark/rtor_config_pumf.json",
			db_files_dir: "../testdata/db_snapshots/rtor/",
		},
	}

	// Clear INTERFACE tables that could fail the SET request
	rc := getConfigDbClient(t, "")
	keys, _ := rc.Keys(context.Background(), "INTERFACE|*").Result()
	if _, err := rc.Del(context.Background(), keys...).Result(); err != nil {
		t.Logf("Failed to delete INTERFACE tables: %v", err)
	}

	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Disable DEBUG logs if enabled
	if v := flag.Lookup("v"); v != nil {
		defer flag.Set("v", v.Value.String())
	}
	flag.Set("v", "2")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Load DB snapshots
			prepareDbUtil(t, "CONFIG_DB", "", filepath.Join(test.db_files_dir, "config_db.json"))
			prepareDbUtil(t, "APPL_STATE_DB", "", filepath.Join(test.db_files_dir, "appl_state_db.json"))
			prepareDbUtil(t, "COUNTERS_DB", "", filepath.Join(test.db_files_dir, "counters_db.json"))
			prepareDbUtil(t, "STATE_DB", "", filepath.Join(test.db_files_dir, "state_db.json"))

			// Read the config
			payload, err := ioutil.ReadFile(test.file)
			if err != nil {
				t.Fatalf("Failed to read config from file")
			}

			req := &pb.SetRequest{
				Replace: []*pb.Update{
					{
						Path: &pb.Path{
							Target: "OC_YANG",
							Elem: []*pb.PathElem{
								{
									Name: "openconfig",
								},
							},
						},
						Val: &pb.TypedValue{
							Value: &pb.TypedValue_JsonIetfVal{JsonIetfVal: payload},
						},
					},
				},
			}

			// Uncomment to enable CPU profiling -- results in sonic-gnmi/artifacts/
			/* cpuprof, profErr := os.OpenFile(fmt.Sprintf("/mnt/artifacts/%s_cpu.prof", test.name), os.O_CREATE|os.O_WRONLY, 0755)
			if profErr != nil {
				t.Fatalf("Failed to open CPU profile file: %v", profErr)
			}
			defer cpuprof.Close()
			if err := pprof.StartCPUProfile(cpuprof); err != nil {
				t.Fatalf("Failed to start CPU profiling: %v", err)
			}
			defer pprof.StopCPUProfile() */

			// Run the benchmark test
			res := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < test.itrs; i++ {
					_, err := gClient.Set(ctx, req)
					if err != nil {
						t.Fatalf("SetRequest failed: %v", err)
					}
				}
			})
			t.Logf("BenchmarkResults:\nItrs=%v\nTime=%v\nMemAllocs=%v\nMemBytes=%v", test.itrs, time.Duration(res.T.Nanoseconds()/int64(test.itrs)), res.MemAllocs/uint64(test.itrs), res.MemBytes/uint64(test.itrs))
		})
	}
}

func TestSubscribeBenchmark(t *testing.T) {
	tests := []struct {
		name  string
		itrs  int
		paths []struct {
			path string
			mode pb.SubscriptionMode
		}
	}{
		{
			
			name:  "Pictor",
			itrs:  1,
			paths: pictorPaths,
		},
		{
			
			name:  "CONTROLLER",
			itrs:  1,
			paths: controllerPaths,
		},
	}

	// Load DB snapshots
	prepareDbUtil(t, "CONFIG_DB", "", "../testdata/db_snapshots/rtor/config_db.json")
	prepareDbUtil(t, "APPL_STATE_DB", "", "../testdata/db_snapshots/rtor/appl_state_db.json")
	prepareDbUtil(t, "COUNTERS_DB", "", "../testdata/db_snapshots/rtor/counters_db.json")
	prepareDbUtil(t, "STATE_DB", "", "../testdata/db_snapshots/rtor/state_db.json")

	// Create the server
	s := createServer(t)
	s.config.EnableTranslation = true
	go runServer(t, s)
	defer s.Stop()

	// The server is ready - now a request is needed.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
	}
	defer conn.Close()
	gClient := pb.NewGNMIClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Disable DEBUG logs if enabled
	if v := flag.Lookup("v"); v != nil {
		defer flag.Set("v", v.Value.String())
	}
	flag.Set("v", "2")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Construct the subscription
			subs := []*pb.Subscription{}
			for _, sub := range test.paths {
				path, err := xpath.ToGNMIPath(sub.path)
				if err != nil {
					t.Fatalf("Failed to convert string to GNMI path: %v", err)
				}
				subs = append(subs, &pb.Subscription{
					Path: path,
					Mode: sub.mode,
				})
			}
			subscription := &pb.SubscribeRequest{
				Request: &pb.SubscribeRequest_Subscribe{
					Subscribe: &pb.SubscriptionList{
						Prefix:       &pb.Path{Origin: "openconfig", Target: "OC_YANG"},
						Mode:         pb.SubscriptionList_STREAM,
						Encoding:     pb.Encoding_PROTO,
						Subscription: subs,
					},
				},
			}

			// Uncomment to enable CPU profiling -- results in sonic-gnmi/artifacts/
			/* cpuprof, profErr := os.OpenFile(fmt.Sprintf("/mnt/artifacts/%s_cpu.prof", test.name), os.O_CREATE|os.O_WRONLY, 0755)
			if profErr != nil {
				t.Fatalf("Failed to open CPU profile file: %v", profErr)
			}
			defer cpuprof.Close()
			if err := pprof.StartCPUProfile(cpuprof); err != nil {
				t.Fatalf("Failed to start CPU profiling: %v", err)
			}
			defer pprof.StopCPUProfile() */

			// Run the benchmark test
			res := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < test.itrs; i++ {
					stream, err := gClient.Subscribe(ctx, grpc.MaxCallRecvMsgSize(6000000))
					if err != nil {
						t.Fatal(err.Error())
					}
					// Send the subscription and wait for sync response
					if err = stream.Send(subscription); err != nil {
						t.Fatalf("Failed to send subscription: %v", err)
					}
					for {
						resp, err := stream.Recv()
						if err != nil {
							t.Fatalf("Failed to receive response: %v", err)
						}
						if resp.GetSyncResponse() {
							break
						}
					}
					stream.CloseSend()
				}
			})
			t.Logf("BenchmarkResults:\nItrs=%v\nTime=%v\nMemAllocs=%v\nMemBytes=%v", test.itrs, time.Duration(res.T.Nanoseconds()/int64(test.itrs)), res.MemAllocs/uint64(test.itrs), res.MemBytes/uint64(test.itrs))
		})
	}
}

func init() {
	// Enable logs at UT setup
	flag.Lookup("v").Value.Set("10")
	flag.Lookup("log_dir").Value.Set("/tmp/telemetrytest")

	// Inform gNMI server to use redis tcp localhost connection
	sdc.UseRedisLocalTcpPort = true
}

func TestMain(m *testing.M) {
	defer test_utils.MemLeakCheck()
	m.Run()
}
