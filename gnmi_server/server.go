package gnmi

import (
	"bytes"
	"container/ring"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/sonic-mgmt-common/cvl/custom_validation"
	"github.com/Azure/sonic-mgmt-common/translib"
	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	"github.com/sonic-net/sonic-gnmi/metric_recorder"
	"github.com/sonic-net/sonic-gnmi/pathz_authorizer"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	gnmi_extpb "github.com/openconfig/gnmi/proto/gnmi_ext"
	gnoi_diag "github.com/openconfig/gnoi/diag"
	"github.com/openconfig/gnoi/factory_reset"
	gnoi_file "github.com/openconfig/gnoi/file"
	gnoi_healthz "github.com/openconfig/gnoi/healthz"
	gnoi_os "github.com/openconfig/gnoi/os"
	gnoi_system_pb "github.com/openconfig/gnoi/system"
	gnsiAuthzpb "github.com/openconfig/gnsi/authz"
	gnsiCertzpb "github.com/openconfig/gnsi/certz"
	gnsiCredzpb "github.com/openconfig/gnsi/credentialz"
	gnsiPathzpb "github.com/openconfig/gnsi/pathz"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	gnmipt "github.com/sonic-net/sonic-gnmi/gnmi_server/pathtransl"
	spb "github.com/sonic-net/sonic-gnmi/proto/gnmi_sonic"
	gnoi_blackbox_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/blackbox"
	gnoi_burnin_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/burnin"
	gnoi_debug_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/debug"
	gnoi_ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"
	gnoi_qual_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/qualification"
	gnoi_whitebox_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/whitebox"
	spb_gnoi "github.com/sonic-net/sonic-gnmi/proto/sonic_gnoi"
	spb_jwt_gnoi "github.com/sonic-net/sonic-gnmi/proto/sonic_gnoi/jwt"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"
	testcert "github.com/sonic-net/sonic-gnmi/testdata/tls"
	"golang.org/x/net/context"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"google.golang.org/grpc"
	"google.golang.org/grpc/authz"
	"google.golang.org/grpc/authz/audit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/tls/certprovider"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/security/advancedtls"
	"google.golang.org/grpc/status"

)

// Path to the `/var/log/telemetry-con` directory on the `host` side.
const (
	HostVarLogPath            = "/var/log"
	authLogPath               = "/host_var/log/messages"
	StackTraceFileNamePrefix  = "stack-trace"
	StackTraceFileNameSuffix  = ".txt"
	StackTraceFileNamePattern = StackTraceFileNamePrefix + ".*" + StackTraceFileNameSuffix
	authzRefreshingInterval   = 5 * time.Second
	AF4                       = 0x80 // expected Traffic Class for observer gNxI connections.
	NC1                       = 0xC0 // expected Traffic Class for controller gNxI connections.
)

var (
	muPath                        = &sync.RWMutex{}
	supportedEncodings            = []gnmipb.Encoding{gnmipb.Encoding_JSON, gnmipb.Encoding_JSON_IETF, gnmipb.Encoding_PROTO}
	keepaliveTime                 = 1 * time.Second
	keepaliveTimeout              = 20 * time.Second
	keepaliveMinTime              = 1 * time.Second
	keepaliveMaxIdle              = time.Duration(0)
	dbusCaller         ssc.Caller = &ssc.DbusCaller{}
	muTOS                         = &sync.RWMutex{}
	IPToConn                      = map[net.Addr]net.Conn{}
	successfulSets                = 0
	failedSets                    = 0
)

func resetDbusCaller() {
	dbusCaller = &ssc.DbusCaller{}
}

// Server manages a single gNMI Server implementation. Each client that connects
// via Subscribe or Get will receive a stream of updates based on the requested
// path. Set request is processed by server too.
type Server struct {
	s             *grpc.Server
	lis           net.Listener
	config        *Config
	cMu           sync.Mutex
	clients       map[string]*Client
	certProviders []certprovider.Provider
	// SaveStartupConfig points to a function that is called to save changes of
	// configuration to a file. By default it points to an empty function -
	// the configuration is not saved to a file.
	SaveStartupConfig func() error
	SsHelper          common_utils.SystemStateHelperInterface
	// ReqFromMaster point to a function that is called to verify if the request
	// comes from a master controller.
	ReqFromMaster func(req *gnmipb.SetRequest, masterEID *uint128) error
	masterEID     uint128
	// gNOI Servers
	debugServer *GNOIDebugServer
	ldsServer   *GNOILdsServer
	osServer    *OSServer
	fileServer  *GNOIFileServer
	hlthServer  *GNOIHealthzServer
	// gNSI Servers
	authzWatcher *authz.FileWatcherInterceptor
	recorder     *metric_recorder.SecurityMetricRecorder
	gnsiAuthz    *GNSIAuthzServer
	gnsiCertz    *GNSICertzServer
	gnsiCredz    *GNSICredentialzServer
	gnsiPathz    *GNSIPathzServer
	// NSF/ISSU Helper
	WarmRestartHelper common_utils.WarmRestartHelperInterface
	// Healthz Debug Data Handler
	debugHandler *debugHandler
	getReqTimes  *reqTimes
	setReqTimes  *reqTimes
	// DB Journals
	configDbJournal *DbJournal
	// GetResponse Caches
	GetConfigCache *GetResponseCache
	// Connection Management/Limits
	ConnectionManager *ConnectionManager

	// Mandatory gRPC embeddings
	gnoi_diag.UnimplementedDiagServer
	gnoi_burnin_pb.UnimplementedBurninServer
	factory_reset.UnimplementedFactoryResetServer
	// UnimplementedSystemServer is embedded to satisfy SystemServer interface requirements
	gnoi_system_pb.UnimplementedSystemServer
	gnoi_file.UnimplementedFileServer
	gnoi_whitebox_pb.UnimplementedWhiteBoxTestServer
}

type AuthTypes map[string]bool

// OSConfig is a collection of values for OSServer.
type OSConfig struct {
	ImgDir          string                       // Path to the directory where image is stored.
	ProcessTrfReady func(string) (string, error) // Function that handles TrancontrollerrReady request.
	ProcessTrfEnd   func(string) (string, error) // Function that handles TrancontrollerrEnd request.
}

// Config is a collection of values for Server
type Config struct {
	// Port for the Server to listen on. If 0 or unset the Server will pick a port
	// for this Server.
	Port                int64
	LogLevel            int
	StreamingThreshold  int
	UnaryThreshold      int
	UserAuth            AuthTypes
	EnableTranslibWrite bool
	EnableNativeWrite   bool
	ZmqPort             string
	IdleConnDuration    int
	ConfigTableName     string
	EnableTranslation   bool
	GetOptions          func(*Config) ([]grpc.ServerOption, []certprovider.Provider, error)
	AuthzPolicy         bool   // Enable authz policy.
	AuthzPolicyFile     string // Path to JSON file with authz policies.
	AuthzMetaFile       string // Path to JSON file with authz metadata.
	PathzPolicy         bool   // Enable gNMI pathz policy.
	PathzPolicyFile     string // Path to gNMI pathz policy file.
	PathzMetaFile       string // Path to JSON file with pathz metadata.
	EnableSRXfmr        bool   // Enable Set-Replace transformation.
	CacheResponses      bool   // Cache gNMI responses when possible.
	SshCredMetaFile     string // Path to JSON file with SSH server credential metadata.
	ConsoleCredMetaFile string // Path to JSON file with console credential metadata.
	CertzMetaFile       string // Path to JSON file with gRPC credential metadata.
	// mTLS flags
	CaCertLnk     string // Path to symlink pointing to current CA certificate.
	SrvCertLnk    string // Path to symlink pointing to current server's certificate.
	SrvKeyLnk     string // Path to symlink pointing to current server's private key.
	CaCertFile    string // Path to the first CA certificate.
	SrvCertFile   string // Path to the first server's certificate.
	SrvKeyFile    string // Path to the first server's private key.
	CertCRLConfig string // Path to the CRL directory. Disable if empty.
	IntManFile    string // Path to the Integrity Manifest file.
	// gNOI
	OSCfg    *OSConfig
	SsHelper common_utils.SystemStateHelperInterface
}

type authzLogger struct {
	recorder *metric_recorder.SecurityMetricRecorder
}

func (al *authzLogger) Log(event *audit.Event) {
	if !event.Authorized {
		log.V(lvl.INFO).Infof("user %s does not have permission on RPC %s", event.Principal, event.FullMethodName)
	}
	var service, rpc string
	strs := strings.Split(event.FullMethodName, "/")
	if len(strs) == 3 {
		service = strs[1]
		rpc = strs[2]
		al.recorder.Record(metric_recorder.AuthzRecord{Permitted: event.Authorized, Service: service, Rpc: rpc})
	} else {
		log.V(lvl.ERROR).Infof("invalid RPC method %s", event.FullMethodName)
	}
}

type loggerBuilder struct {
	recorder *metric_recorder.SecurityMetricRecorder
}

func (loggerBuilder) Name() string {
	return "authz_logger"
}

func (lb *loggerBuilder) Build(audit.LoggerConfig) audit.Logger {
	return &authzLogger{
		recorder: lb.recorder,
	}
}

func (*loggerBuilder) ParseLoggerConfig(config json.RawMessage) (audit.LoggerConfig, error) {
	return nil, nil
}

var AuthLock sync.Mutex
var maMu sync.Mutex

func (i AuthTypes) String() string {
	if i["none"] {
		return ""
	}
	b := new(bytes.Buffer)
	for key, value := range i {
		if value {
			fmt.Fprintf(b, "%s ", key)
		}
	}
	return b.String()
}

func (i AuthTypes) Any() bool {
	if i["none"] {
		return false
	}
	for _, value := range i {
		if value {
			return true
		}
	}
	return false
}

func (i AuthTypes) Enabled(mode string) bool {
	if i["none"] {
		return false
	}
	if value, exist := i[mode]; exist && value {
		return true
	}
	return false
}

func (i AuthTypes) Set(mode string) error {
	modes := strings.Split(mode, ",")
	for _, m := range modes {
		m = strings.Trim(m, " ")
		if m == "none" || m == "" {
			i["none"] = true
			return nil
		}

		if _, exist := i[m]; !exist {
			return fmt.Errorf("Expecting one or more of 'cert', 'password' or 'jwt'")
		}
		i[m] = true
	}
	return nil
}

func (i AuthTypes) Unset(mode string) error {
	modes := strings.Split(mode, ",")
	for _, m := range modes {
		m = strings.Trim(m, " ")
		if _, exist := i[m]; !exist {
			return fmt.Errorf("Expecting one or more of 'cert', 'password' or 'jwt'")
		}
		i[m] = false
	}
	return nil
}

// SrvTestConfig returns test mTLS server configuration to be used to start gNMI/gNOI server in test environment.
func SrvTestConfig(cfg *Config) ([]grpc.ServerOption, []certprovider.Provider, error) {
	certBuf, keyBuf, err := testcert.NewCert()
	if err != nil {
		log.V(0).Infof("could not generate test server credentials: %s\n", err)
		return nil, nil, fmt.Errorf("could not generate test server credentials: %s", err)
	}
	srvCert := filepath.Dir(cfg.SrvCertLnk) + "/server_test_cert.pem"
	if err = os.WriteFile(srvCert, certBuf.Bytes(), 0600); err != nil {
		log.V(0).Infof("could not write test server certificate: %s\n", err)
		return nil, nil, err
	}
	srvKey := filepath.Dir(cfg.SrvCertLnk) + "/server_test_key.pem"
	if err = os.WriteFile(srvKey, keyBuf.Bytes(), 0600); err != nil {
		log.V(0).Infof("could not write test server key: %s\n", err)
		return nil, nil, err
	}

	return SrvAdvConfig(cfg)
}

// SrvAdvConfig returns mTLS server configuration to be used to start gNMI/gNOI server with rotating certificates.
func SrvAdvConfig(cfg *Config) ([]grpc.ServerOption, []certprovider.Provider, error) {
	muPath.Lock()
	defer muPath.Unlock()

	log.V(1).Infof("Setting server credentials using: %v; %v; %v; %v; %v; %v;", cfg.CaCertLnk, cfg.CaCertFile, cfg.SrvCertLnk, cfg.SrvCertFile, cfg.SrvKeyLnk, cfg.SrvKeyFile)
	if cfg.CaCertFile != "" && !isSymlinkValid(cfg.CaCertLnk) {
		if _, err := os.Stat(cfg.CaCertFile); err != nil {
			log.V(0).Infof("could not read CA certificate: %s\n", err)
			return nil, nil, err
		}
		atomicSetCACert(cfg, cfg.CaCertFile)
	}
	if !isSymlinkValid(cfg.SrvCertLnk) || !isSymlinkValid(cfg.SrvKeyLnk) {
		if _, err := os.Stat(cfg.SrvCertFile); err != nil {
			log.V(0).Infof("could not read server certificate: %s\n", err)
			return nil, nil, err
		}
		if _, err := os.Stat(cfg.SrvKeyFile); err != nil {
			log.V(0).Infof("could not read server key: %s\n", err)
			return nil, nil, err
		}
		atomicSetSrvCertKeyPair(cfg, cfg.SrvCertFile, cfg.SrvKeyFile)
	}

	providers := []certprovider.Provider{}
	identityOptions := advancedtls.IdentityCertificateOptions{
		// Read the certificate and the key for every new connection.
		GetIdentityCertificatesForServer: func(*tls.ClientHelloInfo) ([]*tls.Certificate, error) {
			muPath.RLock()
			defer muPath.RUnlock()

			cert, err := tls.LoadX509KeyPair(cfg.SrvCertLnk, cfg.SrvKeyLnk)
			if err != nil {
				log.V(0).Infof("could not load server key pair: %s\n", err)
				return nil, fmt.Errorf("could not load server key pair: %s", err)
			}
			return []*tls.Certificate{&cert}, nil
		},
	}

	serverOption := &advancedtls.Options{
		IdentityOptions: identityOptions,
		AdditionalPeerVerification: func(params *advancedtls.HandshakeVerificationInfo) (*advancedtls.PostHandshakeVerificationResults, error) {
			return &advancedtls.PostHandshakeVerificationResults{}, nil
		},
		RequireClientCert: false,
		VerificationType:  advancedtls.SkipVerification,
	}
	if cfg.CaCertFile != "" {
		serverOption.RootOptions = advancedtls.RootCertificateOptions{
			// Read the CA certificate for every new connection.
			GetRootCertificates: func(params *advancedtls.ConnectionInfo) (*advancedtls.RootCertificates, error) {
				muPath.RLock()
				defer muPath.RUnlock()

				caCertPem, err := os.ReadFile(cfg.CaCertLnk)
				if err != nil {
					log.V(lvl.ERROR).Infof("could not read CA certificate: %s\n", err)
					return nil, fmt.Errorf("could not read CA certificate: %s", err)
				}
				certPool := x509.NewCertPool()
				if ok := certPool.AppendCertsFromPEM(caCertPem); !ok {
					log.V(lvl.ERROR).Infoln("failed to append CA certificate")
					return nil, fmt.Errorf("failed to append CA certificate")
				}
				return &advancedtls.RootCertificates{TrustCerts: certPool}, nil
			},
		}
		// If the server want the client to send certificates.
		serverOption.RequireClientCert = true
		// Doing only the certificate check.
		serverOption.VerificationType = advancedtls.CertVerification
		// CRL config.
		if cfg.CertCRLConfig != "" {
			if _, err := os.ReadDir(filepath.Join(cfg.CertCRLConfig, "crl")); err != nil {
				log.V(lvl.ERROR).Infof("CRL Config not found")
				return nil, nil, err
			}
			p, err := advancedtls.NewFileWatcherCRLProvider(advancedtls.FileWatcherOptions{
				CRLDirectory:    filepath.Join(cfg.CertCRLConfig, "crl"),
				RefreshDuration: time.Minute,
			})
			if err != nil {
				log.V(lvl.ERROR).Infof("Failed to create CRL Provider: %v", err)
				return nil, nil, err
			}
			serverOption.RevocationOptions = &advancedtls.RevocationOptions{
				DenyUndetermined: false,
				CRLProvider:      p,
			}
		}
	}

	serverCreds, err := advancedtls.NewServerCreds(serverOption)
	if err != nil {
		log.V(0).Infof("could not create server credentials: %s\n", err)
		cleanupProviders(providers)
		return nil, nil, err
	}
	return []grpc.ServerOption{grpc.Creds(serverCreds)}, providers, nil
}

// New returns an initialized Server.
func NewServer(config *Config) (*Server, error) {
	if config == nil {
		return nil, errors.New("config not provided")
	}

	var opts []grpc.ServerOption
	var providers []certprovider.Provider
	var err error

	common_utils.InitCounters()

	if config.GetOptions != nil {
		if opts, providers, err = config.GetOptions(config); err != nil {
			cleanupProviders(providers)
			return nil, err
		}
	}
	opts = append(opts,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: keepaliveMaxIdle,
			Time:              keepaliveTime,
			Timeout:           keepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinTime,
			PermitWithoutStream: true,
		}))

	PanicRecover := func(p interface{}) (err error) {
		debug.PrintStack()
		log.V(lvl.ERROR).Info(p)
		file, err := os.CreateTemp(HostVarLogPath, StackTraceFileNamePattern)
		if err != nil {
			log.V(lvl.ERROR).Info(err)
			return err
		}
		defer file.Close()
		if _, err := file.Write(debug.Stack()); err != nil {
			log.V(lvl.ERROR).Info(err)
		}
		return status.Errorf(codes.Unknown, "%v", p)
	}
	recopts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(PanicRecover),
	}

	recorder, err := metric_recorder.NewSecurityMetricRecorder("gnxi", authLogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SecurityMetricRecorder: %v", err)
	}

	// Set authorization policy.
	var authzWatcher *authz.FileWatcherInterceptor
	if config.AuthzPolicy {
		audit.RegisterLoggerBuilder(&loggerBuilder{
			recorder: recorder,
		})

		authzWatcher, err = authz.NewFileWatcher(config.AuthzPolicyFile, authzRefreshingInterval)
		if err != nil {
			return nil, err
		} else {
			opts = append(opts, grpc.ChainStreamInterceptor(
				authzWatcher.StreamInterceptor,
				grpc_recovery.StreamServerInterceptor(recopts...)))
			opts = append(opts, grpc.ChainUnaryInterceptor(
				authzWatcher.UnaryInterceptor,
				grpc_recovery.UnaryServerInterceptor(recopts...)))
		}
	} else {
		opts = append(opts, grpc.ChainStreamInterceptor(grpc_recovery.StreamServerInterceptor(recopts...)))
		opts = append(opts, grpc.ChainUnaryInterceptor(grpc_recovery.UnaryServerInterceptor(recopts...)))
	}

	s := grpc.NewServer(opts...)
	reflection.Register(s)

	srv := &Server{
		s:                 s,
		authzWatcher:      authzWatcher,
		config:            config,
		clients:           map[string]*Client{},
		certProviders:     providers,
		SaveStartupConfig: saveOnSetDisabled,
		// ReqFromMaster point to a function that is called to verify if
		// the request comes from a master controller.
		ReqFromMaster: ReqFromMasterDisabledMA,
		masterEID:     uint128{High: 0, Low: 0},
		recorder:      recorder,
	}

	srv.SsHelper, err = common_utils.NewSystemStateHelper()
	if err != nil {
		return nil, fmt.Errorf("failed to create SystemStateHelper: %v", err)
	}
	if srv.config.Port < 0 {
		srv.config.Port = 0
	}
	srv.lis, err = net.Listen("tcp", fmt.Sprintf(":%d", srv.config.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to open listener port %d: %v", srv.config.Port, err)
	}
	gnmipb.RegisterGNMIServer(srv.s, srv)

	// gNOI Server Registration
	srv.osServer = &OSServer{
		Server:          srv,
		ProcessTrfReady: srv.config.OSCfg.ProcessTrfReady,
		ProcessTrfEnd:   srv.config.OSCfg.ProcessTrfEnd,
	}
	// Register the gNOI Servers
	gnoi_os.RegisterOSServer(srv.s, srv.osServer)
	srv.fileServer = NewGNOIFileServer(srv)
	gnoi_file.RegisterFileServer(srv.s, srv.fileServer)
	gnoi_burnin_pb.RegisterBurninServer(srv.s, srv)
	gnoi_blackbox_pb.RegisterBlackBoxTestServer(srv.s, srv)
	gnoi_diag.RegisterDiagServer(srv.s, srv)
	gnoi_whitebox_pb.RegisterWhiteBoxTestServer(srv.s, srv)
	srv.debugServer = &GNOIDebugServer{Server: srv}
	gnoi_debug_pb.RegisterDebugServer(srv.s, srv.debugServer)
	srv.ldsServer = NewGNOILdsServer(srv)
	gnoi_ocs_pb.RegisterLdsServer(srv.s, srv.ldsServer)
	srv.hlthServer = NewGNOIHealthzServer(srv)
	gnoi_healthz.RegisterHealthzServer(srv.s, srv.hlthServer)
	gnoi_qual_pb.RegisterPacketLinkQualServer(srv.s, srv)
	factory_reset.RegisterFactoryResetServer(srv.s, srv)
	// Register the gNSI Servers
	srv.gnsiAuthz = NewGNSIAuthzServer(srv)
	gnsiAuthzpb.RegisterAuthzServer(srv.s, srv.gnsiAuthz)
	srv.gnsiCertz = NewGNSICertzServer(srv)
	gnsiCertzpb.RegisterCertzServer(srv.s, srv.gnsiCertz)
	srv.gnsiCredz = NewGNSICredentialzServer(srv)
	gnsiCredzpb.RegisterCredentialzServer(srv.s, srv.gnsiCredz)
	srv.gnsiPathz = NewGNSIPathzServer(srv)
	gnsiPathzpb.RegisterPathzServer(srv.s, srv.gnsiPathz)

	spb_jwt_gnoi.RegisterSonicJwtServiceServer(srv.s, srv)
	if srv.config.EnableTranslibWrite || srv.config.EnableNativeWrite {
		gnoi_system_pb.RegisterSystemServer(srv.s, srv)
	}
	if srv.config.EnableTranslibWrite {
		spb_gnoi.RegisterSonicServiceServer(srv.s, srv)
	}
	spb_gnoi.RegisterDebugServer(srv.s, srv)

	srv.debugHandler, err = srv.CreateDebugHandler()
	if err != nil {
		return nil, fmt.Errorf("failed to create DebugHandler: %v", err)
	}
	srv.getReqTimes = &reqTimes{
		mu:    sync.Mutex{},
		ringL: ring.New(10),
	}
	srv.setReqTimes = &reqTimes{
		mu:    sync.Mutex{},
		ringL: ring.New(10),
	}

	srv.configDbJournal, err = NewDbJournal("CONFIG_DB")
	if err != nil {
		return nil, fmt.Errorf("failed to create CONFIG_DB Journal: %v", err)
	}
	if srv.config.CacheResponses {
		srv.GetConfigCache = NewGetResponseCache()
	}

	srv.ConnectionManager = CreateConnectionManager(srv.config.StreamingThreshold, srv.config.UnaryThreshold)

	// Set up WarmRestartHelper for NSF.
	srv.WarmRestartHelper, err = common_utils.NewWarmRestartHelper(srv.HandleNSFStateNotifications)
	if err != nil || srv.WarmRestartHelper == nil {
		return nil, fmt.Errorf("failed to create NewWarmRestartHelper: %v", err)
	}
	// Register telemetry for NSF on startup
	if err := srv.WarmRestartHelper.Initialize("telemetry", "telemetry"); err != nil {
		return nil, fmt.Errorf("failed to initialize telemetry for NSF with WarmRestartHelper: %v", err)
	}
	if err := srv.WarmRestartHelper.RegisterWarmBootInfo(true, false, true, false); err != nil {
		return nil, fmt.Errorf("failed to register telemetry for NSF with WarmRestartHelper: %v", err)
	}
	// Handle Warmboot if coming up from NSF
	if srv.WarmRestartHelper.CheckWarmStart(true) {
		srv.WarmRestartHelper.SetWarmStartState(common_utils.INITIALIZED)
		srv.WarmRestartHelper.SetWarmStartState(common_utils.RECONCILED)
		srv.WarmRestartHelper.SetFreezeStatus(true)
		log.V(lvl.INFO).Info("Warmboot: server starting in freeze mode")
		if !srv.WarmRestartHelper.WaitForUnfreeze() {
			go srv.WarmRestartHelper.WaitForReconciliation()
		}
	}
	log.V(1).Infof("Created Server on %s, read-only: %t", srv.Address(), !srv.config.EnableTranslibWrite)
	return srv, nil
}

// listenerWrapper set AF4 TOS for gNxI connections.
type listenerWrapper struct {
	net.Listener
}

func (l *listenerWrapper) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		log.V(lvl.ERROR).Infof("listener.Accept() failed: %v", err)
		return nil, err
	}
	if c.RemoteAddr().(*net.TCPAddr).IP.To16() != nil && c.RemoteAddr().(*net.TCPAddr).IP.To4() == nil {
		log.V(lvl.DEBUG).Infof("%v is IPv6", c.RemoteAddr())
		if err := ipv6.NewConn(c).SetTrafficClass(NC1); err != nil {
			return nil, fmt.Errorf("failed to set NC1 IPv6 TC: %v", err)
		}
	} else {
		log.V(lvl.DEBUG).Infof("%v is IPv4", c.RemoteAddr())
		if err := ipv4.NewConn(c).SetTOS(NC1); err != nil {
			return nil, fmt.Errorf("failed to set NC1 IPv4 TOS: %v", err)
		}
	}
	log.V(lvl.DEBUG).Infof("ToS/TC set to NC1 on %v for %v", l.Listener.Addr(), c.RemoteAddr())
	muTOS.Lock()
	defer muTOS.Unlock()
	IPToConn[c.RemoteAddr()] = c
	return c, nil
}

// cleanupIPToConn deletes an entry in `IPToConn` that is not needed anymore once we attempted to set the ToS/TC.
func cleanupIPToConn(ctx context.Context) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		log.V(lvl.DEBUG).Infof("cleanupIPToConn:failed to get peer from ctx")
		return
	}
	if pr.Addr == net.Addr(nil) {
		log.V(lvl.DEBUG).Infof("cleanupIPToConn: failed to get peer address")
		return
	}
	muTOS.Lock()
	defer muTOS.Unlock()
	delete(IPToConn, pr.Addr)
}

func setTOSToAF4AndCleanupIPToConn(ctx context.Context) error {
	defer cleanupIPToConn(ctx)
	return setTOSToAF4(ctx)
}

func setTOSToAF4(ctx context.Context) error {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return fmt.Errorf("setTOSToAF4:failed to get peer from ctx")
	}
	if pr.Addr == net.Addr(nil) {
		return fmt.Errorf("setTOSToAF4: failed to get peer address")
	}
	log.V(lvl.DEBUG).Infof("setTOSToAF4: changing ToS/TC for conncetion from %v", pr)
	muTOS.RLock()
	defer muTOS.RUnlock()
	c, ok := IPToConn[pr.Addr]
	if !ok {
		log.V(lvl.DEBUG).Infof("setTOSToAF4: could not find a connection from %v, but that might be OK as ToS/TC has to be set only once", pr.Addr)
		return nil
	}
	if pr.Addr != c.RemoteAddr() {
		return fmt.Errorf("setTOSToAF4: peer (%v) != c.RemoteAddr (%v)", pr.Addr, c.RemoteAddr())
	}
	if pr.Addr.(*net.TCPAddr).IP.To16() != nil && pr.Addr.(*net.TCPAddr).IP.To4() == nil {
		log.V(lvl.DEBUG).Infof("setTOSToAF4: %v is IPv6", c.RemoteAddr())
		if err := ipv6.NewConn(c).SetTrafficClass(AF4); err != nil {
			return fmt.Errorf("setTOSToAF4: failed to set AF4 IPv6 TC: %v", err)
		}
	} else {
		log.V(lvl.DEBUG).Infof("setTOSToAF4: %v is IPv4", c.RemoteAddr())
		if err := ipv4.NewConn(c).SetTOS(AF4); err != nil {
			return fmt.Errorf("setTOSToAF4: failed to set AF4 IPv4 TOS: %v", err)
		}
	}
	return nil
}

// Serve will start the Server serving and block until closed.
func (srv *Server) Serve() error {
	s := srv.s
	if s == nil {
		return fmt.Errorf("Serve() failed: not initialized")
	}
	return srv.s.Serve(&listenerWrapper{srv.lis})
}

func (srv *Server) Stop() {
	if srv == nil {
		return
	}
	srv.Cleanup()
}

// Address returns the port the Server is listening to.
func (srv *Server) Address() string {
	addr := srv.lis.Addr().String()
	return strings.Replace(addr, "[::]", "localhost", 1)
}

// Port returns the port the Server is listening to.
func (srv *Server) Port() int64 {
	return srv.config.Port
}

func (srv *Server) HandleNSFStateNotifications(msg *redis.Message) {
	log.V(lvl.INFO).Infof("HandleNSFStateNotifications received message: %v %v", msg.Channel, msg.Payload)

	// Only process one request at a time
	srv.WarmRestartHelper.LockNotifHandler()
	defer srv.WarmRestartHelper.UnlockNotifHandler()

	op, _, _, err := processMsgPayload(msg.Payload)
	if err != nil {
		log.V(lvl.INFO).Infof("HandleNSFStateNotifications: Failed to process payload - %v", err)
		return
	}
	switch op {
	case common_utils.RegistrationFreezeKey:
		// Handle Freeze phase
		log.V(lvl.INFO).Infof("HandleNSFStateNotifications received freeze notification: %v", op)
		// Update warm restart performance table
		srv.WarmRestartHelper.UpdateAppWarmBootStageStart(common_utils.STAGE_FREEZE)
		// Set freeze status to true
		srv.WarmRestartHelper.SetFreezeStatus(true)
		srv.WarmRestartHelper.SetWarmStartState(common_utils.FROZEN)
		if err := srv.CloseExistingClientsOnFreeze(); err != nil {
			log.V(lvl.ERROR).Infof("HandleNSFStateNotifications: Error while enabling freeze mode: %v", err)
			srv.WarmRestartHelper.SetWarmStartState(common_utils.FAILED)
			return
		}
		srv.WarmRestartHelper.SetWarmStartState(common_utils.QUIESCENT)
	case common_utils.RegistrationUnfreezeKey:
		log.V(lvl.INFO).Infof("HandleNSFStateNotifications received unfreeze notification: %v", op)
		// Update warm restart performance table
		srv.WarmRestartHelper.UpdateAppWarmBootStageStart(common_utils.STAGE_UNFREEZE)
		srv.WarmRestartHelper.SetFreezeStatus(false)
		srv.WarmRestartHelper.SetWarmStartState(common_utils.COMPLETED)
	case common_utils.RegistrationCheckpointKey:
		// Handle Checkpointing phase
		log.V(lvl.INFO).Infof("HandleNSFStateNotifications received checkpoint notification: %v", op)
	case common_utils.RegistrationReconciliationKey:
		// Handle Reconciliation phase
		log.V(lvl.INFO).Infof("HandleNSFStateNotifications received reconiliciation notification: %v", op)
	default:
		log.V(lvl.INFO).Infof("HandleNSFStateNotifications: Invalid notification received - %v", op)
	}
}

// CloseExistingClientsOnFreeze sets the freeze status to true, closes pending gNMI RPC connections, and sets warm start state to `frozen` or `failed`
func (srv *Server) CloseExistingClientsOnFreeze() error {
	if srv.WarmRestartHelper == nil {
		return fmt.Errorf("CloseExistingClientsOnFreeze: invalid nil wrh pointer")
	}
	// Close in-flight and pending connections for ongoing Subscriptions.
	// Closing of the client queue will be triggered which will cause the client
	// context to be cancelled and exit of the receive/send goroutines for each client.
	srv.cMu.Lock()
	defer srv.cMu.Unlock()

	for _, client := range srv.clients {
		client.Close()
	}
	for _, client := range srv.clients {
		// Wait until all child go routines have exited to delete the client.
		client.w.Wait()
		log.V(lvl.INFO).Infof("NSF Freeze Mode: closed %v Subscription with %v", client.subscribe.Mode, client.String())
		delete(srv.clients, client.String())
	}

	return nil
}

func authenticate(config *Config, ctx context.Context) (context.Context, error) {
	var err error
	success := false
	rc, ctx := common_utils.GetContext(ctx)
	if !config.UserAuth.Any() {
		//No Auth enabled
		rc.Auth.AuthEnabled = false
		return ctx, nil
	}

	rc.Auth.AuthEnabled = true
	if config.UserAuth.Enabled("password") {
		ctx, err = BasicAuthenAndAuthor(ctx)
		if err == nil {
			success = true
		}
	}
	if !success && config.UserAuth.Enabled("jwt") {
		_, ctx, err = JwtAuthenAndAuthor(ctx)
		if err == nil {
			success = true
		}
	}
	if !success && config.UserAuth.Enabled("cert") {
		ctx, err = ClientCertAuthenAndAuthor(ctx, config.ConfigTableName)
		if err == nil {
			success = true
		}
	}

	//Allow for future authentication mechanisms here...

	if !success {
		return ctx, status.Error(codes.Unauthenticated, "Unauthenticated")
	}
	log.V(5).Infof("authenticate user %v, roles %v", rc.Auth.User, rc.Auth.Roles)

	return ctx, nil
}

// Subscribe implements the gNMI Subscribe RPC.
func (s *Server) Subscribe(stream gnmipb.GNMI_SubscribeServer) error {
	// Reject Subscribe while NSF Freeze is ongoing
	if s.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNMI Subscribe RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNMI Subscribe RPC disabled since NSF is ongoing!")
	}
	start := time.Now()
	ctx := stream.Context()
	ctx, err := authenticate(s.config, ctx)
	if err != nil {
		return err
	}

	defer cleanupIPToConn(ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return grpc.Errorf(codes.InvalidArgument, "failed to get peer from ctx")
		//return fmt.Errorf("failed to get peer from ctx")
	}
	if pr.Addr == net.Addr(nil) {
		return grpc.Errorf(codes.InvalidArgument, "failed to get peer address")
	}

	log.V(lvl.INFO).Infof("Entering Subscribe RPC with client: %v ", pr.Addr)
	defer func() {
		log.V(lvl.INFO).Infof("Exiting Subscribe RPC after %v with client: %v ", time.Since(start), pr.Addr)
	}()

	/* TODO: authorize the user
	msg, ok := credentials.AuthorizeUser(ctx)
	if !ok {
		log.V(lvl.INFO).Infof("denied a Set request: %v", msg)
		return nil, status.Error(codes.PermissionDenied, msg)
	}
	*/

	pathzProcessor := s.gnsiPathz.pathzProcessor
	if !s.config.PathzPolicy {
		pathzProcessor = nil
	}
	c := NewClient(pr.Addr, pathzProcessor, s.recorder, s.ConnectionManager)

	c.setLogLevel(s.config.LogLevel)
	c.setEnableTranslation(s.config.EnableTranslation)

	s.cMu.Lock()
	if oc, ok := s.clients[c.String()]; ok {
		log.V(2).Infof("Delete duplicate client %s", oc)
		oc.Close()
		delete(s.clients, c.String())
	}
	s.clients[c.String()] = c
	s.cMu.Unlock()

	err = c.Run(stream)
	s.cMu.Lock()
	delete(s.clients, c.String())
	s.cMu.Unlock()

	log.Flush()
	return err
}

// checkEncodingAndModel checks whether encoding and models are supported by the server. Return error if anything is unsupported.
func (s *Server) checkEncodingAndModel(encoding gnmipb.Encoding, models []*gnmipb.ModelData) error {
	hasSupportedEncoding := false
	for _, supportedEncoding := range supportedEncodings {
		if encoding == supportedEncoding {
			hasSupportedEncoding = true
			break
		}
	}
	if !hasSupportedEncoding {
		return fmt.Errorf("unsupported encoding: %s", gnmipb.Encoding_name[int32(encoding)])
	}

	return nil
}

func ParseOrigin(paths []*gnmipb.Path) (string, error) {
	origin := ""
	if len(paths) == 0 {
		return origin, nil
	}
	for i, path := range paths {
		if i == 0 {
			origin = path.Origin
		} else {
			if origin != path.Origin {
				return "", status.Error(codes.Unimplemented, "Origin conflict in path")
			}
		}
	}
	return origin, nil
}

func IsNativeOrigin(origin string) bool {
	return origin == "sonic-db"
}

// Get implements the Get RPC in gNMI spec.
func (s *Server) Get(ctx context.Context, req *gnmipb.GetRequest) (*gnmipb.GetResponse, error) {
	// Reject Get while NSF Freeze is ongoing
	if s.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNMI Get RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNMI Get RPC disabled since NSF is ongoing!")
	}
	start := time.Now()
	clientStr := "unknown"
	pr, ok := peer.FromContext(ctx)
	if ok {
		clientStr = pr.Addr.String()
	}
	defer func() { log.V(lvl.INFO).Infof("Get() finished in %v for client=%v", time.Since(start), clientStr) }()

	if !hasMasterEID(req.GetExtension()) {
		log.V(lvl.DEBUG).Info("GET from an observer")
		if err := setTOSToAF4AndCleanupIPToConn(ctx); err != nil {
			log.V(lvl.ERROR).Info(err)
			return nil, err
		}
	} else {
		log.V(lvl.DEBUG).Info("GET from a controller")
	}

	common_utils.IncCounter(common_utils.GNMI_GET)
	ctx, err := authenticate(s.config, ctx)
	if err != nil {
		common_utils.IncCounter(common_utils.GNMI_GET_FAIL)
		return nil, err
	}

	// gNMI path based authorization
	if s.config.PathzPolicy && len(req.GetPath()) != 0 {
		newPaths := []*gnmipb.Path{}
		user, err := getUsername(ctx)
		if err != nil {
			log.V(lvl.WARNING).Infof("GetRequest User not found: %s", err.Error())
			return nil, err
		}
		for _, path := range req.GetPath() {
			// Only process the authorized paths in the request.
			r, err := s.gnsiPathz.pathzProcessor.AuthorizeWithPrefix(user, req.GetPrefix(), path, gnsiPathzpb.Mode_MODE_READ)
			if err != nil || r.Action != gnsiPathzpb.Action_ACTION_PERMIT {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: false,
					Rpc:       "get",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), path),
				})
				continue
			}
			s.recorder.Record(metric_recorder.PathzRecord{
				Permitted: true,
				Rpc:       "get",
				Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), path),
			})
			newPaths = append(newPaths, path)
		}
		if len(newPaths) == 0 {
			return nil, status.Error(codes.PermissionDenied, fmt.Sprintf("Read Rejected by pathz policy for user: %v", user))
		}
		req.Path = newPaths
	}

	connectionKey, valid := s.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer s.ConnectionManager.Remove(connectionKey) // remove key from connection list

	if s.getReqTimes != nil {
		defer s.getReqTimes.recordReqTime(pr, start)
	}

	// Verify the Get RPC's type and save it in our context
	if _, ok := gnmipb.GetRequest_DataType_name[int32(req.GetType())]; !ok {
		common_utils.IncCounter(common_utils.GNMI_GET_FAIL)
		return nil, status.Errorf(codes.Unimplemented, "Unable to retrieve request type: %v", req.GetType())
	}
	common_utils.SetReqParam(ctx, common_utils.KeyGetDataType, req.GetType())

	if err = s.checkEncodingAndModel(req.GetEncoding(), req.GetUseModels()); err != nil {
		common_utils.IncCounter(common_utils.GNMI_GET_FAIL)
		return nil, status.Error(codes.Unimplemented, err.Error())
	}

	isGetConfig := false
	if s.config.CacheResponses && IsGetConfigRequest(req) {
		isGetConfig = true
		if resp := s.GetConfigCache.GetResponse(); resp != nil {
			return resp, nil
		}
	}

	// Tries to translate the request if needed.
	var translCtx gnmipt.TranslatorCtx = nil
	if s.config.EnableTranslation == true {
		translCtx = gnmipt.TranslGetRequest(req)
	}

	target := ""
	origin := ""
	prefix := req.GetPrefix()
	if prefix != nil {
		target = prefix.GetTarget()
		origin = prefix.Origin
	}

	paths := req.GetPath()
	extensions := req.GetExtension()
	encoding := req.GetEncoding()
	log.V(lvl.DEBUG).Infof("GetRequest paths: %v", paths)

	var dc sdc.Client

	if target == "OTHERS" {
		dc, err = sdc.NewNonDbClient(paths, prefix)
	} else if _, ok, _, _ := sdc.IsTargetDb(target); ok {
		dc, err = sdc.NewDbClient(paths, prefix)
	} else {
		if origin == "" {
			origin, err = ParseOrigin(paths)
			if err != nil {
				return nil, err
			}
		}
		if check := IsNativeOrigin(origin); check {
			dc, err = sdc.NewMixedDbClient(paths, prefix, origin, encoding, s.config.ZmqPort)
		} else {
			dc, err = sdc.NewTranslClient(prefix, paths, ctx, encoding, extensions)
		}
	}

	if err != nil {
		common_utils.IncCounter(common_utils.GNMI_GET_FAIL)
		return nil, status.Error(codes.NotFound, err.Error())
	}
	defer dc.Close()
	notifications := make([]*gnmipb.Notification, len(paths))
	spbValues, err := dc.Get(nil)
	if err != nil {
		common_utils.IncCounter(common_utils.GNMI_GET_FAIL)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	switch encoding {
	case gnmipb.Encoding_JSON:
		fallthrough
	case gnmipb.Encoding_JSON_IETF:
		for index, spbValue := range spbValues {
			update := &gnmipb.Update{
				Path: spbValue.GetPath(),
				Val:  spbValue.GetVal(),
			}

			notifications[index] = &gnmipb.Notification{
				Timestamp: spbValue.GetTimestamp(),
				Prefix:    prefix,
				Update:    []*gnmipb.Update{update},
			}
		}
	case gnmipb.Encoding_PROTO:
		for index, spbValue := range spbValues {
			notifications[index] = spbValue.Notification
		}
	default:
		// Never come here since the call to checkEncodingAndModel validates
		// the request is either PROTO or JSON encoded.
	}

	resp := &gnmipb.GetResponse{Notification: notifications}

	// Tries to translate the response if needed.
	if s.config.EnableTranslation == true {
		gnmipt.TranslGetResponse(resp, translCtx)
	}

	if s.config.CacheResponses && isGetConfig {
		s.GetConfigCache.Cache(resp)
	}

	return resp, nil
}

// saveOnSetEnabled saves configuration to a file
func SaveOnSetEnabled() error {
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		log.V(0).Infof("Saving startup config failed to create dbus client: %v", err)
		return err
	}
	if err := sc.ConfigSave("/etc/sonic/config_db.json"); err != nil {
		log.V(0).Infof("Saving startup config failed: %v", err)
		return err
	} else {
		log.V(1).Infof("Success! Startup config has been saved!")
	}
	return nil
}

// SaveOnSetDisabeld does nothing.
func saveOnSetDisabled() error { return nil }

func hasMasterEID(ext []*gnmi_extpb.Extension) bool {
	// It can be one of many extensions, so iterate through them to find it.
	for _, e := range ext {
		if ma := e.GetMasterArbitration(); ma != nil {
			return true
		}
	}
	return false
}

func (s *Server) Set(ctx context.Context, req *gnmipb.SetRequest) (*gnmipb.SetResponse, error) {
	// Reject Set while NSF Freeze is ongoing
	if s.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNMI Set RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNMI Set RPC disabled since NSF is ongoing!")
	}
	start := time.Now()
	clientStr := "unknown"
	pr, ok := peer.FromContext(ctx)
	if ok {
		clientStr = pr.Addr.String()
	}
	log.V(lvl.INFO).Infof("Set() starting for client=%v", clientStr)
	defer func() { log.V(lvl.INFO).Infof("Set() finished in %v for client=%v", time.Since(start), clientStr) }()

	if !hasMasterEID(req.GetExtension()) {
		log.V(lvl.DEBUG).Info("SET from an observer")
		if err := setTOSToAF4AndCleanupIPToConn(ctx); err != nil {
			log.V(lvl.ERROR).Info(err)
			return nil, err
		}
	} else {
		log.V(lvl.DEBUG).Info("SET from a controller")
	}

	e := s.ReqFromMaster(req, &s.masterEID)
	if e != nil {
		return nil, e
	}

	common_utils.IncCounter(common_utils.GNMI_SET)
	if s.config.EnableTranslibWrite == false && s.config.EnableNativeWrite == false {
		common_utils.IncCounter(common_utils.GNMI_SET_FAIL)
		return nil, grpc.Errorf(codes.Unimplemented, "GNMI is in read-only mode")
	}
	ctx, err := authenticate(s.config, ctx)
	if err != nil {
		common_utils.IncCounter(common_utils.GNMI_SET_FAIL)
		return nil, err
	}

	// gNMI path based authorization
	if s.config.PathzPolicy {
		user, err := getUsername(ctx)
		if err != nil {
			log.V(lvl.WARNING).Infof("SetRequest User not found: %s", err.Error())
			return nil, err
		}
		permitted := true
		for _, path := range req.GetDelete() {
			r, err := s.gnsiPathz.pathzProcessor.AuthorizeWithPrefix(user, req.GetPrefix(), path, gnsiPathzpb.Mode_MODE_WRITE)
			if err != nil || r.Action != gnsiPathzpb.Action_ACTION_PERMIT {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: false,
					Rpc:       "set",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), path),
				})
				permitted = false
			} else {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: true,
					Rpc:       "set",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), path),
				})
			}
		}
		for _, update := range req.GetReplace() {
			r, err := s.gnsiPathz.pathzProcessor.AuthorizeWithPrefix(user, req.GetPrefix(), update.GetPath(), gnsiPathzpb.Mode_MODE_WRITE)
			if err != nil || r.Action != gnsiPathzpb.Action_ACTION_PERMIT {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: false,
					Rpc:       "set",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), update.GetPath()),
				})
				permitted = false
			} else {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: true,
					Rpc:       "set",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), update.GetPath()),
				})
			}
		}
		for _, update := range req.GetUpdate() {
			r, err := s.gnsiPathz.pathzProcessor.AuthorizeWithPrefix(user, req.GetPrefix(), update.GetPath(), gnsiPathzpb.Mode_MODE_WRITE)
			if err != nil || r.Action != gnsiPathzpb.Action_ACTION_PERMIT {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: false,
					Rpc:       "set",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), update.GetPath()),
				})
				permitted = false
			} else {
				s.recorder.Record(metric_recorder.PathzRecord{
					Permitted: true,
					Rpc:       "set",
					Path:      pathz_authorizer.PrintPathWithPrefix(req.GetPrefix(), update.GetPath()),
				})
			}
		}
		if !permitted {
			return nil, status.Error(codes.PermissionDenied, fmt.Sprintf("Write Rejected by pathz policy for user: %v", user))
		}
	}

	if s.SsHelper.IsSystemCritical() {
		log.V(lvl.ERROR).Info("Write to DB disabled since system is in Critical State!")
		return nil, status.Errorf(codes.Internal, "Write to DB disabled since system is in Critical State: %s", s.SsHelper.GetSystemCriticalReason())
	}

	connectionKey, valid := s.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer s.ConnectionManager.Remove(connectionKey)

	if s.setReqTimes != nil {
		defer s.setReqTimes.recordReqTime(pr, start)
	}

	// Prior to making any changes, signal that port cycling should cease so that
	// it does not overwrite any of our changes.
	rClient := db.RedisClient(db.ConfigDB)
	defer db.CloseRedisClient(rClient)

	if disablePortCyclingErr := disablePortCycling(rClient); disablePortCyclingErr != nil {
		recordFailedSet(rClient)
		return nil, status.Errorf(codes.Aborted, "Failed to disable Port-Cycling with error %w.", disablePortCyclingErr)
	}

	var results []*gnmipb.UpdateResult

	// Tries to translate the request if needed.
	var translCtx gnmipt.TranslatorCtx = nil
	if s.config.EnableTranslation == true {
		translCtx = gnmipt.TranslSetRequest(req)
	}

	/* Fetch the prefix. */
	prefix := req.GetPrefix()
	origin := ""
	if prefix != nil {
		origin = prefix.Origin
	}
	extensions := req.GetExtension()
	encoding := gnmipb.Encoding_JSON_IETF

	var dc sdc.Client
	paths := req.GetDelete()
	for _, path := range req.GetReplace() {
		paths = append(paths, path.GetPath())
	}
	for _, path := range req.GetUpdate() {
		paths = append(paths, path.GetPath())
	}
	if origin == "" {
		origin, err = ParseOrigin(paths)
		if err != nil {
			return nil, err
		}
	}
	if check := IsNativeOrigin(origin); check {
		if s.config.EnableNativeWrite == false {
			common_utils.IncCounter(common_utils.GNMI_SET_FAIL)
			return nil, grpc.Errorf(codes.Unimplemented, "GNMI native write is disabled")
		}
		dc, err = sdc.NewMixedDbClient(paths, prefix, origin, encoding, s.config.ZmqPort)
	} else {
		if s.config.EnableTranslibWrite == false {
			common_utils.IncCounter(common_utils.GNMI_SET_FAIL)
			return nil, grpc.Errorf(codes.Unimplemented, "Translib write is disabled")
		}
		/* Create Transl client. */
		dc, err = sdc.NewTranslClient(prefix, nil, ctx, encoding, extensions)
	}

	if err != nil {
		common_utils.IncCounter(common_utils.GNMI_SET_FAIL)
		return nil, status.Error(codes.NotFound, err.Error())
	}
	defer dc.Close()

	/* DELETE */
	for _, path := range req.GetDelete() {
		log.V(lvl.DEBUG).Infof("Delete path: %v", path)

		res := gnmipb.UpdateResult{
			Path: path,
			Op:   gnmipb.UpdateResult_DELETE,
		}

		/* Add to Set response results. */
		results = append(results, &res)
	}

	/* REPLACE */
	for _, path := range req.GetReplace() {
		log.V(lvl.DEBUG).Infof("Replace path: %v ", path)

		res := gnmipb.UpdateResult{
			Path: path.GetPath(),
			Op:   gnmipb.UpdateResult_REPLACE,
		}
		/* Add to Set response results. */
		results = append(results, &res)
	}

	/* UPDATE */
	for _, path := range req.GetUpdate() {
		log.V(lvl.DEBUG).Infof("Update path: %v ", path)

		res := gnmipb.UpdateResult{
			Path: path.GetPath(),
			Op:   gnmipb.UpdateResult_UPDATE,
		}
		/* Add to Set response results. */
		results = append(results, &res)
	}
	err = dc.Set(req.GetDelete(), req.GetReplace(), req.GetUpdate())

	// For either Set RPC failure or success, remove port unlock required signal if present.
	if unlockKeys, keysErr := rClient.Keys(context.Background(), "PORT_UNLOCK|*").Result(); keysErr == nil && len(unlockKeys) > 0 {
		if delErr := rClient.Del(context.Background(), "PORT_UNLOCK|*").Err(); delErr != nil {
			log.V(lvl.ERROR).Infof("Error in removing the port unlock required signal after Set RPC returned: %v", delErr.Error())
		} else {
			log.V(lvl.DEBUG).Infof("Successfully removed the port unlock required signal after Set RPC returned.")
		}
	}

	if err != nil {
		common_utils.IncCounter(common_utils.GNMI_SET_FAIL)
		recordFailedSet(rClient)
		// Unlock any DPB ports that may have been locked.
		if rClient != nil {
			if keys, err1 := rClient.Keys(context.Background(), "PORT_STATE|*").Result(); err1 == nil && len(keys) > 0 {
				if err1 := rClient.Del(context.Background(), "PORT_STATE|*").Err(); err1 != nil {
					log.V(lvl.ERROR).Infof("Error in unlocking ports during after Set RPC returned: %v", err1.Error())
				} else {
					log.V(lvl.DEBUG).Infof("Successfully unlocked ports  after Set RPC returned.")
				}
			}
		}
		// Ref Count check failure.
		if custom_validation.FetchRefCountCheckStatus() {
			custom_validation.SetRefCountCheckStatus(false)
			return nil, status.Errorf(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Aborted, err.Error())
	}

	s.SaveStartupConfig()
	resp := &gnmipb.SetResponse{
		Prefix:   req.GetPrefix(),
		Response: results,
	}

	// Tries to translate the response if needed.
	if s.config.EnableTranslation == true {
		gnmipt.TranslSetResponse(resp, translCtx)
	}

	recordSuccessfulSet(rClient)
	return resp, nil
}

func disablePortCycling(rc *redis.Client) error {
	if rc == nil {
		return errors.New("Redis Client is unexpectedly nil.")
	}
	return rc.HSet(context.Background(), "DYNAMIC_BOOTSTRAP_CYCLE_PORTS|local", "enable", "false").Err()
}

func recordSuccessfulSet(rc *redis.Client) {
	if rc == nil {
		return
	}
	successfulSets += 1
	rc.HSet(context.Background(), "UMF_STATS|local", "successful-sets", strconv.Itoa(successfulSets))
}
func recordFailedSet(rc *redis.Client) {
	if rc == nil {
		return
	}
	failedSets += 1
	rc.HSet(context.Background(), "UMF_STATS|local", "failed-sets", strconv.Itoa(failedSets))
}

func (s *Server) Capabilities(ctx context.Context, req *gnmipb.CapabilityRequest) (*gnmipb.CapabilityResponse, error) {
	// Reject while NSF Freeze is ongoing
	if s.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNMI Capabilities RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNMI Capabilities RPC disabled since NSF is ongoing!")
	}
	if !hasMasterEID(req.GetExtension()) {
		log.V(lvl.DEBUG).Info("CAPABILITIES from an observer")
		if err := setTOSToAF4AndCleanupIPToConn(ctx); err != nil {
			log.V(lvl.ERROR).Info(err)
			return nil, err
		}
	} else {
		log.V(lvl.DEBUG).Info("CAPABILITIES from a controller")
	}
	ctx, err := authenticate(s.config, ctx)
	if err != nil {
		return nil, err
	}
	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := s.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer s.ConnectionManager.Remove(connectionKey)

	extensions := req.GetExtension()

	/* Fetch the client capabitlities. */
	var supportedModels []gnmipb.ModelData
	dc, _ := sdc.NewTranslClient(nil, nil, ctx, gnmipb.Encoding_JSON_IETF, extensions)
	supportedModels = append(supportedModels, dc.Capabilities()...)
	dc, _ = sdc.NewMixedDbClient(nil, nil, "", gnmipb.Encoding_JSON_IETF, s.config.ZmqPort)
	supportedModels = append(supportedModels, dc.Capabilities()...)

	suppModels := make([]*gnmipb.ModelData, len(supportedModels))

	for index, model := range supportedModels {
		suppModels[index] = &gnmipb.ModelData{
			Name:         model.Name,
			Organization: model.Organization,
			Version:      model.Version,
		}
	}

	sup_bver := spb.SupportedBundleVersions{
		BundleVersion: translib.GetYangBundleVersion().String(),
		BaseVersion:   translib.GetYangBaseVersion().String(),
	}
	sup_msg, _ := proto.Marshal(&sup_bver)
	ext := gnmi_extpb.Extension{}
	ext.Ext = &gnmi_extpb.Extension_RegisteredExt{
		RegisteredExt: &gnmi_extpb.RegisteredExtension{
			Id:  spb.SUPPORTED_VERSIONS_EXT,
			Msg: sup_msg}}
	exts := []*gnmi_extpb.Extension{&ext}

	return &gnmipb.CapabilityResponse{SupportedModels: suppModels,
		SupportedEncodings: supportedEncodings,
		GNMIVersion:        "0.7.0",
		Extension:          exts}, nil
}

func (s *Server) ChangeLogLevel(stopRequest, stoppedResponse chan bool) {
	//The WaitGroup is used to ensure that the goroutine is running before leaving this function.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// For unit test, when goroutine finished,
		// send notification to unit test through done channel.
		if stoppedResponse != nil {
			defer func() {
				stoppedResponse <- true
			}()
		}

		ns, _ := sdcfg.GetDbDefaultNamespace()
		addr, _ := sdcfg.GetDbTcpAddr("CONFIG_DB", ns)
		dbId, _ := sdcfg.GetDbId("CONFIG_DB", ns)
		redisDB := db.TransactionalRedisClientWithOpts(&redis.Options{
			Network:     "tcp",
			Addr:        addr,
			Password:    "",
			DB:          dbId,
			DialTimeout: 0,
		})
		defer db.CloseRedisClient(redisDB)

		// 4 is the CONFIG_DB database index
		pubsub := redisDB.PSubscribe(context.Background(), "__keyspace@4__:TELEMETRY|gnmi")
		defer pubsub.Close()
		if _, err := pubsub.Receive(context.Background()); err != nil {
			log.V(lvl.DEBUG).Infof("Could not receive channel for vmodule: %s\n", err)
		}

		channel := pubsub.Channel()

		wg.Done()
		for {
			select {
			case msg := <-channel:
				if !strings.Contains(msg.Payload, "hset") {
					continue
				}

				if logLevelPerFile, err := redisDB.HGet(context.Background(), "TELEMETRY|gnmi", "vmodule").Result(); err == nil {
					if vm := flag.Lookup("vmodule"); vm != nil {
						s.cMu.Lock()
						if err := vm.Value.Set(logLevelPerFile); err != nil {
							log.V(lvl.ERROR).Infof("vmodule flag update failed! Error: %v", err)
						}
						s.cMu.Unlock()
					}
				}
			case <-stopRequest: // Wait for request to terminate the infinite loop.
				return
			}
		}
	}()

	wg.Wait()
}

// Obtain the user name as the last element of the SPIFFE ID.
func getUsername(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", grpc.Errorf(codes.Unauthenticated, "failed to get peer from ctx")
	}
	tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", grpc.Errorf(codes.Unauthenticated, "no tls info was found")
	}
	spiffe := tlsInfo.SPIFFEID
	if spiffe == nil {
		return "", grpc.Errorf(codes.Unauthenticated, "failed to get SPIFFE ID")
	}
	path := spiffe.Path
	usernamePos := strings.LastIndex(path, "/")
	if usernamePos == -1 {
		return "", status.Errorf(codes.Unauthenticated, "failed to get username from SPIFFE ID: %s", spiffe)
	}
	return path[usernamePos+1:], nil
}

type uint128 struct {
	High uint64
	Low  uint64
}

func (lh *uint128) Compare(rh *uint128) int {
	if rh == nil {
		// For MA disabled case, EID supposed to be 0.
		rh = &uint128{High: 0, Low: 0}
	}
	if lh.High > rh.High {
		return 1
	}
	if lh.High < rh.High {
		return -1
	}
	if lh.Low > rh.Low {
		return 1
	}
	if lh.Low < rh.Low {
		return -1
	}
	return 0
}

// ReqFromMasterEnabledMA returns true if the request is sent by the master
// controller.
func ReqFromMasterEnabledMA(req *gnmipb.SetRequest, masterEID *uint128) error {
	// Read the election_id.
	reqEID := uint128{High: 0, Low: 0}
	hasMaExt := false
	// It can be one of many extensions, so iterate through them to find it.
	for _, e := range req.GetExtension() {
		ma := e.GetMasterArbitration()
		if ma == nil {
			continue
		}

		hasMaExt = true
		// The Master Arbitration descriptor has been found.
		if ma.ElectionId == nil {
			return status.Errorf(codes.InvalidArgument, "MA: ElectionId missing")
		}

		if ma.Role != nil {
			// Role will be implemented later.
			return status.Errorf(codes.Unimplemented, "MA: Role is not implemented")
		}

		reqEID = uint128{High: ma.ElectionId.High, Low: ma.ElectionId.Low}
		// Use the election ID that is in the last extension, so, no 'break' here.
	}

	if !hasMaExt {
		log.V(0).Infof("MA: No Master Arbitration in setRequest extension, masterEID %v is not updated", masterEID)
		return nil
	}

	maMu.Lock()
	defer maMu.Unlock()
	switch masterEID.Compare(&reqEID) {
	case 1: // This Election ID is smaller than the known Master Election ID.
		return status.Errorf(codes.PermissionDenied, "Election ID is smaller than the current master. Rejected. Master EID: %v. Current EID: %v.", masterEID, reqEID)
	case -1: // New Master Election ID received!
		log.V(0).Infof("New master has been elected with %v\n", reqEID)
		*masterEID = reqEID
	}
	return nil
}

// ReqFromMasterDisabledMA always returns true. It is used when Master Arbitration
// is disabled.
func ReqFromMasterDisabledMA(req *gnmipb.SetRequest, masterEID *uint128) error {
	return nil
}

func cleanupProviders(ps []certprovider.Provider) {
	for _, p := range ps {
		p.Close()
	}
}

// Cleanup stops the gNMI/gNOI server and does required cleanup.
func (srv *Server) Cleanup() {
	if srv.s != nil {
		srv.s.Stop()
	}
	cleanupProviders(srv.certProviders)
	if srv.recorder != nil {
		srv.recorder.Close()
	}
	if srv.authzWatcher != nil {
		srv.authzWatcher.Close()
	}
	if srv.WarmRestartHelper != nil {
		srv.WarmRestartHelper.Close()
	}
	if srv.debugHandler != nil {
		srv.debugHandler.Close()
	}
	if srv.configDbJournal != nil {
		srv.configDbJournal.Close()
	}
	if srv.GetConfigCache != nil {
		srv.GetConfigCache.Close()
	}
	if srv.ConnectionManager != nil {
		srv.ConnectionManager.Close()
	}
}
