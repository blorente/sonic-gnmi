package main

import (
	"flag"
	"fmt"

	/* The following imports are removed since we commented out the startGNMIServer
	 * and iNotifyCertMonitoring functions which came from the community.
	"io"
	"io/ioutil"
	"sync/atomic"
	*/
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	gnmi "github.com/sonic-net/sonic-gnmi/gnmi_server"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	/* The following imports are removed since we commented out the startGNMIServer
	 * and iNotifyCertMonitoring functions which came from the community.
	testcert "github.com/sonic-net/sonic-gnmi/testdata/tls"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	*/)

type ServerControlValue int

const (
	ServerStop    ServerControlValue = iota // 0
	ServerStart   ServerControlValue = iota // 1
	ServerRestart ServerControlValue = iota // 2
)

type TelemetryConfig struct {
	UserAuth              *gnmi.AuthTypes
	Port                  *int
	LogLevel              *int
	CaCert                *string
	ServerCert            *string
	ServerKey             *string
	ConfigTableName       *string
	ZmqAddress            *string
	ZmqPort               *string
	Insecure              *bool
	NoTLS                 *bool
	AllowNoClientCert     *bool
	JwtRefInt             *uint64
	JwtValInt             *uint64
	GnmiTranslibWrite     *bool
	GnmiNativeWrite       *bool
	StreamingThreshold    *int
	UnaryThreshold        *int
	WithMasterArbitration *bool
	WithSaveOnSet         *bool
	IdleConnDuration      *int

	// Not in upstream:
	CacheResponses      *bool
	CertCRLConfig       *string
	IntManFile          *string
	EnableSRXfmr        *bool
	AuthzMetaFile       *string
	AuthPolicyEnabled   *bool
	AuthzPolicyFile     *string
	PathzEnabled        *bool
	PathzMetaFile       *string
	PathzPolicyFile     *string
	ConsoleCredMetaFile *string
	CertzMetaFile       *string
	SshCredMetaFile     *string
	ImgDirPath          *string
}

const (
	portTbl     = "PORT_TABLE"
	portInitKey = "PortInitDone"
	pollIntv    = 3 * time.Second
	pollTimeout = 30 * time.Second
)

func main() {
	err := runTelemetry(os.Args)
	if err != nil {
		log.Errorf("Unable to setup telemetry config due to err: %v", err)
	}
}

func runTelemetry(args []string) error {
	/* Glog flags like -logtostderr have to be part of the global flagset.
	   Because we use a custom flagset to avoid the use of global var and improve
	   testability, we have to parse cmd line args two different times such that
	   in the first parse, cmd line args will contain global flags and flag.Parse() will be called.
	   The second parse, cmd line args will contain flags only relevant to telemetry, and our custom flagset will
	   call Parse().
	*/
	glogFlags, telemetryFlags := parseOSArgs()
	os.Args = glogFlags
	flag.Parse() // glog flags will be populated after global flag parse

	os.Args = telemetryFlags
	fs := flag.NewFlagSet(args[0], flag.ExitOnError)
	telemetryCfg, cfg, err := setupFlags(fs) // telemetry flags will be populated after second parse
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	// serverControlSignal channel is a channel that will be used to notify gnmi server to start, stop, restart, depending of syscall or cert updates
	var serverControlSignal = make(chan ServerControlValue, 1)
	var stopSignalHandler = make(chan bool, 1)
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGTERM, syscall.SIGQUIT)

	wg.Add(1)

	go signalHandler(serverControlSignal, sigchannel, stopSignalHandler, &wg)

	wg.Add(1)
	go startGNMIServerGoog(telemetryCfg, cfg, serverControlSignal, stopSignalHandler, &wg)

	wg.Wait()
	return nil
}

func getGlogFlagsMap() map[string]bool {
	// glog flags: https://pkg.go.dev/github.com/golang/glog
	return map[string]bool{
		"-log_dir":          true,
		"-log_link":         true,
		"-logbuflevel":      true,
		"-logtostderr":      true,
		"-alsologtostderr":  true,
		"-v":                true,
		"-stderrthreshold":  true,
		"-vmodule":          true,
		"-log_backtrace_at": true,
		"-logtostdout":      true,
		"-alsologtosyslog":  true,
		"-syslogthreshold":  true,
		"-logfirstn":        true,
		"-logresettime":     true,
		"-logmapsize":       true,
	}
}

func parseOSArgs() ([]string, []string) {
	glogFlags := []string{os.Args[0]}
	telemetryFlags := []string{os.Args[0]}
	glogFlagsMap := getGlogFlagsMap()

	/* Command line options in Go can be specified in a few different ways which
	   we should account for when manually parsing them.  There is the command
	   line option with no arguments (./telemetry --insecure) and the case with
	   an argument (./telemetry --port=12345).  The argument may be provided
	   in two different ways:
	     ./telemetry --port 12345
	     ./telemetry --port=12345
	   Whitespace is not accepted when using the option=value format.
	   Note that telemetry does not support positional arguments today, so
	   supporting arguments without a corresponding commandline option is not
	   supported.
	*/
	standAloneVal := ""
	for i := len(os.Args) - 1; i > 0; i-- {
		if !strings.HasPrefix(os.Args[i], "-") {
			// Since we do not start with a '-' treat this token as a value for
			// the preceding command.
			standAloneVal = os.Args[i]
			continue
		}

		option, optionVal, _ := strings.Cut(os.Args[i], "=")
		optionWithVal := option
		if optionVal != "" || standAloneVal != "" {
			// Since positional arguments are not supported at most one of
			// optionVal or standAloneVal will be a non-empty string
			optionWithVal = option + "=" + optionVal + standAloneVal
		}
		if option == "-v" {
			glogFlags = append(glogFlags, optionWithVal)
			telemetryFlags = append(telemetryFlags, optionWithVal)
		} else if glogFlagsMap[option] {
			glogFlags = append(glogFlags, optionWithVal)
		} else {
			telemetryFlags = append(telemetryFlags, optionWithVal)
		}
		standAloneVal = ""
	}
	return glogFlags, telemetryFlags
}

func setupFlags(fs *flag.FlagSet) (*TelemetryConfig, *gnmi.Config, error) {
	telemetryCfg := &TelemetryConfig{
		UserAuth:              &gnmi.AuthTypes{"password": false, "cert": false, "jwt": false},
		Port:                  fs.Int("port", -1, "port to listen on"),
		LogLevel:              fs.Int("v", 2, "log level of process"),
		CaCert:                fs.String("ca_crt", "", "CA certificate for client certificate validation. Optional."),
		ServerCert:            fs.String("server_crt", "", "TLS server certificate"),
		ServerKey:             fs.String("server_key", "", "TLS server private key"),
		ConfigTableName:       fs.String("config_table_name", "", "Config table name"),
		ZmqAddress:            fs.String("zmq_address", "", "Orchagent ZMQ address, deprecated, please use zmq_port."),
		ZmqPort:               fs.String("zmq_port", "", "Orchagent ZMQ port, when not set or empty string telemetry server will switch to Redis based communication channel."),
		Insecure:              fs.Bool("insecure", false, "Skip providing TLS cert and key, for testing only!"),
		NoTLS:                 fs.Bool("noTLS", false, "disable TLS, for testing only!"),
		AllowNoClientCert:     fs.Bool("allow_no_client_auth", false, "When set, telemetry server will request but not require a client certificate."),
		JwtRefInt:             fs.Uint64("jwt_refresh_int", 900, "Seconds before JWT expiry the token can be refreshed."),
		JwtValInt:             fs.Uint64("jwt_valid_int", 3600, "Seconds that JWT token is valid for."),
		GnmiTranslibWrite:     fs.Bool("gnmi_translib_write", gnmi.ENABLE_TRANSLIB_WRITE, "Enable gNMI translib write for management framework"),
		GnmiNativeWrite:       fs.Bool("gnmi_native_write", gnmi.ENABLE_NATIVE_WRITE, "Enable gNMI native write"),
		StreamingThreshold:    fs.Int("streaming-threshold", 12, "max number of streaming client connections"),
		UnaryThreshold:        fs.Int("unary-threshold", 100, "max number of unary client connections"),
		WithMasterArbitration: fs.Bool("with-master-arbitration", false, "Enables master arbitration policy."),
		WithSaveOnSet:         fs.Bool("with-save-on-set", false, "Enables save-on-set."),
		IdleConnDuration:      fs.Int("idle_conn_duration", 5, "Seconds before server closes idle connections"),
		ImgDirPath:            fs.String("img_dir", "/tmp/host_tmp", "Directory path where image will be trancontrollerrred."),
		CacheResponses:        fs.Bool("cache_responses", true, "Cache gNMI responses when possible"),
		CertCRLConfig:         fs.String("cert_crl_dir", "", "CRL directory. Disable if empty."),
		IntManFile:            fs.String("integrity_manifest_file", "", "Full path name of integrity manifest file."),
		EnableSRXfmr:          fs.Bool("with-repl-xfmr", false, "Enable gNMI SetReplace Transformer"),
		AuthzMetaFile:         fs.String("authz_meta", "/keys/authz-version.json", "authz policy metadata JSON file"),
		AuthPolicyEnabled:     fs.Bool("authz_policy_enabled", false, "Enable authz policy. Require insecure flag to be false."),
		AuthzPolicyFile:       fs.String("authorization_policy_file", "/keys/authorization_policy.json", "Full path name of the JSON authorization policy file."),
		PathzEnabled:          fs.Bool("gnmi_pathz_enabled", false, "Enable gNMI path based authorization. Require insecure flag to be false."),
		PathzMetaFile:         fs.String("pathz_meta", "/keys/pathz-version.json", "pathz policy metadata JSON file"),
		PathzPolicyFile:       fs.String("gnmi_pathz_file", "/keys/pathz_policy.pb.txt", "Full path name of the gNMI authorization policy file."),
		ConsoleCredMetaFile:   fs.String("console_meta", "/keys/console-version.json", "console credentials metadata JSON file"),
		CertzMetaFile:         fs.String("grpc_meta", "/keys/grpc-version.json", "gRPC credentials metadata JSON file"),
		SshCredMetaFile:       fs.String("ssh_meta", "/keys/ssh-version.json", "ssh server credentials metadata JSON file"),
	}

	fs.Var(telemetryCfg.UserAuth, "client_auth", "Client auth mode(s) - none,cert,password,jwt")
	fs.Parse(os.Args[1:])

	var defUserAuth gnmi.AuthTypes
	if *telemetryCfg.GnmiTranslibWrite {
		//In read/write mode we want to enable auth by default.
		defUserAuth = gnmi.AuthTypes{"password": true, "cert": false, "jwt": true}
	} else {
		defUserAuth = gnmi.AuthTypes{"jwt": false, "password": false, "cert": false}
	}

	if isFlagPassed(fs, "client_auth") {
		log.V(1).Infof("client_auth provided")
	} else {
		log.V(1).Infof("client_auth not provided, using defaults.")
		telemetryCfg.UserAuth = &defUserAuth
	}

	switch {
	case *telemetryCfg.Port <= 0:
		log.Warning("port must be > 0: %v; Using default port 9339", *telemetryCfg.Port)
		*telemetryCfg.Port = 9339
	}

	switch {
	case *telemetryCfg.StreamingThreshold < 0:
		log.Warningf("streaming threshold must be >= 0: %v; Using default threshold 12", *telemetryCfg.StreamingThreshold)
		*telemetryCfg.StreamingThreshold = 12
	}

	switch {
	case *telemetryCfg.UnaryThreshold < 0:
		log.Warningf("unary threshold must be >= 0: %v; Using default threshold 100", *telemetryCfg.UnaryThreshold)
		*telemetryCfg.UnaryThreshold = 100
	}

	switch {
	case *telemetryCfg.IdleConnDuration < 0:
		log.Warningf("idle_conn_duration must be >= 0: %v; Using default 5 seconds", *telemetryCfg.IdleConnDuration)
		*telemetryCfg.IdleConnDuration = 5
	}

	switch {
	case *telemetryCfg.LogLevel < 0:
		*telemetryCfg.LogLevel = 2
		log.V(lvl.INFO).Infof("Log level must be greater than 0, setting to default value of 2")
	}

	if !*telemetryCfg.NoTLS && !*telemetryCfg.Insecure {
		switch {
		case *telemetryCfg.ServerCert == "":
			return nil, nil, fmt.Errorf("serverCert must be set.")
		case *telemetryCfg.ServerKey == "":
			return nil, nil, fmt.Errorf("serverKey must be set.")
		}
	}

	// Move to new function
	gnmi.JwtRefreshInt = time.Duration(*telemetryCfg.JwtRefInt * uint64(time.Second))
	gnmi.JwtValidInt = time.Duration(*telemetryCfg.JwtValInt * uint64(time.Second))

	oscfg := &gnmi.OSConfig{
		ImgDir:          *telemetryCfg.ImgDirPath,
		ProcessTrfReady: gnmi.ProcessInstallFromBackEnd,
		ProcessTrfEnd:   gnmi.ProcessInstallFromBackEnd,
	}
	cfg := &gnmi.Config{}
	cfg.Port = int64(*telemetryCfg.Port)
	cfg.EnableTranslibWrite = bool(*telemetryCfg.GnmiTranslibWrite)
	cfg.EnableNativeWrite = bool(*telemetryCfg.GnmiNativeWrite)
	cfg.LogLevel = int(*telemetryCfg.LogLevel)
	cfg.StreamingThreshold = int(*telemetryCfg.StreamingThreshold)
	cfg.UnaryThreshold = int(*telemetryCfg.UnaryThreshold)
	cfg.IdleConnDuration = int(*telemetryCfg.IdleConnDuration)
	cfg.ConfigTableName = string(*telemetryCfg.ConfigTableName)

	// TODO: After other dependent projects are migrated to ZmqPort, remove ZmqAddress
	zmqAddress := *telemetryCfg.ZmqAddress
	zmqPort := *telemetryCfg.ZmqPort
	if zmqPort == "" {
		if zmqAddress != "" {
			// ZMQ address format: "tcp://127.0.0.1:1234"
			zmqPort = strings.Split(zmqAddress, ":")[2]
		}
	}

	cfg.ZmqPort = zmqPort

	//
	// Remaining statements are not upstreamed
	//
	cfg.EnableTranslation = true
	cfg.UserAuth = gnmi.AuthTypes(*telemetryCfg.UserAuth)
	cfg.CaCertLnk = "/keys/ca_cert.lnk"
	cfg.CaCertFile = string(*telemetryCfg.CaCert)
	cfg.SrvCertLnk = "/keys/server_cert.lnk"
	cfg.SrvCertFile = string(*telemetryCfg.ServerCert)
	cfg.SrvKeyLnk = "/keys/server_key.lnk"
	cfg.SrvKeyFile = string(*telemetryCfg.ServerKey)
	cfg.CertCRLConfig = string(*telemetryCfg.CertCRLConfig)
	cfg.IntManFile = string(*telemetryCfg.IntManFile)
	cfg.OSCfg = oscfg

	cfg.AuthzMetaFile = string(*telemetryCfg.AuthzMetaFile)
	cfg.AuthzPolicy = *telemetryCfg.AuthPolicyEnabled && !*telemetryCfg.Insecure
	cfg.AuthzPolicyFile = string(*telemetryCfg.AuthzPolicyFile)
	cfg.ConsoleCredMetaFile = string(*telemetryCfg.ConsoleCredMetaFile)
	cfg.CertzMetaFile = string(*telemetryCfg.CertzMetaFile)
	cfg.SshCredMetaFile = string(*telemetryCfg.SshCredMetaFile)
	cfg.CacheResponses = bool(*telemetryCfg.CacheResponses)
	//cfg.EnableSRXfmr = bool(*telemetryCfg.EnableSRXfmr)

	cfg.PathzPolicy = *telemetryCfg.PathzEnabled && !*telemetryCfg.Insecure
	cfg.PathzMetaFile = string(*telemetryCfg.PathzMetaFile)
	cfg.PathzPolicyFile = string(*telemetryCfg.PathzPolicyFile)

	if *telemetryCfg.CaCert != "" {
		cfg.CaCertLnk = filepath.Dir(*telemetryCfg.CaCert) + "/ca_cert.lnk"
	}
	if *telemetryCfg.ServerCert != "" {
		cfg.SrvCertLnk = filepath.Dir(*telemetryCfg.ServerCert) + "/server_cert.lnk"
	}
	if *telemetryCfg.ServerKey != "" {
		cfg.SrvKeyLnk = filepath.Dir(*telemetryCfg.ServerKey) + "/server_key.lnk"
	}
	if !*telemetryCfg.Insecure {
		cfg.GetOptions = gnmi.SrvAdvConfig
	}
	if *telemetryCfg.CaCert == "" && telemetryCfg.UserAuth.Enabled("cert") {
		telemetryCfg.UserAuth.Unset("cert")
		log.V(2).Info("client_auth mode cert requires ca_crt option. Disabling cert mode authentication.")
	}

	return telemetryCfg, cfg, nil
}

func isFlagPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func startGNMIServerGoog(telemetryCfg *TelemetryConfig, cfg *gnmi.Config, serverControlSignal chan ServerControlValue, stopSignalHandler chan<- bool, wg *sync.WaitGroup) {
	defer wg.Done()

	gnmi.GenerateJwtSecretKey()

	s, err := gnmi.NewServer(cfg)
	if err != nil {
		log.Errorf("Failed to create gNMI server: %v", err)
		return
	}

	// Start watching for log level change requests
	logLevelWatchStop := make(chan bool, 1)
	defer func() { logLevelWatchStop <- true }()
	s.ChangeLogLevel(logLevelWatchStop, nil)

	if *telemetryCfg.WithSaveOnSet {
		s.SaveStartupConfig = gnmi.SaveOnSetEnabled
	}
	if *telemetryCfg.WithMasterArbitration {
		s.ReqFromMaster = gnmi.ReqFromMasterEnabledMA
	}

	log.V(1).Infof("Auth Modes: %s", telemetryCfg.UserAuth)
	log.V(1).Infof("Starting RPC server on address: %s", s.Address())

	if !s.WarmRestartHelper.CheckWarmStart(false) {
		waitForPortInitDone()
	}

	go func() {
		if err := s.Serve(); err != nil {
			log.Errorf("Serve returned with err: %v", err)
		}
	}()

	serverControlValue := <-serverControlSignal
	log.V(1).Infof("Received signal for gnmi server to close")
	s.Stop()
	log.Flush()
	if serverControlValue == ServerStop {
		stopSignalHandler <- true
		log.Flush()
		return
	}
}

func waitForPortInitDone() {
	// Open a RedisDB connection to the AppDB.
	log.V(lvl.INFO).Info("Creating connection to ApplDB to wait for PortInit")
	d, err := db.NewDB(db.Options{
		DBNo:               db.ApplDB,
		TableNameSeparator: ":",
		KeySeparator:       ":",
	})
	if err != nil {
		log.V(lvl.ERROR).Infof("waitForPortInitDone failed to create DB connection: %s", err)
		return
	}
	// Wait for the ports to be initialized.
	for timeout := time.After(pollTimeout); true; {
		select {
		case <-timeout:
			log.V(lvl.ERROR).Infof("waitForPortInitDone failed to find condition")
			return
		default:
			log.V(lvl.INFO).Info("Waiting for PortInitDone to be set before Telemetry will start")
			value, err := d.GetEntry(&db.TableSpec{Name: portTbl}, db.Key{Comp: []string{portInitKey}})
			if err != nil {
				log.V(lvl.WARNING).Infof("waitForPortInitDone failed to get keys: %s", err)
				continue
			}
			if len(value.Field) == 1 {
				log.V(lvl.INFO).Infof("waitForPortInitDone succeeded")
				return
			} else {
				log.V(lvl.WARNING).Infof("waitForPortInitDone continue due to number of field in PortInitDone table is not 1")
			}
		}
		time.Sleep(pollIntv)
	}
}

/* Google is not using this helper to watch certificates and restart the server.
func iNotifyCertMonitoring(watcher *fsnotify.Watcher, telemetryCfg *TelemetryConfig, serverControlSignal chan<- ServerControlValue, testReadySignal chan<- int, certLoaded *int32) {
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		if testReadySignal != nil { // for testing only
			testReadySignal <- 0
		}
		for {
			select {
			case event := <-watcher.Events:
				if event.Name != "" && (filepath.Ext(event.Name) == ".cert" || filepath.Ext(event.Name) == ".crt" ||
					filepath.Ext(event.Name) == ".cer" || filepath.Ext(event.Name) == ".pem" ||
					filepath.Ext(event.Name) == ".key") {
					log.V(1).Infof("Inotify watcher has received event: %v", event)
					if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
						log.V(1).Infof("Cert File has been modified: %s", event.Name)
						serverControlSignal <- ServerStart // let server know that a write/create event occurred
						done <- true
						return
					}
					if event.Op&fsnotify.Remove == fsnotify.Remove || event.Op&fsnotify.Rename == fsnotify.Rename {
						log.V(1).Infof("Cert file has been deleted: %s", event.Name)
						serverControlSignal <- ServerRestart   // let server know that a remove/rename event occurred
						if atomic.LoadInt32(certLoaded) == 1 { // Should continue monitoring if certs are not present
							done <- true
							return
						}
					}
				}
			case err := <-watcher.Errors:
				if err != nil {
					log.Errorf("Received error event when watching cert: %v", err)
					serverControlSignal <- ServerStop
					done <- true
					return // If watcher is unable to access cert file stop monitoring
				}
			}
		}
	}()

	telemetryCertDirectory := filepath.Dir(*telemetryCfg.ServerCert)

	log.V(1).Infof("Begin cert monitoring on %s", telemetryCertDirectory)

	err := watcher.Add(telemetryCertDirectory) // Adding watcher to cert directory
	if err != nil {
		log.Errorf("Received error when adding watcher to cert directory: %v", err)
		serverControlSignal <- ServerStop
		done <- true
	}

	<-done
	log.V(6).Infof("Closing cert rotation monitoring")
}
*/

func signalHandler(serverControlSignal chan<- ServerControlValue, sigchannel <-chan os.Signal, stopSignalHandler <-chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	select {
	case <-sigchannel:
		log.V(6).Infof("Sending signal stop to server because of syscall received")
		serverControlSignal <- ServerStop
		return
	case <-stopSignalHandler:
		return
	}
}

/* We are using startGNMIServerGoog rather than this version from the community.
func startGNMIServer(telemetryCfg *TelemetryConfig, cfg *gnmi.Config, serverControlSignal chan ServerControlValue, stopSignalHandler chan<- bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		var opts []grpc.ServerOption
		var certLoaded int32
		atomic.StoreInt32(&certLoaded, 0) // Not loaded

		if !*telemetryCfg.NoTLS {
			var certificate tls.Certificate
			var err error
			if *telemetryCfg.Insecure {
				certificate, err = testcert.NewCert()
				if err != nil {
					log.Errorf("could not load server key pair: %s", err)
					return
				}
			} else {
				watcher, err := fsnotify.NewWatcher()
				if err != nil {
					log.Errorf("Received error when creating fsnotify watcher %v", err)
				}
				if watcher != nil {
					go iNotifyCertMonitoring(watcher, telemetryCfg, serverControlSignal, nil, &certLoaded)
				}
				certificate, err = tls.LoadX509KeyPair(*telemetryCfg.ServerCert, *telemetryCfg.ServerKey)
				if err != nil {
					computeSHA512Checksum(*telemetryCfg.ServerCert)
					computeSHA512Checksum(*telemetryCfg.ServerKey)
					log.Errorf("could not load server key pair: %s", err)
					for {
						serverControlValue := <-serverControlSignal
						if serverControlValue == ServerStop {
							return // server called to shutdown
						}
						if serverControlValue == ServerStart {
							break // retry loading certs after cert has been written or created
						}
						// We don't care if file is deleted here as we will only want to check
						// if certs have been created or written to, else we will wait again
					}
					continue
				}
			}

			tlsCfg := &tls.Config{
				ClientAuth:               tls.RequireAndVerifyClientCert,
				Certificates:             []tls.Certificate{certificate},
				MinVersion:               tls.VersionTLS12,
				CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
				PreferServerCipherSuites: true,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
					tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				},
			}

			if *telemetryCfg.AllowNoClientCert {
				// RequestClientCert will ask client for a certificate but won't
				// require it to proceed. If certificate is provided, it will be
				// verified.
				tlsCfg.ClientAuth = tls.RequestClientCert
			}

			if *telemetryCfg.CaCert != "" {
				caCertLoaded := true
				ca, err := ioutil.ReadFile(*telemetryCfg.CaCert)
				if err != nil {
					log.Errorf("could not read CA certificate: %s", err)
					caCertLoaded = false
				}
				certPool := x509.NewCertPool()
				if ok := certPool.AppendCertsFromPEM(ca); !ok {
					log.Errorf("failed to append CA certificate")
					caCertLoaded = false
				}
				if !caCertLoaded {
					for {
						serverControlValue := <-serverControlSignal
						if serverControlValue == ServerStop {
							return // server called to shutdown
						}
						if serverControlValue == ServerStart {
							break // retry loading certs after cert has been written or created
						}
					}
					continue
				}
				tlsCfg.ClientCAs = certPool
			} else {
				if telemetryCfg.UserAuth.Enabled("cert") {
					telemetryCfg.UserAuth.Unset("cert")
					log.Warning("client_auth mode cert requires ca_crt option. Disabling cert mode authentication.")
				}
			}

			atomic.StoreInt32(&certLoaded, 1) // Certs have loaded

			keep_alive_params := keepalive.ServerParameters{
				MaxConnectionIdle: time.Duration(*telemetryCfg.IdleConnDuration) * time.Second, // duration in which idle connection will be closed, default is inf
			}

			opts = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsCfg))}

			if *telemetryCfg.IdleConnDuration > 0 { // non inf case
				opts = append(opts, grpc.KeepaliveParams(keep_alive_params))
			}

			cfg.UserAuth = telemetryCfg.UserAuth

			gnmi.GenerateJwtSecretKey()
		}

		s, err := gnmi.NewServer(cfg, opts)
		if err != nil {
			log.Errorf("Failed to create gNMI server: %v", err)
			return
		}
		if *telemetryCfg.WithSaveOnSet {
			s.SaveStartupConfig = gnmi.SaveOnSetEnabled
		}

		if *telemetryCfg.WithMasterArbitration {
			s.ReqFromMaster = gnmi.ReqFromMasterEnabledMA
		}

		log.V(1).Infof("Auth Modes: %v", telemetryCfg.UserAuth)
		log.V(1).Infof("Starting RPC server on address: %s", s.Address())

		go func() {
			log.V(1).Infof("GNMI Server started serving")
			if err := s.Serve(); err != nil {
				log.Errorf("Serve returned with err: %v", err)
			}
		}()

		serverControlValue := <-serverControlSignal
		log.V(1).Infof("Received signal for gnmi server to close")
		s.Stop()
		if serverControlValue == ServerStop {
			stopSignalHandler <- true
			log.Flush()
			return
		}
		// Both ServerStart and ServerRestart will loop and restart server
		// We use different value to distinguish between write/create and remove/rename
	}
}

func computeSHA512Checksum(file string) {
	currentTime := time.Now().UTC()
	f, err := os.Open(file)
	if err != nil {
		log.Errorf("Unable to open %s, got err %s", file, err)
	}
	defer f.Close()

	hasher := sha512.New()
	if _, err := io.Copy(hasher, f); err != nil {
		log.Errorf("Unable to create hash for %s, got err %s", file, err)
	}
	hash := hasher.Sum(nil)
	log.V(1).Infof("SHA512 hash of %s: %s at time %s", file, hex.EncodeToString(hash), currentTime.String())
}
*/
