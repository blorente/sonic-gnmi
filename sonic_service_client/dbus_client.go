package host_service

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/sonic-net/sonic-gnmi/common_utils"

	"github.com/godbus/dbus/v5"
	log "github.com/golang/glog"
)

var (
	burninMu  sync.Mutex
	debugMu   sync.Mutex
	fileMu    sync.Mutex
	healthzMu sync.Mutex
	osMu      sync.Mutex
	sysMu     sync.Mutex
	resetMu   sync.Mutex
)

type Service interface {
	ConfigReload(fileName string) error
	ConfigSave(fileName string) error
	ApplyPatchYang(fileName string) error
	ApplyPatchDb(fileName string) error
	CreateCheckPoint(cpName string) error
	DebugTunnel(req string) (string, error)
	DeleteCheckPoint(cpName string) error
	BurninStart(req string) (string, error)
	BurninStop(req string) (string, error)
	BurninResults(req string) (string, error)
	FileRemove(fileName string) (string, error)
	HealthzAck(req string) (string, error)
	HealthzCheck(req string) (string, error)
	HealthzCollect(req string) (string, error)
	OSInstall(req string) (string, error)
	OSActivate(req string) (string, error)
	OSVerify(req string) (string, error)
	SystemOptics(req string) (string, error)
	WhiteboxSet(cmd string) (string, error)
	StopService(service string) error
	RestartService(service string) error
	SSHMgmtSet(cmd string) error
	SSHCheckpoint(action CredzCheckpointAction) error
	ConsoleSet(cmd string) error
	ConsoleCheckpoint(action CredzCheckpointAction) error
	FactoryReset(cmd string) (string, error)
}

type CredzCheckpointAction string

const (
	CredzCPCreate  CredzCheckpointAction = ".create_checkpoint"
	CredzCPDelete  CredzCheckpointAction = ".delete_checkpoint"
	CredzCPRestore CredzCheckpointAction = ".restore_checkpoint"
	NamePrefix                           = "org.SONiC.HostService."
	PathPrefix                           = "/org/SONiC/HostService/"
)

type DbusClient struct {
	busNamePrefix string
	busPathPrefix string
	intNamePrefix string
	caller        Caller
	channel       chan struct{}
}

type Caller interface {
	DbusApi(busName string, busPath string, intName string, timeout int, args ...interface{}) (string, error)
}

type DbusCaller struct{}

type FakeDbusCaller struct {
	Msg string
}

type FailDbusCaller struct{}

type SpyDbusCaller struct {
	Command chan []string
}

func NewDbusClient(caller Caller) (Service, error) {
	var client DbusClient
	if caller == nil {
		return nil, fmt.Errorf("You must supply a DbusCaller")
	}
	client.busNamePrefix = NamePrefix
	client.busPathPrefix = PathPrefix
	client.intNamePrefix = NamePrefix
	client.caller = caller

	return &client, nil
}

func (c *FakeDbusCaller) DbusApi(busName string, busPath string, intName string, timeout int, args ...interface{}) (string, error) {
	if c.Msg != "" {
		return fmt.Sprintf("%v", c.Msg), nil
	}
	return fmt.Sprintf("%v %v", intName, args), nil
}

func (_ *FailDbusCaller) DbusApi(busName string, busPath string, intName string, timeout int, args ...interface{}) (string, error) {
	return "", fmt.Errorf("%v %v", intName, args)
}

func (c *SpyDbusCaller) DbusApi(busName string, busPath string, intName string, timeout int, args ...interface{}) (string, error) {
	resp := []string{intName}
	for _, el := range args {
		resp = append(resp, fmt.Sprintf("%v", el))
	}
	c.Command <- resp
	return "", nil
}

func (_ *DbusCaller) DbusApi(busName string, busPath string, intName string, timeout int, args ...interface{}) (string, error) {
	common_utils.IncCounter(common_utils.DBUS)
	conn, err := dbus.SystemBus()
	log.V(2).Infof("DBUS Call: %v %v", intName, args)
	if err != nil {
		log.V(2).Infof("Failed to connect to system bus: %v", err)
		common_utils.IncCounter(common_utils.DBUS_FAIL)
		return "", err
	}

	ch := make(chan *dbus.Call, 1)
	obj := conn.Object(busName, dbus.ObjectPath(busPath))
	obj.Go(intName, 0, ch, args...)
	select {
	case call := <-ch:
		if call.Err != nil {
			common_utils.IncCounter(common_utils.DBUS_FAIL)
			return "", call.Err
		}
		result := call.Body
		if len(result) == 0 {
			common_utils.IncCounter(common_utils.DBUS_FAIL)
			return "", fmt.Errorf("Dbus result is empty %v", result)
		}
		if ret, ok := result[0].(int32); ok {
			if ret == 0 {
				if _, ok := result[1].(string); !ok {
					return "", fmt.Errorf("Dbus result is invalid: second element is not string.")
				}
				return result[1].(string), nil
			} else {
				if len(result) != 2 {
					common_utils.IncCounter(common_utils.DBUS_FAIL)
					return "", fmt.Errorf("Dbus result is invalid %v", result)
				}
				if msg, check := result[1].(string); check {
					common_utils.IncCounter(common_utils.DBUS_FAIL)
					return "", fmt.Errorf(msg)
				} else {
					common_utils.IncCounter(common_utils.DBUS_FAIL)
					return "", fmt.Errorf("Invalid result message type %v %v", result[1], reflect.TypeOf(result[1]))
				}
			}
		} else {
			common_utils.IncCounter(common_utils.DBUS_FAIL)
			return "", fmt.Errorf("Invalid result type %v %v", result[0], reflect.TypeOf(result[0]))
		}
	case <-time.After(time.Duration(timeout) * time.Second):
		log.V(2).Infof("DbusApi: timeout")
		common_utils.IncCounter(common_utils.DBUS_FAIL)
		return "", fmt.Errorf("Timeout %v", timeout)
	}
}

func (c *DbusClient) ConfigReload(config string) error {
	common_utils.IncCounter(common_utils.DBUS_CONFIG_RELOAD)
	modName := "config"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".reload"
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, config)
	return err
}

func (c *DbusClient) ConfigSave(fileName string) error {
	common_utils.IncCounter(common_utils.DBUS_CONFIG_SAVE)
	// Upsteam uses the "config" module but our backend is in the "cfg_mgmt" module.
	//modName := "config"
	modName := "cfg_mgmt"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".save"
	// Upstream passes a filename as an option but we do not since we rely on a
	// default set in our Host Services backend.
	//_, err := DbusApi(busName, busPath, intName, 10, fileName)
	emptyOptions := []string{}
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, emptyOptions)
	return err
}

func (c *DbusClient) ApplyPatchYang(patch string) error {
	common_utils.IncCounter(common_utils.DBUS_APPLY_PATCH_YANG)
	modName := "gcu"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".apply_patch_yang"
	_, err := c.caller.DbusApi(busName, busPath, intName, 180, patch)
	return err
}

func (c *DbusClient) ApplyPatchDb(patch string) error {
	common_utils.IncCounter(common_utils.DBUS_APPLY_PATCH_DB)
	modName := "gcu"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".apply_patch_db"
	_, err := c.caller.DbusApi(busName, busPath, intName, 180, patch)
	return err
}

func (c *DbusClient) CreateCheckPoint(fileName string) error {
	common_utils.IncCounter(common_utils.DBUS_CREATE_CHECKPOINT)
	modName := "gcu"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".create_checkpoint"
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, fileName)
	return err
}

func (c *DbusClient) DeleteCheckPoint(fileName string) error {
	common_utils.IncCounter(common_utils.DBUS_DELETE_CHECKPOINT)
	modName := "gcu"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".delete_checkpoint"
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, fileName)
	return err
}

func (c *DbusClient) OSInstall(req string) (string, error) {
	modName := "gnoi_os_mgmt"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".install"

	osMu.Lock()
	defer osMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_OS_INSTALL)
	return c.caller.DbusApi(busName, busPath, intName, 10, req)
}

func (c *DbusClient) OSActivate(req string) (string, error) {
	modName := "gnoi_os_mgmt"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".activate"

	osMu.Lock()
	defer osMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_OS_ACTIVATE)
	return c.caller.DbusApi(busName, busPath, intName, 10, req)
}

func (c *DbusClient) OSVerify(req string) (string, error) {
	modName := "gnoi_os_mgmt"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".verify"

	osMu.Lock()
	defer osMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_OS_VERIFY)
	return c.caller.DbusApi(busName, busPath, intName, 10, req)
}

func (c *DbusClient) SystemOptics(req string) (string, error) {
	modName := "gpins_infra_host"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".exec_cmd"

	sysMu.Lock()
	defer sysMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_SYSTEM_OPTICS)
	return c.caller.DbusApi(busName, busPath, intName, 10, req)
}

func (c *DbusClient) FileRemove(fileName string) (string, error) {
	modName := "gpins_infra_host"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".exec_cmd"

	fileMu.Lock()
	defer fileMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_FILE_REMOVE)
	return c.caller.DbusApi(busName, busPath, intName, 10, "rm "+fileName)
}

func (c *DbusClient) BurninStart(req string) (string, error) {
	modName := "gnoi_burnin"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".start"

	burninMu.Lock()
	defer burninMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_BURNIN_START)
	return c.caller.DbusApi(busName, busPath, intName, 10, []string{req})
}

func (c *DbusClient) BurninStop(req string) (string, error) {
	modName := "gnoi_burnin"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".stop"

	burninMu.Lock()
	defer burninMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_BURNIN_STOP)
	return c.caller.DbusApi(busName, busPath, intName, 10, []string{req})
}

func (c *DbusClient) BurninResults(req string) (string, error) {
	modName := "gnoi_burnin"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".get_results"

	burninMu.Lock()
	defer burninMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_BURNIN_RESULTS)
	return c.caller.DbusApi(busName, busPath, intName, 10, []string{req})
}

func (c *DbusClient) HealthzAck(req string) (string, error) {
	modName := "debug_info"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".ack"

	healthzMu.Lock()
	defer healthzMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_HEALTHZ_ACK)
	return c.caller.DbusApi(busName, busPath, intName, 10, []string{req})
}

func (c *DbusClient) HealthzCheck(req string) (string, error) {
	modName := "debug_info"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".check"

	healthzMu.Lock()
	defer healthzMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_HEALTHZ_CHECK)
	return c.caller.DbusApi(busName, busPath, intName, 10, []string{req})
}

func (c *DbusClient) HealthzCollect(req string) (string, error) {
	modName := "debug_info"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".collect"

	healthzMu.Lock()
	defer healthzMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_HEALTHZ_COLLECT)
	return c.caller.DbusApi(busName, busPath, intName, 10, []string{req})
}

func (c *DbusClient) DebugTunnel(req string) (string, error) {
	modName := "gpins_infra_host"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".exec_cmd"

	debugMu.Lock()
	defer debugMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_DEBUG_TUNNEL)
	return c.caller.DbusApi(busName, busPath, intName, 10, req)
}

func (c *DbusClient) WhiteboxSet(cmd string) (string, error) {
	modName := "gnoi_whitebox"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".set_controller_connection"

	debugMu.Lock()
	defer debugMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_WHITEBOX_SET)
	return c.caller.DbusApi(busName, busPath, intName, 10, cmd)
}

func (c *DbusClient) StopService(service string) error {
	common_utils.IncCounter(common_utils.DBUS_STOP_SERVICE)
	modName := "systemd"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".stop_service"
	_, err := c.caller.DbusApi(busName, busPath, intName, 90, service)
	return err
}

func (c *DbusClient) RestartService(service string) error {
	common_utils.IncCounter(common_utils.DBUS_RESTART_SERVICE)
	modName := "systemd"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".restart_service"
	_, err := c.caller.DbusApi(busName, busPath, intName, 90, service)
	return err
}

func (c *DbusClient) ConsoleSet(cmd string) error {
	modName := "gnsi_console"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".set"

	common_utils.IncCounter(common_utils.GNSI_CREDZ_SET)
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, []string{cmd})
	return err
}

func (c *DbusClient) SSHMgmtSet(cmd string) error {
	modName := "ssh_mgmt"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".set"

	common_utils.IncCounter(common_utils.GNSI_CREDZ_SET)
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, []string{cmd})
	return err
}

func (c *DbusClient) ConsoleCheckpoint(action CredzCheckpointAction) error {
	modName := "gnsi_console"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + string(action)

	common_utils.IncCounter(common_utils.GNSI_CREDZ_CHECKPOINT)
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, "")
	return err
}

func (c *DbusClient) SSHCheckpoint(action CredzCheckpointAction) error {
	modName := "ssh_mgmt"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + string(action)

	common_utils.IncCounter(common_utils.GNSI_CREDZ_CHECKPOINT)
	_, err := c.caller.DbusApi(busName, busPath, intName, 10, "")
	return err
}

func (c *DbusClient) FactoryReset(cmd string) (string, error) {
	modName := "gnoi_reset"
	busName := c.busNamePrefix + modName
	busPath := c.busPathPrefix + modName
	intName := c.intNamePrefix + modName + ".issue_reset"

	resetMu.Lock()
	defer resetMu.Unlock()
	common_utils.IncCounter(common_utils.GNOI_FACTORY_RESET)
	return c.caller.DbusApi(busName, busPath, intName, 10, cmd)
}
