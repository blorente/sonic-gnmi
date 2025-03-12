package common_utils

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

// The functionality in this file is meant to make sonic-net/sonic-swss-common/common/warm_restart.cpp
//   available in Go.
//
//  External API's:
//  func StringToWarmStartState(state string) (WarmStartState, error)
//  func StringToWarmBootNotification(notification string) (WarmBootNotification, error)
//  func Initialize(appName string, dockerName string) error
//  func RegisterWarmBootInfo(waitForFreeze bool, waitForCheckpoint bool, waitForReconciliation bool, stopOnFreeze bool) error
//  func CheckWarmStart(incrRestoreCnt bool) bool
//  func IsWarmStart() bool
//  func IsNSFOngoing() bool
//  func IsSystemWarmRebootEnabled() bool
//  func GetWarmStartState(appName string) WarmStartState
//  func SetWarmStartState(state WarmStartState)
//  func WaitForUnfreeze() bool
//  func WaitForReconciliation()

// These constants are meant to be used outside of this package:

const (
	NsfManagerNotificationChannel = "NSF_MANAGER_COMMON_NOTIFICATION_CHANNEL"
	RegistrationFreezeKey         = "freeze"
	RegistrationUnfreezeKey       = "unfreeze"
	RegistrationCheckpointKey     = "checkpoint"
	RegistrationReconciliationKey = "reconciliation"
	RegistrationStopOnFreezeKey   = "stop_on_freeze"
	PerformanceStartTimeKey       = "start-timestamp"
	PerformanceFinishTimeKey      = "finish-timestamp"
	PerformanceStatusKey          = "status"
)

type WarmStartState int
type WarmStartStage int

const (
	INITIALIZED WarmStartState = iota
	RESTORED
	REPLAYED
	RECONCILED
	WSDISABLED
	WSUNKNOWN
	FROZEN
	QUIESCENT
	CHECKPOINTED
	COMPLETED
	FAILED
)

const (
	STAGE_FREEZE WarmStartStage = iota
	STAGE_CHECKPOINT
	STAGE_RECONCILIATION
	STAGE_UNFREEZE
)

type WarmBootNotification int

const (
	InvalidNotification WarmBootNotification = iota
	Freeze
	Unfreeze
	Checkpoint
)

// These constants are meant for internal use within this file:
const (
	stateDb                           = "STATE_DB"
	configDb                          = "CONFIG_DB"
	warmRestartStateTable             = "WARM_RESTART_TABLE"
	warmRestartConfigTable            = "WARM_RESTART"
	warmRestartRegistrationStateTable = "WARM_RESTART_REGISTRATION_TABLE"
	warmRestartEnableStateTable       = "WARM_RESTART_ENABLE_TABLE"
	warmRestartPerformanceTable       = "WARM_RESTART_PERFORMANCE_TABLE"
	timestampKey                      = "timestamp"
	restoreCountKey                   = "restore_count"
	systemKey                         = "system"
	enableKey                         = "enable"
	stateKey                          = "state"
	namespace                         = ""
)

const (
	reconciliationSleep = 3 * time.Second
)

var warmBootStateToStringMap = map[WarmStartState]string{
	INITIALIZED:  "initialized",
	RESTORED:     "restored",
	REPLAYED:     "replayed",
	RECONCILED:   "reconciled",
	WSDISABLED:   "disabled",
	WSUNKNOWN:    "unknown",
	FROZEN:       "frozen",
	QUIESCENT:    "quiescent",
	CHECKPOINTED: "checkpointed",
	COMPLETED:    "completed",
	FAILED:       "failed",
}

var warmBootStateToStageMap = map[WarmStartState]WarmStartStage{
	FROZEN:       STAGE_FREEZE,
	QUIESCENT:    STAGE_FREEZE,
	CHECKPOINTED: STAGE_CHECKPOINT,
	RECONCILED:   STAGE_RECONCILIATION,
	COMPLETED:    STAGE_UNFREEZE,
}

var warmBootStageToStringMap = map[WarmStartStage]string{
	STAGE_FREEZE:         "freeze",
	STAGE_CHECKPOINT:     "checkpoint",
	STAGE_RECONCILIATION: "reconciliation",
	STAGE_UNFREEZE:       "unfreeze",
}

// String converts a WarmStartState into a string.
func (state WarmStartState) String() string {
	if n, ok := warmBootStateToStringMap[state]; ok {
		return n
	}
	log.V(lvl.ERROR).Infof("Invalid WarmStartState")
	return "invalid:invalid"
}

var warmBootStateLookupMap = map[string]WarmStartState{
	"initialized":  INITIALIZED,
	"restored":     RESTORED,
	"replayed":     REPLAYED,
	"reconciled":   RECONCILED,
	"disabled":     WSDISABLED,
	"unknown":      WSUNKNOWN,
	"frozen":       FROZEN,
	"quiescent":    QUIESCENT,
	"checkpointed": CHECKPOINTED,
	"completed":    COMPLETED,
	"failed":       FAILED,
}

var requiredApps = []string{"orchagent", "p4rt", "teammgrd"}

// StringToWarmBootState converts a string into a WarmStartState.
func StringToWarmStartState(state string) (WarmStartState, error) {
	if n, ok := warmBootStateLookupMap[state]; ok {
		return n, nil
	}
	errString := fmt.Sprintf("Unrecognized WarmStartState string: %s", state)
	log.V(lvl.DEBUG).Infof(errString)
	return WSUNKNOWN, fmt.Errorf(errString)
}

var warmBootNotificationToStringpMap = map[WarmBootNotification]string{
	Freeze:     "freeze",
	Unfreeze:   "unfreeze",
	Checkpoint: "checkpoint",
}

// String converts a WarmBootNotification into a string.
func (notification WarmBootNotification) String() string {
	if n, ok := warmBootNotificationToStringpMap[notification]; ok {
		return n
	}
	log.V(lvl.ERROR).Infof("Invalid WarmBootNotification")
	return "invalid:invalid"
}

var warmBootNotificationLookupMap = map[string]WarmBootNotification{
	"freeze":     Freeze,
	"unfreeze":   Unfreeze,
	"checkpoint": Checkpoint,
}

// StringToWarmBootNotification converts a string into a WarmBootNotification.
func StringToWarmBootNotification(notification string) (WarmBootNotification, error) {
	if n, ok := warmBootNotificationLookupMap[notification]; ok {
		return n, nil
	}
	return InvalidNotification, fmt.Errorf("invalid WarmBootNotification string %s", notification)
}

func getTimeString() string {
	return time.Now().Format("2006-01-02.15:04:05.999999")
}

type WarmRestartHelperInterface interface {
	Initialize(appName string, dockerName string) error
	RegisterWarmBootInfo(waitForFreeze bool, waitForCheckpoint bool, waitForReconciliation bool, stopOnFreeze bool) error
	IsNSFOngoing() bool
	IsSystemWarmRebootEnabled() bool
	CheckWarmStart(incrRestoreCnt bool) bool
	IsWarmStart() bool
	GetWarmStartState(appName string) WarmStartState
	SetWarmStartState(state WarmStartState)
	CreateNotifHandler(notifFunc func(*redis.Message)) (*NotifHandler, error)
	SetFreezeStatus(val bool)
	FetchFreezeStatus() bool
	Close()
	WaitForUnfreeze() bool
	WaitForReconciliation()
	LockNotifHandler()
	UnlockNotifHandler()
	UpdateAppWarmBootStageStart(stage WarmStartStage)
}

// WarmRestartHelper provides utilities for querying and receiving system state information.
// NewWarmRestartHelper must be called for a new WarmRestartHelper.
type WarmRestartHelper struct {
	warmstart    *warmStart
	NotifHandler *NotifHandler
}

type warmStart struct {
	appName                 string
	dockerName              string
	stateDb                 *redis.Client
	configDb                *redis.Client
	enabled                 bool
	systemWarmRebootEnabled bool
	initialized             bool
	mu                      sync.Mutex
	restartAppTableName     string
	freezeSet               bool
	warmBootState           WarmStartState
}

type NotifHandler struct {
	Mu       sync.Mutex
	Consumer *NotificationConsumer
}

type WarmStartPerfDbInfo struct {
	key       string
	timestamp string
	status    string
	isStart   bool
}

// NewWarmRestartHelper returns a new WarmRestartHelper.
func NewWarmRestartHelper(notifFunc func(*redis.Message)) (*WarmRestartHelper, error) {
	var err error
	wrh := new(WarmRestartHelper)
	wrh.warmstart = new(warmStart)
	if wrh == nil {
		return nil, fmt.Errorf("WarmRestartHelper is nil")
	}
	wrh.NotifHandler, err = wrh.CreateNotifHandler(notifFunc)
	if err != nil {
		return nil, fmt.Errorf("failed to create NotifHandler: %v", err)
	}
	return wrh, nil
}

// CreateNotifHandler creates the NotifHandler for receiving NSF notifications from NSF Manager.
func (wrh *WarmRestartHelper) CreateNotifHandler(notifFunc func(*redis.Message)) (*NotifHandler, error) {
	if notifFunc == nil {
		return nil, nil
	}
	var err error
	notifHandler := new(NotifHandler)
	if notifHandler.Consumer, err = NewNotificationConsumer(NsfManagerNotificationChannel, notifFunc); err != nil {
		return nil, err
	}
	log.V(lvl.INFO).Info("CreateNotifHandler set up Notification Consumer")
	return notifHandler, nil
}

// LockNotifHandler locks the notification consumer associated with NotifHandler
func (wrh *WarmRestartHelper) LockNotifHandler() {
	if wrh == nil {
		return
	}
	wrh.NotifHandler.Mu.Lock()
}

// UnlockNotifHandler locks the notification consumer associated with NotifHandler
func (wrh *WarmRestartHelper) UnlockNotifHandler() {
	if wrh == nil {
		return
	}
	wrh.NotifHandler.Mu.Unlock()
}

// Close closes the notification consumer associated with NotifHandler
func (wrh *WarmRestartHelper) Close() {
	if wrh == nil {
		return
	}
	wrh.NotifHandler.Mu.Lock()
	defer wrh.NotifHandler.Mu.Unlock()
	wrh.NotifHandler.Consumer.Close()
}

// SetFreezeStatus is used by the server to update the status of the freeze mode.
func (wrh *WarmRestartHelper) SetFreezeStatus(val bool) {
	if wrh == nil {
		return
	}
	wrh.warmstart.mu.Lock()
	defer wrh.warmstart.mu.Unlock()

	wrh.warmstart.freezeSet = val
}

// FetchFreezeStatus is used by the server to get the current status of ongoing freeze mode.
func (wrh *WarmRestartHelper) FetchFreezeStatus() bool {
	if wrh == nil {
		return false
	}
	wrh.warmstart.mu.Lock()
	defer wrh.warmstart.mu.Unlock()
	return wrh.warmstart.freezeSet
}

func (wrh *WarmRestartHelper) Initialize(appName string, dockerName string) error {
	if wrh == nil {
		return fmt.Errorf("WarmRestartHelper is nil")
	}
	wrh.warmstart.mu.Lock()
	defer wrh.warmstart.mu.Unlock()

	if wrh.warmstart.initialized {
		return fmt.Errorf("warmStart already initialized")
	}

	if len(appName) == 0 {
		return fmt.Errorf("appName is empty")
	}

	if len(dockerName) == 0 {
		return fmt.Errorf("dockerName is empty")
	}

	var err error
	if wrh.warmstart.stateDb, err = getRedisDBClient(); err != nil {
		return err
	}
	if wrh.warmstart.configDb = db.TransactionalRedisClient(db.ConfigDB); wrh.warmstart.configDb == nil {
		return fmt.Errorf("Failed to create ConfigDb connection: %v", wrh.warmstart.configDb)
	}
	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		return err
	}
	wrh.warmstart.appName = appName
	wrh.warmstart.dockerName = dockerName
	wrh.warmstart.enabled = false
	wrh.warmstart.systemWarmRebootEnabled = false
	wrh.warmstart.initialized = true
	wrh.warmstart.restartAppTableName = warmRestartStateTable + stateDbSep + appName
	wrh.warmstart.freezeSet = false
	wrh.warmstart.warmBootState = WSUNKNOWN

	return nil
}

func (wrh *WarmRestartHelper) RegisterWarmBootInfo(waitForFreeze bool, waitForCheckpoint bool, waitForReconciliation bool, stopOnFreeze bool) error {
	if wrh == nil {
		return fmt.Errorf("WarmRestartHelper is nil")
	}
	if !wrh.warmstart.initialized {
		errString := fmt.Sprintf("RegisterWarmBootInfo: Warmstart hasn't been initialized")
		log.V(lvl.DEBUG).Infof(errString)
		return fmt.Errorf(errString)
	}
	if stopOnFreeze && (waitForFreeze || waitForCheckpoint) {
		return fmt.Errorf("Can't stop and wait on freeze or checkpoint")
	}

	fvp := make(map[string]string)
	fvp[RegistrationFreezeKey] = strconv.FormatBool(waitForFreeze)
	fvp[RegistrationCheckpointKey] = strconv.FormatBool(waitForCheckpoint)
	fvp[RegistrationReconciliationKey] = strconv.FormatBool(waitForReconciliation)
	fvp[RegistrationStopOnFreezeKey] = strconv.FormatBool(stopOnFreeze)

	fvp[timestampKey] = getTimeString()

	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		return err
	}

	tableName := warmRestartRegistrationStateTable + stateDbSep + wrh.warmstart.dockerName + stateDbSep + wrh.warmstart.appName
	return wrh.warmstart.stateDb.HSet(context.Background(), tableName, fvp).Err()
}

// Caller must confirm that warmstart.initialized is true.
func writeRestoreCount(count uint64, db *redis.Client, restartAppTableName string) error {
	countString := strconv.FormatUint(count, 10)
	if _, err := db.HSet(context.Background(), restartAppTableName, restoreCountKey, countString).Result(); err != nil {
		errString := fmt.Sprintf("Error writing restore_count, tableName: %s, err = %v", restartAppTableName, err)
		log.V(lvl.DEBUG).Infof(errString)
		return fmt.Errorf(errString)
	}
	return nil
}

// Caller must confirm that warmstart.initialized is true.
func readRestoreCount(db *redis.Client, restartAppTableName string) string {
	value, err := db.HGet(context.Background(), restartAppTableName, restoreCountKey).Result()
	if err != nil {
		log.V(lvl.ERROR).Infof("Error reading restore_count, tableName: %s, err = %v", restartAppTableName, err)
		value = ""
	}
	return value
}

// Caller must confirm that warmstart.initialized is true.
func (wrh *WarmRestartHelper) checkAndUpdateRestoreCount(incrRestoreCnt bool, db *redis.Client, restartAppTableName string) (uint64, bool) {
	countStr := readRestoreCount(db, restartAppTableName)
	if countStr == "" {
		log.V(lvl.ERROR).Infof("Restore count is empty")
		writeRestoreCount(0, db, restartAppTableName)
		return 0, false
	}

	var err error
	var count uint64
	if count, err = strconv.ParseUint(countStr, 10, 32); err != nil {
		log.V(lvl.ERROR).Infof("Error converting restore_count: %s, err = %v",
			countStr, err)
		writeRestoreCount(0, db, restartAppTableName)
		return 0, false
	}
	if incrRestoreCnt {
		count++
		writeRestoreCount(count, db, restartAppTableName)
		// App is performing warm bootup. Update reconciliation start time
		// in the performance table.
		wrh.UpdateAppWarmBootStageStart(STAGE_RECONCILIATION)
	}
	return count, true
}

func (wrh *WarmRestartHelper) CheckWarmStart(incrRestoreCnt bool) bool {
	if wrh == nil {
		return false
	}
	wrh.warmstart.mu.Lock()
	defer wrh.warmstart.mu.Unlock()

	if !wrh.warmstart.initialized {
		log.V(lvl.ERROR).Infof("CheckWarmStartState: Warmstart hasn't been initialized")
		return false
	}

	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		log.V(lvl.ERROR).Infof("CheckWarmStartState: failed to get StateDB seperator: %v", err)
		return false
	}

	tableName := warmRestartEnableStateTable + stateDbSep + systemKey
	if value, _ := wrh.warmstart.stateDb.HGet(context.Background(), tableName, enableKey).Result(); value == "true" {
		wrh.warmstart.enabled = true
		wrh.warmstart.systemWarmRebootEnabled = true
	}

	tableName = warmRestartEnableStateTable + stateDbSep + wrh.warmstart.dockerName
	if value, _ := wrh.warmstart.stateDb.HGet(context.Background(), tableName, enableKey).Result(); value == "true" {
		wrh.warmstart.enabled = true
	}

	// For cold start, the whole state db will be flushed including warm start table.
	// Create the entry for this app here.
	if !wrh.warmstart.enabled {
		writeRestoreCount(0, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
		wrh.updateStateOnColdBoot()
		return false
	}

	count, ret := wrh.checkAndUpdateRestoreCount(incrRestoreCnt, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	if !ret {
		wrh.warmstart.enabled = false
		wrh.warmstart.systemWarmRebootEnabled = false
		writeRestoreCount(0, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
		wrh.updateStateOnColdBoot()
		return false
	}
	log.V(lvl.DEBUG).Infof("%s doing warm start, restore count %d", wrh.warmstart.appName, count)

	return true
}

func (wrh *WarmRestartHelper) updateStateOnColdBoot() {
	if wrh == nil {
		return
	}
	state := RECONCILED
	if wrh.isStateVerificationBootupEnabled() {
		state = COMPLETED
	}
	wrh.SetWarmStartState(state)
}

func (wrh *WarmRestartHelper) setWarmStartEnabled(enabled bool) {
	if wrh == nil {
		return
	}
	wrh.warmstart.mu.Lock()
	defer wrh.warmstart.mu.Unlock()

	wrh.warmstart.enabled = enabled
}

func (wrh *WarmRestartHelper) IsWarmStart() bool {
	if wrh == nil {
		return false
	}
	return wrh.warmstart.enabled
}

func (wrh *WarmRestartHelper) setSystemWarmRebootEnabled(enabled bool) {
	if wrh == nil {
		return
	}
	wrh.warmstart.mu.Lock()
	defer wrh.warmstart.mu.Unlock()

	wrh.warmstart.systemWarmRebootEnabled = enabled
}

func (wrh *WarmRestartHelper) IsSystemWarmRebootEnabled() bool {
	if wrh == nil {
		return false
	}
	return wrh.warmstart.systemWarmRebootEnabled
}

func (wrh *WarmRestartHelper) IsNSFOngoing() bool {
	if wrh == nil {
		return false
	}
	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		log.V(lvl.ERROR).Infof("IsNSFOngoing: failed to get StateDB seperator: %v", err)
		return false
	}

	tableName := warmRestartEnableStateTable + stateDbSep + systemKey
	if value, _ := wrh.warmstart.stateDb.HGet(context.Background(), tableName, enableKey).Result(); value == "true" {
		return true
	}
	return false
}

func (wrh *WarmRestartHelper) GetWarmStartState(appName string) WarmStartState {
	if !wrh.warmstart.initialized {
		log.V(lvl.ERROR).Infof("GetWarmSTartState: Warmstart hasn't been initialized")
		return WSUNKNOWN
	}

	var retState WarmStartState
	if appName == wrh.warmstart.appName && wrh.warmstart.warmBootState != WSUNKNOWN {
		/* Cache is up-to-date. Read state from cache. */
		retState = wrh.warmstart.warmBootState
		return retState
	}
	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		log.V(lvl.ERROR).Infof("GetWarmStartState: failed to get StateDB seperator: %v", err)
		return WSUNKNOWN
	}

	tableName := warmRestartStateTable + stateDbSep + appName
	value, err := wrh.warmstart.stateDb.HGet(context.Background(), tableName, stateKey).Result()
	if err != nil {
		log.V(lvl.ERROR).Infof("Error reading state, tableName: %s, err = %v", tableName, err)
		return WSUNKNOWN
	}

	retState, err = StringToWarmStartState(value)
	if err != nil {
		log.V(lvl.ERROR).Infof("GetWarmSTartState: StringToWarmStartState returned err = %v", err)
		return WSUNKNOWN
	}

	log.V(lvl.DEBUG).Infof("%s warm start state get %s",
		wrh.warmstart.appName, retState.String())

	return retState
}

// Set the WarmStart FSM state for a particular application.
func (wrh *WarmRestartHelper) SetWarmStartState(state WarmStartState) {
	if !wrh.warmstart.initialized {
		log.V(lvl.ERROR).Infof("SetWarmSTartState: Warmstart hasn't been initialized")
		return
	}

	// Update warm-boot stage end-time before updating the state to make
	// sure that the application's end-time is earlier than the overall
	// stage end-time.
	// Do not update performance in cold boot default state update scenario.
	// This is when an application sets RECONCILED or COMPLETED based on
	// state verification flag by calling updateStateOnColdBoot().
	if !((state == RECONCILED || state == COMPLETED) && !wrh.IsWarmStart()) {
		wrh.updateAppWarmBootStageEnd(state)
	}

	fvp := make(map[string]string)
	fvp[stateKey] = state.String()
	fvp[timestampKey] = getTimeString()
	if err := wrh.warmstart.stateDb.HSet(context.Background(), wrh.warmstart.restartAppTableName, fvp).Err(); err != nil {
		log.V(lvl.ERROR).Infof("Error setting state: %s, tableName: %s, err = %v",
			state.String(), wrh.warmstart.restartAppTableName, err)
		return
	}

	wrh.warmstart.warmBootState = state
	log.V(lvl.INFO).Infof("%s warm start state changed to %s",
		wrh.warmstart.appName, state.String())
}

func (wrh *WarmRestartHelper) isStateVerificationEnabled(attribute string) bool {
	if wrh == nil {
		return false
	}
	configDbSep, err := sdcfg.GetDbSeparator(configDb, namespace)
	if err != nil {
		log.V(lvl.ERROR).Infof("isStateVerificationEnabled: failed to get ConfigDB seperator: %v", err)
	}

	tableName := warmRestartConfigTable + configDbSep + systemKey
	if value, _ := wrh.warmstart.configDb.HGet(context.Background(), tableName, attribute).Result(); value == "true" {
		return true
	}
	return false
}

func (wrh *WarmRestartHelper) isStateVerificationShutdownEnabled() bool {
	return wrh.isStateVerificationEnabled("state_verification_shutdown")
}

func (wrh *WarmRestartHelper) isStateVerificationBootupEnabled() bool {
	return wrh.isStateVerificationEnabled("state_verification_bootup")
}

// Indicates wether the server should wait for an unfreeze notification during startup.
func (wrh *WarmRestartHelper) WaitForUnfreeze() bool {
	return wrh.isStateVerificationBootupEnabled()
}

// Unsets freeze mode when required apps have reconciled.
func (wrh *WarmRestartHelper) WaitForReconciliation() {
	log.V(lvl.INFO).Infof("waitForReconciliation: waiting for %v to reconcile", requiredApps)

	for _, app := range requiredApps {
		for {
			if wrh.GetWarmStartState(app) == RECONCILED {
				log.V(lvl.INFO).Infof("waitForReconciliation: %v has reconciled", app)
				break
			}
			time.Sleep(reconciliationSleep)
		}
	}
	wrh.SetFreezeStatus(false)
}

func (wrh *WarmRestartHelper) updateWarmBootPerformance(info WarmStartPerfDbInfo) {
	if wrh == nil {
		return
	}

	if info.key == "" {
		log.V(lvl.ERROR).Info("Cannot update performance: key is empty")
		return
	}
	if info.timestamp == "" {
		log.V(lvl.ERROR).Info("Cannot update performance: timestamp is empty for key %s", info.key)
		return
	}

	tsField := PerformanceStartTimeKey
	if !info.isStart {
		tsField = PerformanceFinishTimeKey
	}

	fvp := make(map[string]string)
	fvp[tsField] = info.timestamp
	if info.status != "" {
		fvp[PerformanceStatusKey] = info.status
	}
	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		log.V(lvl.ERROR).Infof("updateWarmBootPerformance: failed to get StateDB seperator: %v", err)
	}

	tableName := warmRestartPerformanceTable + stateDbSep + info.key
	if err := wrh.warmstart.stateDb.HSet(context.Background(), tableName, fvp).Err(); err != nil {
		log.V(lvl.ERROR).Infof("Failed to update NSF performance table: %v", err)
	}
}

func (wrh *WarmRestartHelper) updateWarmBootStagePerformance(stage WarmStartStage, info WarmStartPerfDbInfo) {
	if wrh == nil {
		return
	}

	perfKey, ok := warmBootStageToStringMap[stage]
	if !ok {
		log.V(lvl.ERROR).Infof("Cannot update performance: invalid stage %v for key %s", stage, info.key)
		return
	}
	stateDbSep, err := sdcfg.GetDbSeparator(stateDb, namespace)
	if err != nil {
		log.V(lvl.ERROR).Infof("updateWarmBootStagePerformance: failed to get StateDB seperator: %v", err)
	}

	if info.key != "" {
		// Only update warm-boot performance for registered applications.
		registeredTable := warmRestartRegistrationStateTable + stateDbSep + wrh.warmstart.dockerName + stateDbSep + wrh.warmstart.appName
		if _, err := wrh.warmstart.stateDb.Exists(context.Background(), registeredTable).Result(); err != nil {
			log.V(lvl.DEBUG).Infof("Can't update warm-boot performance for unregistered application: %v", wrh.warmstart.appName)
			return
		}
		perfKey += stateDbSep + info.key
	}

	ts := info.timestamp
	if ts == "" {
		ts = getTimeString()
	}

	dbInfo := info
	dbInfo.key = perfKey
	dbInfo.timestamp = ts
	wrh.updateWarmBootPerformance(dbInfo)
}

func (wrh *WarmRestartHelper) UpdateAppWarmBootStageStart(stage WarmStartStage) {
	if wrh == nil {
		return
	}
	info := WarmStartPerfDbInfo{
		key:       wrh.warmstart.appName,
		timestamp: getTimeString(),
		status:    "",
		isStart:   true,
	}
	wrh.updateWarmBootStagePerformance(stage, info)
}

func (wrh *WarmRestartHelper) updateAppWarmBootStageEnd(state WarmStartState) {
	if wrh == nil {
		return
	}

	stage, ok := warmBootStateToStageMap[state]
	if !ok {
		log.V(lvl.DEBUG).Infof("updateAppWarmBootStageEnd: State to Stage mapping not found for state=%v", state)
		return
	}

	status := "success"
	if state == FROZEN || state == QUIESCENT {
		status, _ = warmBootStateToStringMap[state]
	}

	info := WarmStartPerfDbInfo{
		key:       wrh.warmstart.appName,
		timestamp: getTimeString(),
		status:    status,
		isStart:   false,
	}
	wrh.updateWarmBootStagePerformance(stage, info)
}

func (wrh *WarmRestartHelper) updateAppWarmBootStageEndOnFailure(stage WarmStartStage) {
	if wrh == nil {
		return
	}
	info := WarmStartPerfDbInfo{
		key:       wrh.warmstart.appName,
		timestamp: getTimeString(),
		status:    "failure",
		isStart:   false,
	}
	wrh.updateWarmBootStagePerformance(stage, info)
}
