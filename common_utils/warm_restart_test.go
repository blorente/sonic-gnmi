package common_utils

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
)

type expectedDBInfo struct {
	// Performance table key.
	key string
	// status field.
	status string
	// Is start timestamp present.
	start bool
	// Is finish timestamp present.
	finish bool
}

func TestStringToWarmStartState(t *testing.T) {
	arr := [11]WarmStartState{INITIALIZED, RESTORED, REPLAYED, RECONCILED,
		WSDISABLED, WSUNKNOWN, FROZEN, QUIESCENT, CHECKPOINTED,
		COMPLETED, FAILED}
	for _, state := range arr {
		stateStr := state.String()

		newState, err := StringToWarmStartState(stateStr)

		if state != newState || err != nil {
			t.Fatalf("State and newState didn't match")
		}
	}

	newState, err := StringToWarmStartState("undefined-state")
	expectEqual(t, newState, WSUNKNOWN)
	if err == nil {
		t.Fatalf("State and newState didn't match")
	}
}

func TestStringToWarmBootNotification(t *testing.T) {
	arr := [3]WarmBootNotification{Freeze, Unfreeze, Checkpoint}
	for _, notification := range arr {
		notificationStr := notification.String()

		newNotification, _ := StringToWarmBootNotification(notificationStr)

		if notification != newNotification {
			t.Fatalf("Notification and rev_notification didn't match")
		}
	}

	_, err := StringToWarmBootNotification("undefined-notification")
	if err == nil {
		t.Fatalf("Failed to return error on undefined notification")
	}
}

func readKey(t *testing.T, table string, key string) string {
	t.Helper()
	dbc, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get redis DB client: %v\n", err)
	}
	defer db.CloseRedisClient(dbc)

	value, err := dbc.HGet(context.Background(), table, key).Result()
	if err != nil {
		t.Fatalf("Failed to read table = %s key =%s\n", table, key)
	}
	return value
}
func setKey(t *testing.T, table string, key string, value string) {
	t.Helper()
	dbc, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get redis DB client: %v\n", err)
	}
	defer db.CloseRedisClient(dbc)

	err = dbc.HSet(context.Background(), table, key, value).Err()
	if err != nil {
		t.Fatalf("Failed to set table = %s key =%s value = %s err = %v\n", table, key, value, err)
	}
}

func getKeys(t *testing.T, table string) []string {
	t.Helper()
	dbc, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get redis DB client: %v\n", err)
	}
	defer db.CloseRedisClient(dbc)

	keys, err := dbc.Keys(context.Background(), table).Result()
	if err != nil {
		t.Fatalf("Failed to get keys: %v", err)
	}
	return keys
}

func delTable(t *testing.T, table string) {
	t.Helper()
	dbc, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get redis DB client: %v\n", err)
	}
	defer db.CloseRedisClient(dbc)

	err = dbc.Del(context.Background(), table).Err()
	if err != nil {
		t.Fatalf("Failed to delete table %s, err=%v", table, err)
	}
}

func configureStateVerification(t *testing.T, attribute string, value string) {
	t.Helper()
	dbc := db.TransactionalRedisClient(db.ConfigDB)
	if dbc == nil {
		t.Fatalf("Failed to create redis client: %v", dbc)
		return
	}
	defer db.CloseRedisClient(dbc)

	if err := dbc.HSet(context.Background(), "WARM_RESTART|system", attribute, value).Err(); err != nil {
		t.Fatalf("Failed to set state verification: %v", err)
	}
}

func validatePerfEntry(t *testing.T, expected expectedDBInfo) {
	t.Helper()
	dbc, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get redis DB client: %v\n", err)
	}
	defer db.CloseRedisClient(dbc)
	var value string
	key := expected.key

	value, _ = dbc.HGet(context.Background(), "WARM_RESTART_PERFORMANCE_TABLE|"+key, "start-timestamp").Result()
	if expected.start && value == "" {
		t.Fatalf("Expected start-timestamp, got: %v", value)
	} else if !expected.start && value != "" {
		t.Fatalf("Did not expect start-timestamp, got: %v", value)
	}

	value, _ = dbc.HGet(context.Background(), "WARM_RESTART_PERFORMANCE_TABLE|"+key, "finish-timestamp").Result()
	if expected.finish && value == "" {
		t.Fatalf("Expected finish-timestamp, got: %v", value)
	} else if !expected.finish && value != "" {
		t.Fatalf("Did not expect finish-timestamp, got: %v", value)
	}

	value, _ = dbc.HGet(context.Background(), "WARM_RESTART_PERFORMANCE_TABLE|"+key, "status").Result()
	expectEqual(t, value, expected.status)
}

func TestInitialize(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	err := wrh.Initialize("", "testdocker")
	if err == nil {
		t.Fatalf("expected nil from initialize")
	}

	err = wrh.Initialize("testApp", "")
	if err == nil {
		t.Fatalf("expected nil from initialize")
	}

	err = wrh.Initialize("testApp", "testDocker")
	if err != nil {
		t.Fatalf("expected successful initialize")
	}
	expectEqual(t, wrh.warmstart.appName, "testApp")
	expectEqual(t, wrh.warmstart.dockerName, "testDocker")
	expectEqual(t, wrh.warmstart.enabled, false)
	expectEqual(t, wrh.warmstart.systemWarmRebootEnabled, false)
}

func TestRegisterWarmBootInfo(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")
	wrh.RegisterWarmBootInfo(true, true, true, false)

	table := "WARM_RESTART_REGISTRATION_TABLE|testDocker|testApp"
	value := readKey(t, table, "freeze")
	expectEqual(t, value, "true")

	value = readKey(t, table, "checkpoint")
	expectEqual(t, value, "true")

	value = readKey(t, table, "reconciliation")
	expectEqual(t, value, "true")

	wrh.RegisterWarmBootInfo(false, false, true, false)
	value = readKey(t, table, "freeze")
	expectEqual(t, value, "false")

	value = readKey(t, table, "checkpoint")
	expectEqual(t, value, "false")

	wrh.RegisterWarmBootInfo(true, true, false, false)
	value = readKey(t, table, "reconciliation")
	expectEqual(t, value, "false")

	wrh.RegisterWarmBootInfo(false, false, false, true)
	value = readKey(t, table, "stop_on_freeze")
	expectEqual(t, value, "true")

	wrh.RegisterWarmBootInfo(true, false, false, false)
	value = readKey(t, table, "stop_on_freeze")
	expectEqual(t, value, "false")

	// Error case - waitForFreeze and stopOnFreeze cannot both be true
	err := wrh.RegisterWarmBootInfo(true, false, false, true)
	if err == nil {
		t.Fatalf("Expected an error, got: %v", err)
	}

	// Error case - waitForCheckpoint and stopOnFreeze cannot both be true
	err = wrh.RegisterWarmBootInfo(false, true, false, true)
	if err == nil {
		t.Fatalf("Expected an error, got: %v", err)
	}

	// readKey will fatally fail if unable to read key
	// just checking that timestamp exists
	readKey(t, table, "timestamp")
}

func TestReadWriteRestoreCount(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	writeRestoreCount(25, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	count := readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, count, "25")

	writeRestoreCount(37, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	count = readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, count, "37")

	table := "WARM_RESTART_TABLE|testApp"
	value := readKey(t, table, "restore_count")
	expectEqual(t, value, "37")
}

func TestCheckAndUpdateRestoreCount(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	table := "WARM_RESTART_TABLE|testApp"
	setKey(t, table, "restore_count", "")

	count, ret := wrh.checkAndUpdateRestoreCount(false, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, ret, false)
	expectEqual(t, count, uint64(0))

	countStr := readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, countStr, "0")

	setKey(t, table, "restore_count", "abcd")

	count, ret = wrh.checkAndUpdateRestoreCount(false, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, ret, false)
	expectEqual(t, count, uint64(0))

	countStr = readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, countStr, "0")

	setKey(t, table, "restore_count", "17")
	count, ret = wrh.checkAndUpdateRestoreCount(false, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, ret, true)
	expectEqual(t, count, uint64(17))

	setKey(t, table, "restore_count", "20")
	count, ret = wrh.checkAndUpdateRestoreCount(true, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, ret, true)
	expectEqual(t, count, uint64(21))

	countStr = readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)
	expectEqual(t, countStr, "21")
}

func TestCheckWarmStart(t *testing.T) {
	dbc, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get redis DB client: %v", err)
	}
	defer db.CloseRedisClient(dbc)

	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	systemTable := "WARM_RESTART_ENABLE_TABLE|system"
	dockerTable := "WARM_RESTART_ENABLE_TABLE|testDocker"

	// systemTable and dockerTable don't exist
	if err = dbc.Del(context.Background(), systemTable).Err(); err != nil {
		t.Fatalf("Failed to clear warm restart enable table in DB: %v\n", err)
	}

	if err = dbc.Del(context.Background(), dockerTable).Err(); err != nil {
		t.Fatalf("Failed to clear warm restart enable table in DB: %v\n", err)
	}

	ret := wrh.CheckWarmStart(false)
	expectEqual(t, false, ret)
	expectEqual(t, false, wrh.IsWarmStart())
	expectEqual(t, false, wrh.IsSystemWarmRebootEnabled())
	expectEqual(t, "0", readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName))
	expectEqual(t, RECONCILED, wrh.GetWarmStartState("testApp"))

	// Case 1: System False, DockerTable False
	setKey(t, systemTable, "enable", "false")
	setKey(t, dockerTable, "enable", "false")

	ret = wrh.CheckWarmStart(false)
	expectEqual(t, false, ret)
	expectEqual(t, false, wrh.IsWarmStart())
	expectEqual(t, false, wrh.IsSystemWarmRebootEnabled())
	expectEqual(t, "0", readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName))
	expectEqual(t, RECONCILED, wrh.GetWarmStartState("testApp"))

	// Case 2: System False, DockerTable True
	setKey(t, systemTable, "enable", "false")
	setKey(t, dockerTable, "enable", "true")
	writeRestoreCount(5, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)

	ret = wrh.CheckWarmStart(false)
	expectEqual(t, true, ret)
	expectEqual(t, true, wrh.IsWarmStart())
	expectEqual(t, false, wrh.IsSystemWarmRebootEnabled())
	expectEqual(t, "5", readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName))

	ret = wrh.CheckWarmStart(true)
	expectEqual(t, "6", readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName))

	// Case 3: System True, DockerTable False
	setKey(t, systemTable, "enable", "true")
	setKey(t, dockerTable, "enable", "false")
	writeRestoreCount(8, wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName)

	ret = wrh.CheckWarmStart(true)
	expectEqual(t, true, ret)
	expectEqual(t, true, wrh.IsWarmStart())
	expectEqual(t, true, wrh.IsSystemWarmRebootEnabled())
	expectEqual(t, "9", readRestoreCount(wrh.warmstart.stateDb, wrh.warmstart.restartAppTableName))

	// Case 4: System True, AppTable True
	// This is the same as case3: system overrides
}

func TestIsWarmStart(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	wrh.setWarmStartEnabled(false)
	expectEqual(t, false, wrh.IsWarmStart())
	wrh.setWarmStartEnabled(true)
	expectEqual(t, true, wrh.IsWarmStart())
}

func TestIsSystemWarmRebootEnabled(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	wrh.setSystemWarmRebootEnabled(false)
	expectEqual(t, false, wrh.IsSystemWarmRebootEnabled())
	wrh.setSystemWarmRebootEnabled(true)
	expectEqual(t, true, wrh.IsSystemWarmRebootEnabled())
}

func TestIsNSFOngoing(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")
	systemTable := "WARM_RESTART_ENABLE_TABLE|system"

	setKey(t, systemTable, "enable", "false")
	expectEqual(t, false, wrh.IsNSFOngoing())
	setKey(t, systemTable, "enable", "true")
	expectEqual(t, true, wrh.IsNSFOngoing())
}

func TestGetSetWarmStartState(t *testing.T) {
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	table := "WARM_RESTART_TABLE|testApp"
	prevTimestamp := time.Now()
	// Sleep to avoid failing the timestamp comparison due to timestamp precision.
	time.Sleep(time.Millisecond)
	wrh.SetWarmStartState(INITIALIZED)
	value := readKey(t, table, "state")
	expectEqual(t, value, "initialized")
	state := wrh.GetWarmStartState("testApp")
	expectEqual(t, state, INITIALIZED)
	curTimestamp, _ := time.Parse("2006-01-02.15:04:05.999999", readKey(t, table, "timestamp"))
	if !curTimestamp.After(prevTimestamp) {
		t.Fatalf("Invalid timestamp: prev=%v, cur=%v", prevTimestamp, curTimestamp)
	}

	prevTimestamp = time.Now()
	// Sleep to avoid failing the timestamp comparison due to timestamp precision.
	time.Sleep(time.Millisecond)
	wrh.SetWarmStartState(FAILED)
	value = readKey(t, table, "state")
	expectEqual(t, value, "failed")
	state = wrh.GetWarmStartState("testApp")
	expectEqual(t, state, FAILED)
	curTimestamp, _ = time.Parse("2006-01-02.15:04:05.999999", readKey(t, table, "timestamp"))
	if !curTimestamp.After(prevTimestamp) {
		t.Fatalf("Invalid timestamp: prev=%v, cur=%v", prevTimestamp, curTimestamp)
	}

	table = "WARM_RESTART_TABLE|secondTestApp"
	setKey(t, table, "state", "restored")
	state = wrh.GetWarmStartState("secondTestApp")
	expectEqual(t, state, RESTORED)

	setKey(t, table, "state", "frozen")
	state = wrh.GetWarmStartState("secondTestApp")
	expectEqual(t, state, FROZEN)
}

func TestOptionalStateVerification(t *testing.T) {
	delTable(t, "WARM_RESTART_ENABLE_TABLE|system")
	delTable(t, "WARM_RESTART_ENABLE_TABLE|telemetry")
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")

	// Perform checkWarmStart for TestApp running in TestDocker.
	expectEqual(t, wrh.CheckWarmStart(true), false)

	// State verification is disabled by default.
	expectEqual(t, wrh.isStateVerificationShutdownEnabled(), false)
	expectEqual(t, wrh.isStateVerificationBootupEnabled(), false)
	expectEqual(t, wrh.WaitForUnfreeze(), false)
	// Since state verification is disabled by default, warmboot state should be RECONCILED.
	expectEqual(t, wrh.GetWarmStartState("testApp"), RECONCILED)

	// Disable system level warm restart. Verify that checkWarmStart() still
	// updates warmboot state to RECONCILED since state verification is disabled
	// by default.
	setKey(t, "WARM_RESTART_ENABLE_TABLE|system", "enable", "false")
	expectEqual(t, wrh.isStateVerificationShutdownEnabled(), false)
	expectEqual(t, wrh.isStateVerificationBootupEnabled(), false)
	expectEqual(t, wrh.WaitForUnfreeze(), false)
	expectEqual(t, wrh.CheckWarmStart(true), false)
	expectEqual(t, wrh.GetWarmStartState("testApp"), RECONCILED)

	// Enable state verification during shutdown but disable warm restart. Verify that
	// checkWarmStart() updates warmboot state to RECONCILED since state
	// verification during bootup is disabled.
	configureStateVerification(t, "state_verification_shutdown", "true")
	expectEqual(t, wrh.isStateVerificationShutdownEnabled(), true)
	expectEqual(t, wrh.isStateVerificationBootupEnabled(), false)
	expectEqual(t, wrh.WaitForUnfreeze(), false)
	expectEqual(t, wrh.CheckWarmStart(true), false)
	expectEqual(t, wrh.GetWarmStartState("testApp"), RECONCILED)

	// Enable state verification during bootup but disable warm restart. Verify that
	// checkWarmStart() updates warmboot state to COMPLETED since state
	// verification during bootup is enabled.
	configureStateVerification(t, "state_verification_bootup", "true")
	expectEqual(t, wrh.isStateVerificationShutdownEnabled(), true)
	expectEqual(t, wrh.isStateVerificationBootupEnabled(), true)
	expectEqual(t, wrh.WaitForUnfreeze(), true)
	expectEqual(t, wrh.CheckWarmStart(true), false)
	expectEqual(t, wrh.GetWarmStartState("testApp"), COMPLETED)

	// Set warmboot state and enable system level warm restart. Verify that
	// warmboot state reflects the configured state and doesn't get updated by
	// checkWarmStart().
	wrh.SetWarmStartState(INITIALIZED)
	setKey(t, "WARM_RESTART_ENABLE_TABLE|system", "enable", "true")
	expectEqual(t, wrh.CheckWarmStart(true), true)
	expectEqual(t, wrh.GetWarmStartState("testApp"), INITIALIZED)

	// Disable state verification.
	configureStateVerification(t, "state_verification_shutdown", "false")
	configureStateVerification(t, "state_verification_bootup", "false")
	expectEqual(t, wrh.isStateVerificationShutdownEnabled(), false)
	expectEqual(t, wrh.isStateVerificationBootupEnabled(), false)
	expectEqual(t, wrh.WaitForUnfreeze(), false)
}

func TestWaitForReconciliation(t *testing.T) {
	setKey(t, "WARM_RESTART_TABLE|testApp", "state", "reconciled")
	setKey(t, "WARM_RESTART_TABLE|teammgrd", "state", "initialized")
	setKey(t, "WARM_RESTART_TABLE|p4rt", "state", "initialized")
	setKey(t, "WARM_RESTART_TABLE|orchagent", "state", "initialized")
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")
	wrh.SetFreezeStatus(true)
	go wrh.WaitForReconciliation()

	// Make sure the server is in freeze mode and in reconciled state
	expectEqual(t, wrh.FetchFreezeStatus(), true)
	expectEqual(t, wrh.GetWarmStartState("testApp"), RECONCILED)

	// Make sure server is still in freeze mode if one app isn't reconciled yet
	setKey(t, "WARM_RESTART_TABLE|teammgrd", "state", "reconciled")
	setKey(t, "WARM_RESTART_TABLE|p4rt", "state", "reconciled")
	time.Sleep(4 * time.Second)
	expectEqual(t, wrh.FetchFreezeStatus(), true)
	expectEqual(t, wrh.GetWarmStartState("testApp"), RECONCILED)

	// Make sure server is unfrozen when all apps are reconciled
	setKey(t, "WARM_RESTART_TABLE|orchagent", "state", "reconciled")
	time.Sleep(4 * time.Second)
	expectEqual(t, wrh.FetchFreezeStatus(), false)
	expectEqual(t, wrh.GetWarmStartState("testApp"), RECONCILED)
}

func TestWarmStartPerformanceApis(t *testing.T) {
	// Clean up warm restart state for testAppName and warm restart config for
	// testDockerName
	delTable(t, "WARM_RESTART_TABLE|testApp")
	delTable(t, "WARM_RESTART_ENABLE_TABLE|system")
	delTable(t, "WARM_RESTART_ENABLE_TABLE|testDocker")

	// Cleanup performance tables.
	keys := getKeys(t, "WARM_RESTART_PERFORMANCE_TABLE|*")
	for _, key := range keys {
		delTable(t, key)
	}

	// Initialize WarmStart class for TestApp
	wrh, _ := NewWarmRestartHelper(nil)
	wrh.Initialize("testApp", "testDocker")
	// Perform checkWarmStart for TestApp running in TestDocker.
	expectEqual(t, wrh.CheckWarmStart(true), false)
	// Register for warm-boot notifications.
	wrh.RegisterWarmBootInfo(true, true, true, false)

	// Performance entries should not be present during cold boot.
	keys = getKeys(t, "WARM_RESTART_PERFORMANCE_TABLE|*")
	expectEqual(t, len(keys), 0)

	// Enable warm-boot.
	setKey(t, "WARM_RESTART_ENABLE_TABLE|system", "enable", "true")
	expectEqual(t, wrh.CheckWarmStart(true), true)
	// 1 performance entry should be present during warm bootup that indicates
	// the start of reconciliation.
	keys = getKeys(t, "WARM_RESTART_PERFORMANCE_TABLE|*")
	expectEqual(t, len(keys), 1)
	// Cleanup performance tables.
	for _, key := range keys {
		delTable(t, key)
	}

	/****************************************************************
	 * Test for application warm-boot stage performance update APIs.
	 ***************************************************************/
	var expectedInfo expectedDBInfo
	for stage, name := range warmBootStageToStringMap {
		key := name + "|testApp"

		// Key shouldn't be present for this stage until the performance APIs
		// are called.
		keys = getKeys(t, "WARM_RESTART_PERFORMANCE_TABLE|*")
		expectEqual(t, len(keys), 0)

		wrh.UpdateAppWarmBootStageStart(stage)
		expectedInfo = expectedDBInfo{
			key:    key,
			status: "",
			start:  true,
			finish: false,
		}
		validatePerfEntry(t, expectedInfo)

		// updateAppWarmBootStageEndOnFailure() should update finish time and
		// status as failure.
		wrh.updateAppWarmBootStageEndOnFailure(stage)
		expectedInfo = expectedDBInfo{
			key:    key,
			status: "failure",
			start:  true,
			finish: true,
		}
		validatePerfEntry(t, expectedInfo)
		delTable(t, "WARM_RESTART_PERFORMANCE_TABLE|"+key)
	}

	// List of stage and state that will be used as input in the performance
	// APIs along with the expected status.
	inputList := []struct {
		stage  WarmStartStage
		state  WarmStartState
		status string
	}{
		{
			stage:  STAGE_FREEZE,
			state:  FROZEN,
			status: "frozen",
		},
		{
			stage:  STAGE_FREEZE,
			state:  QUIESCENT,
			status: "quiescent",
		},
		{
			stage:  STAGE_CHECKPOINT,
			state:  CHECKPOINTED,
			status: "success",
		},
		{
			stage:  STAGE_RECONCILIATION,
			state:  RECONCILED,
			status: "success",
		},
		{
			stage:  STAGE_UNFREEZE,
			state:  COMPLETED,
			status: "success",
		},
	}
	for _, input := range inputList {
		key := warmBootStageToStringMap[input.stage] + "|testApp"

		wrh.UpdateAppWarmBootStageStart(input.stage)
		wrh.SetWarmStartState(input.state)
		expectedInfo = expectedDBInfo{
			key:    key,
			status: input.status,
			start:  true,
			finish: true,
		}
		validatePerfEntry(t, expectedInfo)
		delTable(t, "WARM_RESTART_PERFORMANCE_TABLE|"+key)
	}
}
