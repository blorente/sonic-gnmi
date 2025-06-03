#!/bin/bash

execute_test() {
    local test_name=$1
    echo "**** Starting ${test_name}"
    if ! [ -f "./${test_name}" ]; then
        mv "/${test_name}" .
    fi
    if [ $LVL -gt 3 ]; then
      redis-cli MONITOR &
    fi
    TEST_OPTS="-test.coverprofile=/mnt/artifacts/${test_name}.lcov -test.timeout=45m -test.v -v=${LVL} -logtostdout -alsologtostderr ${OPTS}"
    echo Command: ./${test_name} ${TEST_OPTS}
    time "./${test_name}" ${TEST_OPTS}
    [ $? -ne 0 ] && exit -1
    echo "**** Finished ${test_name}"
    if [ $LVL -gt 3 ]; then
      killall redis-cli
    fi
}

execute_benchmark() {
    local test_name=$1
    echo "**** Starting benchmark ${test_name}"
    if ! [ -f "./${test_name}" ]; then
        mv "/${test_name}" .
    fi
    # time "./${test_name}" -test.bench=. -test.coverprofile="/mnt/artifacts/${test_name}-bench.lcov" -test.timeout=30m  -test.v -v=0 -logtostdout -alsologtostderr -test.run='^$'
    time "./${test_name}" -test.bench=. -test.timeout=30m  -test.v -v=0 -test.run='^$'
    [ $? -ne 0 ] &&  exit -1
    echo "**** Finished benchmark ${test_name}"
}

execute_telemetery() {
  # Poke the ApplDB to simulate swss signaling port initialization complete
  echo "Preparing DB"
  redis-cli -n 0 HSET "PORT_TABLE:PortInitDone" "done" "true"
  echo "Starting telemetry"
  # Run the telemetry binary, use a variety of command line flags as a spot
  # check that flag parsing is working.  The specific flags and values are
  # not terribly important except for a few like port and insecure.
  /telemetry --port 9339 \
             --insecure --noTLS --client_auth none \
             --gnmi_translib_write --gnmi_native_write \
             --with-save-on-set=true \
             -v=2 -logtostdout -logfirstn=2 \
             --with-master-arbitration=true \
             --cache_responses=true \
             --authz_policy_enabled=false --authorization_policy_file=/keys/authorization_policy.json \
             --gnmi_pathz_enabled=false --gnmi_pathz_file=/keys/pathz_policy.pb.txt  \
             --console_meta=/keys/console-version.json \
             --with-repl-xfmr &
  echo "Telemetry Started"
  # Give it a little time to start
  sleep 10
  echo "Sleep done"
  jobs
  echo "Trying get"
  # Make sure a simple get works
  /gnmi_get  --notls -target_addr localhost:9339 -xpath /openconfig-optical-switch:optical-switch/port-statuses
  STS=$?
  echo "Status is ${STS}"
  if [ ${STS} -ne 0 ]; then
    # When running in kokoro we see telemetry taking a bit longer to start up
    # sometimes.  So, wait a bit longer and try again.
    sleep 10
    jobs
    /gnmi_get  --notls -target_addr localhost:9339 -xpath /openconfig-optical-switch:optical-switch/port-statuses
    STS=$?
    echo "Status after retry is ${STS}"
    [ ${STS} -ne 0 ] &&  exit -1
  fi
  # Shut down the telemetry server
  echo "Stopping telemetry"
  killall telemetry
  echo "Stopped telemetry"
}

# Some versions of containerd set an "unlimited" cap on the number of open
# files.  Some versions of rsyslog use this cap as a loop limit during init
# and the "unlimited" value results in a very large number preventing rsyslog
# from starting in a reasonable amount of time.  We are setting the limit here
# to something reasonable to avoid this.
# https://github.com/rsyslog/rsyslog/issues/5158
ulimit -n 10240
ulimit -a

service rsyslog start
service redis-server start
ip link add bond0 type dummy
ip link add Loopback4 type dummy
ntpd

env
# Load content to the DBs.
python3 /redis_load.py

mkdir -p /workspace/run_dir/
mv /test /workspace/test/
cd /workspace/run_dir

# TestGNMINative:
#  - Fixed so far:
#    - pytest installed, test scripts added to docker image
#    - workdir set to non-root (needed for pytest to find test scripts)
#  - Additional issues need investigation:
#    - DB load failing due to validation rules, gomonkey used to bypass
#      validation but code seems to go end up going down a different
#      path.
# TestClient:
#  - Needs investigation
OPTS="-test.skip TestGNMINative|TestClient"

DFLT_LOG_LVL=2

ALL_TESTS=(gnmi_server_test cs_test db_test transformer_test sonic_db_config_test sonic_data_client_test transl_utils_test translib_test log_test tlerr_test ocbinds_test path_test pathtransl_test common_utils_test metric_recorder_test pathz_authorizer_test platform_test)
TEST_LIST=()
BENCHMARK_LIST=()

if [ ! -z ${TEST_SUITE} ]; then
  # A specific test suite was requested on the command line, run only that
  # suite eith a more verbose log level.  Make sure it is a valid suite though.
  if [[ ! " ${ALL_TESTS[*]} " =~ [[:space:]]${TEST_SUITE}[[:space:]] ]]; then
    echo "Requested test suite \"${TEST_SUITE}\" is not a valid suite."
    echo "Options: ${ALL_TESTS[*]}"
    exit 1
  fi
  TEST_LIST+=(${TEST_SUITE})
  DFLT_LOG_LVL=3
elif [ ! -z ${TEST_PATTERN} ]; then
  # A specific test was requested on the command line (TEST_PATTERN is set) but
  # no test suite was requested (TEST_SUITE is empty).  Assume it is in
  # gnmi_server_test and run only that suite with a more verbose log level.
  TEST_LIST+=(gnmi_server_test)
  DFLT_LOG_LVL=3
else
  # No specific test suite or test case were specified, run everything.
  for t in gnmi_server_test cs_test db_test transformer_test sonic_db_config_test sonic_data_client_test transl_utils_test translib_test log_test tlerr_test ocbinds_test path_test pathtransl_test common_utils_test metric_recorder_test pathz_authorizer_test platform_test; do
    TEST_LIST+=($t)
  done
  BENCHMARK_LIST+=(gnmi_server_test)
fi

if [ ! -z ${TEST_PATTERN} ]; then
  # A test case (or test case pattern) was requested, add it to the options
  OPTS="${OPTS} -test.run ${TEST_PATTERN}"
fi

echo "==== Executing tests: ${TEST_LIST[*]}"

if [ -z ${LVL} ]; then
  # No explicit logging level was provided, use the default level.
  LVL=${DFLT_LOG_LVL}
fi

# Run the tests.
for t in ${TEST_LIST[*]}; do
    execute_test "${t}"
done
# Failures seen in dialout_client_test, skipping for now.
#  - Missing notificaiton for Ethernet1/1 counters?
echo "**** Skipping dialout_client_test until failures are solved"
mkdir -p /workspace/dialout
pushd /workspace/dialout
mv /dialout_client_test .
#time ./dialout_client_test -test.coverprofile="/mnt/artifacts/dialout_client_test.lcov" -test.timeout=30m  -test.v -v=${LVL} -logtostdout -alsologtostderr ${OPTS}
[ $? -ne 0 ] &&  exit -1
popd

# Run the benchmarks.
for b in ${BENCHMARK_LIST}; do
    execute_benchmark "${b}"
done

# Run the telemetry binary and do a simple gNMI request to ensure it has basic
# functionality.
if [ -z ${TEST_SUITE} ] && [ -z ${TEST_PATTERN} ]; then
    execute_telemetery
fi
