#!/bin/bash

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

export OPTS=""
[ -z ${TEST_PATTERN} ] && echo "==== Executing all tests" || export OPTS="${OPTS} -test.run ${TEST_PATTERN}"
export LVL="2"
[ -z ${TEST_PATTERN} ] || export LVL="3"
env

mkdir -p /workspace/run_dir/
mv /test /workspace/test/
cd /workspace/run_dir

echo "**** Starting gnmi_server_test"
mv /gnmi_server_test .
dlv exec --listen=:2345 --headless=true --accept-multiclient --api-version=2 --check-go-version=false ./gnmi_server_test -- -test.timeout=8h -test.v -v=${LVL} -logtostdout -alsologtostderr ${OPTS}
[ $? -ne 0 ] &&  exit -1
echo "**** Finished gnmi_server_test"
