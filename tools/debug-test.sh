#!/bin/bash

# To execute only a specific test (or a group of tests) call this script with
# the pattern defining the test to be executed.
# For example:
# $ tools/run-tests.sh TestGnmiGetSet/Set_and_Get_System_ntp
GOLANG_VERSION="1.22.4"
TEST_PATTERN=""
[ -z $1 ] && echo "==== Executing all tests" || export TEST_PATTERN=$1

gbazelisk run -c dbg --config=remote --collect_code_coverage //gnmi_server:gnmi_server_debug_image \
  &&  docker run  --cap-add=NET_ADMIN --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
                  --env GOLANG_Ver=${GOLANG_VERSION} \
                  --env TEST_PATTERN="${TEST_PATTERN}" \
                  -p 127.0.0.1:2345:2345 -p 127.0.0.1:6479:6379 \
                  --sysctl net.ipv6.conf.all.disable_ipv6=0 \
                  --sysctl net.ipv6.conf.default.disable_ipv6=0 \
                  --sysctl net.ipv6.conf.lo.disable_ipv6=0 \
                  --detach \
                  --name gnmi_server_debug --rm -t bazel/gnmi_server:gnmi_server_debug_image \
  && echo "Sleeping 10s to allow for Delve initialization" \
  && sleep 10 \
  && echo "Done sleeping. Connecting to :2345"
