#!/bin/bash -x
LOG_FILE="build.bazel.log"
rm -rf artifacts/
mkdir artifacts

# Check if the bazel remote execution container is present.  If it isn't
# present, build it.
`docker image ls | grep -q "sonic-rbe-umf-sl * latest"`
STS=$?
if [ ${STS} -ne 0 ]; then
  echo "Build container not present, creating..."
  TOOLS_DIR=$(dirname ${BASH_SOURCE[0]})
  docker build -t sonic-rbe-umf-sl:latest -f ${TOOLS_DIR}/bazel/Dockerfile .
fi

# To execute only a specific test (or a group of tests) call this script with
# the pattern defining the test to be executed.
# For example:
# $ tools/run-tests.sh TestGnmiGetSet/Set_and_Get_System_ntp
TEST_PATTERN=""
[ -z $1 ] && echo "==== Executing all tests" || export TEST_PATTERN=$1

# Allow a single json file to be executed rather than all for the json based
# test.  An environment variable is used to request this.
JSON_TGT=${JSON_TGT}

# Kokoro builds use bazelisk while local builds use gbazelisk
BAZELISK=bazelisk
if [[ "${KOKORO_JOB}" == "" ]]; then
  BAZELISK=gbazelisk
fi

set -e
set -o pipefail

${BAZELISK} run --config=remote --collect_code_coverage --verbose_failures //gnmi_server:gnmi_server_test_image --linkopt=-lusb-1.0 \
&& docker run  --cap-add=NET_ADMIN --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
               --env TEST_PATTERN="${TEST_PATTERN}" \
               --env LVL=${LVL} \
               --env JSON_TGT=${JSON_TGT} \
               --env TEST_SUITE=${TEST_SUITE} \
               --env KOKORO_JOB=${KOKORO_JOB} \
               --sysctl net.ipv6.conf.all.disable_ipv6=0 \
               --sysctl net.ipv6.conf.default.disable_ipv6=0 \
               --sysctl net.ipv6.conf.lo.disable_ipv6=0 \
               --name gnmi_server_test --rm -i -v ${PWD}/artifacts:/mnt/artifacts \
               bazel/gnmi_server:gnmi_server_test_image 2>&1 | tee "${LOG_FILE}"

# Modify and then uncomment this section to cleanup unused images.
# UNUSED=$(docker images | grep -E '<none> +<none>'  |  awk '{ print $3}')
# [ ! -z "${UNUSED}" ] && docker rmi ${UNUSED} >> "${LOG_FILE}"

echo "Done"  | tee -a "${LOG_FILE}"
