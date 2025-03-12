#!/bin/sh

cp ../../sonic-swss-common/goext/swsscommon.i .
swig -go -cgo -c++ -intgosize 64 -I../../sonic-swss-common/common/ ./swsscommon.i
