#!/bin/bash

# To give the rsyslog a chance to fully start:
sleep 2

echo "**** Starting telemetry"
/workspace/telemetry  -v=1 -insecure
echo "**** Finished telemetry"
