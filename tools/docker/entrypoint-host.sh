#!/bin/bash
mv /rsyslog.host /etc/rsyslog.conf
service rsyslog start

service redis-server start

service docker start

# ls -al /

ntpd

mkdir -p /workspace/keys
cp /testdata/mtls/gold/server_1* /workspace/keys
cp /testdata/mtls/gold/ca* /workspace/keys
cp /testdata/mtls/client_1* /workspace/keys

mkdir -p /var/log/tmp/telemetry-con

echo "alias telemetry_bash='docker exec -it telemetry-con /bin/bash'" >> /root/.bashrc

docker load --input /telemetry_bundle.tar
docker images
echo "**** Starting telemetry-con"
docker run \
    --network host \
    --name telemetry-con \
    --rm \
    -v /var/run/redis:/var/run/redis \
    -v /workspace/keys:/keys \
    -v /var/log/tmp/telemetry-con:/var/log \
    bazel/telemetry_image:latest
echo "**** Finished telemetry-con"
