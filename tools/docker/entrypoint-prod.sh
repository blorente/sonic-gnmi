#!/bin/bash
ntpd

export CVL_SCHEMA_PATH=/usr/sbin/schema
export CVL_CFG_FILE=/usr/sbin/cvl_cfg.json
export DB_CONFIG_PATH=/var/run/redis/sonic-db/database_config.json
export YANG_MODELS_PATH=/usr/models/yang

# ls -al

mv rsyslog.conf /etc/rsyslog.conf
mv supervisor.conf /etc/rsyslog.d/
export docker_container_name="telemetry"
[ -f /etc/rsyslog.conf ] && sed -ri "s/%syslogtag%/$docker_container_name#%syslogtag%/;" /etc/rsyslog.conf \
    && sed -ri 's/^\*\.\*/# *.*/;' /etc/rsyslog.conf \
    && echo 'input(type="imfile" File="/var/log/telemetry_err.log" Tag="telemetry" Severity="emerg" Facility="user" PersistStateInterval="1" ruleset="send_to_platform")' >> /etc/rsyslog.conf \
    && echo 'input(type="imfile" File="/var/log/telemetry.log" Tag="telemetry" Severity="info" Facility="user" PersistStateInterval="1" ruleset="send_back")' >> /etc/rsyslog.conf \
    && echo 'module(load="imudp")' >> /etc/rsyslog.conf \
    && echo 'input(type="imudp" port="515" ratelimit.interval="300" ratelimit.burst="800" ruleset="send_to_platform")' >> /etc/rsyslog.conf \
    && echo 'input(type="imuxsock" ruleset="send_to_platform" socket="/dev/log")' >> /etc/rsyslog.conf \
    && echo 'template (name="transit" type="string" string="<%PRI%>%TIMESTAMP:::date-rfc3339% %HOSTNAME% %syslogtag%%msg:::sp-if-no-1st-sp%%msg%")' >> /etc/rsyslog.conf \
    && echo 'ruleset(name="send_to_platform") {' >> /etc/rsyslog.conf \
    && echo '  *.* action(type="omfwd" Target="127.0.0.1" Port="514" protocol="udp" Template="ForwardFormatInContainer")' >> /etc/rsyslog.conf \
    && echo '  stop' >> /etc/rsyslog.conf \
    && echo '}' >> /etc/rsyslog.conf \
    && echo 'ruleset(name="send_back") {' >> /etc/rsyslog.conf \
    && echo '  *.* action(type="omfwd" target="127.0.0.1" port="515" protocol="udp" Template="transit")' >> /etc/rsyslog.conf \
    && echo '  stop' >> /etc/rsyslog.conf \
    && echo '}' >> /etc/rsyslog.conf
cat /etc/rsyslog.conf

mv /database_config.json /var/run/redis/sonic-db/database_config.json
mv /start.sh /usr/bin/start.sh && chmod +x /usr/bin/start.sh
mv /telemetry.sh /usr/bin/telemetry.sh && chmod +x /usr/bin/telemetry.sh

# TODO Remove next line when the problem with importing `swsssdk`` is solved.
sed -ri 's/import swsssdk/# import swsssdk/;' /supervisor-proc-exit-listener
mv /supervisor-proc-exit-listener /usr/bin/supervisor-proc-exit-listener  && chmod +x /usr/bin/supervisor-proc-exit-listener

mkdir -p /etc/supervisor/conf.d
mv /supervisord.conf /etc/supervisor/conf.d/supervisord.conf
mv /critical_processes /etc/supervisor/critical_processes
mkdir -p /usr/share/sonic/templates/ && mv telemetry_vars.j2 /usr/share/sonic/templates/

chmod a+x /sonic-config-engine/sonic-cfggen
PATH=${PATH}:/sonic-config-engine

# TODO Remove next 2 lines when the problem with importing `swsscommon` is solved.
echo "Trying to execute sonic-cfggen..."
sonic-cfggen

mkdir -p /workspace
cd /workspace
mv /telemetry .

echo "**** telemetry-con: Starting supervisord"
supervisord -c /etc/supervisor/conf.d/supervisord.conf
echo "**** telemetry-con: Finished supervisord"
