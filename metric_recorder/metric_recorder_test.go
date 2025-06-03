package metric_recorder

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func verifyDB(t *testing.T, db *redis.Client, key, field, exp string) {
	value, err := db.HGet(context.Background(), key, field).Result()
	if err != nil {
		t.Fatalf("Failed to read DB for key: %v", key)
	}
	if value != exp {
		t.Fatalf("Got %v, exp %v", value, exp)
	}
}

func TestMetricRecorder(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "sshtest")
	if err != nil {
		t.Fatalf("Failed to setup tmp file:%v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	r, err := NewSecurityMetricRecorder("gnxi", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create new recorder")
	}
	defer r.Close()

	r.Record(AuthnSuccessRecord{User: "user1"})
	r.Record(AuthnFailureRecord{Reason: "reason1"})
	r.Record(AuthzRecord{Permitted: true, Service: "service1", Rpc: "rpc1"})
	r.Record(AuthzRecord{Permitted: true, Service: "service1", Rpc: "rpc1"})
	r.Record(PathzRecord{Permitted: false, Rpc: "get", Path: "/"})
	time.Sleep((intervalSeconds + 3) * time.Second)

	db, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to create DB client")
	}

	verifyDB(t, db, authnTable+"|gnxi|success|user1", countAttr, "1")
	verifyDB(t, db, authnTable+"|gnxi|failure|reason1", countAttr, "1")
	verifyDB(t, db, authzTable+"|gnxi|service1|rpc1|permitted", countAttr, "2")
	verifyDB(t, db, pathzTable+"|get|/|denied", countAttr, "1")

	r.Record(AuthnSuccessRecord{User: "user2"})
	r.Record(AuthnFailureRecord{Reason: "reason1"})
	r.Record(AuthzRecord{Permitted: true, Service: "service1", Rpc: "rpc1"})
	r.Record(AuthzRecord{Permitted: false, Service: "service1", Rpc: "rpc1"})
	r.Record(PathzRecord{Permitted: false, Rpc: "get", Path: "/"})
	time.Sleep((intervalSeconds + 3) * time.Second)

	verifyDB(t, db, authnTable+"|gnxi|success|user1", countAttr, "1")
	verifyDB(t, db, authnTable+"|gnxi|success|user2", countAttr, "1")
	verifyDB(t, db, authnTable+"|gnxi|failure|reason1", countAttr, "2")
	verifyDB(t, db, authzTable+"|gnxi|service1|rpc1|permitted", countAttr, "3")
	verifyDB(t, db, authzTable+"|gnxi|service1|rpc1|denied", countAttr, "1")
	verifyDB(t, db, pathzTable+"|get|/|denied", countAttr, "2")
}

func TestMetricSSH(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "sshtest")
	if err != nil {
		t.Fatalf("Failed to setup tmp file:%v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	r, err := NewSecurityMetricRecorder("gnxi", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create new recorder")
	}

	defer r.Close()

	tests := []struct {
		desc     string
		lines    []string
		a        uint64
		acceptTs uint64
		r        uint64
		rejectTs uint64
	}{
		{"oneAccepted",
			oneAccepted,
			1,
			512460306000006000, // 1986-03-28T23:05:06.000006-07:00
			0,
			0},
		{"twoRejected",
			twoRejected,
			0,
			0,
			3,
			512460318000014000}, // 1986-03-28T23:05:18.000014-07:00
		{"debianRejected",
			debianRejected,
			0,
			0,
			1,
			1739919312516640000}, // 2025-02-18T22:55:12.516640+0000
		{"debianDisconnectReconnect",
			debianDisconnectReconnect,
			1,
			1740005321710776000, // 2025-02-19T22:48:41.710776+00:00
			0,
			0},
	}

	expected := collectCounters(t, sshTable)
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			for _, line := range tt.lines {
				tmpFile.WriteString(line + "\n")
			}
			tmpFile.Sync()
			expected.AccessAccepts += tt.a
			if tt.acceptTs != 0 {
				expected.LastAccessAccept = tt.acceptTs
			}
			expected.AccessRejects += tt.r
			if tt.rejectTs != 0 {
				expected.LastAccessReject = tt.rejectTs
			}
			found := collectCounters(t, sshTable)
			if !reflect.DeepEqual(expected, found) {
				t.Errorf("SSH counters failed accept check:\nCounter mismatch:\nWanted:%+v\nGot:%+v", expected, found)
			}
		})
	}
}

func TestMetricConsole(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "consoleTest")
	if err != nil {
		t.Fatalf("Failed to setup tmp file:%v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	r, err := NewSecurityMetricRecorder("gnxi", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create new recorder")
	}

	defer r.Close()

	tests := []struct {
		desc     string
		lines    []string
		a        uint64
		acceptTs uint64
		r        uint64
		rejectTs uint64
	}{
		{"oneAccepted",
			oneAccepted,
			1,
			512460306000006000, // 1986-03-28T23:05:06.000006-07:00
			0,
			0},
		{"twoRejected",
			twoRejected,
			0,
			0,
			2,
			512460314000014000}, // 1986-03-28T23:05:14.000014-07:00
	}

	expected := collectCounters(t, consoleTable)
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			for _, line := range tt.lines {
				tmpFile.WriteString(line + "\n")
			}
			tmpFile.Sync()
			expected.AccessAccepts += tt.a
			if tt.acceptTs != 0 {
				expected.LastAccessAccept = tt.acceptTs
			}
			expected.AccessRejects += tt.r
			if tt.rejectTs != 0 {
				expected.LastAccessReject = tt.rejectTs
			}
			found := collectCounters(t, consoleTable)
			if !reflect.DeepEqual(expected, found) {
				t.Errorf("Console counters failed accept check:\nCounter mismatch:\nWanted:%+v\nGot:%+v", expected, found)
			}
		})
	}
}

func collectCounters(t *testing.T, table string) accessCounters {
	// Gets access counters from DB and parses it into a struct
	t.Helper()
	time.Sleep(250 * time.Millisecond)
	db, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to create DB client")
	}
	var counter accessCounters

	if err := db.HMGet(context.Background(), table, acceptsField, lastAField, rejectsField, lastRField).Scan(&counter); err != nil {
		t.Errorf("Failed to collect counters: %v", err)
	}
	t.Logf("Collected Counters: %+v", counter)
	return counter
}

var oneAccepted = []string{
	"1986-03-28T23:05:01.000001-07:00 switch1 auth.info sshd-session[1]: Connection from ip1 port 1 on ip1a port 22 rdomain \"\"",
	"1986-03-28T23:05:02.000002-07:00 switch1 auth.info sshd-session[1]: Failed publickey for root from ip1 port 1 ssh2: ECDSA SHA256:1",
	"1986-03-28T23:05:03.000003-07:00 switch1 auth.info sshd-session[2]: Connection from ip2 port 2 on ip2a port 22 rdomain \"\"",
	"1986-03-28T23:05:04.000004-07:00 switch1 auth.info sshd-session[1]: Failed publickey for root from ip1 port 1 ssh2: ECDSA-CERT SHA256:2 ID good@gnxi (serial 1) CA RSA SHA256:3",
	"1986-03-28T23:05:05.000005-07:00 switch1 auth.info sshd-session[1]: Certificate extension \"nope@gnxi.com\" is not supported",
	"1986-03-28T23:05:06.000006-07:00 switch1 auth.info sshd-session[1]: Accepted certificate ID \"good@gnxi.com\" (serial 2) signed by RSA CA SHA256:4 via /dir this one is :ACCEPTED",
	"1986-03-28T23:05:06.000006-07:00 switch1 auth.info login[10]: root logged in on /dev/ttyS0",
	"1986-03-28T23:05:07.000007-07:00 switch1 auth.info sshd-session[3]: Connection from ip3 port 3 on ip3a port 22 rdomain \"\"",
	"NOP message",
}

var twoRejected = []string{
	"1986-03-28T23:05:08.000008-07:00 switch1 auth.info sshd-session[1]: Postponed publickey for root from ip1 port 1 ssh2 [preauth]",
	"1986-03-28T23:05:09.000009-07:00 switch1 auth.info sshd-session[1]: Certificate extension \"nope@gnxi.com\" is not supported",
	"1986-03-28T23:05:09.000009-07:00 switch1 auth.warning login[11]: login attempt with non-existent user on /X",
	"1986-03-28T23:05:10.000010-07:00 switch1 auth.info sshd-session[2]: kex_exchange_identification: Connection closed by remote host :this one is REJECTED",
	"1986-03-28T23:05:11.000011-07:00 switch1 auth.info sshd-session[2]: Connection closed by ip2 port 2",
	"1986-03-28T23:05:12.000012-07:00 switch1 auth.info sshd-session[1]: Accepted certificate ID \"good@gnxi\" (serial 2) signed by RSA CA SHA256:4 via /dir",
	"1986-03-28T23:05:13.000013-07:00 switch1 auth.info sshd-session[3]: Failed publickey for root from ip3 port 3 ssh2: RSA SHA256:3",
	"1986-03-28T23:05:14.000014-07:00 switch1 auth.warning login[12]: invalid password for 'root' on /X",
	"1986-03-28T23:05:14.000014-07:00 switch1 auth.info sshd-session[3]: Connection closed by authenticating user root ip3 port 3 [preauth] :this one is REJECTED",
	"1986-03-28T23:05:15.000014-07:00 switch1 auth.info sshd-session[4]: Connection from 1:2:3 port 1 on 4:5:6 port 22 rdomain",
	"1986-03-28T23:05:16.000014-07:00 switch1 auth.err sshd-session[4]: error: Certificate does not contain an authorized principal",
	"1986-03-28T23:05:17.000014-07:00 switch1 auth.info sshd-session[4]: Failed publickey for root from 1:2:3 port 1 ssh2: ECDSA-CERT SHA256:key2 ID user@gnxi (serial 123) CA RSA SHA256:key",
	"1986-03-28T23:05:18.000014-07:00 switch1 auth.crit sshd-session[4]: fatal: Timeout before authentication for 1:2:3 port 1 :this one is REJECTED",
}

var debianRejected = []string{
	"2025-02-18T22:55:12.516640+00:00 sonic auth.info inbandmgr#sshd[300710]: Invalid user invaliduser from 2001:4860:f803:3fd::c port 47104",
	"2025-02-18T22:55:16.060221+00:00 sonic authpriv.notice inbandmgr#sshd[300710]: pam_unix(sshd:auth): check pass; user unknown",
	"2025-02-18T22:55:16.060354+00:00 sonic authpriv.notice inbandmgr#sshd[300710]: pam_unix(sshd:auth): authentication failure; logname= uid=0 euid=0 tty=ssh ruser= rhost=2001:4860:f803:3fd::c",
	"2025-02-18T22:55:18.211473+00:00 sonic auth.info inbandmgr#sshd[300710]: Failed password for invalid user invaliduser from 2001:4860:f803:3fd::c port 47104 ssh2",
}

var debianDisconnectReconnect = []string{
	"2025-02-19T22:48:33.552331+00:00 sonic auth.info inbandmgr#sshd[2556433]: Received disconnect from 2001:4860:f803:3fd::e port 52388:11: disconnected by user",
	"2025-02-19T22:48:33.552915+00:00 sonic auth.info inbandmgr#sshd[2556433]: Disconnected from user admin 2001:4860:f803:3fd::e port 52388",
	"2025-02-19T22:48:33.554380+00:00 sonic authpriv.info inbandmgr#sshd[2556425]: pam_unix(sshd:session): session closed for user admin",
	"2025-02-19T22:48:41.710776+00:00 sonic auth.info inbandmgr#sshd[2566322]: Accepted password for admin from 2001:4860:f803:3fd::3 port 60924 ssh2",
	"2025-02-19T22:48:41.713809+00:00 sonic authpriv.info inbandmgr#sshd[2566322]: pam_unix(sshd:session): session opened for user admin(uid=1000) by (uid=0)",
}
