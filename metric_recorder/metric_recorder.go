package metric_recorder

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"

	log "github.com/golang/glog"
	lru "github.com/hashicorp/golang-lru"
	"github.com/nxadm/tail"
	"github.com/redis/go-redis/v9"
)

const (
	dbName          = "STATE_DB"
	authnTable      = "AUTHN_TABLE"
	authzTable      = "AUTHZ_TABLE"
	pathzTable      = "PATHZ_TABLE"
	consoleTable    = "CREDENTIALS|CONSOLE_METRICS"
	sshTable        = "CREDENTIALS|SSH_HOST"
	acceptsField    = "access_accepts"
	rejectsField    = "access_rejects"
	lastAField      = "last_access_accept"
	lastRField      = "last_access_reject"
	countAttr       = "count"
	timestampAttr   = "timestamp"
	cacheSize       = 5000
	intervalSeconds = 30
	timeLayout      = "2006-01-02T15:04:05.999999-07:00"
	// These refer to the match group for regex log parsing
	matchTime = 1
	matchID   = 4
)

var (
	consoleRE       = regexp.MustCompile(`^(?P<time>[\d]{4}-[\d]{2}-[\d]{2}T[\d]+:[\d]+:[\d]+\.[\d]+[-+][\d:]+) \[?\w+\]? auth(priv)?\.(info|warning|notice) (inbandmgr\#)?login\[(?P<pid>[\d]+)]:`)
	consoleAccept   = regexp.MustCompile(`^(?P<time>[\d]{4}-[\d]{2}-[\d]{2}T[\d]+:[\d]+:[\d]+\.[\d]+[-+][\d:]+) \[?\w+\]? auth(priv)?\.(info|warning) (inbandmgr\#)?login\[(?P<pid>[\d]+)]:.+(logged in|session opened)`)
	consoleReject   = regexp.MustCompile(`^(?P<time>[\d]{4}-[\d]{2}-[\d]{2}T[\d]+:[\d]+:[\d]+\.[\d]+[-+][\d:]+) \[?\w+\]? auth(priv)?\.(warning|notice) (inbandmgr\#)?login\[(?P<pid>[\d]+)]:.+(login attempt|invalid password|authentication failure)`)
	sshRE           = regexp.MustCompile(`^(?P<time>[\d]{4}-[\d]{2}-[\d]{2}T[\d]+:[\d]+:[\d]+\.[\d]+[-+][\d:]+) \[?\w+\]? auth\.(info|crit) (inbandmgr\#)?sshd.*\[(?P<pid>[\d]+)]:`)
	sshStart        = regexp.MustCompile(`^.*? sshd.*\[([\d]+)]: Connection from`)
	sshAccept       = regexp.MustCompile(`^.*? sshd.*\[([\d]+)]: Accepted (key|publickey|password|certificate ID)`)
	sshReject       = regexp.MustCompile(`^.*? sshd.*\[([\d]+)]: ((kex_exchange_identification: )?Connection closed by|Invalid user|fatal: Timeout before authentication)`)
	sshDebianAccept = regexp.MustCompile(`^.*? inbandmgr\#sshd\[([\d]+)]: Accepted (key|publickey|password|certificate ID)`)
	sshDebianReject = regexp.MustCompile(`^.*? inbandmgr\#sshd\[([\d]+)]: ((kex_exchange_identification: )?Connection closed by|Invalid user|fatal: Timeout before authentication)`)
)

// AuthnSuccessRecord stores a successful authentication record.
type AuthnSuccessRecord struct {
	User string
}

// AuthnFailureRecord stores a failed authentication record.
type AuthnFailureRecord struct {
	Reason string
}

// AuthzRecord stores a gRPC authorization record.
type AuthzRecord struct {
	Permitted bool
	Service   string
	Rpc       string
}

// PathzRecord stores a gNMI path based authorization record.
type PathzRecord struct {
	Permitted bool
	Rpc       string
	Path      string
}

type metric struct {
	count     int64
	timestamp int64
}

type accessCounters struct {
	AccessRejects    uint64 `redis:"access_rejects"`
	LastAccessReject uint64 `redis:"last_access_reject"`
	AccessAccepts    uint64 `redis:"access_accepts"`
	LastAccessAccept uint64 `redis:"last_access_accept"`
}

type accessEvent int64

const (
	none accessEvent = iota
	accept
	reject
)

// SecurityMetricRecorder helps to collect authentication and authorization metrics and report them to Redis DB.
type SecurityMetricRecorder struct {
	server   string
	db       *redis.Client
	cache    *lru.Cache           // Store all record entries
	tmpCache map[interface{}]bool // Store the updated entries for the interval
	ticker   *time.Ticker
	done     chan bool
	wg       sync.WaitGroup
	mux      sync.Mutex
	ssh      accessCounters
	console  accessCounters
	sshMux   sync.Mutex
}

// NewSecurityMetricRecorder returns a new SecurityMetricRecorder.
func NewSecurityMetricRecorder(server string, authLog string) (*SecurityMetricRecorder, error) {
	r := new(SecurityMetricRecorder)
	r.server = server
	var err error
	if r.db, err = getRedisDBClient(); err != nil {
		return nil, err
	}
	if r.cache, err = lru.New(cacheSize); err != nil {
		return nil, err
	}
	r.tmpCache = map[interface{}]bool{}
	r.ticker = time.NewTicker(intervalSeconds * time.Second)
	r.done = make(chan bool, 1)
	r.wg.Add(1)
	go r.startPeriodicTasks()
	r.wg.Add(1)
	go r.startAccessWatcher(authLog)
	return r, nil
}

// Returns true if a console line match was found, false otherwise
func (r *SecurityMetricRecorder) handleConsoleLines(line string) bool {
	found := false
	if cMatch := consoleRE.FindStringSubmatch(line); cMatch != nil {
		pTime, tErr := time.Parse(timeLayout, cMatch[matchTime])
		if tErr != nil {
			log.V(lvl.ERROR).Infof("AccessWatcher failed to parse time: %v, : %e", cMatch[matchTime], tErr)
			return false
		}
		found = true
		if consoleAccept.MatchString(line) {
			r.console.AccessAccepts += 1
			r.console.LastAccessAccept = uint64(pTime.UnixNano())
			r.writeConsoleCounters(accept)
		} else if consoleReject.MatchString(line) {
			r.console.AccessRejects += 1
			r.console.LastAccessReject = uint64(pTime.UnixNano())
			r.writeConsoleCounters(reject)
		}
	}
	return found
}

// Returns true if a debian ssh line match was found, false otherwise
// The sshRE signature matches both supported versions of ssh,
// so we can reuse the match results, namely the timestamp
func (r *SecurityMetricRecorder) handleDebianSshLines(line string, t string) bool {
	found := false
	pTime, tErr := time.Parse(timeLayout, t)
	if tErr != nil {
		log.V(lvl.ERROR).Infof("AccessWatcher failed to parse time: %v, : %e", t, tErr)
	}
	if sshDebianAccept.MatchString(line) {
		found = true
		r.ssh.AccessAccepts += 1
		r.ssh.LastAccessAccept = uint64(pTime.UnixNano())
		r.writeSSHCounters(accept)
	} else if sshDebianReject.MatchString(line) {
		found = true
		r.ssh.AccessRejects += 1
		r.ssh.LastAccessReject = uint64(pTime.UnixNano())
		r.writeSSHCounters(reject)
	}
	return found
}

func (r *SecurityMetricRecorder) handleSwitchLinuxSsh(line string, idCache *lru.Cache, match []string) bool {
	found := false
	id, pErr := strconv.ParseUint(match[matchID], 10, 0)
	if pErr != nil {
		log.V(lvl.ERROR).Infof("AccessWatcher failed to parse ID:%v, : %e", match[matchID], pErr)
		return false
	}
	if idCache.Contains(id) {
		event := none
		if sshAccept.MatchString(line) {
			event = accept
		} else if sshReject.MatchString(line) {
			event = reject
		}
		if event != none {
			pTime, tErr := time.Parse(timeLayout, match[matchTime])
			if tErr != nil {
				log.V(lvl.ERROR).Infof("AccessWatcher failed to parse time: %v, : %e", match[matchTime], tErr)
				return false
			}
			if event == accept {
				r.ssh.AccessAccepts += 1
				r.ssh.LastAccessAccept = uint64(pTime.UnixNano())
			} else if event == reject {
				r.ssh.AccessRejects += 1
				r.ssh.LastAccessReject = uint64(pTime.UnixNano())
			}
			r.writeSSHCounters(event)
			idCache.Remove(id)
			found = true
		}
	} else if sshStart.MatchString(line) {
		found = true
		idCache.Add(id, true)
	}
	return found
}

func (r *SecurityMetricRecorder) startAccessWatcher(authLog string) {
	defer r.wg.Done()
	log.V(lvl.INFO).Infof("%s AccessWatcher watching: %s", r.server, authLog)
	defer log.V(lvl.INFO).Infof("%s AccessWatcher closing", r.server)

	r.initCounters()

	t, err := tail.TailFile(authLog, tail.Config{Follow: true, ReOpen: true, MustExist: true})
	if err != nil {
		log.V(lvl.ERROR).Infof("AccessWatcher failed to open log: %e", err)
		return
	}

	idCache, err := lru.New(1000)
	if err != nil {
		log.V(lvl.ERROR).Infof("AccessWatcher failed to create cache: %e", err)
		return
	}
	// Begin Watching
	for {
		select {
		case <-r.done:
			t.Stop()
			return
		case line := <-t.Lines:
			match := sshRE.FindStringSubmatch(line.Text)
			if match != nil {
				if !r.handleDebianSshLines(line.Text, match[matchTime]) {
					r.handleSwitchLinuxSsh(line.Text, idCache, match)
				}
			} else {
				// Not an SSH log, maybe it's a console log?
				r.handleConsoleLines(line.Text)
			}
		}
	}
}

func (r *SecurityMetricRecorder) writeSSHCounters(event accessEvent) {
	r.sshMux.Lock()
	defer r.sshMux.Unlock()
	if event == accept {
		log.V(lvl.INFO).Infof("SSH access accepted: %v", r.ssh.AccessAccepts)
		r.db.HSet(context.Background(), sshTable, map[string]interface{}{acceptsField: r.ssh.AccessAccepts, lastAField: r.ssh.LastAccessAccept})
	} else if event == reject {
		log.V(lvl.INFO).Infof("SSH access rejected: %v", r.ssh.AccessRejects)
		r.db.HSet(context.Background(), sshTable, map[string]interface{}{rejectsField: r.ssh.AccessRejects, lastRField: r.ssh.LastAccessReject})
	}
}

func (r *SecurityMetricRecorder) writeConsoleCounters(event accessEvent) {
	r.sshMux.Lock()
	defer r.sshMux.Unlock()
	if event == accept {
		log.V(lvl.INFO).Infof("Console access accepted: %v", r.console.AccessAccepts)
		r.db.HSet(context.Background(), consoleTable, map[string]interface{}{acceptsField: r.console.AccessAccepts, lastAField: r.console.LastAccessAccept})
	} else if event == reject {
		log.V(lvl.INFO).Infof("Console access rejected: %v", r.console.AccessRejects)
		r.db.HSet(context.Background(), consoleTable, map[string]interface{}{rejectsField: r.console.AccessRejects, lastRField: r.console.LastAccessReject})
	}
}

func (r *SecurityMetricRecorder) initCounters() {
	defer r.writeSSHCounters(accept)
	defer r.writeSSHCounters(reject)
	defer r.writeConsoleCounters(accept)
	defer r.writeConsoleCounters(reject)

	dbc, err := getRedisDBClient()
	if err != nil {
		log.V(lvl.ERROR).Info("AccessWatcher failed to create DB client")
		return
	}
	defer db.CloseRedisClient(dbc)
	// SSH Counters
	if err := dbc.HMGet(context.Background(), sshTable, acceptsField, lastAField, rejectsField, lastRField).Scan(&r.ssh); err != nil {
		log.V(lvl.ERROR).Infof("AccessWatcher failed to init SSH counters: %v", err)
	}
	// Console Counters
	if err := dbc.HMGet(context.Background(), consoleTable, acceptsField, lastAField, rejectsField, lastRField).Scan(&r.console); err != nil {
		log.V(lvl.ERROR).Infof("AccessWatcher failed to init Console counters: %v", err)
	}
}

// Close releases the resource of the SecurityMetricRecorder.
func (r *SecurityMetricRecorder) Close() {
	if r == nil {
		return
	}
	// Call done twice to close PeriodicTasks and AccessWatcher
	r.done <- true
	r.done <- true
	r.ticker.Stop()
	r.wg.Wait()
	if r.db != nil {
		db.CloseRedisClient(r.db)
	}
}

func getRedisDBClient() (*redis.Client, error) {
	ns, _ := sdcfg.GetDbDefaultNamespace()
	addr, _ := sdcfg.GetDbTcpAddr(dbName, ns)
	id, _ := sdcfg.GetDbId(dbName, ns)
	rclient := db.TransactionalRedisClientWithOpts(&redis.Options{
		Network:     "tcp",
		Addr:        addr,
		Password:    "", // no password set
		DB:          id,
		DialTimeout: 0,
	})
	if rclient == nil {
		return nil, fmt.Errorf("Cannot create redis client.")
	}
	if _, err := rclient.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return rclient, nil
}

func (r *SecurityMetricRecorder) startPeriodicTasks() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ticker.C:
			r.writeToDB()
		case <-r.done:
			return
		}
	}
}

// Record records an event. The input can be AuthnSuccessRecord, AuthnFailureRecord, AuthzRecord, or PathzRecord.
func (r *SecurityMetricRecorder) Record(record interface{}) error {
	r.mux.Lock()
	defer r.mux.Unlock()
	m := metric{
		count:     1,
		timestamp: time.Now().UnixNano(),
	}
	if v, ok := r.cache.Get(record); ok {
		m.count = (v.(metric)).count + 1
	}
	r.cache.Add(record, m)
	r.tmpCache[record] = true
	return nil
}

func (r *SecurityMetricRecorder) writeToDB() {
	r.mux.Lock()
	defer r.mux.Unlock()
	for key, _ := range r.tmpCache {
		v, ok := r.cache.Get(key)
		if !ok {
			continue
		}
		m := v.(metric)
		var k string
		switch rd := key.(type) {
		case AuthnSuccessRecord:
			k = authnTable + "|" + r.server + "|success|" + rd.User
		case AuthnFailureRecord:
			k = authnTable + "|" + r.server + "|failure|" + rd.Reason
		case AuthzRecord:
			k = authzTable + "|" + r.server + "|" + rd.Service + "|" + rd.Rpc + "|permitted"
			if !rd.Permitted {
				k = authzTable + "|" + r.server + "|" + rd.Service + "|" + rd.Rpc + "|denied"
			}
		case PathzRecord:
			k = pathzTable + "|" + rd.Rpc + "|" + rd.Path + "|permitted"
			if !rd.Permitted {
				k = pathzTable + "|" + rd.Rpc + "|" + rd.Path + "|denied"
			}
		default:
			log.V(lvl.ERROR).Infof("Invalid record type")
		}
		if k != "" {
			r.db.HSet(context.Background(), k, countAttr, m.count)
			r.db.HSet(context.Background(), k, timestampAttr, m.timestamp)
		}
	}
	r.tmpCache = map[interface{}]bool{}
}
