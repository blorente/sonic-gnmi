package gnmi

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	gnmipt "github.com/sonic-net/sonic-gnmi/gnmi_server/pathtransl"
	"github.com/sonic-net/sonic-gnmi/metric_recorder"
	"github.com/sonic-net/sonic-gnmi/pathz_authorizer"
	spb "github.com/sonic-net/sonic-gnmi/proto/gnmi_sonic"
	sdc "github.com/sonic-net/sonic-gnmi/sonic_data_client"

	"github.com/Workiva/go-datastructures/queue"
	log "github.com/golang/glog"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	pathzpb "github.com/openconfig/gnsi/pathz"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Client contains information about a subscribe client that has connected to the server.
type Client struct {
	addr              net.Addr
	sendMsg           int64
	recvMsg           int64
	errors            int64
	polled            chan struct{}
	stop              chan struct{}
	once              chan struct{}
	connectionManager *ConnectionManager
	mu                sync.RWMutex
	q                 *queue.PriorityQueue
	qDepthCur         uint64
	qDepthMax         uint64
	subscribe         *gnmipb.SubscriptionList
	// Wait for all sub go routine to finish
	w                 sync.WaitGroup
	fatal             bool
	translCtx         gnmipt.TranslatorCtx
	enableTranslation bool
	logLevel          int
	pathzProcessor    pathz_authorizer.GnmiAuthzProcessorInterface
	recorder          *metric_recorder.SecurityMetricRecorder
	startTime         time.Time
	syncTime          time.Time
}

// Syslog level for error
const logLevelError int = 3
const logLevelDebug int = 7
const logLevelMax int = logLevelDebug

// NewClient returns a new initialized client.
func NewClient(addr net.Addr, pathzProcessor pathz_authorizer.GnmiAuthzProcessorInterface, recorder *metric_recorder.SecurityMetricRecorder, cm *ConnectionManager) *Client {
	pq := queue.NewPriorityQueue(1, false)
	return &Client{
		addr:              addr,
		q:                 pq,
		connectionManager: cm,
		translCtx:         nil,
		enableTranslation: false,
		logLevel:          logLevelError,
		pathzProcessor:    pathzProcessor,
		recorder:          recorder,
		startTime:         time.Now(),
	}
}

func (c *Client) setLogLevel(lvl int) {
	c.logLevel = lvl
}

func (c *Client) setEnableTranslation(enb bool) {
	c.enableTranslation = enb
}

// String returns the target the client is querying.
func (c *Client) String() string {
	return fmt.Sprintf("%s:%p", c.addr.String(), c.q)
}

// Populate SONiC data path from prefix and subscription path.
func (c *Client) populateDbPathSubscrition(sublist *gnmipb.SubscriptionList) ([]*gnmipb.Path, error) {
	var paths []*gnmipb.Path

	prefix := sublist.GetPrefix()
	log.V(6).Infof("prefix : %#v SubscribRequest : %#v", prefix, sublist)

	subscriptions := sublist.GetSubscription()
	if subscriptions == nil {
		return nil, fmt.Errorf("No Subscription")
	}

	for _, subscription := range subscriptions {
		path := subscription.GetPath()
		paths = append(paths, path)
	}

	log.V(6).Infof("gnmi Paths : %v", paths)
	return paths, nil
}

// Run starts the subscribe client. The first message received must be a
// SubscriptionList. Once the client is started, it will run until the stream
// is closed or the schedule completes. For Poll queries the Run will block
// internally after sync until a Poll request is made to the server.
func (c *Client) Run(stream gnmipb.GNMI_SubscribeServer) (err error) {
	defer log.V(lvl.INFO).Infof("Client %s shutdown", c)

	var connectionKey string
	var valid bool

	if stream == nil {
		return grpc.Errorf(codes.FailedPrecondition, "cannot start client: stream is nil")
	}
	ctx := stream.Context()

	defer func() {
		if err != nil {
			c.errors++
		}
	}()

	originalQuery, err := stream.Recv()
	c.recvMsg++
	if err != nil {
		if err == io.EOF {
			return grpc.Errorf(codes.Aborted, "stream EOF received before init")
		}
		return grpc.Errorf(grpc.Code(err), "received error from client")
	}

	newQuery := *originalQuery
	query := &newQuery

	if !hasMasterEID(query.GetExtension()) {
		log.V(lvl.DEBUG).Info("SUBSCRIBE from an observer")
		if err := setTOSToAF4(ctx); err != nil {
			log.V(lvl.ERROR).Info(err)
			return grpc.Errorf(codes.Aborted, "%v", err)
		}
	} else {
		log.V(lvl.DEBUG).Info("SUBSCRIBE from a controller")
	}

	// gNMI path based authorization
	if c.pathzProcessor != nil && newQuery.GetSubscribe() != nil {
		user, err := getUsername(ctx)
		if err != nil {
			return err
		}
		subscriptions := []*gnmipb.Subscription{}
		for _, s := range newQuery.GetSubscribe().GetSubscription() {
			// Only process the authorized paths in the request.
			r, err := c.pathzProcessor.AuthorizeWithPrefix(user, newQuery.GetSubscribe().GetPrefix(), s.GetPath(), pathzpb.Mode_MODE_READ)
			if err != nil || r.Action != pathzpb.Action_ACTION_PERMIT {
				c.recorder.Record(metric_recorder.PathzRecord{
					Permitted: false,
					Rpc:       "subscribe",
					Path:      pathz_authorizer.PrintPathWithPrefix(newQuery.GetSubscribe().GetPrefix(), s.GetPath()),
				})
				continue
			}
			c.recorder.Record(metric_recorder.PathzRecord{
				Permitted: true,
				Rpc:       "subscribe",
				Path:      pathz_authorizer.PrintPathWithPrefix(newQuery.GetSubscribe().GetPrefix(), s.GetPath()),
			})
			subscriptions = append(subscriptions, s)
		}
		if len(subscriptions) == 0 {
			return grpc.Errorf(codes.PermissionDenied, "Unauthorized request. Rejected by pathz policy.")
		}
		newQuery.GetSubscribe().Subscription = subscriptions
	}

	log.V(lvl.DEBUG).Infof("Client %s recieved initial query %v", c, query)

	// Tries to translate the subscribe request if needed.
	if c.enableTranslation {
		c.translCtx = gnmipt.TranslSubscribeRequest(query)
	}

	c.subscribe = query.GetSubscribe()
	extensions := query.GetExtension()

	if c.subscribe == nil {
		return grpc.Errorf(codes.InvalidArgument, "first message must be SubscriptionList: %q", query)
	}

	prefix := c.subscribe.GetPrefix()
	origin := prefix.GetOrigin()
	target := prefix.GetTarget()

	paths, err := c.populateDbPathSubscrition(c.subscribe)
	if err != nil {
		return grpc.Errorf(codes.NotFound, "Invalid subscription path: %v %q", err, query)
	}

	if o, err := ParseOrigin(paths); err != nil {
		return err // origin conflict within paths
	} else if len(origin) == 0 {
		origin = o // Use origin from paths if not given in prefix
	} else if len(o) != 0 && o != origin {
		return status.Error(codes.InvalidArgument, "Origin conflict between prefix and paths")
	}

	var dc sdc.Client

	mode := c.subscribe.GetMode()

	if connectionKey, valid = c.connectionManager.Add(c.addr, query.String(), mode != gnmipb.SubscriptionList_ONCE); !valid {
		return grpc.Errorf(codes.Unavailable, "Server connections are at capacity.")
	}
	defer c.connectionManager.Remove(connectionKey) // remove key from connection list

	log.V(lvl.DEBUG).Infof("mode=%v, origin=%q, target=%q", mode, origin, target)

	if origin == "openconfig" {
		dc, err = sdc.NewTranslClient(prefix, paths, ctx, gnmipb.Encoding_JSON_IETF, extensions, sdc.TranslWildcardOption{})
	} else if IsNativeOrigin(origin) {
		dc, err = sdc.NewMixedDbClient(paths, prefix, origin, gnmipb.Encoding_JSON_IETF, "", "")
	} else if len(origin) != 0 {
		return grpc.Errorf(codes.Unimplemented, "Unsupported origin: %s", origin)
	} else if target == "" {
		// This and subsequent conditions handle target based path identification
		// when origin == "". As per the spec it should have been treated as "openconfig".
		// But we take a deviation and stick to legacy logic for backward compatibility
		return grpc.Errorf(codes.Unimplemented, "Empty target data not supported")
	} else if target == "OTHERS" {
		dc, err = sdc.NewNonDbClient(paths, prefix)
	} else if (target == "EVENTS") && (mode == gnmipb.SubscriptionList_STREAM) {
		dc, err = sdc.NewEventClient(paths, prefix, c.logLevel)
	} else if _, ok, _, _ := sdc.IsTargetDb(target); ok {
		dc, err = sdc.NewDbClient(paths, prefix)
	} else {
		/* For any other target or no target create new Transl Client. */
		dc, err = sdc.NewTranslClient(prefix, paths, ctx, gnmipb.Encoding_JSON_IETF, extensions, sdc.TranslWildcardOption{})
	}

	if err != nil {
		return grpc.Errorf(codes.NotFound, "%v", err)
	}

	defer dc.Close()

	switch mode {
	case gnmipb.SubscriptionList_STREAM:
		c.stop = make(chan struct{}, 1)
		c.w.Add(1)
		go dc.StreamRun(c.q, c.stop, &c.w, c.subscribe)
	case gnmipb.SubscriptionList_POLL:
		c.polled = make(chan struct{}, 1)
		c.polled <- struct{}{}
		c.w.Add(1)
		go dc.PollRun(c.q, c.polled, &c.w, c.subscribe)
	case gnmipb.SubscriptionList_ONCE:
		c.once = make(chan struct{}, 1)
		c.once <- struct{}{}
		c.w.Add(1)
		go dc.OnceRun(c.q, c.once, &c.w, c.subscribe)
	default:
		return grpc.Errorf(codes.InvalidArgument, "Unkown subscription mode: %q", query)
	}

	log.V(lvl.DEBUG).Infof("Client %s running", c)
	go c.recv(stream)
	err = c.send(stream, dc, mode == gnmipb.SubscriptionList_ONCE)
	c.Close()
	// Wait until all child go routines exited
	c.w.Wait()
	return grpc.Errorf(codes.InvalidArgument, "%s", err)
}

// Closing of client queue is triggered upon end of stream receive or stream error
// or fatal error of any client go routine .
// it will cause cancle of client context and exit of the send goroutines.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.V(lvl.DEBUG).Infof("Client %s Close, sendMsg %v recvMsg %v errors %v", c, c.sendMsg, c.recvMsg, c.errors)
	if c.stop != nil {
		close(c.stop)
		c.stop = nil
	}
	if c.polled != nil {
		close(c.polled)
		c.polled = nil
	}
	if c.once != nil {
		close(c.once)
		c.once = nil
	}
	if c.q != nil {
		if c.q.Disposed() {
			return
		}
		if c.subscribe.Mode != gnmipb.SubscriptionList_ONCE {
			c.q.Dispose()
		}
	}
}

func (c *Client) recv(stream gnmipb.GNMI_SubscribeServer) {
	defer c.Close()

	for {
		log.V(5).Infof("Client %s blocking on stream.Recv()", c)
		event, err := stream.Recv()
		c.recvMsg++

		switch err {
		default:
			log.V(lvl.ERROR).Infof("Client %s received error: %v", c, err)
			return
		case io.EOF:
			log.V(lvl.DEBUG).Infof("Client %s received io.EOF", c)
			if c.subscribe.Mode == gnmipb.SubscriptionList_STREAM {
				// The client->server could be closed after the sending the subscription list.
				// EOF is not a indication of client is not listening.
				// Instead stream.Context() which is signaled once the underlying connection is terminated.
				log.V(lvl.DEBUG).Infof("Waiting for client '%s'", c)
				// This context is done when the client connection is terminated.
				<-stream.Context().Done()
				log.V(lvl.DEBUG).Infof("Client is done '%s'", c)
			}
			return
		case nil:
		}

		if c.subscribe.Mode == gnmipb.SubscriptionList_POLL {
			log.V(lvl.DEBUG).Infof("Client %s received Poll event: %v", c, event)
			if _, ok := event.Request.(*gnmipb.SubscribeRequest_Poll); !ok {
				return
			}
			c.polled <- struct{}{}
			continue
		}
		log.V(lvl.ERROR).Infof("Client %s received invalid event: %s", c, event)
	}
}

// send runs until process Queue returns an error.
func (c *Client) send(stream gnmipb.GNMI_SubscribeServer, dc sdc.Client, exitOnSync bool) error {
	for {
		// Update qDepthCur and qDepthMax if necessary
		c.mu.Lock()
		c.qDepthCur = uint64(c.q.Len())
		c.qDepthMax = max(c.qDepthCur, c.qDepthMax)
		c.mu.Unlock()

		var val *sdc.Value
		items, err := c.q.Get(1)

		if items == nil {
			log.V(1).Infof("%v", err)
			return err
		}
		if err != nil {
			c.errors++
			log.V(lvl.ERROR).Infof("Client.send(): %s exiting, DequeueItem() returned an error:%v", c, err)
			return fmt.Errorf("unexpected queue Gext(1): %v", err)
		}

		var resp *gnmipb.SubscribeResponse

		switch v := items[0].(type) {
		case sdc.Value:
			if resp, err = sdc.ValToResp(v); err != nil {
				c.errors++
				return err
			}
			val = &v
		default:
			log.V(1).Infof("Unknown data type %v for %s in queue", items[0], c)
			c.errors++
		}

		// Tries to translate the response if needed.
		if c.enableTranslation {
			gnmipt.TranslSubscribeResponse(resp, c.translCtx)
		}

		c.sendMsg++
		err = stream.Send(resp)
		if err != nil {
			log.V(1).Infof("Client %s sending error:%v", c, err)
			c.errors++
			dc.FailedSend()
			return err
		}
		if val.SyncResponse {
			c.mu.Lock()
			c.syncTime = time.Now()
			c.mu.Unlock()
		}

		dc.SentOne(val)
		c.printSensitiveStatusUpdates(resp)
		log.V(5).Infof("Client %s done sending, msg count %d, msg %v", c, c.sendMsg, resp)

		if exitOnSync && val.SyncResponse {
			c.mu.Lock()
			defer c.mu.Unlock()
			log.V(lvl.DEBUG).Infof("Client %s done received sync, exiting send", c)
			if c.q != nil && !c.q.Disposed() {
				c.q.Dispose()
			}
			return nil
		}
	}
}

func (c *Client) Info() (result *spb.GnmiSubscriptionClientInfo) {
	result = &spb.GnmiSubscriptionClientInfo{
		IpAddress:           "",
		TcpSourcePort:       0,
		SubscriptionMode:    spb.GnmiSubscriptionClientInfo_MODE_UNKNOWN,
		SubscriptionSubMode: spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN,
		SubscriptionStart:   timestamppb.New(time.Time{}),
		SyncResponseTime:    timestamppb.New(time.Time{}),
		CurrentQueueDepth:   0,
		MaxQueueDepth:       0,
	}
	if c == nil || c.addr == nil {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result.SubscriptionStart = timestamppb.New(c.startTime)
	result.SyncResponseTime = timestamppb.New(c.syncTime)
	switch c.addr.(type) {
	case *net.UDPAddr:
		result.IpAddress = c.addr.(*net.UDPAddr).IP.String()
		result.TcpSourcePort = uint64(c.addr.(*net.UDPAddr).Port)
	case *net.TCPAddr:
		result.IpAddress = c.addr.(*net.TCPAddr).IP.String()
		result.TcpSourcePort = uint64(c.addr.(*net.TCPAddr).Port)
	default:
		return
	}

	result.CurrentQueueDepth = c.qDepthCur
	result.MaxQueueDepth = c.qDepthMax

	if c.subscribe == nil {
		return
	}
	switch c.subscribe.Mode {
	case gnmipb.SubscriptionList_STREAM:
		result.SubscriptionMode = spb.GnmiSubscriptionClientInfo_MODE_STREAM
	case gnmipb.SubscriptionList_POLL:
		result.SubscriptionMode = spb.GnmiSubscriptionClientInfo_MODE_POLL
	case gnmipb.SubscriptionList_ONCE:
		result.SubscriptionMode = spb.GnmiSubscriptionClientInfo_MODE_ONCE
	}

	if result.SubscriptionMode != spb.GnmiSubscriptionClientInfo_MODE_STREAM {
		return
	}

	if c.subscribe.Subscription == nil {
		return
	}
	for _, sub := range c.subscribe.Subscription {
		if sub == nil {
			continue
		}
		switch sub.Mode {
		case gnmipb.SubscriptionMode_TARGET_DEFINED:
			// TODO: Handle this case
			continue
		case gnmipb.SubscriptionMode_ON_CHANGE:
			result.SubscriptionSubMode = spb.GnmiSubscriptionClientInfo_SUB_MODE_ON_CHANGE
		case gnmipb.SubscriptionMode_SAMPLE:
			if result.SubscriptionSubMode == spb.GnmiSubscriptionClientInfo_SUB_MODE_UNKNOWN {
				result.SubscriptionSubMode = spb.GnmiSubscriptionClientInfo_SUB_MODE_SAMPLE_ONLY
			}
		}
	}
	return
}

func (c *Client) printSensitiveStatusUpdates(resp *gnmipb.SubscribeResponse) {
	// msg update:{timestamp:1669844920500203247
	//      prefix:{origin:"openconfig" elem:{name:"interfaces"} elem:{name:"interface" key:{key:"name" value:"Ethernet1/7/3"}} elem:{name:"state"}}
	//      update:{path:{elem:{name:"oper-status"}} val:{string_val:"UP"}}}
	notif := resp.GetUpdate()
	if notif == nil {
		if sync := resp.GetSyncResponse(); sync == true {
			log.V(lvl.INFO).Infof("SyncResponse for %v", c.addr.String())
		}
		return
	}
	pelem := notif.Prefix.Elem
	if !(len(pelem) == 3 &&
		pelem[0].GetName() == "interfaces" &&
		pelem[1].GetName() == "interface" &&
		pelem[2].GetName() == "state") {
		return
	}
	for _, update := range notif.Update {
		elems := update.Path.GetElem()
		for _, elem := range elems {
			switch elem.GetName() {
			case "oper-status":
				log.V(lvl.WARNING).Infof("interface/state/oper-status update: key=%v value()=%v client=%v",
					pelem[1].Key["name"], update.Val.GetStringVal(), c.addr.String())
			case "hardware-port":
				log.V(lvl.WARNING).Infof("interface/state/hardware-port update: key=%v value()=%v client=%v",
					pelem[1].Key["name"], update.Val.GetStringVal(), c.addr.String())
			case "id":
				log.V(lvl.WARNING).Infof("interface/state/id update: key=%v value()=%v client=%v",
					pelem[1].Key["name"], update.Val.GetUintVal(), c.addr.String())
			}
		}
	}
}
