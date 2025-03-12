package gnmi

import (
	"container/ring"
	"errors"
	"net"
	"os"
	"runtime"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	spb "github.com/sonic-net/sonic-gnmi/proto/gnmi_sonic"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/prototext"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type debugHandler struct {
	Mu       sync.Mutex
	Consumer *common_utils.NotificationConsumer
	Producer *common_utils.NotificationProducer
}

type reqTimes struct {
	mu    sync.Mutex
	ringL *ring.Ring
}

// CreateDebugHandler creates the debugHandler necessary for serving debug data
func (s *Server) CreateDebugHandler() (*debugHandler, error) {
	var err error
	debugHandler := new(debugHandler)
	if debugHandler.Consumer, err = common_utils.NewNotificationConsumer("DEBUG_DATA_REQ_CHANNEL", s.ServeDebugData); err != nil {
		return nil, err
	}
	if debugHandler.Producer, err = common_utils.NewNotificationProducer("DEBUG_DATA_RESP_CHANNEL"); err != nil {
		return nil, err
	}
	return debugHandler, nil
}

// Close closes the notification consumer/producer associated with the debugHandler
func (dh *debugHandler) Close() {
	dh.Mu.Lock()
	defer dh.Mu.Unlock()
	dh.Consumer.Close()
	dh.Producer.Close()
}

// ServeDebugData handles new debug data requests, writes the data
// to the specified file, and publishes the status to DEBUG_DATA_RESP_CHANNEL
func (s *Server) ServeDebugData(msg *redis.Message) {
	log.V(lvl.DEBUG).Infof("ServeDebugData received message: %v %v", msg.Channel, msg.Payload)

	// Only process one request at a time
	s.debugHandler.Mu.Lock()
	defer s.debugHandler.Mu.Unlock()

	component, dir, payload, err := processMsgPayload(msg.Payload)
	if err != nil {
		log.V(lvl.ERROR).Infof("ServeDebugData: Failed to process payload - %v", err)
		return
	}
	if component != "telemetry" {
		return
	}
	level, ok := payload["level"]
	if !ok {
		log.V(lvl.ERROR).Infof("ServeDebugData: Failed to get request level - %v", err)
		// Assume the level is all
		level = "all"
	}
	if err := s.WriteDebugData(dir, level); err != nil {
		log.V(lvl.ERROR).Infof("ServeDebugData: Failed to write debug data - %v", err)
		s.debugHandler.Producer.Send(component, dir, map[string]string{"status": "fail", "err_str": err.Error()})
		return
	}
	log.V(lvl.DEBUG).Infof("ServeDebugData: Successfully wrote debug data to %v", dir)
	s.debugHandler.Producer.Send(component, dir, map[string]string{"status": "success", "err_str": ""})
}

// WriteDebugData gets the appropriate debug data and writes it to fileName
func (s *Server) WriteDebugData(dir string, level string) error {
	debugInfo := spb.UmfDebugInfo{}
	switch level {
	case "all":
		fallthrough
	case "critical":
		// Get active subscriptions info
		debugInfo.SubscriptionClientInfo = s.subscriptionInfo()
		if s.getReqTimes != nil {
			debugInfo.GetRequestTimingInfo = s.getReqTimes.readReqTimes()
		}
		if s.setReqTimes != nil {
			debugInfo.SetRequestTimingInfo = s.setReqTimes.readReqTimes()
		}
		debugInfo.DbCounters = readDbCounters()
		debugInfo.NumGoroutines = uint32(runtime.NumGoroutine())
		debugInfo.GoRuntimeMetric = runtimeMetrics()
		if s.ConnectionManager != nil {
			debugInfo.ConnectionManagerStats = s.ConnectionManager.Stats()
		}
		fallthrough
	case "alert":
		break
	default:
		return errors.New("Invalid level requested: " + level)
	}

	file, err := os.Create(dir + "/debug_data.txt")
	if err != nil {
		return err
	}
	defer file.Close()
	out, err := prototext.Marshal(&debugInfo)
	if err != nil {
		return err
	}
	if _, err = file.Write(out); err != nil {
		return err
	}
	return nil
}

func (s *Server) subscriptionInfo() []*spb.GnmiSubscriptionClientInfo {
	var info []*spb.GnmiSubscriptionClientInfo
	s.cMu.Lock()
	defer s.cMu.Unlock()

	for _, client := range s.clients {
		info = append(info, client.Info())
	}
	return info
}

func (rt *reqTimes) recordReqTime(pr *peer.Peer, start time.Time) {
	end := time.Now()
	ip, port := ipAndPortFromPeer(pr)

	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ringL = rt.ringL.Next()
	rt.ringL.Value = &spb.GnmiRequestTimingInfo{
		IpAddress:     ip,
		TcpSourcePort: port,
		StartTime:     timestamppb.New(start),
		EndTime:       timestamppb.New(end),
	}
}

func ipAndPortFromPeer(pr *peer.Peer) (string, uint64) {
	ip := "unknown"
	var port uint64

	if pr != nil && pr.Addr != nil {
		addr := pr.Addr
		switch addr.(type) {
		case *net.UDPAddr:
			ip = addr.(*net.UDPAddr).IP.String()
			port = uint64(addr.(*net.UDPAddr).Port)
		case *net.TCPAddr:
			ip = addr.(*net.TCPAddr).IP.String()
			port = uint64(addr.(*net.TCPAddr).Port)
		default:
		}
	}

	return ip, port
}

func (rt *reqTimes) readReqTimes() []*spb.GnmiRequestTimingInfo {
	var result []*spb.GnmiRequestTimingInfo
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ringL.Do(func(info any) {
		if info != nil && info.(*spb.GnmiRequestTimingInfo) != nil {
			result = append(result, info.(*spb.GnmiRequestTimingInfo))
		}
	})
	return result
}

func readDbCounters() *spb.DbCounters {
	result := &spb.DbCounters{}
	counters := db.RedisClientManagerCounters()
	result.CurrentTransactionalClients = counters.CurTransactionalClients
	result.TotalPoolClientsRequested = counters.TotalPoolClientsRequested
	result.TotalTransactionalClientsRequested = counters.TotalTransactionalClientsRequested
	for db, stats := range counters.PoolStatsPerDB {
		if db == "" || stats == nil {
			continue
		}
		result.DbPoolStats = append(result.DbPoolStats, &spb.DbConnectionPoolStats{
			DbName:                 db,
			Hits:                   stats.Hits,
			Misses:                 stats.Misses,
			Timeouts:               stats.Timeouts,
			TotalActiveConnections: stats.TotalConns,
			IdleConnections:        stats.IdleConns,
			StaleConnections:       stats.StaleConns,
		})
	}
	return result
}

func runtimeMetrics() []*spb.GoRuntimeMetric {
	goMetrics := []*spb.GoRuntimeMetric{}

	// Get descriptions for all supported metrics.
	descs := metrics.All()

	// Create a sample for each metric.
	samples := make([]metrics.Sample, len(descs))
	for i := range samples {
		samples[i].Name = descs[i].Name
	}

	// Sample the metrics.
	metrics.Read(samples)

	// Iterate over all results.
	for _, sample := range samples {
		// Pull out the name and value.
		name, value := sample.Name, sample.Value

		// Handle each sample.
		switch value.Kind() {
		case metrics.KindUint64:
			goMetrics = append(goMetrics, &spb.GoRuntimeMetric{
				Name:  name,
				Value: &spb.GoRuntimeMetric_Uint64Val{Uint64Val: value.Uint64()},
			})
		case metrics.KindFloat64:
			goMetrics = append(goMetrics, &spb.GoRuntimeMetric{
				Name:  name,
				Value: &spb.GoRuntimeMetric_DoubleVal{DoubleVal: value.Float64()},
			})
		case metrics.KindFloat64Histogram:
			// For histograms, we report the median.
			goMetrics = append(goMetrics, &spb.GoRuntimeMetric{
				Name:  name,
				Value: &spb.GoRuntimeMetric_DoubleVal{DoubleVal: medianBucket(value.Float64Histogram())},
			})
		case metrics.KindBad:
			log.V(lvl.ERROR).Infof("Error reading %s metric: %v", name, value)
		default:
			log.V(lvl.DEBUG).Infof("%s: unexpected metric Kind: %v", name, value.Kind())
		}
	}

	return goMetrics
}

func medianBucket(h *metrics.Float64Histogram) float64 {
	total := uint64(0)
	for _, count := range h.Counts {
		total += count
	}
	median := float64(0)
	thresh := total / 2
	total = 0
	for i, count := range h.Counts {
		total += count
		if total >= thresh {
			median = h.Buckets[i]
			break
		}
	}
	return median
}
