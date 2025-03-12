package gnmi

import (
	"context"
	"strings"
	"sync"
	"time"

	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	debug_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/debug"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	debugRateLimit            = 1 * time.Second
	tunnelResponseStatusError = uint32(1)
	tunnelResponseStatusOK    = uint32(0)
)

type GNOIDebugServer struct {
	*Server
	mu sync.Mutex

	debug_pb.UnimplementedDebugServer
}

// TunnelCommand executes a command on the device.
// It also performs rate limiting and command validation.
func (srv *GNOIDebugServer) TunnelCommand(ctx context.Context, req *debug_pb.TunnelCommandRequest) (*debug_pb.TunnelCommandResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI debug TunnelCommand RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI debug TunnelCommand RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)

	log.V(lvl.INFO).Info("GNOI debug: TunnelCommand")
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Validate command
	// Currently we only allow "show" command.
	if req.GetCommand() != "show" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid command")
	}
	elems := []string{req.GetCommand()}
	for _, a := range req.GetArg() {
		elems = append(elems, string(a))
	}
	for _, e := range elems {
		// We do not allow pipe and redirect.
		if strings.ContainsAny(e, " `;&|<>()") {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid command")
		}
	}

	// Add rate limit to the mutex unlock time.
	defer time.Sleep(debugRateLimit)

	resp := debug_pb.TunnelCommandResponse{}
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}

	dbusResp, err := sc.DebugTunnel(strings.Join(elems, " "))
	if err != nil {
		resp.Status = tunnelResponseStatusError
		resp.Stderr = []byte(err.Error())
	} else {
		resp.Status = tunnelResponseStatusOK
		resp.Stdout = []byte(dbusResp)
	}
	return &resp, nil
}
