package gnmi

import (
	"context"

	log "github.com/golang/glog"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	"github.com/openconfig/gnoi/file"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// GNOIFileServer implements the gnoi.file.File service.
type GNOIFileServer struct {
	*Server
}

// NewGNOIFileServer returns initialized GNOIFileServer structure.
func NewGNOIFileServer(srv *Server) *GNOIFileServer {
	return &GNOIFileServer{
		Server: srv,
	}
}

// Get RPC is unimplemented.
func (srv *GNOIFileServer) Get(req *file.GetRequest, stream file.File_GetServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Get RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI file Get RPC disabled since NSF is ongoing!")
	}
	return status.Errorf(codes.Unimplemented, "Method file.Get is unimplemented.")
}

// TrancontrollerrToRemote RPC is unimplemented.
func (srv *GNOIFileServer) TrancontrollerrToRemote(ctx context.Context, req *file.TrancontrollerrToRemoteRequest) (*file.TrancontrollerrToRemoteResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file TrancontrollerrToRemote RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI file TrancontrollerrToRemote RPC disabled since NSF is ongoing!")
	}
	return nil, status.Errorf(codes.Unimplemented, "Method file.TrancontrollerrToRemote is unimplemented.")
}

// Put RPC is unimplemented.
func (srv *GNOIFileServer) Put(stream file.File_PutServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Put RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI file Put RPC disabled since NSF is ongoing!")
	}
	return status.Errorf(codes.Unimplemented, "Method file.Put is unimplemented.")
}

// Stat RPC is unimplemented.
func (srv *GNOIFileServer) Stat(ctx context.Context, req *file.StatRequest) (*file.StatResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Stat RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI file Stat RPC disabled since NSF is ongoing!")
	}
	return nil, status.Errorf(codes.Unimplemented, "Method file.Stat is unimplemented.")
}

// Remove implements the corresponding RPC.
func (srv *GNOIFileServer) Remove(ctx context.Context, req *file.RemoveRequest) (*file.RemoveResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Remove RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI file Remove RPC disabled since NSF is ongoing!")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid nil request.")
	}
	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)
	if req.GetRemoteFile() == "" {
		return nil, status.Error(codes.InvalidArgument, "Invalid request: remote_file field is empty.")
	}
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	_, err = sc.FileRemove(req.GetRemoteFile())
	return &file.RemoveResponse{}, err
}
