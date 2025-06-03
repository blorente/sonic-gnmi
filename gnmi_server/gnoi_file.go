package gnmi

import (
	"context"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	gnoi_file_pb "github.com/openconfig/gnoi/file"
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

func ReadFileStat(path string) (*gnoi_file_pb.StatInfo, error) {
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}

	log.V(2).Infof("Reading file stat at path %s...", path)
	data, err := sc.GetFileStat(path)
	if err != nil {
		log.V(2).Infof("Failed to read file stat at path %s: %v. Error ", path, err)
		return nil, err
	}
	// Parse the data and populate StatInfo
	lastModified, err := strconv.ParseUint(data["last_modified"], 10, 64)
	if err != nil {
		return nil, err
	}

	permissions, err := strconv.ParseUint(data["permissions"], 8, 32)
	if err != nil {
		return nil, err
	}

	size, err := strconv.ParseUint(data["size"], 10, 64)
	if err != nil {
		return nil, err
	}

	umaskStr := data["umask"]
	if strings.HasPrefix(umaskStr, "o") {
		umaskStr = umaskStr[1:] // Remove leading "o"
	}
	umask, err := strconv.ParseUint(umaskStr, 8, 32)
	if err != nil {
		return nil, err
	}

	statInfo := &gnoi_file_pb.StatInfo{
		Path:         data["path"],
		LastModified: lastModified,
		Permissions:  uint32(permissions),
		Size:         size,
		Umask:        uint32(umask),
	}
	return statInfo, nil
}

// Get RPC is unimplemented.
func (srv *GNOIFileServer) Get(req *gnoi_file_pb.GetRequest, stream gnoi_file_pb.File_GetServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Get RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI file Get RPC disabled since NSF is ongoing!")
	}
	return status.Errorf(codes.Unimplemented, "Method file.Get is unimplemented.")
}

// TransferToRemote RPC is unimplemented.
func (srv *GNOIFileServer) TransferToRemote(ctx context.Context, req *gnoi_file_pb.TransferToRemoteRequest) (*gnoi_file_pb.TransferToRemoteResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file TransferToRemote RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI file TransferToRemote RPC disabled since NSF is ongoing!")
	}
	return nil, status.Errorf(codes.Unimplemented, "Method file.TransferToRemote is unimplemented.")
}

// Put RPC is unimplemented.
func (srv *GNOIFileServer) Put(stream gnoi_file_pb.File_PutServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Put RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI file Put RPC disabled since NSF is ongoing!")
	}
	return status.Errorf(codes.Unimplemented, "Method file.Put is unimplemented.")
}

// Stat RPC is unimplemented.
func (srv *GNOIFileServer) Stat(ctx context.Context, req *gnoi_file_pb.StatRequest) (*gnoi_file_pb.StatResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI file Stat RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI file Stat RPC disabled since NSF is ongoing!")
	}

	_, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	path := req.GetPath()
	log.V(1).Info("gNOI: Read File Stat")
	log.V(1).Info("Request: ", req)
	statInfo, err := ReadFileStat(path)
	if err != nil {
		return nil, err
	}
	resp := &gnoi_file_pb.StatResponse{
		Stats: []*gnoi_file_pb.StatInfo{statInfo},
	}
	return resp, nil
}

// Remove implements the corresponding RPC.
func (srv *GNOIFileServer) Remove(ctx context.Context, req *gnoi_file_pb.RemoveRequest) (*gnoi_file_pb.RemoveResponse, error) {
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
	return &gnoi_file_pb.RemoveResponse{}, err
}
