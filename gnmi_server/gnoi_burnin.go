package gnmi

import (
	"context"
	"fmt"
	"strings"

	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	burnin_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/burnin"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	json "google.golang.org/protobuf/encoding/protojson"
)

const (
	maxBurninDurationSecs uint64 = 8400 // 2.3hr
)

func validateStartReq(req *burnin_pb.StartBurninRequest) error {
	var errs []string

	if req.GetDurationSeconds() > maxBurninDurationSecs {
		errs = append(errs, fmt.Sprintf("Duration %d is too long.", req.GetDurationSeconds()))
	}
	if req.GetBurninOperationId() == "" {
		errs = append(errs, fmt.Sprintf("Burnin operation ID is empty."))
	}
	if req.GetOnlineBurnin() {
		errs = append(errs, fmt.Sprintf("Online burnin operation has not been implemented yet."))
	}
	if len(errs) > 0 {
		log.V(lvl.ERROR).Infof("Invalid request: %v\n", strings.Join(errs, ", "))
		return fmt.Errorf("Invalid request: %v", strings.Join(errs, ", "))
	}

	return nil
}

// StartBurnin implements the corresponding RPC.
func (srv *Server) StartBurnin(ctx context.Context, req *burnin_pb.StartBurninRequest) (*burnin_pb.StartBurninResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox StartBurnin RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox StartBurnin RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	if srv.SsHelper.IsSystemCritical() {
		return nil, status.Errorf(codes.Internal, "System is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}

	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)

	log.V(lvl.INFO).Info("gNOI: StartBurnin")

	if err = validateStartReq(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	reqStr, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	if _, err = sc.BurninStart(string(reqStr)); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &burnin_pb.StartBurninResponse{}, nil
}

// StopBurnin implements the corresponding RPC.
func (srv *Server) StopBurnin(ctx context.Context, req *burnin_pb.StopBurninRequest) (*burnin_pb.StopBurninResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox StopBurnin RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox StopBurnin RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	if srv.SsHelper.IsSystemCritical() {
		return nil, status.Errorf(codes.Internal, "System is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}

	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)

	log.V(lvl.INFO).Info("gNOI: StopBurnin")

	if req.GetBurninOperationId() == "" {
		log.V(lvl.ERROR).Infof("Burnin operation ID is empty.")
		return nil, status.Error(codes.InvalidArgument, "Invalid request: Burnin operation ID is empty.")
	}

	reqStr, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	if _, err = sc.BurninStop(string(reqStr)); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &burnin_pb.StopBurninResponse{}, nil
}

// GetBurninResult implements the corresponding RPC.
func (srv *Server) GetBurninResult(ctx context.Context, req *burnin_pb.GetBurninResultRequest) (*burnin_pb.GetBurninResultResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox GetBurninResult RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox GetBurninResult RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	if srv.SsHelper.IsSystemCritical() {
		return nil, status.Errorf(codes.Internal, "System is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}

	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)

	log.V(lvl.INFO).Info("gNOI: GetBurninResult")

	if req.GetBurninOperationId() == "" {
		log.V(lvl.ERROR).Infof("Burnin operation ID is empty.")
		return nil, status.Error(codes.InvalidArgument, "Invalid request: Burnin operation ID is empty.")
	}

	reqStr, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	respStr, err := sc.BurninResults(string(reqStr))
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	resp := &burnin_pb.GetBurninResultResponse{}
	if err := json.Unmarshal([]byte(respStr), resp); err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Cannot unmarshal the response: [%s].", respStr))
	}

	return resp, nil
}
