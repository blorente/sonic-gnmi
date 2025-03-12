package gnmi

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	ospb "github.com/openconfig/gnoi/os"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	json "google.golang.org/protobuf/encoding/protojson"
)

var (
	sem sync.Mutex
)

// OSServer implements gnoi.os.OS service.
type OSServer struct {
	*Server
	ProcessTrfReady func(req string) (string, error)
	ProcessTrfEnd   func(req string) (string, error)
}

// ProcessInstallFromBackEnd makes call via the sonic-host-service.
func ProcessInstallFromBackEnd(req string) (string, error) {
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return "", err
	}
	return sc.OSInstall(req)
}

func handleErrorResponse(f string, a ...any) *ospb.InstallResponse {
	errStr := fmt.Sprintf(f, a...)
	log.V(lvl.ERROR).Infoln(errStr)
	return &ospb.InstallResponse{
		Response: &ospb.InstallResponse_InstallError{
			InstallError: &ospb.InstallError{
				Detail: errStr,
			},
		},
	}
}

func (srv *OSServer) processTrancontrollerrReq(req *ospb.InstallRequest) *ospb.InstallResponse {
	trfReq := req.GetTrancontrollerrRequest()
	if trfReq.GetVersion() == "" {
		log.V(lvl.ERROR).Infoln("TrancontrollerrRequest must contain a valid OS version.")
		return &ospb.InstallResponse{
			Response: &ospb.InstallResponse_InstallError{
				InstallError: &ospb.InstallError{
					Type:   ospb.InstallError_PARSE_FAIL,
					Detail: "TrancontrollerrRequest must contain a valid OS version.",
				},
			},
		}
	}

	// Front end marshals the request, and sends to the sonic-host-service.
	// Back end is expected to return the response in JSON format.
	reqStr, err := json.Marshal(req)
	if err != nil {
		return handleErrorResponse("Failed to marshal TrancontrollerrReady JSON: err: %v, req: %v, reqStr: %v", err, req, reqStr)
	}

	respStr, err := srv.ProcessTrfReady(string(reqStr))
	if err != nil {
		return handleErrorResponse("Received error from OSServer.TrancontrollerrReady: err: %v, reqStr: %v, respStr: %v", err, reqStr, respStr)
	}

	resp := &ospb.InstallResponse{}
	if err := json.Unmarshal([]byte(respStr), resp); err != nil {
		return handleErrorResponse("Failed to unmarshal TrancontrollerrReady JSON: err: %v, respStr: %v", err, respStr)
	}

	return resp
}

func (srv *OSServer) processTrancontrollerrEnd(req *ospb.InstallRequest) *ospb.InstallResponse {

	// Front end marshals the request, and sends to the sonic-host-service.
	// Back end is expected to return the response in JSON format.
	reqStr, err := json.Marshal(req)
	if err != nil {
		return handleErrorResponse("Failed to marshal TrancontrollerrEnd JSON: err: %v, req: %v, reqStr: %v", err, req, reqStr)
	}

	respStr, err := srv.ProcessTrfEnd(string(reqStr))
	if err != nil {
		return handleErrorResponse("Received error from OSServer.TrancontrollerrEnd: err: %v, reqStr: %v, respStr: %v", err, reqStr, respStr)
	}

	resp := &ospb.InstallResponse{}
	if err := json.Unmarshal([]byte(respStr), resp); err != nil {
		return handleErrorResponse("Failed to unmarshal TrancontrollerrEnd JSON: err: %v, respStr: %v", err, respStr)
	}

	return resp
}

func (srv *OSServer) processTrancontrollerrContent(trfCnt []byte, imgPath string) *ospb.InstallResponse {
	errResp := &ospb.InstallResponse{
		Response: &ospb.InstallResponse_InstallError{
			InstallError: &ospb.InstallError{
				Detail: fmt.Sprintf("Failed to open file [%s].", imgPath),
			},
		},
	}

	// If the file doesn't exist, create it, or append to the file
	f, err := os.OpenFile(imgPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.V(lvl.ERROR).Infoln(err)
		return errResp
	}
	if _, err := f.Write(trfCnt); err != nil {
		f.Close()
		log.V(lvl.ERROR).Infoln(err)
		return errResp
	}
	if err := f.Close(); err != nil {
		log.V(lvl.ERROR).Infoln(err)
		return errResp
	}

	return &ospb.InstallResponse{
		Response: &ospb.InstallResponse_TrancontrollerrProgress{
			TrancontrollerrProgress: &ospb.TrancontrollerrProgress{
				BytesReceived: uint64(len(trfCnt)),
			},
		},
	}
}

func (srv *OSServer) getVersionPath(version string) string {
	return srv.config.OSCfg.ImgDir + "/" + version
}

func (srv *OSServer) imageExists(path string) bool {
	if _, err := os.Lstat(path); err == nil {
		return true
	}
	return false
}

func (srv *OSServer) removeIncompleteTrf(imgPath string) {
	if !srv.imageExists(imgPath) {
		return
	}
	log.V(lvl.INFO).Infoln("Remove incomplete image: ", imgPath)
	// Now remove the file.
	if err := os.Remove(imgPath); err != nil {
		log.V(lvl.ERROR).Infoln("Failed to remove incomplete image: ", err)
	}
}

// TODO(b/328077908) Alarms to be implemented later
// func raiseAlarm(err error) {
// 	csh, cshErr := common_utils.NewComponentStateHelper(common_utils.Telemetry)
// 	if cshErr != nil {
// 		log.V(lvl.ERROR).Infof("gNOI OS: failed to create new ComponentStateHelper - %v", cshErr)
// 		return
// 	}
// 	defer csh.Close()

// 	if rcsErr := csh.ReportComponentState(common_utils.ComponentMinor, err.Error()); rcsErr != nil {
// 		log.V(lvl.ERROR).Infof("Failed to raise ComponentMinor Alarm: %v", rcsErr)
// 	}
// }

// Install implements correspondig RPC
func (srv *OSServer) Install(stream ospb.OS_InstallServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI OS Install RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI OS Install RPC disabled since NSF is ongoing!")
	}
	ctx := stream.Context()
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("gNOI: os.Install")

	// Concurrent Install RPCs are not allowed.
	if !sem.TryLock() {
		log.V(lvl.ERROR).Infoln("Concurrent Install RPCs are not allowed.")

		// Send InstallError response.
		err = stream.Send(&ospb.InstallResponse{
			Response: &ospb.InstallResponse_InstallError{
				InstallError: &ospb.InstallError{
					Type:   ospb.InstallError_INSTALL_IN_PROGRESS,
					Detail: "Concurrent Install RPCs are not allowed.",
				},
			},
		})
		if err != nil {
			log.V(lvl.ERROR).Infoln("Error while sending InstallError response: ", err)
			return status.Errorf(codes.Aborted, err.Error())
		}

		return status.Errorf(codes.Aborted, "Concurrent Install RPCs are not allowed.")
	}
	defer sem.Unlock()

	// Receive TrancontrollerrReq message.
	req, err := stream.Recv()
	if err == io.EOF {
		log.V(lvl.ERROR).Infoln("Received EOF instead of TrancontrollerrRequest!")
		return nil
	}
	if err != nil {
		log.V(lvl.ERROR).Infoln("Received error: ", err)
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return status.Errorf(codes.Aborted, err.Error())
	}

	trfReq := req.GetTrancontrollerrRequest()
	if trfReq == nil {
		log.V(lvl.ERROR).Infoln("Did not receive a TrancontrollerrRequest.")
		err = status.Errorf(codes.InvalidArgument, "Expected TrancontrollerrRequest.")
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return err
	}

	resp := srv.processTrancontrollerrReq(req)
	if resp != nil {
		if err := stream.Send(resp); err != nil {
			log.V(lvl.ERROR).Infoln("Error while sending response: ", err)
			// TODO(b/328077908) Alarms to be implemented later
			// raiseAlarm(err)
			return status.Errorf(codes.Aborted, err.Error())
		}
	}
	if resp == nil || resp.GetInstallError() != nil {
		err = status.Errorf(codes.Aborted, "Failed to process TrancontrollerrRequest.")
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return err
	}

	imgPath := srv.getVersionPath(trfReq.GetVersion())
	imgTrfInitiated := false
	for {
		req, err = stream.Recv()
		if err == io.EOF {
			log.V(lvl.INFO).Infoln("Received EOF instead of TrancontrollerrContent request!")
			if imgTrfInitiated {
				srv.removeIncompleteTrf(imgPath)
			}
			return nil
		}
		if err != nil {
			log.V(lvl.ERROR).Infoln("Received error: ", err)
			if imgTrfInitiated {
				srv.removeIncompleteTrf(imgPath)
			}
			// TODO(b/328077908) Alarms to be implemented later
			// raiseAlarm(err)
			return status.Errorf(codes.Aborted, err.Error())
		}

		if trfReq := req.GetTrancontrollerrRequest(); trfReq != nil {
			log.V(lvl.ERROR).Infoln("Received a TrancontrollerrReq out-of-sequence.")
			if imgTrfInitiated {
				srv.removeIncompleteTrf(imgPath)
			}
			err = status.Errorf(codes.InvalidArgument, "Expected TrancontrollerrContent, or TrancontrollerrEnd.")
			// TODO(b/328077908) Alarms to be implemented later
			// raiseAlarm(err)
			return err
		}

		// Trancontrollerrring content is complete.
		if trfEnd := req.GetTrancontrollerrEnd(); trfEnd != nil {
			break
		}

		// Process content trancontrollerr.
		// If image exists, target should have sent Validated | InstallError on TrancontrollerrRequest.
		if !imgTrfInitiated && srv.imageExists(imgPath) {
			resp := &ospb.InstallResponse{
				Response: &ospb.InstallResponse_InstallError{
					InstallError: &ospb.InstallError{
						Detail: fmt.Sprintf("File exists [%s]!", imgPath),
					},
				},
			}
			if err := stream.Send(resp); err != nil {
				log.V(lvl.ERROR).Infoln("Error while sending response: ", err)
			}
			err = status.Errorf(codes.Aborted, "Failed as image exists!")
			// TODO(b/328077908) Alarms to be implemented later
			// raiseAlarm(err)
			return err
		}

		imgTrfInitiated = true
		resp := srv.processTrancontrollerrContent(req.GetTrancontrollerrContent(), imgPath)
		if resp != nil {
			if err := stream.Send(resp); err != nil {
				log.V(lvl.ERROR).Infoln("Error while sending response: ", err)
				srv.removeIncompleteTrf(imgPath)
				// TODO(b/328077908) Alarms to be implemented later
				// raiseAlarm(err)
				return status.Errorf(codes.Aborted, err.Error())
			}
		}
		if resp == nil || resp.GetInstallError() != nil {
			srv.removeIncompleteTrf(imgPath)
			err = status.Errorf(codes.Aborted, "Failed to process TrancontrollerrContent.")
			// TODO(b/328077908) Alarms to be implemented later
			// raiseAlarm(err)
			return err
		}
	}

	// Receive TrancontrollerrEnd message.
	trfEnd := req.GetTrancontrollerrEnd()
	if trfEnd == nil {
		log.V(lvl.ERROR).Infoln("Did not receive a TrancontrollerrEnd")
		srv.removeIncompleteTrf(imgPath)
		err = status.Errorf(codes.InvalidArgument, "Expected TrancontrollerrEnd")
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return err
	}

	resp = srv.processTrancontrollerrEnd(req)
	if resp != nil {
		if err := stream.Send(resp); err != nil {
			log.V(lvl.ERROR).Infoln("Error while sending response: ", err)
			srv.removeIncompleteTrf(imgPath)
			// TODO(b/328077908) Alarms to be implemented later
			// raiseAlarm(err)
			return status.Errorf(codes.Aborted, err.Error())
		}
	}
	if resp == nil || resp.GetInstallError() != nil {
		srv.removeIncompleteTrf(imgPath)
		err = status.Errorf(codes.Aborted, "Failed to process TrancontrollerrEnd.")
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return err
	}

	log.V(lvl.DEBUG).Info("OS.Install is complete.")
	return nil
}

// Activate implements correspondig RPC
func (srv *OSServer) Activate(ctx context.Context, req *ospb.ActivateRequest) (*ospb.ActivateResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI OS Activate RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI OS Activate RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.DEBUG).Info("gNOI: os.Activate")
	// Front end marshals the request, and sends to the sonic-host-service.
	// Back end is expected to return the response in JSON format.
	reqStr, err := json.Marshal(req)
	if err != nil {
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Cannot marshal the Activate request: [%s].", req.String()))
	}

	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}

	respStr, err := sc.OSActivate(string(reqStr))
	if err != nil {
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	resp := &ospb.ActivateResponse{}
	if err := json.Unmarshal([]byte(respStr), resp); err != nil {
		// TODO(b/328077908) Alarms to be implemented later
		// raiseAlarm(err)
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Cannot unmarshal the Activate response: [%s].", respStr))
	}
	return resp, nil
}

// Verify implements correspondig RPC
func (srv *OSServer) Verify(ctx context.Context, req *ospb.VerifyRequest) (*ospb.VerifyResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI OS Verify RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI OS Verify RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.DEBUG).Info("gNOI: os.Verify")
	// Front end marshals the request, and sends to the sonic-host-service.
	// Back end is expected to return the response in JSON format.
	reqStr, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Cannot marshal the Verify request: [%s].", req.String()))
	}

	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	respStr, err := sc.OSVerify(string(reqStr))
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	resp := &ospb.VerifyResponse{}
	if err := json.Unmarshal([]byte(respStr), resp); err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Cannot unmarshal the Verify response: [%s].", respStr))
	}
	return resp, nil
}
