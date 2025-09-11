package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/google/gnxi/utils/credentials"
	bbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/blackbox"
	burnin_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/burnin"
	ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"
	qual_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/qualification"
	dpb "github.com/openconfig/gnoi/diag"
	healthz_pb "github.com/openconfig/gnoi/healthz"
	factory_reset_pb "github.com/openconfig/gnoi/factory_reset"
	file_pb "github.com/openconfig/gnoi/file"
	system_pb "github.com/openconfig/gnoi/system"
	spb "github.com/sonic-net/sonic-gnmi/proto/sonic_gnoi"
	spb_jwt "github.com/sonic-net/sonic-gnmi/proto/sonic_gnoi/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	module     = flag.String("module", "System", "gNOI Module")
	rpc        = flag.String("rpc", "Time", "rpc call in specified module to call")
	target     = flag.String("target", "localhost:9339", "Address:port of gNOI Server")
	jsonArgs   = flag.String("jsonin", "", "RPC Arguments in json format")
	protoArgs  = flag.String("protoin", "", "RPC arguments in proto format")
	intf       = flag.String("intf", "", "Interface name (ex. EthernetX)")
	xcvr       = flag.String("xcvr", "", "Xcvr name (ex. EthernetX)")
	nsf        = flag.String("nsf", "", "NSF Debug Data")
	jwtToken   = flag.String("jwt_token", "", "JWT Token if required")
	targetName = flag.String("target_name", "hostname.com", "The target name use to verify the hostname returned by TLS handshake")
)

func setUserCreds(ctx context.Context) context.Context {
	if len(*jwtToken) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, "access_token", *jwtToken)
	}
	return ctx
}
func main() {
	flag.Parse()
	opts := credentials.ClientCredentials(*targetName)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		cancel()
	}()
	conn, err := grpc.Dial(*target, opts...)
	if err != nil {
		panic(err.Error())
	}

	switch *module {
	case "BlackBox":
		bc := bbpb.NewBlackBoxTestClient(conn)
		switch *rpc {
		case "SetTransceiverState":
			setTransceiverState(bc, ctx)
		case "SetHardwareLinkState":
			setHardwareLinkState(bc, ctx)
		case "SetAlarm":
			setAlarm(bc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Burnin":
		bc := burnin_pb.NewBurninClient(conn)
		switch *rpc {
		case "StartBurnin":
			startBurnin(bc, ctx)
		case "StopBurnin":
			stopBurnin(bc, ctx)
		case "GetBurninResult":
			getBurninResult(bc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Diag":
		dc := dpb.NewDiagClient(conn)
		switch *rpc {
		case "StartBERT":
			startBert(dc, ctx)
		case "StopBERT":
			stopBert(dc, ctx)
		case "GetBERTResult":
			getBertResult(dc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Healthz":
		hc := healthz_pb.NewHealthzClient(conn)
		switch *rpc {
		case "Get":
			getHealth(hc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Lds":
		lc := ocs_pb.NewLdsClient(conn)
		switch *rpc {
		case "LdsCommand":
			ldsCommand(lc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Ocs":
		oc := ocs_pb.NewOcsClient(conn)
		switch *rpc {
		case "SetConnections":
			setConnections(oc, ctx)
		case "GetConnections":
			getConnections(oc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Qual":
		qc := qual_pb.NewPacketLinkQualClient(conn)
		switch *rpc {
		case "StartPktQual":
			startPktQual(qc, ctx)
		case "StopPktQual":
			stopPktQual(qc, ctx)
		case "GetPktQualResult":
			getPktQualResult(qc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "System":
		sc := system_pb.NewSystemClient(conn)
		switch *rpc {
		case "Time":
			systemTime(sc, ctx)
		case "Reboot":
			systemReboot(sc, ctx)
		case "CancelReboot":
			systemCancelReboot(sc, ctx)
		case "RebootStatus":
			systemRebootStatus(sc, ctx)
		case "KillProcess":
			killProcess(sc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "FactoryReset":
		frc := factory_reset_pb.NewFactoryResetClient(conn)
		switch *rpc {
		case "Start":
			startFactoryReset(frc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "File":
		fc := file_pb.NewFileClient(conn)
		switch *rpc {
		case "Stat":
			fileStat(fc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	case "Sonic":
		switch *rpc {
		case "showtechsupport":
			sc := spb.NewSonicServiceClient(conn)
			sonicShowTechSupport(sc, ctx)
		case "copyConfig":
			sc := spb.NewSonicServiceClient(conn)
			copyConfig(sc, ctx)
		case "authenticate":
			sc := spb_jwt.NewSonicJwtServiceClient(conn)
			authenticate(sc, ctx)
		case "imageInstall":
			sc := spb.NewSonicServiceClient(conn)
			imageInstall(sc, ctx)
		case "imageDefault":
			sc := spb.NewSonicServiceClient(conn)
			imageDefault(sc, ctx)
		case "imageRemove":
			sc := spb.NewSonicServiceClient(conn)
			imageRemove(sc, ctx)
		case "refresh":
			sc := spb_jwt.NewSonicJwtServiceClient(conn)
			refresh(sc, ctx)
		case "clearNeighbors":
			sc := spb.NewSonicServiceClient(conn)
			clearNeighbors(sc, ctx)
		default:
			panic("Invalid RPC Name")
		}
	default:
		panic("Invalid Module Name")
	}

}

func killProcess(sc system_pb.SystemClient, ctx context.Context) {
	fmt.Println("Kill Process with optional restart")
	ctx = setUserCreds(ctx)
	req := &system_pb.KillProcessRequest{}
	err := json.Unmarshal([]byte(*jsonArgs), req)
	if err != nil {
		panic(err.Error())
	}
	_, err = sc.KillProcess(ctx, req)
	if err != nil {
		panic(err.Error())
	}
}

func fileStat(fc file_pb.FileClient, ctx context.Context) {
	fmt.Println("File Stat")
	ctx = setUserCreds(ctx)
	req := &file_pb.StatRequest{}
	err := json.Unmarshal([]byte(*jsonArgs), req)
	if err != nil {
		panic(err.Error())
	}
	resp, err := fc.Stat(ctx, req)
	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}

func sonicShowTechSupport(sc spb.SonicServiceClient, ctx context.Context) {
	fmt.Println("Sonic ShowTechsupport")
	ctx = setUserCreds(ctx)
	req := &spb.TechsupportRequest{
		Input: &spb.TechsupportRequest_Input{},
	}

	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.ShowTechsupport(ctx, req)
	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}

func copyConfig(sc spb.SonicServiceClient, ctx context.Context) {
	fmt.Println("Sonic CopyConfig")
	ctx = setUserCreds(ctx)
	req := &spb.CopyConfigRequest{
		Input: &spb.CopyConfigRequest_Input{},
	}
	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.CopyConfig(ctx, req)

	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}
func imageInstall(sc spb.SonicServiceClient, ctx context.Context) {
	fmt.Println("Sonic ImageInstall")
	ctx = setUserCreds(ctx)
	req := &spb.ImageInstallRequest{
		Input: &spb.ImageInstallRequest_Input{},
	}
	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.ImageInstall(ctx, req)

	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}
func imageRemove(sc spb.SonicServiceClient, ctx context.Context) {
	fmt.Println("Sonic ImageRemove")
	ctx = setUserCreds(ctx)
	req := &spb.ImageRemoveRequest{
		Input: &spb.ImageRemoveRequest_Input{},
	}
	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.ImageRemove(ctx, req)

	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}

func imageDefault(sc spb.SonicServiceClient, ctx context.Context) {
	fmt.Println("Sonic ImageDefault")
	ctx = setUserCreds(ctx)
	req := &spb.ImageDefaultRequest{
		Input: &spb.ImageDefaultRequest_Input{},
	}
	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.ImageDefault(ctx, req)

	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}

func authenticate(sc spb_jwt.SonicJwtServiceClient, ctx context.Context) {
	fmt.Println("Sonic Authenticate")
	ctx = setUserCreds(ctx)
	req := &spb_jwt.AuthenticateRequest{}

	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.Authenticate(ctx, req)
	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}

func refresh(sc spb_jwt.SonicJwtServiceClient, ctx context.Context) {
	fmt.Println("Sonic Refresh")
	ctx = setUserCreds(ctx)
	req := &spb_jwt.RefreshRequest{}

	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.Refresh(ctx, req)
	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}

func clearNeighbors(sc spb.SonicServiceClient, ctx context.Context) {
	fmt.Println("Sonic ClearNeighbors")
	ctx = setUserCreds(ctx)
	req := &spb.ClearNeighborsRequest{
		Input: &spb.ClearNeighborsRequest_Input{},
	}
	json.Unmarshal([]byte(*jsonArgs), req)

	resp, err := sc.ClearNeighbors(ctx, req)

	if err != nil {
		panic(err.Error())
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(respstr))
}
