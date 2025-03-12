package gnmi

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	credz "github.com/openconfig/gnsi/credentialz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	credzMu sync.Mutex
)

const (
	sshAccountTbl               string = "SSH_ACCOUNT"
	sshHostTbl                  string = "SSH_HOST"
	consoleAccountTbl           string = "CONSOLE_ACCOUNT"
	sshKeysVersionFld           string = "keys_version"
	sshKeysCreatedOnFld         string = "keys_created_on"
	sshPrincipalsVersionFld     string = "principals_version"
	sshPrincipalsCreatedOnFld   string = "principals_created_on"
	sshCaKeysVersionFld         string = "ca_keys_version"
	sshCaKeysCreatedOnFld       string = "ca_keys_created_on"
	consolePasswordVersionFld   string = "password_version"
	consolePasswordCreatedOnFld string = "password_created_on"
)

type cpType int

const (
	consoleCP cpType = iota
	sshCP
)

var (
	sshKeyTypePrefix = map[credz.KeyType]string{
		credz.KeyType_KEY_TYPE_UNSPECIFIED: "unspecified",
		credz.KeyType_KEY_TYPE_ECDSA_P_256: "ecdsa-sha2-nistp256",
		credz.KeyType_KEY_TYPE_ECDSA_P_521: "ecdsa-sha2-nistp521",
		credz.KeyType_KEY_TYPE_ED25519:     "ssh-ed25519",
		credz.KeyType_KEY_TYPE_RSA_2048:    "ssh-rsa",
		credz.KeyType_KEY_TYPE_RSA_4096:    "ssh-rsa",
	}
)

type GNSICredentialzServer struct {
	*Server
	sshCredMetadata         *SshCredMetadata
	sshCredMetadataCopy     SshCredMetadata
	consoleCredMetadata     *ConsoleCredMetadata
	consoleCredMetadataCopy ConsoleCredMetadata

	credz.UnimplementedCredentialzServer
}

func NewGNSICredentialzServer(srv *Server) *GNSICredentialzServer {
	ret := &GNSICredentialzServer{
		Server:              srv,
		sshCredMetadata:     NewSshCredMetadata(),
		consoleCredMetadata: NewConsoleCredMetadata(),
	}
	if err := ret.loadCredentialFreshness(srv.config.SshCredMetaFile); err != nil {
		log.V(lvl.ERROR).Infof("srv.config.SshCredMetaFile=%s error=%v", srv.config.SshCredMetaFile, err)
	}
	if err := ret.loadConsoleCredentialFreshness(srv.config.ConsoleCredMetaFile); err != nil {
		log.V(lvl.ERROR).Infof("srv.config.ConsoleCredMetaFile=%s error=%v", srv.config.ConsoleCredMetaFile, err)
	}
	ret.sshCredMetadataCopy = *ret.sshCredMetadata
	ret.writeSshHostCredentialsMetadataToDB(sshCaKeysVersionFld, ret.sshCredMetadata.Host.CaKeysVersion)
	ret.writeSshHostCredentialsMetadataToDB(sshCaKeysCreatedOnFld, ret.sshCredMetadata.Host.CaKeysCreatedOn)
	for a, u := range ret.sshCredMetadata.Accounts {
		ret.writeSshAccountCredentialsMetadataToDB(a, sshKeysVersionFld, u.KeysVersion)
		ret.writeSshAccountCredentialsMetadataToDB(a, sshKeysCreatedOnFld, u.KeysCreatedOn)
		ret.writeSshAccountCredentialsMetadataToDB(a, sshPrincipalsVersionFld, u.UsersVersion)
		ret.writeSshAccountCredentialsMetadataToDB(a, sshPrincipalsCreatedOnFld, u.UsersCreatedOn)
	}
	ret.consoleCredMetadataCopy = *ret.consoleCredMetadata
	for a, u := range ret.consoleCredMetadata.Accounts {
		ret.writeConsoleAccountCredentialsMetadataToDB(a, consolePasswordVersionFld, u.PasswordVersion)
		ret.writeConsoleAccountCredentialsMetadataToDB(a, consolePasswordCreatedOnFld, u.PasswordCreatedOn)
	}
	return ret
}

func (srv *GNSICredentialzServer) CanGenerateKey(context.Context, *credz.CanGenerateKeyRequest) (*credz.CanGenerateKeyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CanGenerateKey not implemented")
}

func (srv *GNSICredentialzServer) GetPublicKeys(context.Context, *credz.GetPublicKeysRequest) (*credz.GetPublicKeysResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CanGenerateKey not implemented")
}

// RotateAccountCredentials implements corresponding RPC
func (srv *GNSICredentialzServer) RotateAccountCredentials(stream credz.Credentialz_RotateAccountCredentialsServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNSI.Credentialz.RotateAccountCredentials RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNSI.Credentialz.RotateAccountCredentials RPC disabled since NSF is ongoing!")
	}
	ctx := stream.Context()
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return err
	}
	// Concurrent Rotate{Account|Host} RPCs are not allowed.
	if !credzMu.TryLock() {
		log.V(lvl.ERROR).Infoln("Concurrent Rotate{Account|Host} RPCs are not allowed.")
		return status.Errorf(codes.Aborted, "Concurrent Rotate{Account|Host} RPCs are not allowed.")
	}
	defer credzMu.Unlock()

	log.V(lvl.INFO).Info("gNSI: credz.RotateAccountCredentials")

	consoleBackup := false
	sshBackup := false

	for {
		req, err := stream.Recv()
		if err != nil {
			log.V(lvl.ERROR).Infoln("Received error: ", err)
			if sshBackup {
				srv.checkpointRestore(sshCP)
			}
			if consoleBackup {
				srv.checkpointRestore(consoleCP)
			}
			return status.Errorf(codes.Aborted, err.Error())
		}

		// log.V(lvl.DEBUG).Infof("Received: %T", req.GetRequest())
		var resp *credz.RotateAccountCredentialsResponse

		switch r := req.GetRequest().(type) {
		case *credz.RotateAccountCredentialsRequest_Finalize:
			if endReq := req.GetFinalize(); endReq != nil {
				// This is the last message. All changes are final.
				log.V(lvl.INFO).Infof("Received a Finalize request message %v", endReq)
				if sshBackup {
					srv.checkpointDelete(sshCP)
				}
				if consoleBackup {
					srv.checkpointDelete(consoleCP)
				}
				return nil
			}
			log.V(lvl.ERROR).Info("Finalize request failed")
			return status.Errorf(codes.Aborted, err.Error())
		case *credz.RotateAccountCredentialsRequest_Password:
			log.V(lvl.INFO).Info("Received a Password request")
			if !consoleBackup {
				// log.V(lvl.DEBUG).Infof("Checkpoint create: %v", consoleCP)
				consoleBackup = true
				if err := srv.checkpointCreate(consoleCP); err != nil {
					return err
				}
				defer srv.saveConsoleCredentialsFreshness(srv.config.ConsoleCredMetaFile)
			}
			resp, err = srv.processConsolePassword(req)
		case *credz.RotateAccountCredentialsRequest_Credential:
			if !sshBackup {
				// log.V(lvl.DEBUG).Infof("Checkpoint create: %v", sshCP)
				sshBackup = true
				if err := srv.checkpointCreate(sshCP); err != nil {
					return err
				}
				defer srv.saveCredentialsFreshness(srv.config.SshCredMetaFile)
			}
			resp, err = srv.processSshCred(req)
		case *credz.RotateAccountCredentialsRequest_User:
			if !sshBackup {
				// log.V(lvl.DEBUG).Infof("Checkpoint create: %v", sshCP)
				sshBackup = true
				if err := srv.checkpointCreate(sshCP); err != nil {
					return err
				}
				defer srv.saveCredentialsFreshness(srv.config.SshCredMetaFile)
			}
			resp, err = srv.processSshUser(req)
		default:
			return status.Errorf(codes.Aborted, "Unknown Request: %+v", r)
		}
		if err != nil {
			log.V(lvl.ERROR).Infof("Failed to process request: %v", err)
			if sshBackup {
				srv.checkpointRestore(sshCP)
			}
			if consoleBackup {
				srv.checkpointRestore(consoleCP)
			}
			return status.Errorf(codes.Aborted, err.Error())
		}
		log.V(lvl.INFO).Info("Finished process request")
		if err := stream.Send(resp); err != nil {
			if sshBackup {
				srv.checkpointRestore(sshCP)
			}
			if consoleBackup {
				srv.checkpointRestore(consoleCP)
			}
			return status.Errorf(codes.Aborted, err.Error())
		}
	}
}

// RotateHostParameters implements corresponding RPC
func (srv *GNSICredentialzServer) RotateHostParameters(stream credz.Credentialz_RotateHostParametersServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.Server.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNSI.Credentialz.RotateHostParameters RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNSI.Credentialz.RotateHostParameters RPC disabled since NSF is ongoing!")
	}
	ctx := stream.Context()
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return err
	}

	// Concurrent Rotate{Account|Host}Credentials RPCs are not allowed.
	if !credzMu.TryLock() {
		log.V(lvl.ERROR).Infoln("Concurrent Mutate{Account|Host}Credentials RPCs are not allowed.")
		return status.Errorf(codes.Aborted, "Concurrent Mutate{Account|Host}Credentials RPCs are not allowed.")
	}
	defer credzMu.Unlock()

	log.V(lvl.INFO).Info("gNSI: credz.RotateHostParameters")

	if err := srv.checkpointCreate(sshCP); err != nil {
		return err
	}
	defer srv.saveCredentialsFreshness(srv.config.SshCredMetaFile)

	for {
		req, err := stream.Recv()
		if err != nil {
			log.V(lvl.ERROR).Infoln("Received error: ", err)
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Aborted, err.Error())
		}
		// log.V(lvl.DEBUG).Infof("Received: %T", req.GetRequest())
		var resp *credz.RotateHostParametersResponse

		switch r := req.GetRequest().(type) {
		case *credz.RotateHostParametersRequest_Finalize:
			if endReq := req.GetFinalize(); endReq != nil {
				log.V(lvl.INFO).Infof("Received a Finalize request message! %v", endReq)
				srv.checkpointDelete(sshCP)
				return nil
			}
			log.V(lvl.ERROR).Info("Finalize request failed")
			return status.Errorf(codes.Aborted, err.Error())
		case *credz.RotateHostParametersRequest_SshCaPublicKey:
			resp, err = srv.processSshCaPublicKey(req)
		case *credz.RotateHostParametersRequest_ServerKeys:
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Unimplemented, "ServerKeys Unimplemented")
		case *credz.RotateHostParametersRequest_GenerateKeys:
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Unimplemented, "GenerateKeys Unimplemented")
		case *credz.RotateHostParametersRequest_AuthenticationAllowed:
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Unimplemented, "AuthenticationAllowed Unimplemented")
		case *credz.RotateHostParametersRequest_AuthorizedPrincipalCheck:
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Unimplemented, "AuthorizedPrincipalCheck Unimplemented")
		default:
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Aborted, "Unknown Request: %+v", r)
		}
		if err != nil {
			log.V(lvl.ERROR).Infof("Failed to process request: %v", err)
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Aborted, err.Error())
		}
		log.V(lvl.INFO).Info("Finished process request")
		if err := stream.Send(resp); err != nil {
			srv.checkpointRestore(sshCP)
			return status.Errorf(codes.Aborted, err.Error())
		}
	}
}

func (srv *GNSICredentialzServer) checkpointCreate(source cpType) error {
	log.V(lvl.DEBUG).Infof("Checkpoint Create: %v", source)
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return err
	}
	if source == sshCP {
		log.V(lvl.DEBUG).Info("Creating SSH Checkpoint")
		err := sc.SSHCheckpoint(ssc.CredzCPCreate)
		if err != nil {
			log.V(lvl.ERROR).Infof("Could not create ssh checkpoint:%v ", err)
			return status.Errorf(codes.Internal, "Cannot start the ssh transaction:%v", err)
		}
		srv.checkpointSshCredentialFreshness()
	}
	if source == consoleCP {
		err := sc.ConsoleCheckpoint(ssc.CredzCPCreate)
		if err != nil {
			log.V(lvl.ERROR).Infof("Could not create console checkpoint:%v ", err)
			return status.Errorf(codes.Internal, "Cannot start the console transaction:%v", err)
		}
		srv.checkpointConsoleFreshness()
	}
	return nil
}

func (srv *GNSICredentialzServer) checkpointRestore(source cpType) {
	log.V(lvl.DEBUG).Infof("Checkpoint Restore: %v", source)
	sc, _ := ssc.NewDbusClient(dbusCaller)
	if source == sshCP {
		if err := sc.SSHCheckpoint(ssc.CredzCPRestore); err != nil {
			log.V(lvl.ERROR).Infof("Could not restore from checkpoint: %v", err)
		}
		srv.revertSshCredentialFreshness()
	}
	if source == consoleCP {
		if err := sc.ConsoleCheckpoint(ssc.CredzCPRestore); err != nil {
			log.V(lvl.ERROR).Infof("Could not restore from checkpoint: %v", err)
		}
		srv.revertConsoleCredentialFreshness()
	}
}

func (srv *GNSICredentialzServer) checkpointDelete(source cpType) {
	log.V(lvl.DEBUG).Infof("Checkpoint Delete: %v", source)
	sc, _ := ssc.NewDbusClient(dbusCaller)
	if source == sshCP {
		if err := sc.SSHCheckpoint(ssc.CredzCPDelete); err != nil {
			log.V(lvl.WARNING).Infof("Could not delete checkpoint: %v", err)
		}
	}
	if source == consoleCP {
		if err := sc.ConsoleCheckpoint(ssc.CredzCPDelete); err != nil {
			log.V(lvl.WARNING).Infof("Could not delete checkpoint: %v", err)
		}
	}

}

func (srv *GNSICredentialzServer) processSshCred(req *credz.RotateAccountCredentialsRequest) (*credz.RotateAccountCredentialsResponse, error) {
	credReq := req.GetCredential()
	// log.V(lvl.DEBUG).Infof("Received a RotateAccountCredentials.Credential request message! %v", credReq)

	// Sanity checks.
	if len(credReq.GetCredentials()) == 0 {
		return nil, fmt.Errorf("credentials cannot be empty")
	}
	for _, set := range credReq.GetCredentials() {
		if set.GetVersion() == "" {
			return nil, fmt.Errorf("version cannot be empty")
		}
		if set.GetCreatedOn() == 0 {
			return nil, fmt.Errorf("created_on cannot be empty")
		}
		if len(set.GetAccount()) == 0 {
			return nil, fmt.Errorf("account cannot be empty")
		}
		if len(set.GetAuthorizedKeys()) == 0 {
			return nil, fmt.Errorf("authorized_keys cannot be empty")
		}
	}
	// Build the message to be sent to the back-end.
	var b strings.Builder
	fmt.Fprintf(&b, `{ "SshAccountKeys": [ `)
	for i, set := range credReq.GetCredentials() {
		fmt.Fprintf(&b, `{ "account": "%s", "keys": [`, set.Account)
		for i, key := range set.AuthorizedKeys {
			fmt.Fprintf(&b, ` { "key" : "%s %s %s", "options" : [`, sshKeyTypePrefix[key.GetKeyType()], b64.StdEncoding.EncodeToString(key.AuthorizedKey), key.Description)
			for i, o := range key.Options {
				fmt.Fprintf(&b, ` { "name" : "%v", "value": "%v" }`, o.GetName(), o.GetValue())
				if i < len(key.Options)-1 {
					fmt.Fprintf(&b, `,`)
				}
			}
			fmt.Fprintf(&b, ` ] }`)
			if i < len(set.AuthorizedKeys)-1 {
				fmt.Fprintf(&b, `,`)
			}
		}
		fmt.Fprintf(&b, ` ] }`)
		if i < len(credReq.GetCredentials())-1 {
			fmt.Fprintf(&b, `,`)
		}
	}
	fmt.Fprintf(&b, ` ] }`)
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	if err := sc.SSHMgmtSet(b.String()); err != nil {
		return nil, err
	}
	for _, set := range credReq.GetCredentials() {
		if err := srv.writeSshAccountCredentialsMetadataToDB(set.Account, sshKeysVersionFld, set.GetVersion()); err != nil {
			return nil, err
		}
		if err := srv.writeSshAccountCredentialsMetadataToDB(set.Account, sshKeysCreatedOnFld, strconv.FormatUint(set.GetCreatedOn(), 10)); err != nil {
			return nil, err
		}
	}
	resp := &credz.RotateAccountCredentialsResponse{
		Response: &credz.RotateAccountCredentialsResponse_Credential{},
	}
	return resp, nil
}

func (srv *GNSICredentialzServer) processSshUser(req *credz.RotateAccountCredentialsRequest) (*credz.RotateAccountCredentialsResponse, error) {
	usrReq := req.GetUser()
	// log.V(lvl.DEBUG).Infof("Received a RotateAccountCredentials.User request message! %v", usrReq)

	// Sanity checks.
	if len(usrReq.GetPolicies()) == 0 {
		return nil, fmt.Errorf("policies cannot be empty")
	}
	for _, set := range usrReq.GetPolicies() {
		if set.GetVersion() == "" {
			return nil, fmt.Errorf("version cannot be empty")
		}
		if set.GetCreatedOn() == 0 {
			return nil, fmt.Errorf("created_on cannot be empty")
		}
		if len(set.GetAccount()) == 0 {
			return nil, fmt.Errorf("account cannot be empty")
		}
		if len(set.GetAuthorizedPrincipals().GetAuthorizedPrincipals()) == 0 {
			return nil, fmt.Errorf("authorized_principals cannot be empty")
		}
	}
	// Build the message to be sent to the back-end.
	var b strings.Builder
	fmt.Fprintf(&b, `{ "SshAccountUsers": [`)
	for i, set := range usrReq.GetPolicies() {
		fmt.Fprintf(&b, ` { "account": "%s", "users": [`, set.Account)
		for j, user := range set.GetAuthorizedPrincipals().GetAuthorizedPrincipals() {
			fmt.Fprintf(&b, ` { "name" : "%v", "options" : [`, user.GetAuthorizedUser())
			for k, o := range user.Options {
				fmt.Fprintf(&b, ` { "name" : "%v", "value": "%v" }`, o.GetName(), o.GetValue())
				if k < len(user.Options)-1 {
					fmt.Fprintf(&b, `,`)
				}
			}
			fmt.Fprintf(&b, ` ] }`)
			if j < len(set.GetAuthorizedPrincipals().GetAuthorizedPrincipals())-1 {
				fmt.Fprintf(&b, `,`)
			}
		}
		fmt.Fprintf(&b, ` ] }`)
		if i < len(usrReq.GetPolicies())-1 {
			fmt.Fprintf(&b, `,`)
		}
	}
	fmt.Fprintf(&b, ` ] }`)
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	if err := sc.SSHMgmtSet(b.String()); err != nil {
		return nil, err
	}
	for _, set := range usrReq.GetPolicies() {
		if err := srv.writeSshAccountCredentialsMetadataToDB(set.Account, sshPrincipalsVersionFld, set.GetVersion()); err != nil {
			return nil, err
		}
		if err := srv.writeSshAccountCredentialsMetadataToDB(set.Account, sshPrincipalsCreatedOnFld, strconv.FormatUint(set.GetCreatedOn(), 10)); err != nil {
			return nil, err
		}
	}
	resp := &credz.RotateAccountCredentialsResponse{
		Response: &credz.RotateAccountCredentialsResponse_User{},
	}
	return resp, nil
}

func (srv *GNSICredentialzServer) processConsolePassword(req *credz.RotateAccountCredentialsRequest) (*credz.RotateAccountCredentialsResponse, error) {
	credReq := req.GetPassword()
	// log.V(lvl.DEBUG).Infof("Received a Set Password request message! %v", credReq)

	// Sanity checks.
	if len(credReq.GetAccounts()) == 0 {
		return nil, fmt.Errorf("list of username/password pairs cannot be empty")
	}
	for _, set := range credReq.GetAccounts() {
		if set.GetVersion() == "" {
			return nil, fmt.Errorf("version cannot be empty")
		}
		if set.GetCreatedOn() == 0 {
			return nil, fmt.Errorf("created_on cannot be empty")
		}
		if set.Account == "" {
			return nil, fmt.Errorf("name cannot be empty")
		}
		pwd := set.GetPassword()
		if pwd == nil {
			return nil, fmt.Errorf("password cannot be empty")
		}
		if pwd.GetPlaintext() == "" {
			return nil, fmt.Errorf("password must be plaintext; CryptoHash unimplemented")
		}
	}

	// Build a message to be sent to the back-end.
	var b strings.Builder
	fmt.Fprintf(&b, `{ "ConsolePasswords": [ `)
	for i, set := range credReq.GetAccounts() {
		fmt.Fprintf(&b, `{ "name": "%s", "password" : "%s" }`, set.GetAccount(), set.GetPassword().GetPlaintext())
		if i < len(credReq.GetAccounts())-1 {
			fmt.Fprintf(&b, `,`)
		}
	}
	fmt.Fprintf(&b, ` ] }`)
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	if err := sc.ConsoleSet(b.String()); err != nil {
		return nil, err
	}
	for _, set := range credReq.GetAccounts() {
		if err := srv.writeConsoleAccountCredentialsMetadataToDB(set.GetAccount(), consolePasswordVersionFld, set.GetVersion()); err != nil {
			return nil, err
		}
		if err := srv.writeConsoleAccountCredentialsMetadataToDB(set.GetAccount(), consolePasswordCreatedOnFld, strconv.FormatUint(set.GetCreatedOn(), 10)); err != nil {
			return nil, err
		}
	}
	resp := &credz.RotateAccountCredentialsResponse{
		Response: &credz.RotateAccountCredentialsResponse_Password{},
	}
	return resp, nil
}

func (srv *GNSICredentialzServer) processSshCaPublicKey(req *credz.RotateHostParametersRequest) (*credz.RotateHostParametersResponse, error) {
	credReq := req.GetSshCaPublicKey()
	if credReq == nil {
		return nil, fmt.Errorf(`Unknown request: "%v"`, req)
	}
	// log.V(lvl.DEBUG).Infof("Received a RotateHostParameters request message! %v", credReq)

	// Sanity checks.
	if len(credReq.SshCaPublicKeys) == 0 {
		return nil, fmt.Errorf("CA keys cannot be empty")
	}
	if credReq.GetVersion() == "" {
		return nil, fmt.Errorf("version cannot be empty")
	}
	if credReq.GetCreatedOn() == 0 {
		return nil, fmt.Errorf("created_on cannot be empty")
	}
	for _, key := range credReq.GetSshCaPublicKeys() {
		if len(key.GetPublicKey()) == 0 {
			return nil, fmt.Errorf("CA public key cannot be empty")
		}
	}
	// Build the message to be sent to the back-end.
	var b strings.Builder
	fmt.Fprintf(&b, `{ "SshCaPublicKey": [`)
	for i, key := range credReq.GetSshCaPublicKeys() {
		fmt.Fprintf(&b, ` "%s %s %s"`, sshKeyTypePrefix[key.GetKeyType()], b64.StdEncoding.EncodeToString(key.GetPublicKey()), key.Description)
		if i < len(credReq.GetSshCaPublicKeys())-1 {
			fmt.Fprintf(&b, `,`)
		}
	}
	fmt.Fprintf(&b, ` ] }`)
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	if err := sc.SSHMgmtSet(b.String()); err != nil {
		return nil, err
	}
	if err := srv.writeSshHostCredentialsMetadataToDB(sshCaKeysVersionFld, credReq.GetVersion()); err != nil {
		return nil, err
	}
	if err := srv.writeSshHostCredentialsMetadataToDB(sshCaKeysCreatedOnFld, strconv.FormatUint(credReq.GetCreatedOn(), 10)); err != nil {
		return nil, err
	}
	resp := &credz.RotateHostParametersResponse{
		Response: &credz.RotateHostParametersResponse_SshCaPublicKey{},
	}
	return resp, nil
}

// SSH Helpers
// writeSshAccountCredentialsMetadataToDB writes the credentials freshness data to the DB.
func (srv *GNSICredentialzServer) writeSshAccountCredentialsMetadataToDB(account, fld, val string) error {
	err := writeCredentialsMetadataToDB(sshAccountTbl, account, fld, val)
	if err != nil {
		return err
	}
	meta, ok := srv.sshCredMetadata.Accounts[account]
	if !ok {
		meta = SshAccountVersion{KeysVersion: "unknown", KeysCreatedOn: "0", UsersVersion: "unknown", UsersCreatedOn: "0"}
	}
	switch fld {
	case sshKeysVersionFld:
		meta.KeysVersion = val
	case sshKeysCreatedOnFld:
		meta.KeysCreatedOn = val
	case sshPrincipalsVersionFld:
		meta.UsersVersion = val
	case sshPrincipalsCreatedOnFld:
		meta.UsersCreatedOn = val
	}
	srv.sshCredMetadata.Accounts[account] = meta
	return nil
}

// writeSshHostCredentialsMetadataToDB writes the credentials freshness data to the DB.
func (srv *GNSICredentialzServer) writeSshHostCredentialsMetadataToDB(fld, val string) error {
	err := writeCredentialsMetadataToDB(sshHostTbl, "", fld, val)
	if err != nil {
		return err
	}
	switch fld {
	case sshCaKeysVersionFld:
		srv.sshCredMetadata.Host.CaKeysVersion = val
	case sshCaKeysCreatedOnFld:
		srv.sshCredMetadata.Host.CaKeysCreatedOn = val
	}
	return nil
}

type SshAccountVersion struct {
	KeysVersion    string `json:"keys_version"`
	KeysCreatedOn  string `json:"keys_created_on"`
	UsersVersion   string `json:"users_version"`
	UsersCreatedOn string `json:"users_created_on"`
}

type SshHostVersion struct {
	CaKeysVersion   string `json:"ca_public_keys_version"`
	CaKeysCreatedOn string `json:"ca_public_keys_created_on"`
}
type SshCredMetadata struct {
	Accounts map[string]SshAccountVersion `json:"accounts"`
	Host     SshHostVersion               `json:"host"`
}

func NewSshCredMetadata() *SshCredMetadata {
	return &SshCredMetadata{
		Accounts: make(map[string]SshAccountVersion),
		Host:     SshHostVersion{CaKeysVersion: "unknown", CaKeysCreatedOn: "0"},
	}
}

func (srv *GNSICredentialzServer) checkpointSshCredentialFreshness() {
	srv.sshCredMetadataCopy = *srv.sshCredMetadata
}

func (srv *GNSICredentialzServer) revertSshCredentialFreshness() {
	srv.writeSshHostCredentialsMetadataToDB(sshCaKeysVersionFld, srv.sshCredMetadataCopy.Host.CaKeysVersion)
	srv.writeSshHostCredentialsMetadataToDB(sshCaKeysCreatedOnFld, srv.sshCredMetadataCopy.Host.CaKeysCreatedOn)
	for a, u := range srv.sshCredMetadataCopy.Accounts {
		srv.writeSshAccountCredentialsMetadataToDB(a, sshKeysVersionFld, u.KeysVersion)
		srv.writeSshAccountCredentialsMetadataToDB(a, sshKeysCreatedOnFld, u.KeysCreatedOn)
		srv.writeSshAccountCredentialsMetadataToDB(a, sshPrincipalsVersionFld, u.UsersVersion)
		srv.writeSshAccountCredentialsMetadataToDB(a, sshPrincipalsCreatedOnFld, u.UsersCreatedOn)
	}
}

func (srv *GNSICredentialzServer) saveCredentialsFreshness(path string) error {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(*srv.sshCredMetadata); err != nil {
		log.V(lvl.ERROR).Info(err)
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

func (srv *GNSICredentialzServer) loadCredentialFreshness(path string) error {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, srv.sshCredMetadata)
}

// CONSOLE helpers
// writeConsoleAccountCredentialsMetadataToDB writes the credentials freshness data to the DB.
func (srv *GNSICredentialzServer) writeConsoleAccountCredentialsMetadataToDB(account, fld, val string) error {
	if err := writeCredentialsMetadataToDB(consoleAccountTbl, account, fld, val); err != nil {
		return err
	}
	meta, ok := srv.consoleCredMetadata.Accounts[account]
	if !ok {
		meta = ConsoleAccountVersion{PasswordVersion: "unknown", PasswordCreatedOn: "0"}
	}
	switch fld {
	case consolePasswordVersionFld:
		meta.PasswordVersion = val
	case consolePasswordCreatedOnFld:
		meta.PasswordCreatedOn = val
	}
	srv.consoleCredMetadata.Accounts[account] = meta
	return nil
}

type ConsoleAccountVersion struct {
	PasswordVersion   string `json:"password_version"`
	PasswordCreatedOn string `json:"password_created_on"`
}

type ConsoleCredMetadata struct {
	Accounts map[string]ConsoleAccountVersion `json:"accounts"`
}

func NewConsoleCredMetadata() *ConsoleCredMetadata {
	return &ConsoleCredMetadata{
		Accounts: make(map[string]ConsoleAccountVersion),
	}
}

func (srv *GNSICredentialzServer) checkpointConsoleFreshness() {
	srv.consoleCredMetadataCopy = *srv.consoleCredMetadata
}

func (srv *GNSICredentialzServer) revertConsoleCredentialFreshness() {
	for a, u := range srv.consoleCredMetadataCopy.Accounts {
		srv.writeConsoleAccountCredentialsMetadataToDB(a, consolePasswordVersionFld, u.PasswordVersion)
		srv.writeConsoleAccountCredentialsMetadataToDB(a, consolePasswordCreatedOnFld, u.PasswordCreatedOn)
	}
}

func (srv *GNSICredentialzServer) saveConsoleCredentialsFreshness(path string) error {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(*srv.consoleCredMetadata); err != nil {
		log.V(lvl.ERROR).Info(err)
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

func (srv *GNSICredentialzServer) loadConsoleCredentialFreshness(path string) error {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, srv.consoleCredMetadata)
}
