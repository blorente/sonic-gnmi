package gnmi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	credz "github.com/openconfig/gnsi/credentialz"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	sshMetaPathTest     = "../testdata/gnsi/ssh-version.json"
	consoleMetaPathTest = "../testdata/gnsi/console-version.json"

	expectedSshCreateCmd      = ssc.NamePrefix + "ssh_mgmt" + string(ssc.CredzCPCreate)
	expectedSshDeleteCmd      = ssc.NamePrefix + "ssh_mgmt" + string(ssc.CredzCPDelete)
	expectedSshRestoreCmd     = ssc.NamePrefix + "ssh_mgmt" + string(ssc.CredzCPRestore)
	expectedSshSetCmd         = ssc.NamePrefix + "ssh_mgmt.set"
	expectedConsoleCreateCmd  = ssc.NamePrefix + "gnsi_console" + string(ssc.CredzCPCreate)
	expectedConsoleDeleteCmd  = ssc.NamePrefix + "gnsi_console" + string(ssc.CredzCPDelete)
	expectedConsoleRestoreCmd = ssc.NamePrefix + "gnsi_console" + string(ssc.CredzCPRestore)
	expectedConsoleSetCmd     = ssc.NamePrefix + "gnsi_console.set"
)

func createCredzServer(t *testing.T) *Server {
	t.Helper()
	cfg := testServerConfig(testSrvType)
	cfg.SshCredMetaFile = sshMetaPathTest
	cfg.ConsoleCredMetaFile = consoleMetaPathTest

	return createCustomServer(t, cfg)
}

var sshTests = []struct {
	desc string
	f    func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string)
}{
	{
		desc: "Unimplemented ServerKeys",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateHostParametersRequest{
				Request: &credz.RotateHostParametersRequest_ServerKeys{}}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Recv(); status.Code(err) != codes.Unimplemented {
				t.Errorf("expected: Unimplemented, got: %+v", err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "Unimplemented GenerateKeys",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateHostParametersRequest{
				Request: &credz.RotateHostParametersRequest_GenerateKeys{}}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Recv(); status.Code(err) != codes.Unimplemented {
				t.Errorf("expected: Unimplemented, got: %+v", err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "Unimplemented AuthenticationAllowed",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateHostParametersRequest{
				Request: &credz.RotateHostParametersRequest_AuthenticationAllowed{}}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Recv(); status.Code(err) != codes.Unimplemented {
				t.Errorf("expected: Unimplemented, got: %+v", err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "Unimplemented AuthorizedPrincipalCheck",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateHostParametersRequest{
				Request: &credz.RotateHostParametersRequest_AuthorizedPrincipalCheck{}}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Recv(); status.Code(err) != codes.Unimplemented {
				t.Errorf("expected: Unimplemented, got: %+v", err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: keys, users, finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Credential{
					Credential: &credz.AuthorizedKeysRequest{
						Credentials: []*credz.AccountCredentials{
							{
								Account:   "root",
								Version:   "root-version-1",
								CreatedOn: 123,
								AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
									{
										AuthorizedKey: []byte("Authorized-key #1"),
										Options: []*credz.Option{
											{
												Key:   &credz.Option_Name{Name: "from"},
												Value: "*.sales.example.net,!pc.sales.example.net",
											},
										},
									},
									{
										AuthorizedKey: []byte("Authorized-key #2"),
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetResponse() == nil {
				t.Fatal("expected a message")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountKeys": [ { "account": "root", "keys": [ { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzE= ", "options" : [ { "name" : "from", "value": "*.sales.example.net,!pc.sales.example.net" } ] }, { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzI= ", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}

			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_User{
					User: &credz.AuthorizedUsersRequest{
						Policies: []*credz.UserPolicy{
							{
								Account:   "root",
								Version:   "root-version-2",
								CreatedOn: 123,
								AuthorizedPrincipals: &credz.UserPolicy_SshAuthorizedPrincipals{
									AuthorizedPrincipals: []*credz.UserPolicy_SshAuthorizedPrincipal{
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "alice",
											Options: []*credz.Option{
												{
													Key:   &credz.Option_Name{Name: "from"},
													Value: "*.sales.example.net,!pc.sales.example.net",
												},
											},
										},
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "bob",
										},
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err = c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountUsers": [ { "account": "root", "users": [ { "name" : "alice", "options" : [ { "name" : "from", "value": "*.sales.example.net,!pc.sales.example.net" } ] }, { "name" : "bob", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.create_checkpoint; got: %v", dbus)
			}

			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Finalize{},
			}); err != nil {
				t.Fatal(err.Error())
			}
			if resp, err := c.Recv(); err != io.EOF || resp.GetResponse() != nil {
				t.Fatalf("expected EOF; err: %v; resp: %v", err, resp)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshDeleteCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshDeleteCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: keys, finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {

			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Credential{
					Credential: &credz.AuthorizedKeysRequest{
						Credentials: []*credz.AccountCredentials{
							{
								Account:   "root",
								Version:   "root-version-1",
								CreatedOn: 123,
								AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
									{
										AuthorizedKey: []byte("Authorized-key #1"),
									},
									{
										AuthorizedKey: []byte("Authorized-key #2"),
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountKeys": [ { "account": "root", "keys": [ { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzE= ", "options" : [ ] }, { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzI= ", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Finalize{},
			}); err != nil {
				t.Fatal(err.Error())
			}
			if resp, err := c.Recv(); err != io.EOF || resp.GetResponse() != nil {
				t.Fatalf("expected EOF; err: %v; resp: %v", err, resp)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshDeleteCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshDeleteCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: users, finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_User{
					User: &credz.AuthorizedUsersRequest{
						Policies: []*credz.UserPolicy{
							{
								Account:   "root",
								Version:   "2021-09-10T18:22:46",
								CreatedOn: 1631298166,
								AuthorizedPrincipals: &credz.UserPolicy_SshAuthorizedPrincipals{
									AuthorizedPrincipals: []*credz.UserPolicy_SshAuthorizedPrincipal{
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "alice",
										},
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "bob",
										},
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountUsers": [ { "account": "root", "users": [ { "name" : "alice", "options" : [ ] }, { "name" : "bob", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Finalize{},
			}); err != nil {
				t.Fatal(err.Error())
			}
			if resp, err := c.Recv(); err != io.EOF || resp.GetResponse() != nil {
				t.Fatalf("expected EOF; err: %v; resp: %v", err, resp)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshDeleteCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshDeleteCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: keys, users, no finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Credential{
					Credential: &credz.AuthorizedKeysRequest{
						Credentials: []*credz.AccountCredentials{
							{
								Account:   "root",
								Version:   "root-version-1",
								CreatedOn: 123,
								AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
									{
										AuthorizedKey: []byte("Authorized-key #1"),
									},
									{
										AuthorizedKey: []byte("Authorized-key #2"),
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountKeys": [ { "account": "root", "keys": [ { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzE= ", "options" : [ ] }, { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzI= ", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_User{
					User: &credz.AuthorizedUsersRequest{
						Policies: []*credz.UserPolicy{
							{
								Account:   "root",
								Version:   "root-version-2",
								CreatedOn: 123,
								AuthorizedPrincipals: &credz.UserPolicy_SshAuthorizedPrincipals{
									AuthorizedPrincipals: []*credz.UserPolicy_SshAuthorizedPrincipal{
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "alice",
										},
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "bob",
										},
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err = c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountUsers": [ { "account": "root", "users": [ { "name" : "alice", "options" : [ ] }, { "name" : "bob", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}
			if err = c.CloseSend(); err != nil {
				t.Fatal(err.Error())
			}
			resp, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				t.Fatal("Received unexpected message after closing connection.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: keys, no finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Credential{
					Credential: &credz.AuthorizedKeysRequest{
						Credentials: []*credz.AccountCredentials{
							{
								Account:   "root",
								Version:   "root-version-1",
								CreatedOn: 123,
								AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
									{
										AuthorizedKey: []byte("Authorized-key #1"),
									},
									{
										AuthorizedKey: []byte("Authorized-key #2"),
									},
								},
							},
						},
					},
				},
			})

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountKeys": [ { "account": "root", "keys": [ { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzE= ", "options" : [ ] }, { "key" : "unspecified QXV0aG9yaXplZC1rZXkgIzI= ", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}
			if err = c.CloseSend(); err != nil {
				t.Fatal(err.Error())
			}
			resp, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				t.Fatal("Received unexpected message after closing connection.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: users, no finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_User{
					User: &credz.AuthorizedUsersRequest{
						Policies: []*credz.UserPolicy{
							{
								Account:   "root",
								Version:   "root-version-2",
								CreatedOn: 123,
								AuthorizedPrincipals: &credz.UserPolicy_SshAuthorizedPrincipals{
									AuthorizedPrincipals: []*credz.UserPolicy_SshAuthorizedPrincipal{
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "alice",
										},
										&credz.UserPolicy_SshAuthorizedPrincipal{
											AuthorizedUser: "bob",
										},
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshSetCmd, `[{ "SshAccountUsers": [ { "account": "root", "users": [ { "name" : "alice", "options" : [ ] }, { "name" : "bob", "options" : [ ] } ] } ] }]`}) {
				t.Fatalf("DBUS Failure wanted ssh_mgmt.set; got: %v", dbus)
			}
			if err = c.CloseSend(); err != nil {
				t.Fatal(err.Error())
			}
			resp, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				t.Fatal("Received unexpected message after closing connection.")
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "User scenario: graceful close",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.CloseSend(); err != nil {
				t.Fatal(err.Error())
			}
			resp, err := c.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				t.Fatal("Received unexpected message after closing connection.")
			}

			select {
			case msg := <-ch:
				t.Fatal("Unexpected DBUS msg: %v", msg)
			default:
			}
		},
	},
	{
		desc: "Host scenario: ca_public_key, finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			var err error
			var h credz.Credentialz_RotateHostParametersClient
			run("RotateHostParametersOpen", ch, t, []string{expectedSshCreateCmd}, func() error {
				h, err = sc.RotateHostParameters(ctx)
				return err
			})
			run("RotateHostParametersModifySshCaPublicKey", ch, t,
				[]string{expectedSshSetCmd, `[{ "SshCaPublicKey": [ "ssh-ed25519 VEVTVC1DRVJUICMx test#1", "ssh-rsa VEVTVC1DRVJUICMy test#2" ] }]`},
				func() error {
					err = h.Send(&credz.RotateHostParametersRequest{
						Request: &credz.RotateHostParametersRequest_SshCaPublicKey{
							SshCaPublicKey: &credz.CaPublicKeyRequest{
								Version:   "CA-trust-bundle-1",
								CreatedOn: 123,
								SshCaPublicKeys: []*credz.PublicKey{
									&credz.PublicKey{
										PublicKey:   []byte("TEST-CERT #1"),
										KeyType:     credz.KeyType_KEY_TYPE_ED25519,
										Description: "test#1",
									},
									&credz.PublicKey{
										PublicKey:   []byte("TEST-CERT #2"),
										KeyType:     credz.KeyType_KEY_TYPE_RSA_2048,
										Description: "test#2",
									},
								},
							},
						},
					})
					if err != nil {
						t.Fatal(err.Error())
					}
					resp, err := h.Recv()
					if err != nil {
						t.Fatal(err.Error())
					}
					if resp.GetResponse() == nil {
						t.Fatal("Expected a message.")
					}
					return nil
				})
			run("RotateHostParametersModifyFinalize", ch, t, []string{expectedSshDeleteCmd}, func() error {
				err = h.Send(&credz.RotateHostParametersRequest{
					Request: &credz.RotateHostParametersRequest_Finalize{},
				})
				if err != nil {
					t.Fatal(err.Error())
				}
				if resp, err := h.Recv(); err != io.EOF || resp.GetResponse() != nil {
					t.Fatalf("expected EOF; err: %v; resp: %v", err, resp)
				}
				return nil
			})
		},
	},
	{
		desc: "Host scenario: ca_public_key, no finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			var err error
			var h credz.Credentialz_RotateHostParametersClient
			run("RotateHostParametersOpen", ch, t, []string{expectedSshCreateCmd}, func() error {
				h, err = sc.RotateHostParameters(ctx)
				return err
			})
			run("RotateHostParametersModifySshCaPublicKey", ch, t,
				[]string{expectedSshSetCmd, `[{ "SshCaPublicKey": [ "ssh-ed25519 VEVTVC1DRVJUICMx test#1", "ssh-rsa VEVTVC1DRVJUICMy test#2" ] }]`},
				func() error {
					err = h.Send(&credz.RotateHostParametersRequest{
						Request: &credz.RotateHostParametersRequest_SshCaPublicKey{
							SshCaPublicKey: &credz.CaPublicKeyRequest{
								Version:   "CA-trust-bundle-1",
								CreatedOn: 123,
								SshCaPublicKeys: []*credz.PublicKey{
									&credz.PublicKey{
										PublicKey:   []byte("TEST-CERT #1"),
										KeyType:     credz.KeyType_KEY_TYPE_ED25519,
										Description: "test#1",
									},
									&credz.PublicKey{
										PublicKey:   []byte("TEST-CERT #2"),
										KeyType:     credz.KeyType_KEY_TYPE_RSA_2048,
										Description: "test#2",
									},
								},
							},
						},
					})
					if err != nil {
						t.Fatal(err.Error())
					}
					resp, err := h.Recv()
					if err != nil {
						t.Fatal(err.Error())
					}
					if resp.GetResponse() == nil {
						t.Fatal("Expected a message.")
					}
					return nil
				})
			run("RotateHostParametersCloseConnection", ch, t, []string{expectedSshRestoreCmd}, func() error {
				if err = h.CloseSend(); err != nil {
					t.Fatal(err.Error())
				}
				resp, err := h.Recv()
				if err == nil {
					t.Fatal("Expected an error reporting premature closure of the stream.")
				}
				if status.Code(err) != codes.Aborted {
					t.Fatalf("Unexpected error: %v", err)
				}
				if resp != nil {
					t.Fatal("Received unexpected message after closing connection.")
				}
				return nil
			})
		},
	},
	{
		desc: "Host scenario: graceful close",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			var err error
			var h credz.Credentialz_RotateHostParametersClient
			run("RotateHostParametersOpen", ch, t, []string{expectedSshCreateCmd}, func() error {
				h, err = sc.RotateHostParameters(ctx)
				return err
			})
			run("RotateHostParametersCloseConnection", ch, t, []string{expectedSshRestoreCmd}, func() error {
				if err = h.CloseSend(); err != nil {
					t.Fatal(err.Error())
				}
				resp, err := h.Recv()
				if err == nil {
					t.Fatal("Expected an error reporting premature closure of the stream.")
				}
				if status.Code(err) != codes.Aborted {
					t.Fatalf("Unexpected error: %v", err)
				}
				if resp != nil {
					t.Fatal("Received unexpected message after closing connection.")
				}
				return nil
			})
		},
	},
	{
		desc: "read JSON with version info, fail",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			s := &GNSICredentialzServer{
				sshCredMetadata: NewSshCredMetadata(),
			}
			if err := s.loadCredentialFreshness(""); err == nil {
				t.Fatal("Expected file read error")
			}
		},
	},
	{
		desc: "read JSON with version info, success",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			s := &GNSICredentialzServer{
				sshCredMetadata: NewSshCredMetadata(),
			}
			if err := s.loadCredentialFreshness(sshMetaPathTest); err != nil {
				t.Fatal(err)
			}
		},
	},
	{
		desc: "write JSON with version info, fail",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			s := &GNSICredentialzServer{
				sshCredMetadata: NewSshCredMetadata(),
			}
			if err := s.saveCredentialsFreshness(""); err == nil {
				t.Fatal("Expected write file error")
			}
		},
	},
	{
		desc: "write JSON with version info, success",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			s := &GNSICredentialzServer{
				sshCredMetadata: NewSshCredMetadata(),
			}
			if err := s.saveCredentialsFreshness(sshMetaPathTest); err != nil {
				t.Fatal(err)
			}
		},
	},
	{
		desc: "Reject concurrent account",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{}); err != nil {
				t.Fatal(err)
			}
			c2, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c2.Send(&credz.RotateAccountCredentialsRequest{}); err != nil {
				t.Fatal(err)
			}
			if _, err := c2.Recv(); status.Code(err) != codes.Aborted {
				t.Errorf("expected: Aborted, got: %+v", err)
			}
		},
	},
	{
		desc: "Reject concurrent host",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			c, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateHostParametersRequest{}); err != nil {
				t.Fatal(err)
			}
			c2, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c2.Send(&credz.RotateHostParametersRequest{}); err != nil {
				t.Fatal(err)
			}
			if _, err := c2.Recv(); status.Code(err) != codes.Aborted {
				t.Errorf("expected: Aborted, got: %+v", err)
			}
		},
	},
}

// TestSSHServer tests implementation of gnsi.Ssh server.
func TestGnsiCredzSSHServer(t *testing.T) {
	s := createCredzServer(t)
	go runServer(t, s)
	defer s.Stop()

	metaBackup, err := os.ReadFile(sshMetaPathTest)
	if err != nil {
		t.Fatal(err)
	}
	defer os.WriteFile(sshMetaPathTest, metaBackup, 0600)

	var dbusListener chan []string
	var done chan bool
	// dbusCaller is a package variable
	dbusListener, done, dbusCaller = newMockSshDbusServer()
	defer func() { close(dbusListener); close(done) }()

	// Create a gNSI.ssh client and connect it to the gNSI.ssh server.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()
	credzClient := credz.NewCredentialzClient(conn)

	for _, tc := range sshTests {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Run(tc.desc, func(t *testing.T) {
			tc.f(t, ctx, credzClient, dbusListener)
			credzMu.Lock()
			t.Logf("Finished %s", tc.desc)
			credzMu.Unlock()
		})
		cancel()
	}

	// TODO(b/344081417) Enable code once NSF Freeze is implemented
	// // Test RPCs during NSF freeze mode.
	// s.WarmRestartHelper.SetFreezeStatus(true)
	// t.Run("RotateHostParametersUnavailableDuringFreeze", func(t *testing.T) {
	// 	stream, err := sc.RotateHostParameters(context.Background())
	// 	if err != nil {
	// 		t.Fatal(err.Error())
	// 	}
	// 	if err = stream.Send(&credz.RotateHostParametersRequest{}); err != nil {
	// 		t.Fatal(err.Error())
	// 	}
	// 	_, err = stream.Recv()
	// 	testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	// })
	// s.WarmRestartHelper.SetFreezeStatus(false)

	// Shutdown the mock ssh_mgmt service server.
	done <- true
	// Save the SSH Credentials metadata to a file.
	if err := s.gnsiCredz.saveCredentialsFreshness(s.config.SshCredMetaFile); err != nil {
		t.Fatal(err)
	}
}

func newMockSshDbusServer() (chan []string, chan bool, ssc.Caller) {
	ch := make(chan []string, 10)
	pass := make(chan []string, 10)
	done := make(chan bool, 1)
	go func() {
		checkpoint := false
		var mu sync.Mutex
		for {
			select {
			case <-done: // if cancel() execute
				log.V(lvl.INFO).Infoln("Shutting down the DBUS server.")
				return
			case msg := <-ch:
				mu.Lock()
				log.V(lvl.INFO).Infof("DBUS service received %+v", msg)
				switch msg[0] {
				case expectedSshCreateCmd:
					if checkpoint {
						log.Fatal("mock ssh_mgmt service: checkpoint already exists.")
					}
					checkpoint = true
				case expectedSshDeleteCmd:
					if !checkpoint {
						log.Fatal("mock ssh_mgmt service: checkpoint does not exists.")
					}
					checkpoint = false
				case expectedSshRestoreCmd:
					if !checkpoint {
						log.Fatal("mock ssh_mgmt service: checkpoint does not exists.")
					}
					checkpoint = false
				case expectedSshSetCmd:
					if !checkpoint {
						log.Fatal("mock ssh_mgmt service: set without checkpoint.")
					}
				default:
					log.V(lvl.INFO).Infof("mock ssh_mgmt service: unknown service: %+v", msg)
				}
				mu.Unlock()
				pass <- msg
			}
		}
	}()

	return pass, done, &ssc.SpyDbusCaller{Command: ch}
}

func dbusListen(dbus <-chan []string) []string {
	select {
	case resp := <-dbus:
		return resp
	case <-time.After(time.Second * 3):
		return []string{"dbus listener timed out"}
	}
}

func run(name string, dbus <-chan []string, t *testing.T, cmd []string, f func() error) {
	t.Run(name, func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		finished := make(chan error, 1)
		go func() {
			finished <- f()
			wg.Done()
		}()
		count := 2
		if len(cmd) == 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			select {
			case resp := <-dbus:
				log.V(lvl.INFO).Infof("received on DBUS: %v\n", resp)
				for i := range cmd {
					if cmd[i] != resp[i] {
						t.Errorf("expected: '%v' but got '%v'", cmd, resp)
					}
				}
				if len(cmd) == 2 && !json.Valid([]byte(cmd[1])) {
					t.Errorf("malformed JSON string: '%v'", cmd[1])
				}
			case err := <-finished:
				log.V(lvl.INFO).Infof("f() is done with err=%v\n", err)
				if err != nil {
					t.Error(err.Error())
				}
			case <-time.After(time.Second * 3):
				t.Errorf("did not get expected DBUS message and/or %v() did not finish within 5s", name)
			}
		}
		wg.Wait()
		log.V(lvl.INFO).Infoln("Finished:", name)
	})
}

// CONSOLE

var consoleTests = []struct {
	desc string
	f    func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string)
}{
	{
		desc: "two accounts, finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Account: "alice",
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "password-alice"}},
								Version:   "version-1",
								CreatedOn: 123,
							},
							{
								Account: "bob",
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "password-bob"}},
								Version:   "version-2",
								CreatedOn: 321,
							},
						},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			if resp, err := c.Recv(); err != nil || resp.GetResponse() == nil {
				t.Fatalf("expected Response; err: %v; resp: %v", err, resp)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}

			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleSetCmd, `[{ "ConsolePasswords": [ { "name": "alice", "password" : "password-alice" },{ "name": "bob", "password" : "password-bob" } ] }]`}) {
				t.Fatalf("DBUS Failure wanted gnsi_console.set; got: %v", dbus)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Finalize{},
			}); err != nil {
				t.Fatal(err)
			}
			if resp, err := c.Recv(); err != io.EOF || resp.GetResponse() != nil {
				t.Fatalf("expected EOF; err: %v; resp: %v", err, resp)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleDeleteCmd, ""}) {
				t.Fatalf("DBUS Failure wanted gnsi_console.delete_checkpoint; got: %v", dbus)
			}
		},
	},
	{
		desc: "two accounts, no finalize",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Account: "alice",
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "password-alice"}},
								Version:   "version-1",
								CreatedOn: 123,
							},
							{
								Account: "bob",
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "password-bob"}},
								Version:   "version-2",
								CreatedOn: 321,
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetResponse() == nil {
				t.Fatal("Expected a message.")
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleSetCmd, `[{ "ConsolePasswords": [ { "name": "alice", "password" : "password-alice" },{ "name": "bob", "password" : "password-bob" } ] }]`}) {
				t.Fatalf("DBUS Failure wanted gnsi_console.set; got: %v", dbus)
			}
			if err = c.CloseSend(); err != nil {
				t.Fatal(err)
			}
			resp, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error but did not get it")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatal(err)
			}
			if resp != nil {
				t.Fatal("Received unexpected message after closing connection.")
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "incomplete set request (password), connection closed",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Account:   "alice",
								Version:   "version-1",
								CreatedOn: 123,
							},
						},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			if resp, err := c.Recv(); status.Code(err) != codes.Aborted {
				t.Fatalf("expected Aborted; err: %v; resp: %v", err, resp)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "incomplete set request (password blank), connection closed",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Account:   "alice",
								Version:   "version-1",
								CreatedOn: 123,
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: ""}},
							},
						},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			if resp, err := c.Recv(); status.Code(err) != codes.Aborted {
				t.Fatalf("expected Aborted; err: %v; resp: %v", err, resp)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "incomplete set request (username), connection closed",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "alice-password"}},
								Version:   "version-1",
								CreatedOn: 123,
							},
						},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			_, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error but did not get it")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatal(err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "incomplete set request (version), connection closed",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Account: "alice",
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "alice-password"}},
								CreatedOn: 123,
							},
						},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			_, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error but did not get it")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatal(err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "incomplete set request (created_on), connection closed",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Send(&credz.RotateAccountCredentialsRequest{
				Request: &credz.RotateAccountCredentialsRequest_Password{
					Password: &credz.PasswordRequest{
						Accounts: []*credz.PasswordRequest_Account{
							{
								Account: "alice",
								Password: &credz.PasswordRequest_Password{
									Value: &credz.PasswordRequest_Password_Plaintext{
										Plaintext: "alice-password"}},
								Version: "version-1",
							},
						},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			_, err = c.Recv()
			if err == nil {
				t.Fatal("Expected an error but did not get it")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatal(err)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleCreateCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleCreateCmd, dbus)
			}
			if dbus := dbusListen(ch); !reflect.DeepEqual(dbus, []string{expectedConsoleRestoreCmd, ""}) {
				t.Fatalf("DBUS Failure wanted: [%v ]; got: %+v", expectedConsoleRestoreCmd, dbus)
			}
		},
	},
	{
		desc: "no accounts, no finalize, abrupt close",
		f: func(t *testing.T, _ context.Context, _ credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			// Create a gNSI.console client and connect it to the gNSI.console server.
			tlsConfig := &tls.Config{InsecureSkipVerify: true}
			opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
			// targetAddr := "127.0.0.1:8081"
			conn, err := grpc.Dial(targetAddr, opts...)
			if err != nil {
				t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
			}
			defer conn.Close()
			sc := credz.NewCredentialzClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cancel()
			resp, err := c.Recv()
			if err == nil {
				t.Fatal("Expected an error but did not get it")
			}
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
			if resp != nil {
				t.Fatal("Received unexpected message after closing connection.")
			}
			select {
			case msg := <-ch:
				t.Fatal("Unexpected DBUS msg: %v", msg)
			default:
			}
		},
	},
	{
		desc: "read JSON with version info, fail",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			s := &GNSICredentialzServer{
				consoleCredMetadata: NewConsoleCredMetadata(),
			}
			if err := s.loadConsoleCredentialFreshness(""); err == nil {
				t.Fatal("Expected file read error")
			}
		},
	},
	{
		desc: "read JSON with version info, success",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			s := &GNSICredentialzServer{
				consoleCredMetadata: NewConsoleCredMetadata(),
			}
			if err := s.loadConsoleCredentialFreshness("../testdata/gnsi/console-version.json"); err != nil {
				t.Fatal(err)
			}
		},
	},
	{
		desc: "write JSON with version info, fail",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			s := &GNSICredentialzServer{
				consoleCredMetadata: NewConsoleCredMetadata(),
			}
			if err := s.saveConsoleCredentialsFreshness(""); err == nil {
				t.Fatal("Expected file write error")
			}
		},
	},
	{
		desc: "write JSON with version info, success",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string, targetAddr string) {
			s := &GNSICredentialzServer{
				consoleCredMetadata: NewConsoleCredMetadata(),
			}
			if err := s.saveConsoleCredentialsFreshness("../testdata/gnsi/console-version.json"); err != nil {
				t.Fatal(err)
			}
		},
	},
}

// TestConsoleServer tests implementation of gnsi.Ssh server.
func TestGnsiCredzConsoleServer(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	var dbusListener chan []string
	var done chan bool
	// dbusCaller is a package variable
	dbusListener, done, dbusCaller = newMockConsoleDbusServer()
	defer func() { close(dbusListener); close(done) }()

	// Create a gNSI.ssh client and connect it to the gNSI.ssh server.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()
	sc := credz.NewCredentialzClient(conn)
	for _, tc := range consoleTests {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Run(tc.desc, func(t *testing.T) {
			tc.f(t, ctx, sc, dbusListener, targetAddr)
			credzMu.Lock()
			t.Logf("Finished %s", tc.desc)
			credzMu.Unlock()
		})
		cancel()
	}

	// TODO(b/344081417) Enable code once NSF Freeze is implemented
	// // Test RPCs during NSF freeze mode.
	// s.WarmRestartHelper.SetFreezeStatus(true)
	// t.Run("RotateAccountCredentialsUnavailableDuringFreeze", func(t *testing.T) {
	// 	stream, err := sc.RotateAccountCredentials(context.Background())
	// 	if err != nil {
	// 		t.Fatal(err.Error())
	// 	}
	// 	if err = stream.Send(&credz.RotateAccountCredentialsRequest{}); err != nil {
	// 		t.Fatal(err.Error())
	// 	}
	// 	_, err = stream.Recv()
	// 	testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	// })
	// s.WarmRestartHelper.SetFreezeStatus(false)

	// Shutdown the mock gnsi_console service server.
	done <- true
}

var consoleTestsBadDBUS = []struct {
	desc string
	f    func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string)
}{
	{
		desc: "RPC fails",
		f: func(t *testing.T, ctx context.Context, sc credz.CredentialzClient, ch <-chan []string) {
			if _, err := sc.RotateAccountCredentials(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case msg := <-ch:
				t.Fatal("Unexpected DBUS msg: %v", msg)
			default:
			}
		},
	},
}

// TestConsoleServerNoDBUS tests implementation of gnsi.console server.
func TestGnsiCredzConsoleServerNoDBUS(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	var dbusListener chan []string
	var done chan bool
	// dbusCaller is a package variable
	dbusListener, done, dbusCaller = newFailingConsoleDbusServer()
	defer func() { close(dbusListener); close(done) }()

	// Create a gNSI.ssh client and connect it to the gNSI.ssh server.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()
	credzClient := credz.NewCredentialzClient(conn)

	var mu sync.Mutex
	for _, tc := range consoleTestsBadDBUS {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		t.Run(tc.desc, func(t *testing.T) {
			mu.Lock()
			defer mu.Unlock()
			tc.f(t, ctx, credzClient, dbusListener)
		})
		cancel()
	}

	// Shutdown the mock gnsi_console service server.
	done <- true
	s.gnsiCredz.saveConsoleCredentialsFreshness(s.config.ConsoleCredMetaFile)
}

func newMockConsoleDbusServer() (chan []string, chan bool, ssc.Caller) {
	ch := make(chan []string, 10)
	pass := make(chan []string, 10)
	done := make(chan bool, 1)
	go func() {
		checkpoint := false
		var mu sync.Mutex
		for {
			select {
			case <-done: // if cancel() execute
				log.Infoln("Shutting down the DBUS server.")
				return
			case msg := <-ch:
				mu.Lock()
				fmt.Printf("Received %v. Sending it out.\n", msg)
				switch msg[0] {
				case expectedConsoleCreateCmd:
					if checkpoint {
						log.Fatal("mock gnsi_console service: checkpoint already exists.")
					}
					checkpoint = true
				case expectedConsoleDeleteCmd:
					if !checkpoint {
						log.Fatal("mock gnsi_console service: checkpoint does not exists.")
					}
					checkpoint = false
				case expectedConsoleRestoreCmd:
					if !checkpoint {
						log.Fatal("mock gnsi_console service: checkpoint does not exists.")
					}
					checkpoint = false
				case expectedConsoleSetCmd:
					if !checkpoint {
						log.Fatal("mock gnsi_console service: set without checkpoint.")
					}
				default:
					log.Fatalf(`mock gnsi_console service: unknown service: "%v"`, msg[0])
				}
				mu.Unlock()
				pass <- msg
			}
		}
	}()

	return pass, done, &ssc.SpyDbusCaller{Command: ch}
}

func newFailingConsoleDbusServer() (chan []string, chan bool, ssc.Caller) {
	ch := make(chan []string, 1)
	done := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-done: // if cancel() execute
				log.Infoln("Shutting down the faulty DBUS server.")
				return
			case msg := <-ch:
				fmt.Printf("Received %v. Sending it out.\n", msg)
				ch <- msg
			}
		}
	}()

	return ch, done, &ssc.FailDbusCaller{}
}

func TestGnsiCredzUnimplemented(t *testing.T) {
	cs := GNSICredentialzServer{}
	t.Run("CanGenerateKeyUnimplemented", func(t *testing.T) {
		if _, err := cs.CanGenerateKey(nil, nil); status.Code(err) != codes.Unimplemented {
			t.Errorf("expected: Unimplemented, got: %+v", err)
		}
	})
	t.Run("GetPublicKeysUnimplemented", func(t *testing.T) {
		if _, err := cs.GetPublicKeys(nil, nil); status.Code(err) != codes.Unimplemented {
			t.Errorf("expected: Unimplemented, got: %+v", err)
		}
	})
}

var sshAcctIncompleteMsg = []struct {
	desc string
	msg  *credz.RotateAccountCredentialsRequest
}{
	{
		desc: "user; missing version",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_User{
				User: &credz.AuthorizedUsersRequest{
					Policies: []*credz.UserPolicy{
						{
							Account: "root",
							// Version:   "root-version-2",
							CreatedOn: uint64(time.Now().Unix()),
							AuthorizedPrincipals: &credz.UserPolicy_SshAuthorizedPrincipals{
								AuthorizedPrincipals: []*credz.UserPolicy_SshAuthorizedPrincipal{
									&credz.UserPolicy_SshAuthorizedPrincipal{
										AuthorizedUser: "alice",
									},
								},
							},
						},
					},
				},
			},
		},
	},
	{
		desc: "user; missing users",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_User{
				User: &credz.AuthorizedUsersRequest{
					Policies: []*credz.UserPolicy{
						{
							Account:   "root",
							Version:   "root-version-2",
							CreatedOn: uint64(time.Now().Unix()),
							AuthorizedPrincipals: &credz.UserPolicy_SshAuthorizedPrincipals{
								AuthorizedPrincipals: []*credz.UserPolicy_SshAuthorizedPrincipal{},
							},
						},
					},
				},
			},
		},
	},
	{
		desc: "user; missing user list",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_User{
				User: &credz.AuthorizedUsersRequest{
					Policies: []*credz.UserPolicy{
						{
							Account:   "root",
							Version:   "root-version-2",
							CreatedOn: uint64(time.Now().Unix()),
						},
					},
				},
			},
		},
	},
	{
		desc: "user;missing account",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_User{
				User: &credz.AuthorizedUsersRequest{
					Policies: []*credz.UserPolicy{
						{
							Version:   "root-version-2",
							CreatedOn: uint64(time.Now().Unix()),
						},
					},
				},
			},
		},
	},
	{
		desc: "user; missing timestamp",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_User{
				User: &credz.AuthorizedUsersRequest{
					Policies: []*credz.UserPolicy{
						{
							Account: "root",
							Version: "root-version-2",
						},
					},
				},
			},
		},
	},
	{
		desc: "user; missing user",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_User{},
		},
	},
	{
		desc: "cred; missing account",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_Credential{
				Credential: &credz.AuthorizedKeysRequest{
					Credentials: []*credz.AccountCredentials{
						{
							Version:   "root-version-1",
							CreatedOn: uint64(time.Now().Unix()),
							AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
								{
									AuthorizedKey: []byte("Authorized-key #1"),
									Options: []*credz.Option{
										{
											Key:   &credz.Option_Name{Name: "from"},
											Value: "*.sales.example.net,!pc.sales.example.net",
										},
									},
								},
								{
									AuthorizedKey: []byte("Authorized-key #2"),
									KeyType:       credz.KeyType_KEY_TYPE_UNSPECIFIED,
									Description:   "test#2",
								},
							},
						},
					},
				},
			},
		},
	},
	{
		desc: "cred; missing keys #1",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_Credential{
				Credential: &credz.AuthorizedKeysRequest{
					Credentials: []*credz.AccountCredentials{
						{
							Account:        "root",
							Version:        "root-version-1",
							CreatedOn:      uint64(time.Now().Unix()),
							AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{},
						},
					},
				},
			},
		},
	},
	{
		desc: "cred; missing cred",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_Credential{},
		},
	},
	{
		desc: "cred; missing timestamp",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_Credential{
				Credential: &credz.AuthorizedKeysRequest{
					Credentials: []*credz.AccountCredentials{
						{
							Account: "root",
							Version: "root-version-1",
							AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
								{AuthorizedKey: []byte("Authorized-key #2")},
							},
						},
					},
				},
			},
		},
	},
	{
		desc: "cred; missing account",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_Credential{
				Credential: &credz.AuthorizedKeysRequest{
					Credentials: []*credz.AccountCredentials{
						{
							Version:   "root-version-1",
							CreatedOn: uint64(time.Now().Unix()),
							AuthorizedKeys: []*credz.AccountCredentials_AuthorizedKey{
								{AuthorizedKey: []byte("Authorized-key #2")},
							},
						},
					},
				},
			},
		},
	},
	{
		desc: "cred; missing version",
		msg: &credz.RotateAccountCredentialsRequest{
			Request: &credz.RotateAccountCredentialsRequest_Credential{
				Credential: &credz.AuthorizedKeysRequest{
					Credentials: []*credz.AccountCredentials{
						{
							Account:   "root",
							CreatedOn: uint64(time.Now().Unix()),
						},
					},
				},
			},
		},
	},
}

var sshHostIncompleteMsg = []struct {
	desc string
	msg  *credz.RotateHostParametersRequest
}{
	{
		desc: "host missing request",
		msg: &credz.RotateHostParametersRequest{
			Request: &credz.RotateHostParametersRequest_SshCaPublicKey{},
		},
	},
	{
		desc: "host missing keys #1",
		msg: &credz.RotateHostParametersRequest{
			Request: &credz.RotateHostParametersRequest_SshCaPublicKey{
				SshCaPublicKey: &credz.CaPublicKeyRequest{
					SshCaPublicKeys: []*credz.PublicKey{&credz.PublicKey{
						PublicKey:   []byte{},
						KeyType:     credz.KeyType_KEY_TYPE_UNSPECIFIED,
						Description: "test",
					}},
					Version:   "CA-trust-bundle-1",
					CreatedOn: uint64(time.Now().Unix()),
				},
			},
		},
	},
	{
		desc: "host missing timestamp",
		msg: &credz.RotateHostParametersRequest{
			Request: &credz.RotateHostParametersRequest_SshCaPublicKey{
				SshCaPublicKey: &credz.CaPublicKeyRequest{
					Version:   "CA-trust-bundle-1",
					CreatedOn: uint64(time.Now().Unix()),
				},
			},
		},
	},
	{
		desc: "host missing version",
		msg: &credz.RotateHostParametersRequest{
			Request: &credz.RotateHostParametersRequest_SshCaPublicKey{
				SshCaPublicKey: &credz.CaPublicKeyRequest{
					Version:   "CA-trust-bundle-1",
					CreatedOn: uint64(time.Now().Unix()),
				},
			},
		},
	},
}

func TestGnsiCredzMissingRequests(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	var dbusListener chan []string
	var done chan bool
	// dbusCaller is a package variable
	dbusListener, done, dbusCaller = newMockSshDbusServer()
	defer func() { close(dbusListener); close(done) }()

	// Create a gNSI.ssh client and connect it to the gNSI.ssh server.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()
	sc := credz.NewCredentialzClient(conn)

	for _, m := range sshAcctIncompleteMsg {
		t.Run(m.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := sc.RotateAccountCredentials(ctx)
			if err != nil {
				t.Fatalf("error opening a streaming RotateAccountCredentials RPC: %v", err)
			}
			if err = c.Send(m.msg); err != nil {
				t.Fatalf("error sending an incomplete '%v'message: %v", m.desc, err)
			}
			if _, err := c.Recv(); status.Code(err) != codes.Aborted {
				t.Errorf("expected: Aborted, got: %+v", err)
			}
			if dbus := dbusListen(dbusListener); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Errorf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if dbus := dbusListen(dbusListener); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Errorf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		})
	}
	for _, m := range sshHostIncompleteMsg {
		t.Run(m.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := sc.RotateHostParameters(ctx)
			if err != nil {
				t.Fatalf("error opening a streaming RotateHostParameters RPC: %v", err)
			}
			if err = c.Send(m.msg); err != nil {
				t.Fatalf("error sending an incomplete '%v'message: %v", m.desc, err)
			}
			if _, err := c.Recv(); status.Code(err) != codes.Aborted {
				t.Errorf("expected: Aborted, got: %+v", err)
			}
			if dbus := dbusListen(dbusListener); !reflect.DeepEqual(dbus, []string{expectedSshCreateCmd, ""}) {
				t.Errorf("DBUS Failure wanted: [%v ]; got: %+v", expectedSshCreateCmd, dbus)
			}
			if dbus := dbusListen(dbusListener); !reflect.DeepEqual(dbus, []string{expectedSshRestoreCmd, ""}) {
				t.Errorf("DBUS Failure wanted %v; got: %v", expectedSshRestoreCmd, dbus)
			}
		})
	}
}
