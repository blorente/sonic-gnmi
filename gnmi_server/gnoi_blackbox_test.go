package gnmi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	bbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/blackbox"

	log "github.com/golang/glog"
	"github.com/openconfig/gnoi/types"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// Mock interface implementation that returns success!
type mockBBTrfmSuccess struct{}

func (t mockBBTrfmSuccess) setXcvrState(req string) (string, error) {
	return "{}", nil
}

// Mock interface implementation that returns error!
type mockBBTrfmFailure struct{}

func (t mockBBTrfmFailure) setXcvrState(req string) (string, error) {
	return "", status.Errorf(codes.Internal, fmt.Sprintf("BackEnd returns an error!"))
}

// SetTransceiverState test helper.
type setXcvrTestHelper struct {
	rc *redis.Client
}

func newsetXcvrTestHelper() (*setXcvrTestHelper, error) {
	h := new(setXcvrTestHelper)

	rc, err := getRedisDBClient(appStateDB)
	if err != nil {
		return nil, err
	}

	h.rc = rc
	return h, nil
}

// SetTransceiverState test helper: canonical test setup for Ethernet1234.
func (h *setXcvrTestHelper) setup() error {
	intfName := "Ethernet1234"
	key := strings.Join([]string{appPortTbl, intfName}, ":")
	m := make(map[string]interface{})
	m["alias"] = "Eth1234"
	m["index"] = "1234"
	if err := h.rc.HMSet(context.Background(), key, m).Err(); err != nil {
		log.V(lvl.ERROR).Info("Cannot setup DB for test: ", err.Error())
		return err
	}
	return nil
}

// SetTransceiverState test helper: canonical test cleanup.
func (h *setXcvrTestHelper) close() {
	if h.rc != nil {
		intfName := "Ethernet1234"
		key := strings.Join([]string{appPortTbl, intfName}, ":")
		if err := h.rc.Del(context.Background(), key).Err(); err != nil {
			log.V(lvl.ERROR).Info("Cannot cleanup DB for test: ", err.Error())
		}
		db.CloseRedisClient(h.rc)
	}
}

var testBBCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient)
}{
	{
		desc: "SetTransceiverStateFailsForEmptyInterfaceData",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			_, err := sc.SetTransceiverState(ctx, &bbpb.SetTransceiverStateRequest{})
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments", t)
		},
	},
	{
		desc: "SetTransceiverStateFailsForEmptyRequest",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			_, err = sc.SetTransceiverState(ctx, &bbpb.SetTransceiverStateRequest{})
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments!", t)
		},
	},
	{
		desc: "SetTransceiverStateFailsIfStateIsUnspecified",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			req := &bbpb.SetTransceiverStateRequest{
				TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
					{
						Transceiver: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "components",
								},
								{
									Name: "component",
									Key: map[string]string{
										"name": "Ethernet1234",
									},
								},
							},
						},
					},
				},
			}

			_, err = sc.SetTransceiverState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments!", t)
		},
	},
	{
		desc: "SetTransceiverStateFailsIfTransceiverIsEmpty",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			req := &bbpb.SetTransceiverStateRequest{
				TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
					{
						State: bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE,
					},
				},
			}

			_, err = sc.SetTransceiverState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments!", t)
		},
	},
	{
		desc: "SetTransceiverStateFailsIfDBUSBackEndFails",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			req := &bbpb.SetTransceiverStateRequest{
				TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
					{
						Transceiver: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "components",
								},
								{
									Name: "component",
									Key: map[string]string{
										"name": "Ethernet1234",
									},
								},
							},
						},
						State: bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE,
					},
				},
			}

			// Simulate back end failure.
			bbXfmr = mockBBTrfmFailure{}

			_, err = sc.SetTransceiverState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments!", t)
		},
	},
	{
		desc: "SetTransceiverStateFailsIfNoInterfaceIsAssociatedWithTransceiver",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			req := &bbpb.SetTransceiverStateRequest{
				TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
					{
						Transceiver: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "components",
								},
								{
									Name: "component",
									Key: map[string]string{
										"name": "Ethernet4444",
									},
								},
							},
						},
						State: bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE,
					},
				},
			}

			// Simulate back end success.
			bbXfmr = mockBBTrfmSuccess{}

			_, err = sc.SetTransceiverState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments!", t)
		},
	},
	{
		desc: "SetTransceiverStateTimesout",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			req := &bbpb.SetTransceiverStateRequest{
				TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
					{
						Transceiver: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "components",
								},
								{
									Name: "component",
									Key: map[string]string{
										"name": "Ethernet1234",
									},
								},
							},
						},
						State: bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE,
					},
				},
			}

			// Simulate back end success.
			bbXfmr = mockBBTrfmSuccess{}

			_, err = sc.SetTransceiverState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.Internal, "Timeout!", t)
		},
	},
	{
		desc: "SetTransceiverStateFails",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			np, err := common_utils.NewNotificationProducer(setHwLinkRespCh)
			if err != nil {
				log.Error(err.Error())
				t.Fatalf(err.Error())
			}
			defer np.Close()

			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			sub := stc.PSubscribe(context.Background(), setHwLinkReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.Error(err.Error())
				return
			}

			swssErrSs := []string{
				"SWSS_RC_UNKNOWN", "SWSS_RC_IN_USE", "SWSS_RC_INVALID_PARAM",
				"SWSS_RC_DEADLINE_EXCEEDED", "SWSS_RC_NOT_FOUND", "SWSS_RC_EXISTS", "SWSS_RC_PERMISSION_DENIED",
				"SWSS_RC_FULL", "SWSS_RC_UNIMPLEMENTED", "SWSS_RC_INTERNAL", "SWSS_RC_NO_MEMORY", "SWSS_RC_NOT_EXECUTED", "SWSS_RC_UNAVAIL"}
			for _, swssErrS := range swssErrSs {
				channel := sub.Channel()
				done := make(chan bool, 1)

				// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
				go func() {
					for {
						select {
						case msg := <-channel:
							op, _, _, err := processMsgPayload(msg.Payload)
							if err != nil {
								t.Fatalf(err.Error())
							}
							if op != setHwLinkOp {
								t.Fatalf("Malformed request!")
							}
							intfName := "Ethernet1234"
							hash := map[string]string{
								intfName: strings.Join([]string{swssErrS, "Cannot process!"}, ":"),
							}
							// Publish to notification channel.
							if err := np.Send("SWSS_RC_INTERNAL", "", hash); err != nil {
								log.Error(err.Error())
								t.Fatalf(err.Error())
							}
						case <-done:
							return
						}
					}
				}()
				req := &bbpb.SetTransceiverStateRequest{
					TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
						{
							Transceiver: &types.Path{
								Origin: "openconfig",
								Elem: []*types.PathElem{
									{
										Name: "components",
									},
									{
										Name: "component",
										Key: map[string]string{
											"name": "Ethernet1234",
										},
									},
								},
							},
							State: bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE,
						},
					},
				}

				// Simulate back end success.
				bbXfmr = mockBBTrfmSuccess{}

				resp, err := sc.SetTransceiverState(ctx, req)
				done <- true
				if err != nil {
					t.Fatal("Expected RPC to pass, got error.")
				}
				if len(resp.GetTransceiverResponses()) != 1 {
					t.Fatalf("Expected response for one interface, got %d.", len(resp.GetTransceiverResponses()))
				}
				for _, r := range resp.GetTransceiverResponses() {
					if r.GetStatus().GetCode() != int32(swssToErrorCode(swssErrS)) {
						t.Fatalf("Got %v, expected %v.", r.GetStatus().GetCode(), int32(swssToErrorCode(swssErrS)))
					}
				}
			}
		},
	},
	{
		desc: "SetTransceiverStateSucceeds",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			np, err := common_utils.NewNotificationProducer(setHwLinkRespCh)
			if err != nil {
				log.Error(err.Error())
				t.Fatalf(err.Error())
			}
			defer np.Close()

			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			sub := stc.PSubscribe(context.Background(), setHwLinkReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.Error(err.Error())
				return
			}

			swssS := "SWSS_RC_SUCCESS"
			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, _, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatalf(err.Error())
						}
						if op != setHwLinkOp {
							t.Fatalf("Malformed request!")
						}
						intfName := "Ethernet1234"
						hash := map[string]string{
							intfName: strings.Join([]string{swssS, ""}, ":"),
						}
						// Publish to notification channel.
						if err := np.Send(swssS, "", hash); err != nil {
							log.Error(err.Error())
							t.Fatalf(err.Error())
						}
					case <-done:
						return
					}
				}
			}()
			req := &bbpb.SetTransceiverStateRequest{
				TransceiverRequests: []*bbpb.SetTransceiverStateRequest_TransceiverStateRequest{
					{
						Transceiver: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "components",
								},
								{
									Name: "component",
									Key: map[string]string{
										"name": "Ethernet1234",
									},
								},
							},
						},
						State: bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE,
					},
				},
			}

			// Simulate back end success.
			bbXfmr = mockBBTrfmSuccess{}

			resp, err := sc.SetTransceiverState(ctx, req)
			if err != nil {
				t.Fatal("Expected RPC to pass, got error.")
			}
			if len(resp.GetTransceiverResponses()) != 1 {
				t.Fatalf("Expected response for one interface, got %d.", len(resp.GetTransceiverResponses()))
			}
			for _, r := range resp.GetTransceiverResponses() {
				if r.GetStatus().GetCode() != int32(codes.OK) {
					t.Fatalf("Got %v, expected %v.", r.GetStatus().GetCode(), int32(codes.OK))
				}
			}
		},
	},
	{
		desc: "SetHardwareLinkStateFailsForInvalidInterface",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			req := &bbpb.SetHardwareLinkStateRequest{
				LinkRequests: []*bbpb.SetHardwareLinkStateRequest_HardwareLinkStateInfo{
					{
						Interface: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "interfaces",
								},
								{
									Name: "interface",
									Key: map[string]string{
										"name": "EthernetX1234",
									},
								},
							},
						},
						Enabled: false,
					},
				},
			}

			_, err := sc.SetHardwareLinkState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.InvalidArgument, "Invalid arguments!", t)
		},
	},
	{
		desc: "SetHardwareLinkStateTimeout",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			// Setup DB.
			cc, err := getRedisDBClient(configDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(cc)

			intfName := "EthernetX1234"
			key := getKey([]string{portKeyPrefix, intfName})
			if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
				t.Fatalf("Cannot setup DB for test: %v.", err.Error())
			}
			// Cleanup DB.
			defer func() {
				if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
					t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
				}
			}()
			req := &bbpb.SetHardwareLinkStateRequest{
				LinkRequests: []*bbpb.SetHardwareLinkStateRequest_HardwareLinkStateInfo{
					{
						Interface: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "interfaces",
								},
								{
									Name: "interface",
									Key: map[string]string{
										"name": intfName,
									},
								},
							},
						},
						Enabled: false,
					},
				},
			}

			_, err = sc.SetHardwareLinkState(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.DeadlineExceeded, "Response timeout!", t)
		},
	},
	{
		desc: "SetHardwareLinkStateFails",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			// Setup DB.
			cc, err := getRedisDBClient(configDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(cc)

			intfName := "EthernetX1234"
			key := getKey([]string{portKeyPrefix, intfName})
			if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
				t.Fatalf("Cannot setup DB for test: %v.", err.Error())
			}
			// Cleanup DB.
			defer func() {
				if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
					t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
				}
			}()

			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			np, err := common_utils.NewNotificationProducer(setHwLinkRespCh)
			if err != nil {
				log.Error(err.Error())
				t.Fatalf(err.Error())
			}
			defer np.Close()

			sub := stc.PSubscribe(context.Background(), setHwLinkReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.Error(err.Error())
				return
			}

			swssErrSs := []string{
				"SWSS_RC_UNKNOWN", "SWSS_RC_IN_USE", "SWSS_RC_INVALID_PARAM",
				"SWSS_RC_DEADLINE_EXCEEDED", "SWSS_RC_NOT_FOUND", "SWSS_RC_EXISTS", "SWSS_RC_PERMISSION_DENIED",
				"SWSS_RC_FULL", "SWSS_RC_UNIMPLEMENTED", "SWSS_RC_INTERNAL", "SWSS_RC_NO_MEMORY", "SWSS_RC_NOT_EXECUTED", "SWSS_RC_UNAVAIL"}
			for _, swssErrS := range swssErrSs {
				channel := sub.Channel()
				done := make(chan bool, 1)

				// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case msg := <-channel:
							op, _, _, err := processMsgPayload(msg.Payload)
							if err != nil {
								t.Fatalf(err.Error())
							}
							if op != setHwLinkOp {
								t.Fatalf("Malformed request!")
							}
							hash := map[string]string{
								intfName: strings.Join([]string{swssErrS, "Cannot process!"}, ":"),
							}
							// Publish to notification channel.
							if err := np.Send("SWSS_RC_INTERNAL", "", hash); err != nil {
								log.Error(err.Error())
								t.Fatalf(err.Error())
							}
						case <-done:
							return
						}
					}
				}()
				req := &bbpb.SetHardwareLinkStateRequest{
					LinkRequests: []*bbpb.SetHardwareLinkStateRequest_HardwareLinkStateInfo{
						{
							Interface: &types.Path{
								Origin: "openconfig",
								Elem: []*types.PathElem{
									{
										Name: "interfaces",
									},
									{
										Name: "interface",
										Key: map[string]string{
											"name": intfName,
										},
									},
								},
							},
							Enabled: true,
						},
					},
				}

				resp, err := sc.SetHardwareLinkState(ctx, req)
				done <- true
				wg.Wait()
				if err != nil {
					t.Fatal("Expected RPC to pass, got error.")
				}
				if len(resp.GetLinkResponses()) != 1 {
					t.Fatalf("Expected response for one interface, got %d.", len(resp.GetLinkResponses()))
				}
				for _, r := range resp.GetLinkResponses() {
					if r.GetStatus().GetCode() != int32(swssToErrorCode(swssErrS)) {
						t.Fatalf("Got %v, expected %v.", r.GetStatus().GetCode(), int32(swssToErrorCode(swssErrS)))
					}
				}
			}
		},
	},
	{
		desc: "SetHardwareLinkStateSucceeds",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			// Setup DB.
			cc, err := getRedisDBClient(configDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(cc)

			intfName := "EthernetX1234"
			key := getKey([]string{portKeyPrefix, intfName})
			if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
				t.Fatalf("Cannot setup DB for test: %v.", err.Error())
			}
			// Cleanup DB.
			defer func() {
				if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
					t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
				}
			}()

			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			np, err := common_utils.NewNotificationProducer(setHwLinkRespCh)
			if err != nil {
				log.Error(err.Error())
				t.Fatalf(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), setHwLinkReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.Error(err.Error())
				return
			}

			swssS := "SWSS_RC_SUCCESS"

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, _, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatalf(err.Error())
						}
						if op != setHwLinkOp {
							t.Fatalf("Malformed request!")
						}
						hash := map[string]string{
							intfName: strings.Join([]string{swssS, ""}, ":"),
						}
						// Publish to notification channel.
						if err := np.Send(swssS, "", hash); err != nil {
							log.Error(err.Error())
							t.Fatalf(err.Error())
						}
					case <-done:
						return
					}
				}
			}()
			req := &bbpb.SetHardwareLinkStateRequest{
				LinkRequests: []*bbpb.SetHardwareLinkStateRequest_HardwareLinkStateInfo{
					{
						Interface: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "interfaces",
								},
								{
									Name: "interface",
									Key: map[string]string{
										"name": intfName,
									},
								},
							},
						},
						Enabled: true,
					},
				},
			}

			resp, err := sc.SetHardwareLinkState(ctx, req)
			if err != nil {
				t.Fatal("Expected RPC to pass, got error.")
			}
			if len(resp.GetLinkResponses()) != 1 {
				t.Fatalf("Expected response for one interface, got %d.", len(resp.GetLinkResponses()))
			}
			for _, r := range resp.GetLinkResponses() {
				if r.GetStatus().GetCode() != int32(codes.OK) {
					t.Fatalf("Got %v, expected %v.", r.GetStatus().GetCode(), int32(codes.OK))
				}
			}
		},
	},
	{
		desc: "SetAlarmFailsForInvalidComponent",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			_, err := sc.SetAlarm(ctx, &bbpb.SetAlarmRequest{Id: "test", Resource: "dummy:dummy"})
			testErr(err, codes.InvalidArgument, "Invalid component for SetAlarm!", t)
		},
	},
	{
		desc: "SetAlarmWithCriticalSeveritySucceeds",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			sdb, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer db.CloseRedisClient(sdb)
			for comp, _ := range validComp() {
				req := &bbpb.SetAlarmRequest{
					Id:          "Test Alarm",
					Resource:    comp,
					Description: "ERROR: Panic!",
					Severity:    bbpb.OpenconfigAlarmTypesOPENCONFIGALARMSEVERITY_OPENCONFIGALARMTYPESOPENCONFIGALARMSEVERITY_CRITICAL,
				}
				_, err := sc.SetAlarm(ctx, req)
				if err != nil {
					t.Fatal(err.Error())
				}
				if err = sdb.Del(context.Background(), getKey([]string{compStTbl, comp})).Err(); err != nil {
					t.Fatal(err.Error())
				}
			}
		},
	},
	{
		desc: "SetAlarmWithMinorSeveritySucceeds",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			sdb, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer db.CloseRedisClient(sdb)
			for comp, _ := range validComp() {
				req := &bbpb.SetAlarmRequest{
					Id:          "Test Alarm",
					Resource:    comp,
					Description: "MINOR: Panic!",
					Severity:    bbpb.OpenconfigAlarmTypesOPENCONFIGALARMSEVERITY_OPENCONFIGALARMTYPESOPENCONFIGALARMSEVERITY_MINOR,
				}
				_, err := sc.SetAlarm(ctx, req)
				if err != nil {
					t.Fatal(err.Error())
				}
				if err = sdb.Del(context.Background(), getKey([]string{compStTbl, comp})).Err(); err != nil {
					t.Fatal(err.Error())
				}
			}
		},
	},
	{
		desc: "SetAlarmWithEqptTypeSucceeds",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			sdb, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer db.CloseRedisClient(sdb)
			for comp, _ := range validComp() {
				req := &bbpb.SetAlarmRequest{
					Id:          "Test Alarm",
					Resource:    comp,
					Description: "INACTIVE: Panic!",
					Severity:    bbpb.OpenconfigAlarmTypesOPENCONFIGALARMSEVERITY_OPENCONFIGALARMTYPESOPENCONFIGALARMSEVERITY_WARNING,
					Type:        bbpb.OpenconfigAlarmTypesOPENCONFIGALARMTYPEID_OPENCONFIGALARMTYPESOPENCONFIGALARMTYPEID_EQPT,
				}
				_, err := sc.SetAlarm(ctx, req)
				if err != nil {
					t.Fatal(err.Error())
				}
				if err = sdb.Del(context.Background(), getKey([]string{compStTbl, comp})).Err(); err != nil {
					t.Fatal(err.Error())
				}
			}
		},
	},
	{
		desc: "VerifyStateSucceeds",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			cdb, err := getRedisDBClient(configDB)
			if err != nil {
				return
			}
			defer db.CloseRedisClient(cdb)
			sdb, err := getRedisDBClient(stateDB)
			if err != nil {
				return
			}
			defer db.CloseRedisClient(sdb)

			sub := cdb.Subscribe(context.Background(), stateVerReqChan)
			if _, err = sub.Receive(context.Background()); err != nil {
				return
			}
			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()
			// Start a goroutine to listen to the notification channel and write to State DB.
			go func() {
				for {
					select {
					case msg := <-channel:
						var req []string
						err := json.Unmarshal([]byte(msg.Payload), &req)
						if err != nil {
							continue
						}
						if len(req) != 2 {
							continue
						}
						hash := make(map[string]interface{})
						hash["timestamp"] = req[1]
						hash["status"] = "pass"
						hash["err_str"] = ""
						if err := sdb.HMSet(context.Background(), getKey([]string{stateVerRespTbl, req[0]}), hash).Err(); err != nil {
							return
						}
					case <-done:
						return
					}
				}
			}()

			resp, err := sc.VerifyState(ctx, &bbpb.VerifyStateRequest{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetSuccess() == false {
				t.Fatalf("Expect overall state verification to be true, got false.")
			}
			if len(resp.GetResults()) == 0 {
				t.Fatalf("Expect results, got none.")
			}
			for _, r := range resp.GetResults() {
				if r.GetStatus().GetCode() != int32(codes.OK) {
					t.Fatalf("Got %v, expect %v.", r.GetStatus().GetCode(), int32(codes.OK))
				}
			}
			// Sleep for 1 second so that the next state verification test will use a different timestamp.
			time.Sleep(time.Second)
		},
	},
	{
		desc: "VerifyStateFails",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			cdb, err := getRedisDBClient(configDB)
			if err != nil {
				return
			}
			defer db.CloseRedisClient(cdb)
			sdb, err := getRedisDBClient(stateDB)
			if err != nil {
				return
			}
			defer db.CloseRedisClient(sdb)

			sub := cdb.Subscribe(context.Background(), stateVerReqChan)
			if _, err = sub.Receive(context.Background()); err != nil {
				return
			}
			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()
			// Start a goroutine to listen to the notification channel and write to State DB.
			go func() {
				for {
					select {
					case msg := <-channel:
						var req []string
						err := json.Unmarshal([]byte(msg.Payload), &req)
						if err != nil {
							continue
						}
						if len(req) != 2 {
							continue
						}
						hash := make(map[string]interface{})
						hash["timestamp"] = req[1]
						hash["status"] = "fail"
						hash["err_str"] = "Some reason"
						if err := sdb.HMSet(context.Background(), getKey([]string{stateVerRespTbl, req[0]}), hash).Err(); err != nil {
							return
						}
					case <-done:
						return
					}
				}
			}()

			resp, err := sc.VerifyState(ctx, &bbpb.VerifyStateRequest{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetSuccess() == true {
				t.Fatalf("Expect overall state verification to be false, got true.")
			}
			if len(resp.GetResults()) == 0 {
				t.Fatalf("Expect results, got none.")
			}
			for _, r := range resp.GetResults() {
				if r.GetStatus().GetCode() != int32(codes.Internal) {
					t.Fatalf("Got %v, expect %v.", r.GetStatus().GetCode(), int32(codes.Internal))
				}
				if r.GetStatus().GetMessage() != "Some reason" {
					t.Fatalf("Got %v, expect %v.", r.GetStatus().GetMessage(), "Some reason")
				}
			}
			// Sleep for 1 second so that the next state verification test will use a different timestamp.
			time.Sleep(time.Second)
		},
	},
	{
		desc: "VerifyStateFailsOnComponentTimeout",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			verifyStateTimeout = 5 * time.Second
			defer func() { verifyStateTimeout = 80 * time.Second }()
			resp, err := sc.VerifyState(ctx, &bbpb.VerifyStateRequest{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetSuccess() == true {
				t.Fatalf("Expect overall state verification to be false, got true.")
			}
			for _, r := range resp.GetResults() {
				if r.GetStatus().GetCode() != int32(codes.DeadlineExceeded) {
					t.Fatalf("Got %v, expect %v.", r.GetStatus().GetCode(), int32(codes.DeadlineExceeded))
				}
			}
		},
	},
}

var testBBFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient)
}{
	{
		desc: "SetTransceiverStateFailsAsBackEndReturnsAnError",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			_, err := sc.SetTransceiverState(ctx, &bbpb.SetTransceiverStateRequest{})
			testErr(err, codes.Internal, "BackEnd returns an error!", t)
		},
	},
}

var testBBRespFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient)
}{
	{
		desc: "SetTransceiverStateFailsAsBackEndReturnsInvalidResponse",
		f: func(ctx context.Context, t *testing.T, sc bbpb.BlackBoxTestClient) {
			_, err := sc.SetTransceiverState(ctx, &bbpb.SetTransceiverStateRequest{})
			testErr(err, codes.Internal, "Cannot unmarshal the response", t)
		},
	},
}

// Tests BlackBox Test services.
func TestGnoiBlackboxTest(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc := bbpb.NewBlackBoxTestClient(conn)

	// Simulate back end success.
	bbXfmr = mockBBTrfmSuccess{}
	for _, test := range testBBCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, sc)
		})
	}

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("SetTransceiverStateUnavailableDuringFreeze", func(t *testing.T) {
		_, err = sc.SetTransceiverState(ctx, &bbpb.SetTransceiverStateRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("SetHardwareLinkStateUnavailableDuringFreeze", func(t *testing.T) {
		_, err = sc.SetHardwareLinkState(ctx, &bbpb.SetHardwareLinkStateRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("SetAlarmUnavailableDuringFreeze", func(t *testing.T) {
		_, err = sc.SetAlarm(ctx, &bbpb.SetAlarmRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("VerifyStateUnavailableDuringFreeze", func(t *testing.T) {
		_, err = sc.VerifyState(ctx, &bbpb.VerifyStateRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)
}
