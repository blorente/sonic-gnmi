package gnmi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	debugpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/debug"

	log "github.com/golang/glog"
	"github.com/openconfig/gnoi/healthz"
	types "github.com/openconfig/gnoi/types"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

// compareStringSlices compares 2 arrays of string, return True if they contains the same set of elements.
func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int)
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}

// removeFields deletes ids in Redis table with key equal to idKey.
func removeFields(idKey string, ids map[string]string, sc *redis.Client) error {
	for id, _ := range ids {
		if err := sc.HDel(context.Background(), idKey, id).Err(); err != nil {
			return fmt.Errorf("Cannot delete entry for key: %s, id: %s.", idKey, id)
		}
	}
	return nil
}

var testHealthzCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client)
}{
	{
		desc: "HealthzGetFailsForInvalidComponent",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			_, err := sc.Get(ctx, &healthz.GetRequest{})
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)
		},
	},
	{
		desc: "HealthzGetFailsForInvalidLogicalPort",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			req := &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "interfaces",
						},
						{
							Name: "interface",
							Key: map[string]string{
								"name": "Ethernet1234",
							},
						},
					},
				},
			}
			_, err := sc.Get(ctx, req)
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)
		},
	},
	{
		desc: "HealthzGetTimesoutForIntfWithNoResponse",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			// Setup DB.
			intfName := "Ethernet1234"
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

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			_, err = sc.Get(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.DeadlineExceeded, "Response timeout!", t)
		},
	},
	{
		desc: "HealthzGetForIntfTimesoutWhenResponsesDoNotMatch",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			// Setup DB.
			intfName := "Ethernet1234"
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

			np, err := common_utils.NewNotificationProducer(intfHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), intfHlthReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getIntfDbgDataOp {
							t.Fatal("Malformed request!")
						}
						hash := map[string]string{
							intfDbgDataFld: "some phy debug data",
						}
						// Publish to notification channel. Change data field. Front end cannot match.
						if err := np.Send(op, data+"[ some random data ]", hash); err != nil {
							t.Fatal(err.Error())
						}

						hash = map[string]string{
							"[ some random field ]": "some phy debug data",
						}
						// Publish to notification channel. Change field in the map. Front end cannot match.
						if err := np.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-done:
						return
					}
				}
			}()

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			_, err = sc.Get(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.DeadlineExceeded, "Response timeout!", t)
		},
	},
	{
		desc: "HealthzGetForIntfFailsForXcvrInfo",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			// Setup DB.
			intfName := "Ethernet1234"
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

			np, err := common_utils.NewNotificationProducer(intfHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), intfHlthReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getIntfDbgDataOp {
							t.Fatal("Malformed request!")
						}
						hash := map[string]string{
							intfDbgDataFld: "some phy debug data",
						}
						// Publish to notification channel.
						if err := np.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-done:
						return
					}
				}
			}()

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}

			if _, err = sc.Get(ctx, req); err == nil {
				t.Fatal("Expected failure!")
			}
		},
	},
	{
		desc: "HealthzGetForIntfSucceeds",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			h, err := newsetXcvrTestHelper()
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := h.setup(); err != nil {
				t.Fatal("Cannot setup DB for test: ", err.Error())
			}
			defer h.close()

			// Setup DB.
			intfName := "Ethernet1234"
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

			np, err := common_utils.NewNotificationProducer(intfHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), intfHlthReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getIntfDbgDataOp {
							t.Fatal("Malformed request!")
						}
						hash := map[string]string{
							intfDbgDataFld: "some phy debug data",
						}
						// Publish to notification channel.
						if err := np.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-done:
						return
					}
				}
			}()

			npx, err := common_utils.NewNotificationProducer(xcvrHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer npx.Close()

			subx := stc.Subscribe(context.Background(), xcvrHlthReqCh)
			if _, err := subx.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channelx := subx.Channel()
			donex := make(chan bool, 1)
			defer func() { donex <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channelx:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getXcvrDbgDataOp {
							t.Fatal("Malformed request!")
						}
						m := map[string][]string{
							"1": {"1", "23", "456"},
							"7": {"890"},
						}
						respStr, err := json.Marshal(m)
						if err != nil {
							t.Fatal(err.Error())
						}
						hash := map[string]string{
							xcvrDbgDataFld: string(respStr),
						}
						// Publish to notification channel.
						if err := npx.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-donex:
						return
					}
				}
			}()

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			if _, err := sc.Get(ctx, req); err != nil {
				t.Fatal("Expected success, got ", err.Error())
			}
		},
	},
	{
		desc: "HealthzGetTimesoutForXcvrWithNoResponse",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			_, err := sc.Get(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.DeadlineExceeded, "Response timeout!", t)
		},
	},
	{
		desc: "HealthzGetForXcvrTimesoutWhenResponsesDoNotMatch",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			np, err := common_utils.NewNotificationProducer(xcvrHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), xcvrHlthReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getXcvrDbgDataOp {
							t.Fatal("Malformed request!")
						}
						m := map[string][]string{
							"1": {"1", "23", "456"},
						}
						respStr, err := json.Marshal(m)
						if err != nil {
							t.Fatal(err.Error())
						}
						hash := map[string]string{
							xcvrDbgDataFld: string(respStr),
						}
						// Publish to notification channel. Change data field. Front end cannot match.
						if err := np.Send(op, data+"[ some random data ]", hash); err != nil {
							t.Fatal(err.Error())
						}

						hash = map[string]string{
							"[ some random field ]": string(respStr),
						}
						// Publish to notification channel. Change field in the map. Front end cannot match.
						if err := np.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-done:
						return
					}
				}
			}()

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			_, err = sc.Get(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.DeadlineExceeded, "Response timeout!", t)
		},
	},
	{
		desc: "HealthzGetForXcvrFailsForMalformedResponse",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			np, err := common_utils.NewNotificationProducer(xcvrHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), xcvrHlthReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getXcvrDbgDataOp {
							t.Fatal("Malformed request!")
						}
						m := map[string][]string{
							"someFieldsButANumber": {"1", "23", "456"},
						}
						respStr, err := json.Marshal(m)
						if err != nil {
							t.Fatal(err.Error())
						}
						hash := map[string]string{
							xcvrDbgDataFld: string(respStr),
						}
						// Publish to notification channel. Change data field. Front end cannot match.
						if err := np.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-done:
						return
					}
				}
			}()

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			_, err = sc.Get(ctx, req)
			if err == nil {
				t.Fatal("Expected failure!")
			}
			testErr(err, codes.Internal, "Cannot parse eeprom page number", t)
		},
	},
	{
		desc: "HealthzGetForXcvrSucceeds",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			stc, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(stc)

			np, err := common_utils.NewNotificationProducer(xcvrHlthRespCh)
			if err != nil {
				t.Fatal(err.Error())
			}
			defer np.Close()

			sub := stc.Subscribe(context.Background(), xcvrHlthReqCh)
			if _, err := sub.Receive(context.Background()); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return
			}

			channel := sub.Channel()
			done := make(chan bool, 1)
			defer func() { done <- true }()

			// Start a goroutine to listen to the notification channel (request) and write to the notification channel (response).
			go func() {
				for {
					select {
					case msg := <-channel:
						op, data, _, err := processMsgPayload(msg.Payload)
						if err != nil {
							t.Fatal(err.Error())
						}
						if op != getXcvrDbgDataOp {
							t.Fatal("Malformed request!")
						}
						m := map[string][]string{
							"1": {"1", "23", "456"},
						}
						respStr, err := json.Marshal(m)
						if err != nil {
							t.Fatal(err.Error())
						}
						hash := map[string]string{
							xcvrDbgDataFld: string(respStr),
						}
						// Publish to notification channel.
						if err := np.Send(op, data, hash); err != nil {
							t.Fatal(err.Error())
						}
					case <-done:
						return
					}
				}
			}()

			req := &healthz.GetRequest{
				Path: &types.Path{
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
			}
			if _, err := sc.Get(ctx, req); err != nil {
				t.Fatal("Expected success, got ", err.Error())
			}
		},
	},
	{
		desc: "HealthzGetForInvalidPaths",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			req := &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "invalid",
						},
						{
							Name: "component",
							Key: map[string]string{
								"name": "all",
							},
						},
						{
							Name: "healthz",
						},
						{
							Name: "alert-info",
						},
					},
				},
			}
			_, err := sc.Get(ctx, req)
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)

			req = &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "components",
						},
						{
							Name: "invalid",
							Key: map[string]string{
								"name": "all",
							},
						},
						{
							Name: "healthz",
						},
						{
							Name: "alert-info",
						},
					},
				},
			}
			_, err = sc.Get(ctx, req)
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)

			req = &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "components",
						},
						{
							Name: "component",
							Key: map[string]string{
								"invalid": "all",
							},
						},
						{
							Name: "healthz",
						},
						{
							Name: "alert-info",
						},
					},
				},
			}
			_, err = sc.Get(ctx, req)
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)

			req = &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "components",
						},
						{
							Name: "component",
							Key: map[string]string{
								"name": "all",
							},
						},
						{
							Name: "invalid",
						},
						{
							Name: "alert-info",
						},
					},
				},
			}
			_, err = sc.Get(ctx, req)
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)

			req = &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "components",
						},
						{
							Name: "component",
							Key: map[string]string{
								"name": "all",
							},
						},
						{
							Name: "healthz",
						},
						{
							Name: "invalid",
						},
					},
				},
			}
			_, err = sc.Get(ctx, req)
			testErr(err, codes.Unimplemented, "Healthz.Get is unimplemented", t)
		},
	},
	// TODO(b/334459152) Mock DBUS calls for these tests.
	// {
	// 	desc: "HealthzGetForDebugDataFailsForHostServiceError",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	return 1, "", fmt.Errorf("error sending message to DBUS")
	// 		// }

	// 		req := &healthz.GetRequest{
	// 			Path: &types.Path{
	// 				Origin: "openconfig",
	// 				Elem: []*types.PathElem{
	// 					{
	// 						Name: "components",
	// 					},
	// 					{
	// 						Name: "component",
	// 						Key: map[string]string{
	// 							"name": "all",
	// 						},
	// 					},
	// 					{
	// 						Name: "healthz",
	// 					},
	// 					{
	// 						Name: "alert-info",
	// 					},
	// 				},
	// 			},
	// 		}
	// 		_, err := sc.Get(ctx, req)
	// 		testErr(err, codes.Internal, "Error", t)
	// 	},
	// },
	// {
	// 	desc: "HealthzGetForDebugDataFailsForHostServiceErrorCode",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	return 1, "", nil
	// 		// }

	// 		req := &healthz.GetRequest{
	// 			Path: &types.Path{
	// 				Origin: "openconfig",
	// 				Elem: []*types.PathElem{
	// 					{
	// 						Name: "components",
	// 					},
	// 					{
	// 						Name: "component",
	// 						Key: map[string]string{
	// 							"name": "all",
	// 						},
	// 					},
	// 					{
	// 						Name: "healthz",
	// 					},
	// 					{
	// 						Name: "alert-info",
	// 					},
	// 				},
	// 			},
	// 		}
	// 		_, err := sc.Get(ctx, req)
	// 		testErr(err, codes.Internal, "Host service error", t)
	// 	},
	// },
	// {
	// 	desc: "HealthzGetForDebugDataFailsForNotExistingFile",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	return 0, "invalid", nil
	// 		// }

	// 		req := &healthz.GetRequest{
	// 			Path: &types.Path{
	// 				Origin: "openconfig",
	// 				Elem: []*types.PathElem{
	// 					{
	// 						Name: "components",
	// 					},
	// 					{
	// 						Name: "component",
	// 						Key: map[string]string{
	// 							"name": "all",
	// 						},
	// 					},
	// 					{
	// 						Name: "healthz",
	// 					},
	// 					{
	// 						Name: "alert-info",
	// 					},
	// 				},
	// 			},
	// 		}
	// 		_, err := sc.Get(ctx, req)
	// 		testErr(err, codes.Internal, "Error", t)
	// 	},
	// },
	// {
	// 	desc: "HealthzGetForDebugDataFailsForCheckError",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		artifactColTimeout = 30 * time.Second
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	if call == "debug_info.check" {
	// 		// 		return 1, "", fmt.Errorf("error sending message to DBUS")
	// 		// 	}
	// 		// 	return 0, "invalid", nil
	// 		// }

	// 		req := &healthz.GetRequest{
	// 			Path: &types.Path{
	// 				Origin: "openconfig",
	// 				Elem: []*types.PathElem{
	// 					{
	// 						Name: "components",
	// 					},
	// 					{
	// 						Name: "component",
	// 						Key: map[string]string{
	// 							"name": "all",
	// 						},
	// 					},
	// 					{
	// 						Name: "healthz",
	// 					},
	// 					{
	// 						Name: "alert-info",
	// 					},
	// 				},
	// 			},
	// 		}
	// 		_, err := sc.Get(ctx, req)
	// 		testErr(err, codes.Internal, "Error", t)
	// 	},
	// },
	// {
	// 	desc: "HealthzAcknowledgeForDebugDataForHostServiceError",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	return 1, "", fmt.Errorf("error sending message to DBUS")
	// 		// }
	// 		req := &healthz.AcknowledgeRequest{
	// 			Id: "file",
	// 		}
	// 		_, err := sc.Acknowledge(ctx, req)
	// 		testErr(err, codes.Internal, "Error", t)
	// 	},
	// },
	// {
	// 	desc: "HealthzAcknowledgeForDebugDataForHostServiceErrorCode",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	return 1, "", nil
	// 		// }
	// 		req := &healthz.AcknowledgeRequest{
	// 			Id: "file",
	// 		}
	// 		_, err := sc.Acknowledge(ctx, req)
	// 		testErr(err, codes.Internal, "Host service error", t)
	// 	},
	// },
	// {
	// 	desc: "HealthzGetForDebugDataSucceeds",
	// 	f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
	// 		testFile := "test_file"
	// 		// sendToHostService = func(call, msg string) (int32, string, error) {
	// 		// 	return 0, testFile, nil
	// 		// }

	// 		// Create test file
	// 		file := make([]byte, 1048576)
	// 		if _, err := rand.Read(file); err != nil {
	// 			t.Fatal("Fail to generate random file")
	// 		}
	// 		if err := os.WriteFile(testFile, file, 0777); err != nil {
	// 			t.Fatal("Fail to generate random file")
	// 		}
	// 		defer func() {
	// 			os.Remove(testFile)
	// 		}()

	// 		// Healthz Get
	// 		getReq := &healthz.GetRequest{
	// 			Path: &types.Path{
	// 				Origin: "openconfig",
	// 				Elem: []*types.PathElem{
	// 					{
	// 						Name: "components",
	// 					},
	// 					{
	// 						Name: "component",
	// 						Key: map[string]string{
	// 							"name": "all",
	// 						},
	// 					},
	// 					{
	// 						Name: "healthz",
	// 					},
	// 					{
	// 						Name: "alert-info",
	// 					},
	// 				},
	// 			},
	// 		}
	// 		getResp, err := sc.Get(ctx, getReq)
	// 		if err != nil {
	// 			t.Fatal("Expected success, got ", err.Error())
	// 		}
	// 		if len(getResp.GetComponent().GetArtifacts()) != 1 {
	// 			t.Fatal("Expected 1 artifact, got ", len(getResp.GetComponent().GetArtifacts()))
	// 		}

	// 		// Healthz Artifact
	// 		artifactReq := &healthz.ArtifactRequest{
	// 			Id: getResp.GetComponent().GetArtifacts()[0].GetId(),
	// 		}
	// 		stream, err := sc.Artifact(ctx, artifactReq)
	// 		if err != nil {
	// 			t.Fatal("Expected success, got ", err.Error())
	// 		}
	// 		var artifact []byte
	// 		for {
	// 			artifactResp, err := stream.Recv()
	// 			if err == io.EOF {
	// 				break
	// 			}
	// 			if err != nil {
	// 				t.Fatal("Expected success, got ", err.Error())
	// 			}
	// 			if b, ok := artifactResp.GetContents().(*healthz.ArtifactResponse_Bytes); ok {
	// 				artifact = append(artifact, b.Bytes...)
	// 			}
	// 		}
	// 		if bytes.Compare(artifact, file) != 0 {
	// 			t.Fatal("Artifcat files corrupted")
	// 		}

	// 		// Healthz Acknowledge
	// 		acknowledgeReq := &healthz.AcknowledgeRequest{
	// 			Id: getResp.GetComponent().GetArtifacts()[0].GetId(),
	// 		}
	// 		if _, err := sc.Acknowledge(ctx, acknowledgeReq); err != nil {
	// 			t.Fatal("Expected success, got ", err.Error())
	// 		}
	// 	},
	// },
	{
		desc: "HealthzGetForNSFSucceeds",
		f: func(ctx context.Context, t *testing.T, sc healthz.HealthzClient, cc *redis.Client) {
			req := &healthz.GetRequest{
				Path: &types.Path{
					Origin: "openconfig",
					Elem: []*types.PathElem{
						{
							Name: "components",
						},
						{
							Name: "component",
							Key: map[string]string{
								"name": "NSF",
							},
						},
					},
				},
			}
			gotResp, err := sc.Get(ctx, req)
			if err != nil {
				t.Fatalf("Expected success, got err = %v; resp = %v", err.Error(), gotResp)
			}

			// Sampling of test cases testing for correctness at granularity.
			nsfDebugData := new(debugpb.NSFDebugData)
			if err := gotResp.GetComponent().GetHealthz().UnmarshalTo(nsfDebugData); err != nil {
				t.Fatalf("Expected success, got err = %v; resp = %v", err.Error(), nsfDebugData)
			}
			wantStatus := "in-progress"
			if gotStatus := nsfDebugData.GetNsfStatus(); gotStatus != wantStatus {
				t.Fatalf("NSF status, got = %v; want = %v", gotStatus, wantStatus)
			}
			// Registration Table entries.
			regInfo := nsfDebugData.GetWarmbootRegistrationInfo()
			wantEntries := 1
			if gotEntries := len(regInfo); gotEntries != wantEntries {
				t.Fatalf("Number of Registration Table entries, got = %v; want = %v", gotEntries, wantEntries)
			}
			// Stop_on_freeze for application.
			if regInfo[0].GetStopOnFreeze() {
				t.Fatalf("For stop_on_freeze for application %v, expected false, got true", regInfo[0])
			}

			// State Table entries.
			stateInfo := nsfDebugData.GetWarmbootStateInfo()
			wantEntries = 2 // Telemetry and Syncd
			if gotEntries := len(stateInfo); gotEntries != wantEntries {
				t.Fatalf("Number of State Table entries, got = %v; want = %v", gotEntries, wantEntries)
			}
			var wantState, wantCount = "", uint64(0)
			for _, app := range stateInfo {
				switch app.GetApplication() {
				case "telemetry":
					wantState = "reconciled"
				case "syncd":
					wantState = "checkpointed"
				}
				// Current state of application.
				if gotState := app.GetState(); gotState != wantState {
					t.Fatalf("For application %v, got state = %v, want state = %v", app.GetApplication(), gotState, wantState)
				}
				// Current restore count for application.
				if gotCount := app.GetRestoreCount(); gotCount != wantCount {
					t.Fatalf("For application %v, got restore_count = %v, want restore_count = %v", app, gotCount, wantCount)
				}
			}

			// Performance Table entries.
			perfInfo := nsfDebugData.GetLastWarmbootPerformanceInfo()
			// System performance status.
			wantKey := "system"
			if gotKey := perfInfo.GetSystemPerformance().GetKey(); gotKey != wantKey {
				t.Fatalf("For system, got key = %v, want key = %v", gotKey, wantKey)
			}
			if gotStatus := perfInfo.GetSystemPerformance().GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
				t.Fatalf("For system, got status = %v, want status = %v", gotStatus, wantStatus)
			}
			// Stage performance status.
			pspInfo := perfInfo.GetPerStagePerformance()
			wantEntries = 1
			if gotEntries := len(pspInfo); gotEntries != wantEntries {
				t.Fatalf("Number of Stage Performance Table entries, got = %v; want = %v", gotEntries, wantEntries)
			}
			wantKey = "freeze"
			if gotKey := pspInfo[0].GetKey(); gotKey != wantKey {
				t.Fatalf("For stage, got key = %v, want key = %v", gotKey, wantKey)
			}
			wantState = "success"
			if gotState := pspInfo[0].GetStatusTimestamp().GetStatus(); gotState != wantState {
				t.Fatalf("For stage, got state = %v, want state = %v", gotState, wantState)
			}
			// Application performance status.
			wantEntries = 2
			papInfo := perfInfo.GetPerAppPerformance()
			if gotEntries := len(papInfo); gotEntries != wantEntries {
				t.Fatalf("Number of Stage Performance Table entries, got = %v; want = %v", gotEntries, wantEntries)
			}
			for _, pap := range papInfo {
				switch {
				case pap.GetApplication() == "telemetry" && pap.GetWarmbootStage() == "freeze":
					wantStatus = "quiescent"
					if gotStatus := pap.GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
						t.Fatalf("For telemetry|freeze, got status = %v, want status = %v", gotStatus, wantStatus)
					}
				case pap.GetApplication() == "syncd" && pap.GetWarmbootStage() == "reconciliation":
					wantStatus = "failure"
					if gotStatus := pap.GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
						t.Fatalf("For syncd|reconciliation, got status = %v, want status = %v", gotStatus, wantStatus)
					}
				}
			}

			// Performance History Table entries.
			hipbInfo := nsfDebugData.GetWarmbootHistoricalInfoPerBoot()
			if gotEntries := len(hipbInfo); gotEntries != wantEntries {
				t.Fatalf("Number of Performance History Table entries, got = %v; want = %v", gotEntries, wantEntries)
			}
			for _, hipb := range hipbInfo {
				var wantFWVersion string
				switch hipb.GetBootCount() {
				case 0:
					wantFWVersion = "gpins_daily_20240104_13_RC00"
					if gotFWVersion := hipb.GetFirmwareVersion(); gotFWVersion != wantFWVersion {
						t.Fatalf("For firmware version, got = %v, want status = %v", gotFWVersion, wantFWVersion)
					}
					wantEntries = 1
					info := hipb.GetWarmbootPerformanceInfo().GetPerStagePerformance()
					if gotEntries := len(info); gotEntries != wantEntries {
						t.Fatalf("Number of Stage Performance Table entries, got = %v; want = %v", gotEntries, wantEntries)
					}
					wantStatus = "failure"
					if gotStatus := info[0].GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
						t.Fatalf("For freeze stage, got status = %v, want status = %v", gotStatus, wantStatus)
					}
					ainfo := hipb.GetWarmbootPerformanceInfo().GetPerAppPerformance()
					if gotEntries := len(ainfo); gotEntries != wantEntries {
						t.Fatalf("Number of Stage Performance Table entries, got = %v; want = %v", gotEntries, wantEntries)
					}
					if gotStatus := ainfo[0].GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
						t.Fatalf("For syncd|reconciliation, got status = %v, want status = %v", gotStatus, wantStatus)
					}
				case 1:
					wantFWVersion = "gpins_daily_20240104_13_RC01"
					if gotFWVersion := hipb.GetFirmwareVersion(); gotFWVersion != wantFWVersion {
						t.Fatalf("For firmware version, got status = %v, want status = %v", gotFWVersion, wantFWVersion)
					}
					wantEntries = 1
					info := hipb.GetWarmbootPerformanceInfo().GetPerStagePerformance()
					if gotEntries := len(info); gotEntries != wantEntries {
						t.Fatalf("Number of Stage Performance Table entries, got = %v; want = %v", gotEntries, wantEntries)
					}
					wantStatus = "failure"
					if gotStatus := info[0].GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
						t.Fatalf("For freeze stage, got status = %v, want status = %v", gotStatus, wantStatus)
					}
					wantEntries = 2
					hpapInfo := hipb.GetWarmbootPerformanceInfo().GetPerAppPerformance()
					if gotEntries := len(hpapInfo); gotEntries != wantEntries {
						t.Fatalf("Number of Performance History Per App entries, got = %v; want = %v", gotEntries, wantEntries)
					}
					for _, hpap := range hpapInfo {
						switch {
						case hpap.GetApplication() == "telemetry" && hpap.GetWarmbootStage() == "unfreeze":
							wantStatus = "quiescent"
							if gotStatus := hpap.GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
								t.Fatalf("For telemetry|freeze, got status = %v, want status = %v", gotStatus, wantStatus)
							}
						case hpap.GetApplication() == "syncd" && hpap.GetWarmbootStage() == "reconciliation":
							wantStatus = "failure"
							if gotStatus := hpap.GetStatusTimestamp().GetStatus(); gotStatus != wantStatus {
								t.Fatalf("For syncd|reconciliation, got status = %v, want status = %v", gotStatus, wantStatus)
							}
						}
					}
				}
			}
			// SyncD statistics.
			syncdInfo := nsfDebugData.GetSyncdWarmbootStats()
			want := uint64(97)
			if got := syncdInfo.GetAsicOperations(); got != want {
				t.Fatalf("For syncd: total asic operations, got = %v, want = %v", got, want)
			}
			// Syncd object operations.
			syncdObjInfo := syncdInfo.GetSyncdObjectOperation()
			wantEntries = 3
			if gotEntries := len(syncdObjInfo); gotEntries != wantEntries {
				t.Fatalf("Number of SyncD Object Table entries, got = %v; want = %v", gotEntries, wantEntries)
			}
			for _, so := range syncdObjInfo {
				sobjType := so.GetObjectType()
				switch sobjType {
				case "SAI_OBJECT_TYPE_ACL_ENTRY":
					wantType := "set"
					if so.GetAsicOperationType() != wantType {
						t.Fatalf("For syncd object %s, got asic operation type = %v, want = %v", sobjType, so.GetAsicOperationType(), wantType)
					}
					want = uint64(95)
					if so.GetNumberOfAsicOperations() != want {
						t.Fatalf("For syncd object %s, got number of asic operations = %v, want = %v", sobjType, so.GetNumberOfAsicOperations(), want)
					}
				case "SAI_OBJECT_TYPE_TUNNEL":
					if so.GetAsicOperationType() != "create" && so.GetAsicOperationType() != "remove" {
						t.Fatalf("For syncd object %s, got asic operation type = %v, want = create/remove", sobjType, so.GetAsicOperationType())
					}
					want = uint64(1)
					if so.GetNumberOfAsicOperations() != want {
						t.Fatalf("For syncd object %s, got number of asic operations = %v, want = %v", sobjType, so.GetNumberOfAsicOperations(), want)
					}
				default:
					t.Fatalf("Invalid syncd object operation %v", sobjType)
				}
			}
			// Syncd random match info.
			syncdRandomMatches := syncdInfo.GetRandomMatchInfo()
			if syncdRandomMatches.GetRandomMatches() != 4 {
				t.Fatalf("SyncdRandomMatchInfo.RandomMatches is invalid - expected: 4, got: %v", syncdRandomMatches.GetRandomMatches())
			}
			randomMatchObjects := syncdRandomMatches.GetRandomMatchObjects()
			if len(randomMatchObjects) != 1 {
				t.Fatalf("SyncdRandomMatchInfo.RandomMatchObjects length is invalid - expected: 1, got: %v", len(randomMatchObjects))
			}
			randomMatchObj := randomMatchObjects[0]
			if randomMatchObj.GetObjectType() != "SAI_OBJECT_TYPE_NEXT_HOP_GROUP" {
				t.Fatalf("SyncdRandomMatchObjectInfo.ObjectType is invalid - expected: SAI_OBJECT_TYPE_NEXT_HOP_GROUP, got: %v", randomMatchObj.GetObjectType())
			}
			expectedOids := []string{"oid:0x50000000007d7", "oid:0x50000000007d6", "oid:0x50000000007d9", "oid:0x50000000007da"}
			if !reflect.DeepEqual(randomMatchObj.GetOid(), expectedOids) {
				t.Fatalf("SyncdRandomMatchObjectInfo.Oid is invalid - expected: %v, got: %v", expectedOids, randomMatchObj.GetOid())
			}
			// ACL recarving table verification
			src, err := getRedisDBClient(stateDB)
			if err != nil {
				t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
			}
			defer db.CloseRedisClient(src)
			aclRecarvingInfo := nsfDebugData.GetP4RtAclRecarvingTelemetry()
			// Verify cases where field exists with non-empty value
			if !compareStringSlices(aclRecarvingInfo.GetAddedTables(), []string{"acl_ingress_table", "acl_pre_ingress_vlan_table"}) {
				t.Fatalf("aclRecarvingInfo.GetAddedTables is invalid - expected: ['acl_ingress_table', 'acl_pre_ingress_vlan_table'], got: %v", aclRecarvingInfo.GetAddedTables())
			}
			if !compareStringSlices(aclRecarvingInfo.GetModifiedEssentialTables(), []string{"acl_ingress_security_table"}) {
				t.Fatalf("aclRecarvingInfo.GetModifiedEssentialTables is invalid - expected: ['acl_ingress_table', 'acl_pre_ingress_vlan_table'], got: %v", aclRecarvingInfo.GetModifiedEssentialTables())
			}
			// Verify case where field does not exist
			if aclRecarvingInfo.GetModifiedNonEssentialTables() == nil {
				t.Fatalf("aclRecarvingInfo.GetModifiedNonEssentialTables is invalid - expected: nil , got: %v.", aclRecarvingInfo.GetModifiedNonEssentialTables())
			}
			// Verify case where field exists with empty value
			if aclRecarvingInfo.GetRemovedTables() == nil {
				t.Fatalf("aclRecarvingInfo.GetRemovedTables is invalid - expected: nil , got: %v.", aclRecarvingInfo.GetRemovedTables())
			}
			// Delete fields in P4RT table, and verify GetP4RtAclRecarvingTelemetry return nil if field does not exist, otherwise should return non-nil value if value exists.
			deleteIDs := map[string]string{}
			deleteIDs["added_tables"] = "acl_ingress_table,acl_pre_ingress_vlan_table"
			deleteIDs["modified_essential_tables"] = "acl_ingress_security_table"
			deleteIDs["removed_tables"] = ""
			deleteIDs["modified_nonessential_tables"] = ""
			if removeFields("P4RT_TELEMETRY|acl_recarving", deleteIDs, src) != nil {
				t.Fatalf("Fail to cleanup field in P4RT_TELEMETRY|acl_recarving: %v.", err.Error())
			}
			gotResp, err = sc.Get(ctx, req)
			if err != nil {
				t.Fatalf("Expected success, got err = %v; resp = %v", err.Error(), gotResp)
			}
			nsfDebugData = new(debugpb.NSFDebugData)
			if err := gotResp.GetComponent().GetHealthz().UnmarshalTo(nsfDebugData); err != nil {
				t.Fatalf("Expected success, got err = %v; resp = %v", err.Error(), nsfDebugData)
			}
			aclRecarvingInfo = nsfDebugData.GetP4RtAclRecarvingTelemetry()
			if aclRecarvingInfo.GetAddedTables() != nil {
				t.Fatalf("aclRecarvingInfo.GetAddedTables() is invalid - expected: nil, got: %v", aclRecarvingInfo.GetAddedTables())
			}
			if aclRecarvingInfo.GetModifiedEssentialTables() != nil {
				t.Fatalf("aclRecarvingInfo.GetModifiedEssentialTables() is invalid - expected: nil, got: %v", aclRecarvingInfo.GetModifiedEssentialTables())
			}
			if aclRecarvingInfo.GetModifiedEssentialTables() != nil {
				t.Fatalf("aclRecarvingInfo.GetModifiedNonEssentialTables() is invalid - expected: nil, got: %v", aclRecarvingInfo.GetModifiedNonEssentialTables())
			}
			if aclRecarvingInfo.GetRemovedTables() != nil {
				t.Fatalf("GetRemovedTables is invalid - expected: nil , got: %v.", aclRecarvingInfo.GetRemovedTables())
			}
		},
	},
}

// TestHealthzServer tests implementation of gnoi.Healthz server.
func TestHealthzServer(t *testing.T) {
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

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
	}
	defer db.CloseRedisClient(cc)

	sc := healthz.NewHealthzClient(conn)
	for _, test := range testHealthzCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, sc, cc)
		})
	}
}
