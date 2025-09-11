package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/golang/protobuf/proto"
	dpb "github.com/openconfig/gnoi/diag"
	types "github.com/openconfig/gnoi/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

const (
	testId string = "testId"
	alias  string = "alias"
)

// Tests Diag services.
func TestDiag(t *testing.T) {
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

	sc := dpb.NewDiagClient(conn)

	stc, err := getRedisDBClient(stateDB)
	if err != nil {
		t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
	}
	defer db.CloseRedisClient(stc)

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		t.Fatalf("Cannot connect to the redis server: %v.", err.Error())
	}
	defer db.CloseRedisClient(cc)

	t.Run("StartBertFailsIfTestIdIsNotSet", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "BERT ID is empty.", t)
	})
	t.Run("StartBertFailsIfInterfaceIsNotSet", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is nil.", t)
	})
	t.Run("StartBertFailsIfInterfaceHasNonCompliantOrigin", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					Interface: &types.Path{
						Origin: "someOrigin",
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartBertFailsIfInterfaceHasNoPathElement", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					Interface: &types.Path{
						Origin: "openconfig",
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartBertFailsIfInterfaceHasWrongNumberOfPathElements", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					Interface: &types.Path{
						Origin: "openconfig",
						Elem: []*types.PathElem{
							{
								Name: "interfaces",
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartBertFailsIfInterfaceHasUnexpectedKeyInPathElementsMap", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					Interface: &types.Path{
						Origin: "openconfig",
						Elem: []*types.PathElem{
							{
								Name: "interfaces",
								Key: map[string]string{
									"name": "Ethernet0",
								},
							},
							{
								Name: "interface",
								Key: map[string]string{
									"name": "Ethernet0",
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartBertFailsIfInterfaceHasWrongKeyInPathElementsMap", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
									"randomKey": "Ethernet0",
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartBertFailsIfInterfaceHasExtraKeysInPathElementsMap", func(t *testing.T) {
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
									"name":      "Ethernet0",
									"randomKey": "randomValue",
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartBertFailsIfPrbsPolynomialIsUnknown", func(t *testing.T) {
		// Setup DB.
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
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_UNKNOWN,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "PRBS polynomial is not set.", t)
	})
	t.Run("StartBertFailsIfDurationIsTooLong", func(t *testing.T) {
		var duration uint32 = 123456
		// Setup DB.
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
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: duration,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, fmt.Sprintf("Duration %d is too long.", duration), t)
	})
	t.Run("StartBertFailsIfDurationIsTooShort", func(t *testing.T) {
		var duration uint32 = 0
		// Setup DB.
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
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: duration,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, fmt.Sprintf("Duration %d is too short.", duration), t)
	})
	t.Run("StartBertFailsIfIDIsInUse", func(t *testing.T) {
		// Setup DB.
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
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}

		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()

		// ID is successfully written while processing the first Start request. Re-using it should fail!
		req = &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
			},
		}

		_, err = sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "ID: testId exists.", t)
	})
	t.Run("StartBertSucceedsIfOnePortRequestSucceeds", func(t *testing.T) {
		// Setup DB.
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
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
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
									"name": "Ethernet1",
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_UNKNOWN,
					TestDurationInSecs: 20,
				},
			},
		}

		resp, err := sc.StartBERT(ctx, req)
		if err != nil {
			t.Fatal("Expected no error.")
		}

		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()

		nResp := len(resp.GetPerPortResponses())
		if nResp != 2 {
			t.Error("Response size: expected ", 2, ", received ", nResp)
		}
		nInvalid := 0
		for _, presp := range resp.GetPerPortResponses() {
			if presp.GetStatus() != dpb.BertStatus_BERT_STATUS_OK {
				nInvalid++
			}
		}
		if nInvalid >= nResp {
			t.Error("Number of invalid per port response ", nInvalid, ", total number of per port response ", nResp)
		}
	})
	t.Run("StartBertFailsIfAllPortFails", func(t *testing.T) {
		var duration uint32 = 123456
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
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
									"name": "Ethernet0",
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: duration,
				},
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
									"name": "Ethernet1",
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_UNKNOWN,
					TestDurationInSecs: 20,
				},
			},
		}

		_, err := sc.StartBERT(ctx, req)
		testErr(err, codes.InvalidArgument, fmt.Sprintf("Duration %d is too long.", duration), t)
		testErr(err, codes.InvalidArgument, "PRBS polynomial is not set.", t)
		testErr(err, codes.InvalidArgument, "Interface is invalid.", t)
	})
	t.Run("StartBertSucceeds", func(t *testing.T) {
		// Setup DB.
		intf1Name := "EthernetX1234"
		key1 := getKey([]string{portKeyPrefix, intf1Name})
		if err := cc.HSet(context.Background(), key1, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		intf2Name := "EthernetX1235"
		key2 := getKey([]string{portKeyPrefix, intf2Name})
		if err := cc.HSet(context.Background(), key2, alias, "EthX1235").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := cc.HDel(context.Background(), key1, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
			if err := cc.HDel(context.Background(), key2, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &dpb.StartBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StartBERTRequest_PerPortRequest{
				{
					Interface: &types.Path{
						Origin: "openconfig-interfaces",
						Elem: []*types.PathElem{
							{
								Name: "interfaces",
							},
							{
								Name: "interface",
								Key: map[string]string{
									"name": intf1Name,
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS31,
					TestDurationInSecs: 10,
				},
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
									"name": intf2Name,
								},
							},
						},
					},
					PrbsPolynomial:     dpb.PrbsPolynomial_PRBS_POLYNOMIAL_PRBS23,
					TestDurationInSecs: 20,
				},
			},
		}

		resp, err := sc.StartBERT(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}

		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()

		nResp := len(resp.GetPerPortResponses())
		if nResp != 2 {
			t.Error("Response size: expected ", 2, ", received ", nResp)
		}
		nInvalid := 0
		for _, presp := range resp.GetPerPortResponses() {
			if presp.GetStatus() != dpb.BertStatus_BERT_STATUS_OK {
				nInvalid++
			}
		}
		if nInvalid >= nResp {
			t.Error("Number of invalid per port response ", nInvalid, ", total number of per port response ", nResp)
		}
	})
	t.Run("StopBertFailsIfTestIdIsNotSet", func(t *testing.T) {
		_, err := sc.StopBERT(ctx, &dpb.StopBERTRequest{})
		testErr(err, codes.InvalidArgument, "BERT ID is empty.", t)
	})
	t.Run("StopBertFailsIfOperationIDIsNotFound", func(t *testing.T) {
		req := &dpb.StopBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StopBERTRequest_PerPortRequest{
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
									"name": "Ethernet0",
								},
							},
						},
					},
				},
			},
		}

		_, err := sc.StopBERT(ctx, req)
		testErr(err, codes.InvalidArgument, "Operation ID is not found", t)
	})
	t.Run("StopBertSucceeds", func(t *testing.T) {
		// Setup DB.
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
		intf := &types.Path{
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
		}
		// Setup DB.
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName, "").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &dpb.StopBERTRequest{
			BertOperationId: testId,
			PerPortRequests: []*dpb.StopBERTRequest_PerPortRequest{
				{
					Interface: intf,
				},
			},
		}

		resp, err := sc.StopBERT(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}
		nResp := len(resp.GetPerPortResponses())
		if nResp != 1 {
			t.Error("Response size: expected ", 1, ", received ", nResp)
		}
	})
	t.Run("GetBertResultSucceedsWhenIDIsTooOld", func(t *testing.T) {
		// Setup ID.
		// Too old timestamp.
		ts := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTID}), testId, ts.UnixNano()).Err(); err != nil {
			t.Fatalf("Cannot setup DB for test ID: %v.", err.Error())
		}
		// Cleanup ID.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test ID: %v.", err.Error())
			}
		}()
		intfName := "EthernetX1234"
		intf := &types.Path{
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
		}
		// Setup result.
		r := &dpb.GetBERTResultResponse_PerPortResponse{
			Interface:       intf,
			BertOperationId: testId,
		}
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName, proto.MarshalTextString(r)).Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup result.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: true,
		}

		resp, err := sc.GetBERTResult(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}
		nResp := len(resp.GetPerPortResponses())
		if nResp != 0 {
			t.Error("Response size: expected ", 0, ", received ", nResp)
		}
	})
	t.Run("GetBertResultSucceedsWhenIDIsNew", func(t *testing.T) {
		// Setup ID.
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTID}), testId, time.Now().UnixNano()).Err(); err != nil {
			t.Fatalf("Cannot setup DB for test ID: %v.", err.Error())
		}
		// Cleanup ID.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test ID: %v.", err.Error())
			}
		}()
		intfName := "EthernetX1234"
		intf := &types.Path{
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
		}
		// Setup result.
		r := &dpb.GetBERTResultResponse_PerPortResponse{
			Interface:       intf,
			BertOperationId: testId,
		}
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName, proto.MarshalTextString(r)).Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup result.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: true,
		}

		resp, err := sc.GetBERTResult(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}
		nResp := len(resp.GetPerPortResponses())
		if nResp != 1 {
			t.Error("Response size: expected ", 1, ", received ", nResp)
		}
	})
	t.Run("GetBertResultSucceedsOnEmptyState", func(t *testing.T) {
		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: true,
		}

		resp, err := sc.GetBERTResult(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}
		nResp := len(resp.GetPerPortResponses())
		if nResp != 0 {
			t.Error("Response size: expected ", 0, ", received ", nResp)
		}
	})
	t.Run("GetBertResultFailsIfTestIdIsNotSet", func(t *testing.T) {
		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: false,
		}

		_, err := sc.GetBERTResult(ctx, req)
		testErr(err, codes.InvalidArgument, "BERT ID is empty.", t)
	})
	t.Run("GetBertResultFailsIfTestIdIsNotFound", func(t *testing.T) {
		// Setup DB.
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
		intf := &types.Path{
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
		}

		// Setup DB.
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName, "").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		otherId := "someOtherTestId"
		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: false,
			BertOperationId:    otherId,
			PerPortRequests: []*dpb.GetBERTResultRequest_PerPortRequest{
				{
					Interface: intf,
				},
			},
		}

		_, err := sc.GetBERTResult(ctx, req)
		testErr(err, codes.InvalidArgument, "Result is not found", t)
	})
	t.Run("GetBertResultFailsIfInterfaceIsNotFound", func(t *testing.T) {
		// Setup DB.
		intfName := "EthernetX1234"
		// Setup DB.
		otherIntf := "SomeOtherIntf"
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), otherIntf, "").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), otherIntf).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: false,
			BertOperationId:    testId,
			PerPortRequests: []*dpb.GetBERTResultRequest_PerPortRequest{
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
				},
			},
		}

		_, err := sc.GetBERTResult(ctx, req)
		testErr(err, codes.InvalidArgument, "is not valid", t)
	})
	t.Run("GetBertResultFailsIfIntfResultIsNotFound", func(t *testing.T) {
		// Setup DB.
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
		// Setup DB.
		otherIntf := "SomeOtherIntf"
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), otherIntf, "").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), otherIntf).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: false,
			BertOperationId:    testId,
			PerPortRequests: []*dpb.GetBERTResultRequest_PerPortRequest{
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
				},
			},
		}

		_, err := sc.GetBERTResult(ctx, req)
		testErr(err, codes.InvalidArgument, "Result is not found", t)
	})
	t.Run("GetBertResultSucceedsIfIntfResultIsSet", func(t *testing.T) {
		// Setup DB.
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
		intf := &types.Path{
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
		}

		// Setup DB.
		r := &dpb.GetBERTResultResponse_PerPortResponse{
			Interface:       intf,
			BertOperationId: testId,
		}
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName, proto.MarshalTextString(r)).Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getBERTRes, testId}), intfName).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		req := &dpb.GetBERTResultRequest{
			ResultFromAllPorts: false,
			BertOperationId:    testId,
			PerPortRequests: []*dpb.GetBERTResultRequest_PerPortRequest{
				{
					Interface: intf,
				},
			},
		}

		resp, err := sc.GetBERTResult(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}
		nResp := len(resp.GetPerPortResponses())
		if nResp != 1 {
			t.Error("Response size: expected ", 1, ", received ", nResp)
		}
	})

	// Test BERT capability.
	s.lqHelper.SetBertCapability(false)
	t.Run("StartBertFailsIfPlatformDoesNotSupportBert", func(t *testing.T) {
		_, err := sc.StartBERT(ctx, &dpb.StartBERTRequest{})
		testErr(err, codes.Unimplemented, "RPC is not supported!", t)
	})
	t.Run("StopBertFailsIfPlatformDoesNotSupportBert", func(t *testing.T) {
		_, err := sc.StopBERT(ctx, &dpb.StopBERTRequest{})
		testErr(err, codes.Unimplemented, "RPC is not supported!", t)
	})
	t.Run("GetBertResultFailsIfPlatformDoesNotSupportBert", func(t *testing.T) {
		_, err := sc.GetBERTResult(ctx, &dpb.GetBERTResultRequest{})
		testErr(err, codes.Unimplemented, "RPC is not supported!", t)
	})
	s.lqHelper.SetBertCapability(true)

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("StartBertUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.StartBERT(ctx, &dpb.StartBERTRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("StopBertUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.StopBERT(ctx, &dpb.StopBERTRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("GetBertResultUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.GetBERTResult(ctx, &dpb.GetBERTResultRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)

	// Simulate helper failure - returns critical error!
	savedSsHelper := s.SsHelper
	s.SsHelper = mockSystemStateHelperFailure{}

	t.Run("StartBertFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.StartBERT(ctx, &dpb.StartBERTRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})
	t.Run("StopBertFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.StopBERT(ctx, &dpb.StopBERTRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})
	t.Run("GetBertResultFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.GetBERTResult(ctx, &dpb.GetBERTResultRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})

	s.SsHelper.Close()
	s.SsHelper = savedSsHelper
}
