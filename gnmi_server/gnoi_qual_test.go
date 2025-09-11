package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/golang/protobuf/proto"
	types "github.com/openconfig/gnoi/types"
	qualpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/qualification"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

// Tests PacketLinkQual services.
func TestPacketLinkQual(t *testing.T) {
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

	sc := qualpb.NewPacketLinkQualClient(conn)
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

	t.Run("StartPacketQualificationFailsIfInterfaceIsNotSet", func(t *testing.T) {
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id:                                  testId,
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is nil.", t)
	})
	t.Run("StartPacketQualificationFailsIfPathElementsAreNotSetInTheInterface", func(t *testing.T) {
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
					Interface: &types.Path{
						Origin: "openconfig",
					},
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartPacketQualificationFailsIfPathElementsMapIsInvalidInTheInterface", func(t *testing.T) {
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Interface is malformed.", t)
	})
	t.Run("StartPacketQualificationSucceeds", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		// Cleanup DB. Successful Start RPC performs DB operations.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getPktID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}
	})
	t.Run("StartPacketQualificationFailsIfIdIsNotSet", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Config ID is empty.", t)
	})
	t.Run("StartPacketQualificationFailsIfMinWaitBeforePrepIsOutOfRange", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					NumPreparationPackets:        20,
					PreparationTimeoutSeconds:    30,
					QualificationDurationSeconds: 40,
					QualificationEnd:             qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		for _, duration := range []int32{int32(maxTestDurationSecs) + 1, 0, -1} {
			for _, preq := range req.GetConfigs() {
				preq.MinimumWaitBeforePreparationSeconds = duration
			}

			_, err = sc.StartPacketQualification(ctx, req)
			testErr(err, codes.InvalidArgument, fmt.Sprintf("Minimum wait time %d is out-of-range.", duration), t)
		}
	})
	t.Run("StartPacketQualificationFailsIfPrepTimeoutIsOutOfRange", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		for _, duration := range []int32{int32(maxTestDurationSecs) + 1, 0, -1} {
			for _, preq := range req.GetConfigs() {
				preq.PreparationTimeoutSeconds = duration
			}

			_, err = sc.StartPacketQualification(ctx, req)
			testErr(err, codes.InvalidArgument, fmt.Sprintf("Preparation timeout %d is out-of-range.", duration), t)
		}
	})
	t.Run("StartPacketQualificationFailsIfQualDurationIsOutOfRange", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		for _, duration := range []int32{int32(maxTestDurationSecs) + 1, 0, -1} {
			for _, preq := range req.GetConfigs() {
				preq.QualificationDurationSeconds = duration
			}

			_, err = sc.StartPacketQualification(ctx, req)
			testErr(err, codes.InvalidArgument, fmt.Sprintf("Qualification duration %d is out-of-range.", duration), t)
		}
	})
	t.Run("StartPacketQualificationFailsIfEndIsNotSet", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
				},
			},
		}
		_, err := sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Qualification end is not set.", t)
	})
	t.Run("StartPacketQualificationFailsForInvalidPrepPacketsOnNearEnd", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		for _, numPckts := range []int64{-1, 0} {
			for _, preq := range req.GetConfigs() {
				preq.NumPreparationPackets = numPckts
			}

			_, err = sc.StartPacketQualification(ctx, req)
			testErr(err, codes.InvalidArgument, fmt.Sprintf("Number of preparation packets %d should be positive.", numPckts), t)
		}
	})
	t.Run("StartPacketQualificationFailsIfIDExists", func(t *testing.T) {
		// Setup port table in config DB.
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
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key1, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
			if err := cc.HDel(context.Background(), key2, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		// Cleanup DB. Successful Start RPC performs DB operations.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getPktID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()

		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
					Interface: &types.Path{
						Origin: "openconfig",
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}

		// Re-using the same ID should result in failure.
		req = &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 11,
					NumPreparationPackets:               21,
					PreparationTimeoutSeconds:           31,
					QualificationDurationSeconds:        41,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}
		_, err = sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Config ID exists!", t)
	})
	t.Run("StartPacketQualificationSucceedsIfOneIDIsUnique", func(t *testing.T) {
		// Setup port table in config DB.
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
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key1, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
			if err := cc.HDel(context.Background(), key2, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		// Cleanup DB. Successful Start RPC performs DB operations.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getPktID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
					Interface: &types.Path{
						Origin: "openconfig",
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
				{
					Id: testId, // repeated ID.
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		resp, err := sc.StartPacketQualification(ctx, req)
		if err != nil {
			t.Fatal("Expected no error.")
		}
		nResp := len(resp.GetResponses())
		if nResp != 2 {
			t.Error("Response size: expected ", 2, ", received ", nResp)
		}
		nInvalid := 0
		for _, presp := range resp.GetResponses() {
			if presp.GetStatus().GetCode() != int32(codes.OK) {
				nInvalid++
			}
		}
		if nInvalid >= nResp {
			t.Error("Number of invalid per port response ", nInvalid, ", total number of per port response ", nResp)
		}
	})
	t.Run("StartPacketQualificationSucceedsIfAtLeastOneConfSucceeds", func(t *testing.T) {
		// Setup port table in config DB.
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
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key1, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
			if err := cc.HDel(context.Background(), key2, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		// Cleanup DB. Successful Start RPC performs DB operations.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getPktID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
					Interface: &types.Path{
						Origin: "openconfig",
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
				{
					Id: "testId-2",
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					// Qualification end is not set.
				},
			},
		}

		resp, err := sc.StartPacketQualification(ctx, req)
		if err != nil {
			t.Fatal("Expected no error.")
		}
		nResp := len(resp.GetResponses())
		if nResp != 2 {
			t.Error("Response size: expected ", 2, ", received ", nResp)
		}
		nInvalid := 0
		for _, presp := range resp.GetResponses() {
			if presp.GetStatus().GetCode() != int32(codes.OK) {
				nInvalid++
			}
		}
		if nInvalid >= nResp {
			t.Error("Number of invalid per port response ", nInvalid, ", total number of per port response ", nResp)
		}
	})
	t.Run("StartPacketQualificationFailsIfAllConfigsFail", func(t *testing.T) {
		// Setup port table in config DB.
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
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key1, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
			if err := cc.HDel(context.Background(), key2, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					// ID is missing.
					Interface: &types.Path{
						Origin: "openconfig",
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
				{
					Id: "testId-2",
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					// Qualification end is not set.
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, fmt.Sprintf("Config ID is empty."), t)
		testErr(err, codes.InvalidArgument, "Qualification end is not set.", t)
	})
	t.Run("StopPacketQualificationFailsIfIdIsNotSet", func(t *testing.T) {
		req := &qualpb.StopPacketQualificationRequest{
			Ids: []string{
				"",
			},
		}
		_, err := sc.StopPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Config ID is empty.", t)
		testErr(err, codes.InvalidArgument, "Config ID doesn't exist!", t)
	})
	t.Run("StopPacketQualificationFailsIfIdDoesNotExist", func(t *testing.T) {
		req := &qualpb.StopPacketQualificationRequest{
			Ids: []string{
				testId,
			},
		}
		_, err := sc.StopPacketQualification(ctx, req)
		testErr(err, codes.InvalidArgument, "Config ID doesn't exist", t)
	})
	t.Run("StopPacketQualificationSucceeds", func(t *testing.T) {
		// Setup port table in config DB.
		intfName := "EthernetX1234"
		key := getKey([]string{portKeyPrefix, intfName})
		if err := cc.HSet(context.Background(), key, alias, "EthX1234").Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup port table.
		defer func() {
			if err := cc.HDel(context.Background(), key, alias).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()
		// Cleanup DB. Successful Start RPC performs DB operations.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getPktID}), testId).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB: %v.", err.Error())
			}
		}()
		req := &qualpb.StartPacketQualificationRequest{
			Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
				{
					Id: testId,
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
					MinimumWaitBeforePreparationSeconds: 10,
					NumPreparationPackets:               20,
					PreparationTimeoutSeconds:           30,
					QualificationDurationSeconds:        40,
					QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
				},
			},
		}

		_, err := sc.StartPacketQualification(ctx, req)
		if err != nil {
			t.Fatal(err.Error())
		}

		stopReq := &qualpb.StopPacketQualificationRequest{
			Ids: []string{
				testId,
				"",
			},
		}
		resp, err := sc.StopPacketQualification(ctx, stopReq)
		if err != nil {
			t.Fatal("Expected no error.")
		}
		nResp := len(resp.GetResponses())
		if nResp != 2 {
			t.Error("Response size: expected ", 2, ", received ", nResp)
		}
		nInvalid := 0
		for _, presp := range resp.GetResponses() {
			if presp.GetStatus().GetCode() != int32(codes.OK) {
				nInvalid++
			}
		}
		if nInvalid >= nResp {
			t.Error("Number of invalid per port response ", nInvalid, ", total number of per port response ", nResp)
		}
	})
	t.Run("GetPacketQualificationResultFailsIfAllIdsFail", func(t *testing.T) {
		req := &qualpb.GetPacketQualificationResultRequest{
			Ids: []string{
				testId,
			},
		}
		_, err := sc.GetPacketQualificationResult(ctx, req)
		testErr(err, codes.InvalidArgument, "Result is not found", t)
	})
	t.Run("GetPacketQualificationResultSucceeds", func(t *testing.T) {
		// Setup DB.
		r := &qualpb.QualificationResults{
			NumSentPackets: 10,
			NumOkPackets:   10,
		}
		if err := stc.HSet(context.Background(), getKey([]string{lqResult, getPktRes, testId}), msgFld, proto.MarshalTextString(r)).Err(); err != nil {
			t.Fatalf("Cannot setup DB for test: %v.", err.Error())
		}
		// Cleanup DB.
		defer func() {
			if err := stc.HDel(context.Background(), getKey([]string{lqResult, getPktRes, testId}), msgFld).Err(); err != nil {
				t.Fatalf("Cannot cleanup DB for test: %v.", err.Error())
			}
		}()

		req := &qualpb.GetPacketQualificationResultRequest{
			Ids: []string{
				testId,
				"testId-2",
			},
		}
		resp, err := sc.GetPacketQualificationResult(ctx, req)
		if err != nil {
			t.Fatal("Expected no error.")
		}
		nResp := len(resp.GetResponses())
		if nResp != 2 {
			t.Error("Response size: expected ", 2, ", received ", nResp)
		}
		nInvalid := 0
		for _, presp := range resp.GetResponses() {
			if presp.GetStatus().GetCode() != int32(codes.OK) {
				nInvalid++
			}
		}
		if nInvalid >= nResp {
			t.Error("Number of invalid per port response ", nInvalid, ", total number of per port response ", nResp)
		}
	})

	// Test packet based link qualification capability.
	s.lqHelper.SetPktLqCapability(false)
	t.Run("StartPacketQualificationFailsIfPlatformDoesNotSupport", func(t *testing.T) {
		_, err := sc.StartPacketQualification(ctx, &qualpb.StartPacketQualificationRequest{})
		testErr(err, codes.Unimplemented, "RPC is not supported!", t)
	})
	t.Run("StopPacketQualificationFailsIfPlatformDoesNotSupport", func(t *testing.T) {
		_, err := sc.StopPacketQualification(ctx, &qualpb.StopPacketQualificationRequest{})
		testErr(err, codes.Unimplemented, "RPC is not supported!", t)
	})
	t.Run("GetPacketQualificationResultFailsIfPlatformDoesNotSupport", func(t *testing.T) {
		_, err := sc.GetPacketQualificationResult(ctx, &qualpb.GetPacketQualificationResultRequest{})
		testErr(err, codes.Unimplemented, "RPC is not supported!", t)
	})
	s.lqHelper.SetPktLqCapability(true)

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("StartPacketQualificationUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.StartPacketQualification(ctx, &qualpb.StartPacketQualificationRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("StopPacketQualificationUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.StopPacketQualification(ctx, &qualpb.StopPacketQualificationRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("GetPacketQualificationResultUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.GetPacketQualificationResult(ctx, &qualpb.GetPacketQualificationResultRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)

	// Simulate helper failure - returns critical error!
	savedSsHelper := s.SsHelper
	s.SsHelper = mockSystemStateHelperFailure{}

	t.Run("StartPacketQualificationFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.StartPacketQualification(ctx, &qualpb.StartPacketQualificationRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})
	t.Run("StopPacketQualificationFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.StopPacketQualification(ctx, &qualpb.StopPacketQualificationRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})
	t.Run("GetPacketQualificationResultFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.GetPacketQualificationResult(ctx, &qualpb.GetPacketQualificationResultRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})

	s.SsHelper.Close()
	s.SsHelper = savedSsHelper
}
