package pathtransl

import (
	"testing"

	"github.com/golang/protobuf/proto"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

type translTestCase struct {
	name         string
	origReq      string
	origResp     string
	expectedReq  string
	expectedResp string
}

func testCaseName(tc translTestCase) string {
	return "Test case:" + tc.name
}

var testOCModules = []string{"openconfig-interfaces:interfaces", "openconfig-lacp:lacp", "openconfig-platform:components", "openconfig-qos:qos", "openconfig-sampling:sampling", "openconfig-system:system"}
var testContainerToModuleMap = map[string]string{"components": "platform"}

func TestGetPathType(t *testing.T) {
	cases := []struct {
		name       string
		requestStr string
		expResp    ocPathType
	}{
		{
			name: "OC Path No Origin",
			requestStr: `prefix:<elem:<name:"openconfig">
			                     elem:<name:"interfaces">>
						 path:<elem:<name:"interface"
						             key:<key:"name"value:"Ethernet0">>
						       elem:<name:"config">>
						 encoding:JSON_IETF`,
			expResp: ptOCPath,
		},
		{
			name: "OC Path No Origin, prefix",
			requestStr: `prefix:<elem:<name:"openconfig">>
			             path:<elem:<name:"interfaces">
						       elem:<name:"interface"
							         key:<key:"name"value:"Ethernet0">>
							   elem:<name:"config">>
						 encoding:JSON_IETF`,
			expResp: ptOCPathPrefix,
		},
		{
			name: "Non-OC Path No Origin",
			requestStr: `prefix:<elem:<name:"gifnocnepo">
			                     elem:<name:"interfaces">>
						 path:<elem:<name:"interface"
						             key:<key:"name"value:"Ethernet0">>
							   elem:<name:"config">>
						 encoding:JSON_IETF`,
			expResp: ptNonOCPath,
		},
		{
			name: "OC Path w/ Origin",
			requestStr: `prefix:<origin:"openconfig"
			                     elem:<name:"interfaces">>
						 path:<elem:<name:"interface"
						             key:<key:"name"value:"Ethernet0">>
						       elem:<name:"config">>
						 encoding:JSON_IETF`,
			expResp: ptOCPath,
		},
		{
			name: "OC Path Prefix w/ Origin",
			requestStr: `prefix:<origin:"openconfig">
						 path:<elem:<name:"interfaces">
						       elem:<name:"interface"
						             key:<key:"name"value:"Ethernet0">>
						       elem:<name:"config">>
						 encoding:JSON_IETF`,
			expResp: ptOCPathPrefix,
		},
		{
			name: "Non-OC Path w/ Origin",
			requestStr: `prefix:<origin:"gifnocnepo"
			                     elem:<name:"interfaces">>
						 path:<elem:<name:"interface"
						             key:<key:"name"value:"Ethernet0">>
						       elem:<name:"config">>
						 encoding:JSON_IETF`,
			expResp: ptNonOCPath,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var pbReq gnmipb.GetRequest
			var prefix *gnmipb.Path
			if tc.requestStr != "" {
				err := proto.UnmarshalText(tc.requestStr, &pbReq)
				if err != nil {
					t.Fatalf("malformed request string, err %v\n", err)
				}
				prefix = pbReq.GetPrefix()
			} else {
				prefix = nil
			}
			pt := getPathType(prefix)
			if pt != tc.expResp {
				t.Fatalf("expected path type %v, got %v\n", tc.expResp, pt)
			}
		})
	}
}

func TestTranslGet(t *testing.T) {
	t.Log("Running test for GetRequest translation")
	/* Update the list of OpenConfig models in use for the purposes of this test.
	 * This insulates us from needing updates to expected results as the set of
	 * models supported changes. */
	newOCTranslCtx()
	AllOCModules = testOCModules
	containerToModuleMap = testContainerToModuleMap

	for _, tc := range getTestCases {
		t.Run(tc.name, func(t *testing.T) {
			var req gnmipb.GetRequest

			if err := proto.UnmarshalText(tc.origReq, &req); err != nil {
				t.Fatalf("Failed to convert request to pb: %v", err)
			}

			ctx := TranslGetRequest(&req)
			if tc.expectedReq == "" && ctx != nil {
				t.Fatalf("Expected nil context for empty request translation, got: %v", ctx)
			} else if tc.expectedReq != "" && ctx == nil {
				t.Fatalf("Expected non-nil context for request translation")
			}

			if translText := marshalProtoMessagePacked(&req); translText != tc.expectedReq {
				t.Fatalf("Translated request not equal to expected:\ngot : %s\nwant: %s", translText, tc.expectedReq)
			}

			// If there is a response, test the response as well.
			if tc.origResp == "" {
				return // continue to next case
			}

			var resp gnmipb.GetResponse

			if err := proto.UnmarshalText(tc.origResp, &resp); err != nil {
				t.Fatalf("Failed to convert response to pb: %v", err)
			}
			TranslGetResponse(&resp, ctx)
			if tc.expectedResp != "" {
				if translText := marshalProtoMessagePacked(&resp); translText != tc.expectedResp {
					t.Fatalf("Translated response not equal to expected:\ngot : %s\nwant: %s", translText, tc.expectedResp)
				}
			}
		})
	}
}

func TestTranslSet(t *testing.T) {
	t.Log("Running test for SetRequest translation")
	/* Update the list of OpenConfig models in use for the purposes of this test.
	 * This insulates us from needing updates to expected results as the set of
	 * models supported changes. */
	newOCTranslCtx()
	AllOCModules = testOCModules
	containerToModuleMap = testContainerToModuleMap

	for _, tc := range setTestCases {
		t.Run(tc.name, func(t *testing.T) {
			var req gnmipb.SetRequest

			if err := proto.UnmarshalText(tc.origReq, &req); err != nil {
				t.Fatalf("Failed to convert request to pb: %v", err)
			}

			ctx := TranslSetRequest(&req)
			if tc.expectedReq == "" && ctx != nil {
				t.Fatalf("Expected nil context for empty request translation, got: %v", ctx)
			} else if tc.expectedReq != "" && ctx == nil {
				t.Fatalf("Expected non-nil context for request translation")
			}

			if translText := marshalProtoMessagePacked(&req); translText != tc.expectedReq {
				t.Fatalf("Translated request not equal to expected:\ngot : %s\nwant: %s", translText, tc.expectedReq)
			}

			// If there is a response, test the response as well.
			if tc.origResp == "" {
				return // continue to next case
			}

			var resp gnmipb.SetResponse

			if err := proto.UnmarshalText(tc.origResp, &resp); err != nil {
				t.Fatalf("Failed to convert response to pb: %v", err)
			}
			TranslSetResponse(&resp, ctx)
			if tc.expectedResp != "" {
				if translText := marshalProtoMessagePacked(&resp); translText != tc.expectedResp {
					t.Fatalf("Translated response not equal to expected:\ngot : %s\nwant: %s", translText, tc.expectedResp)
				}
			}
		})
	}
}

func TestTranslSubscribe(t *testing.T) {
	t.Log("Running test for SubscribeRequest translation")
	/* Update the list of OpenConfig models in use for the purposes of this test.
	 * This insulates us from needing updates to expected results as the set of
	 * models supported changes. */
	newOCTranslCtx()
	AllOCModules = testOCModules
	containerToModuleMap = testContainerToModuleMap

	for _, tc := range subscribeTestCases {
		t.Run(tc.name, func(t *testing.T) {
			var req gnmipb.SubscribeRequest

			if err := proto.UnmarshalText(tc.origReq, &req); err != nil {
				t.Fatalf("Failed to convert request to pb: %v", err)
			}

			ctx := TranslSubscribeRequest(&req)
			if tc.expectedReq == "" && ctx != nil {
				t.Fatalf("Expected nil context for empty request translation, got: %v", ctx)
			} else if tc.expectedReq != "" && ctx == nil {
				t.Fatalf("Expected non-nil context for request translation")
			}

			if translText := marshalProtoMessagePacked(&req); translText != tc.expectedReq {
				t.Fatalf("Translated request not equal to expected:\ngot : %s\nwant: %s", translText, tc.expectedReq)
			}

			// If there is a response, test the response as well.
			if tc.origResp == "" {
				return // continue to next case
			}

			var resp gnmipb.SubscribeResponse

			if err := proto.UnmarshalText(tc.origResp, &resp); err != nil {
				t.Fatalf("Failed to convert response to pb: %v", err)
			}
			TranslSubscribeResponse(&resp, ctx)
			if tc.expectedResp != "" {
				if translText := marshalProtoMessagePacked(&resp); translText != tc.expectedResp {
					t.Fatalf("Translated response not equal to expected:\ngot : %s\nwant: %s", translText, tc.expectedResp)
				}
			}
		})
	}
}

var getTestCases = []translTestCase{
	translTestCase{
		name:         "Request without prefix",
		origReq:      "prefix:<>path:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq:  "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>encoding:JSON_IETF",
		origResp:     "notification:<timestamp:1602868442293974761 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
		expectedResp: "notification:<timestamp:1602868442293974761prefix:<>update:<path:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
	},
	translTestCase{
		name:         "Request without prefix, multi-paths",
		origReq:      "prefix:<>path:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq:  "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		origResp:     "notification:<timestamp:1602875924325256421 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>notification:<timestamp:1602875924325256421 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet1\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
		expectedResp: "notification:<timestamp:1602875924325256421prefix:<>update:<path:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>notification:<timestamp:1602875924325256421prefix:<>update:<path:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet1\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
	},
	translTestCase{
		name:        "Request with prefix",
		origReq:     "prefix:<elem:<name:\"openconfig\">>path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq: "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>encoding:JSON_IETF",
	},
	translTestCase{
		name:        "Request with prefix, multi-paths",
		origReq:     "prefix:<elem:<name:\"openconfig\">>path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq: "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
	},
	translTestCase{
		name:        "Request with prefix path, multi-paths",
		origReq:     "prefix:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">>path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq: "prefix:<elem:<name:\"openconfig-interfaces:interfaces\">>path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
	},
	translTestCase{
		name:        "Request without prefix, multi-paths with origin",
		origReq:     "prefix:<>path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq: "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
	},
	translTestCase{
		name:        "Request with prefix origin, multi-paths",
		origReq:     "prefix:<origin:\"openconfig\">path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq: "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
	},
	translTestCase{
		name:         "Request with prefix origin path, multi-paths",
		origReq:      "prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">target:\"intf\">path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		expectedReq:  "prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"intf\">path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>encoding:JSON_IETF",
		origResp:     "notification:<timestamp:1602872893888968791 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"intf\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>notification:<timestamp:1602872893888968791 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"intf\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet1\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
		expectedResp: "notification:<timestamp:1602872893888968791prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">target:\"intf\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>notification:<timestamp:1602872893888968791prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">target:\"intf\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet1\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
	},
	translTestCase{
		name:        "Request with single root prefix",
		origReq:     "prefix:<origin:\"openconfig\">encoding:JSON_IETF",
		expectedReq: "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">>path:<elem:<name:\"openconfig-lacp:lacp\">>path:<elem:<name:\"openconfig-platform:components\">>path:<elem:<name:\"openconfig-qos:qos\">>path:<elem:<name:\"openconfig-sampling:sampling\">>path:<elem:<name:\"openconfig-system:system\">>encoding:JSON_IETF",
	},
	translTestCase{
		name:        "Request with single root prefix with CONFIG type",
		origReq:     "prefix:<origin:\"openconfig\">encoding:JSON_IETF type:CONFIG",
		expectedReq: "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">>path:<elem:<name:\"openconfig-lacp:lacp\">>path:<elem:<name:\"openconfig-platform:components\">>path:<elem:<name:\"openconfig-qos:qos\">>path:<elem:<name:\"openconfig-sampling:sampling\">>path:<elem:<name:\"openconfig-system:system\">>type:CONFIGencoding:JSON_IETF",
	},
	translTestCase{
		name:         "Request with single root prefix PROTO",
		origReq:      "prefix:<origin:\"openconfig\" target:\"arbitraryTarget\">encoding:PROTO",
		expectedReq:  "prefix:<target:\"arbitraryTarget\">path:<elem:<name:\"openconfig-interfaces:interfaces\">>path:<elem:<name:\"openconfig-lacp:lacp\">>path:<elem:<name:\"openconfig-platform:components\">>path:<elem:<name:\"openconfig-qos:qos\">>path:<elem:<name:\"openconfig-sampling:sampling\">>path:<elem:<name:\"openconfig-system:system\">>encoding:PROTO",
		origResp:     "notification:<timestamp:7 prefix:<target:\"arbitraryTarget\">update:<path:<elem:<name:\"foo\">elem:<name:\"bar\"key:<key:\"name\"value:\"xyz\">>elem:<name:\"baz\">>val:<string_val:\"TheValue\">>>",
		expectedResp: "notification:<timestamp:7prefix:<origin:\"openconfig\"target:\"arbitraryTarget\">update:<path:<elem:<name:\"foo\">elem:<name:\"bar\"key:<key:\"name\"value:\"xyz\">>elem:<name:\"baz\">>val:<string_val:\"TheValue\">>>",
	},
	translTestCase{
		name:         "Prefix without origin, combine paths",
		origReq:      "prefix:<elem:<name:\"openconfig\">>encoding:JSON_IETF",
		expectedReq:  "prefix:<>path:<elem:<name:\"openconfig-interfaces:interfaces\">>path:<elem:<name:\"openconfig-lacp:lacp\">>path:<elem:<name:\"openconfig-platform:components\">>path:<elem:<name:\"openconfig-qos:qos\">>path:<elem:<name:\"openconfig-sampling:sampling\">>path:<elem:<name:\"openconfig-system:system\">>encoding:JSON_IETF",
		origResp:     "notification:<timestamp:123 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>notification:<timestamp:123 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet1\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet1\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
		expectedResp: "notification:<timestamp:123prefix:<origin:\"openconfig\">update:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet0\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"},\\\"openconfig-interfaces:config\\\":{\\\"description\\\":\\\"\\\",\\\"enabled\\\":true,\\\"mtu\\\":9100,\\\"name\\\":\\\"Ethernet1\\\",\\\"type\\\":\\\"iana-if-type:ethernetCsmacd\\\"}}\">>>",
	},
}

var setTestCases = []translTestCase{
	translTestCase{
		name:         "Request with prefix origin path",
		origReq:      "prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">>update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"config\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		expectedReq:  "prefix:<elem:<name:\"openconfig-interfaces:interfaces\">>update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"config\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		origResp:     "prefix:<elem:<name:\"openconfig-interfaces:interfaces\">>response:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>op:UPDATE>",
		expectedResp: "prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">>response:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>op:UPDATE>",
	},
	translTestCase{
		name:         "Request with prefix origin only",
		origReq:      "prefix:<origin:\"openconfig\">update:<path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"config\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		expectedReq:  "prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"config\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		origResp:     "prefix:<>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>op:UPDATE>",
		expectedResp: "prefix:<origin:\"openconfig\">response:<path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>op:UPDATE>",
	},
	translTestCase{
		name:         "Request without prefix",
		origReq:      "update:<path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"config\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		expectedReq:  "update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>val:<json_ietf_val:\"{\\\"config\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		origResp:     "response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>op:UPDATE>",
		expectedResp: "response:<path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet10\">>elem:<name:\"config\">>op:UPDATE>",
	},
	translTestCase{
		name:         "Request with single root prefix, update only with one",
		origReq:      "prefix:<origin:\"openconfig\">update:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		expectedReq:  "prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		origResp:     "prefix:<>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>op:UPDATE>",
		expectedResp: "prefix:<origin:\"openconfig\">response:<op:UPDATE>",
	},
	translTestCase{
		name:         "Request with single root prefix, replace only with two",
		origReq:      "prefix:<origin:\"openconfig\">replace:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"},\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>",
		expectedReq:  "prefix:<>replace:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>replace:<path:<elem:<name:\"openconfig-qos:qos\">>val:<json_ietf_val:\"{\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>",
		origResp:     "prefix:<>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>op:REPLACE>response:<path:<elem:<name:\"openconfig-qos:qos\">>op:REPLACE>",
		expectedResp: "prefix:<origin:\"openconfig\">response:<op:REPLACE>",
	},
	translTestCase{
		name:         "Request with single root prefix, replace and update",
		origReq:      "prefix:<origin:\"openconfig\">replace:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"},\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>update:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		expectedReq:  "prefix:<>replace:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>replace:<path:<elem:<name:\"openconfig-qos:qos\">>val:<json_ietf_val:\"{\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		origResp:     "prefix:<>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>op:REPLACE>response:<path:<elem:<name:\"openconfig-qos:qos\">>op:REPLACE>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>op:UPDATE>",
		expectedResp: "prefix:<origin:\"openconfig\">response:<op:REPLACE>response:<op:UPDATE>",
	},
	translTestCase{
		name:        "Request with single root prefix, empty delete with update",
		origReq:     "prefix:<origin:\"openconfig\">delete:<>update:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"},\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>",
		expectedReq: "prefix:<>delete:<elem:<name:\"openconfig-interfaces:interfaces\">>delete:<elem:<name:\"openconfig-lacp:lacp\">>delete:<elem:<name:\"openconfig-platform:components\">>delete:<elem:<name:\"openconfig-qos:qos\">>delete:<elem:<name:\"openconfig-sampling:sampling\">>delete:<elem:<name:\"openconfig-system:system\">>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>update:<path:<elem:<name:\"openconfig-qos:qos\">>val:<json_ietf_val:\"{\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>",
	},
	translTestCase{
		name:         "Request with single root prefix, replace and normal update",
		origReq:      "prefix:<origin:\"openconfig\">replace:<val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"},\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>update:<path:<elem:<name:\"interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		expectedReq:  "prefix:<>replace:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>replace:<path:<elem:<name:\"openconfig-qos:qos\">>val:<json_ietf_val:\"{\\\"openconfig-qos:qos\\\":{\\\"qos-settings\\\":\\\"nothingspecial\\\"}}\">>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:interfaces\\\":{\\\"description\\\":\\\"testgnmicli\\\"}}\">>",
		origResp:     "prefix:<>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>op:REPLACE>response:<path:<elem:<name:\"openconfig-qos:qos\">>op:REPLACE>response:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>op:UPDATE>",
		expectedResp: "prefix:<origin:\"openconfig\">response:<op:REPLACE>response:<path:<elem:<name:\"interfaces\">>op:UPDATE>",
	},
}

var subscribeTestCases = []translTestCase{
	translTestCase{
		name:         "Request with prefix origin path",
		origReq:      "subscribe:<prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">target:\"YANG\">subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"YANG\">subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"YANG\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">target:\"YANG\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:         "Request with prefix origin only",
		origReq:      "subscribe:<prefix:<origin:\"openconfig\"target:\"YANG\">subscription:<path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<target:\"YANG\">subscription:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<target:\"YANG\">update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<origin:\"openconfig\"target:\"YANG\">update:<path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:         "Request without prefix, with path origin",
		origReq:      "subscribe:<prefix:<>subscription:<path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<>subscription:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<>update:<path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:         "Request with prefix path",
		origReq:      "subscribe:<prefix:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">target:\"YANG\">subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"YANG\">subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">target:\"YANG\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<elem:<name:\"openconfig\">elem:<name:\"interfaces\">target:\"YANG\">update:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:        "Request with single root prefix",
		origReq:     "subscribe:<prefix:<origin:\"openconfig\"target:\"YANG\">>",
		expectedReq: "subscribe:<prefix:<target:\"YANG\">subscription:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>>subscription:<path:<elem:<name:\"openconfig-lacp:lacp\">>>subscription:<path:<elem:<name:\"openconfig-platform:components\">>>subscription:<path:<elem:<name:\"openconfig-qos:qos\">>>subscription:<path:<elem:<name:\"openconfig-sampling:sampling\">>>subscription:<path:<elem:<name:\"openconfig-system:system\">>>>",
	},
	translTestCase{
		name:        "Request with single root prefix with subscription mode",
		origReq:     "subscribe:<prefix:<origin:\"openconfig\" target:\"YANG\">subscription:<mode:SAMPLE sample_interval:5000000000>mode:1>",
		expectedReq: "subscribe:<prefix:<target:\"YANG\">subscription:<path:<elem:<name:\"openconfig-interfaces:interfaces\">>mode:SAMPLEsample_interval:5000000000>subscription:<path:<elem:<name:\"openconfig-lacp:lacp\">>mode:SAMPLEsample_interval:5000000000>subscription:<path:<elem:<name:\"openconfig-platform:components\">>mode:SAMPLEsample_interval:5000000000>subscription:<path:<elem:<name:\"openconfig-qos:qos\">>mode:SAMPLEsample_interval:5000000000>subscription:<path:<elem:<name:\"openconfig-sampling:sampling\">>mode:SAMPLEsample_interval:5000000000>subscription:<path:<elem:<name:\"openconfig-system:system\">>mode:SAMPLEsample_interval:5000000000>mode:ONCE>",
	},
	translTestCase{
		name:         "Request without prefix, Response with prefix",
		origReq:      "subscribe:<prefix:<>subscription:<path:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<>subscription:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">>update:<path:<elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">>update:<path:<elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:         "Request with prefix origin only, Response with prefix path",
		origReq:      "subscribe:<prefix:<origin:\"openconfig\">subscription:<path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<>subscription:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">>update:<path:<elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">>update:<path:<elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:         "Request with prefix path, Response with no prefix",
		origReq:      "subscribe:<prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">>subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<elem:<name:\"openconfig-interfaces:interfaces\">>subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<>update:<path:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<origin:\"openconfig\">update:<path:<elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
	translTestCase{
		name:         "Request with prefix path, Response with more prefix elems",
		origReq:      "subscribe:<prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">>subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLE sample_interval:5000000000>>",
		expectedReq:  "subscribe:<prefix:<elem:<name:\"openconfig-interfaces:interfaces\">>subscription:<path:<elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">elem:<name:\"in-pkts\">>mode:SAMPLEsample_interval:5000000000>>",
		origResp:     "update:<timestamp:1602898347247232827 prefix:<elem:<name:\"openconfig-interfaces:interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">>update:<path:<elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
		expectedResp: "update:<timestamp:1602898347247232827prefix:<origin:\"openconfig\"elem:<name:\"interfaces\">elem:<name:\"interface\"key:<key:\"name\"value:\"Ethernet0\">>elem:<name:\"state\">elem:<name:\"counters\">>update:<path:<elem:<name:\"in-pkts\">>val:<json_ietf_val:\"{\\\"openconfig-interfaces:in-pkts\\\":\\\"210\\\"}\">>>",
	},
}
