package gnmi

import (
	"sync"
	"testing"
	"time"

	pb "github.com/openconfig/gnmi/proto/gnmi"
)

func TestCache(t *testing.T) {
	rc := NewGetResponseCache()
	defer rc.Close()
	gr := &pb.GetResponse{}

	// Sleeping to allow the GetResponseCache to subscribe to the DB
	time.Sleep(1 * time.Second)

	rc.Cache(gr)

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.response == nil {
		t.Fatal("Failed to cache response!")
	}
	if rc.valid == false || rc.listeningToNotifications == false {
		t.Fatal("Cache should be valid after caching!")
	}
}

func TestGetResponse(t *testing.T) {
	tests := []struct {
		desc                     string
		resp                     *pb.GetResponse
		valid                    bool
		listeningToNotifications bool
		wantErr                  bool
	}{
		{
			desc:                     "ValidCacheValidResponse",
			resp:                     &pb.GetResponse{},
			valid:                    true,
			listeningToNotifications: true,
			wantErr:                  false,
		},
		{
			desc:                     "ValidCacheInvalidResponse",
			resp:                     nil,
			valid:                    true,
			listeningToNotifications: true,
			wantErr:                  true,
		},
		{
			desc:                     "InvalidCacheValidResponse",
			resp:                     &pb.GetResponse{},
			valid:                    false,
			listeningToNotifications: true,
			wantErr:                  true,
		},
		{
			desc:                     "InvalidCacheInvalidResponse",
			resp:                     nil,
			valid:                    false,
			listeningToNotifications: true,
			wantErr:                  true,
		},
		{
			desc:                     "ValidCacheValidResponseNotRunning",
			resp:                     &pb.GetResponse{},
			valid:                    true,
			listeningToNotifications: false,
			wantErr:                  true,
		},
		{
			desc:                     "ValidCacheInvalidRequest",
			resp:                     nil,
			valid:                    true,
			listeningToNotifications: true,
			wantErr:                  true,
		},
		{
			desc:                     "ValidCacheNilRequest",
			resp:                     nil,
			valid:                    true,
			listeningToNotifications: true,
			wantErr:                  true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			rc := &GetResponseCache{
				mu:                       &sync.Mutex{},
				response:                 test.resp,
				valid:                    test.valid,
				listeningToNotifications: test.listeningToNotifications,
				done:                     make(chan bool, 1),
			}
			defer rc.Close()

			r := rc.GetResponse()
			if test.wantErr != (r == nil) {
				t.Fatalf("wantErr=%v but got resp=%v", test.wantErr, r)
			}
		})
	}
}

func TestSetListening(t *testing.T) {
	tests := []struct {
		desc                     string
		listeningToNotifications bool
	}{
		{
			desc:                     "SetTrue",
			listeningToNotifications: true,
		},
		{
			desc:                     "SetFalse",
			listeningToNotifications: false,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			rc := NewGetResponseCache()
			defer rc.Close()

			rc.setListening(test.listeningToNotifications)
			rc.mu.Lock()
			defer rc.mu.Unlock()
			if rc.listeningToNotifications != test.listeningToNotifications {
				t.Fatalf("listeningToNotifications mismatch! got:%v, want:%v", rc.listeningToNotifications, test.listeningToNotifications)
			}
		})
	}
}

func TestListening(t *testing.T) {
	rc := NewGetResponseCache()
	rc.Close()

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.listeningToNotifications != false {
		t.Fatal("GetResponseCache is still in listeningToNotifications state after closing!")
	}
}

func TestIsGetConfigRequest(t *testing.T) {
	tests := []struct {
		desc        string
		req         *pb.GetRequest
		isGetConfig bool
	}{
		{
			desc: "GetConfig",
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_CONFIG,
				Encoding: pb.Encoding_JSON_IETF,
			},
			isGetConfig: true,
		},
		{
			desc: "GetState",
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Type:     pb.GetRequest_STATE,
				Encoding: pb.Encoding_JSON_IETF,
			},
			isGetConfig: false,
		},
		{
			desc: "GetAll",
			req: &pb.GetRequest{
				Prefix: &pb.Path{
					Elem: []*pb.PathElem{
						&pb.PathElem{
							Name: "openconfig",
						}},
				},
				Encoding: pb.Encoding_JSON_IETF,
			},
			isGetConfig: false,
		},
		{
			desc:        "EmptyRequest",
			req:         &pb.GetRequest{},
			isGetConfig: false,
		},
		{
			desc:        "NilRequest",
			req:         nil,
			isGetConfig: false,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			if isgc := IsGetConfigRequest(test.req); test.isGetConfig != isgc {
				t.Fatalf("Error with req=%v -- want:%v, got:%v", test.req, isgc)
			}
		})
	}
}
