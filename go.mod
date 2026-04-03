module github.com/sonic-net/sonic-gnmi

go 1.25.0

require (
	github.com/Azure/sonic-mgmt-common v0.0.0-20240124151252-99052691c3aa
	github.com/Workiva/go-datastructures v1.0.52
	github.com/agiledragon/gomonkey/v2 v2.12.0
	github.com/c9s/goprocinfo v0.0.0-20191125144613-4acdd056c72d
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/fsnotify/fsnotify v1.6.0
	github.com/godbus/dbus/v5 v5.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/glog v1.2.4
	github.com/golang/protobuf v1.5.4
	github.com/google/gnxi v0.0.0-20191016182648-6697a080bc2d
	github.com/google/go-tpm v0.9.3
	github.com/google/gofuzz v1.2.0
	github.com/google/gousb v1.1.3
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/kylelemons/godebug v1.1.0
	github.com/msteinert/pam v0.0.0-20190215180659-f29b9f28d6f9
	github.com/nxadm/tail v1.4.11
	github.com/openconfig/gnmi v0.11.0
	github.com/openconfig/gnoi v0.4.1-0.20240320162840-dbdca7782474
	github.com/openconfig/gnsi v1.7.0
	github.com/openconfig/ygot v0.13.1
	github.com/pkg/errors v0.9.1
	github.com/redis/go-redis/v9 v9.6.1
	github.com/xeipuuv/gojsonschema v1.2.0
	golang.org/x/crypto v0.32.0
	golang.org/x/net v0.34.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f
	google.golang.org/grpc v1.71.0
	google.golang.org/grpc/security/advancedtls v1.0.0
	google.golang.org/protobuf v1.36.5
	gopkg.in/yaml.v2 v2.2.4
)

require (
	cel.dev/expr v0.19.1 // indirect
	github.com/antchfx/jsonquery v1.1.4 // indirect
	github.com/antchfx/xmlquery v1.3.1 // indirect
	github.com/antchfx/xpath v1.1.10 // indirect
	github.com/bgentry/speakeasy v0.1.0 // indirect
	github.com/cenkalti/backoff/v4 v4.1.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20241223141626-cff3c89139a3 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/maruel/natural v1.1.1 // indirect
	github.com/openconfig/goyang v0.2.9 // indirect
	github.com/openconfig/grpctunnel v0.0.0-20220819142823-6f5422b8ca70 // indirect
	github.com/philopon/go-toposort v0.0.0-20170620085441-9be86dbd762f // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/tredoe/osutil v1.5.0 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20180127040702-4e3ac2762d5f // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250106144421-5f5ef82da422 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
)

replace github.com/Azure/sonic-mgmt-common => ../sonic-mgmt-common

replace github.com/golang/glog => github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
