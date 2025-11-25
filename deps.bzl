"""Legacy Go dependencies that cannot be migrated to go_deps due to cross-repo visibility issues.

These repositories reference other Bazel repos (like @linux_pam from the main repo)
that aren't visible in the go_deps module extension namespace. This is a known bzlmod limitation:
https://github.com/bazelbuild/bazel/issues/19301
"""

load("@gazelle//:deps.bzl", "go_repository")

def _ext_impl(m):
    # openconfig/gnoi - references @com_github_grpc_grpc//bazel:cc_grpc_library.bzl
    # Cannot use go_deps because of cross-repo dependency visibility
    go_repository(
        name = "com_github_openconfig_gnoi",
        importpath = "github.com/openconfig/gnoi",
        sum = "h1:7u+4jc9kEuaXMYHCLLW2eRO0WC3mElx+0/t/xqRtYJ4=",
        version = "v0.4.1-0.20240320162840-dbdca7782474",
    )

    # msteinert/pam - needs @//:third_party_pam which isn't visible in go_deps
    # Using custom BUILD file with cgo linking to hermetic PAM library
    go_repository(
        name = "com_github_msteinert_pam",
        build_file_generation = "off",
        importpath = "github.com/msteinert/pam",
        patch_cmds = [
            """cat > BUILD.bazel << 'EOF'
load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "pam",
    srcs = [
        "callback.go",
        "transaction.c",
        "transaction.go",
    ],
    cgo = True,
    cdeps = [
        "@//:third_party_pam",
    ],
    copts = ["-Wall", "-std=c99"],
    clinkopts = ["-ldl"],
    importpath = "github.com/msteinert/pam",
    visibility = ["//visibility:public"],
)
EOF
""",
        ],
        sum = "h1:ZivaaKmjs9q90zi6I4gTLW6tbVGtlBjellr3hMYaly0=",
        version = "v0.0.0-20190215180659-f29b9f28d6f9",
    )

    # openconfig/gnsi - uses pre-generated .pb.go files instead of proto compilation
    # References @rules_proto_grpc_cpp which is now available via inject_repo
    go_repository(
        name = "com_github_openconfig_gnsi",
        build_file_proto_mode = "disable",
        importpath = "github.com/openconfig/gnsi",
        patch_cmds = [
            # Fix rules_proto_grpc import path for bzlmod
            "sed -i 's|@rules_proto_grpc//cpp:defs.bzl|@rules_proto_grpc_cpp//:defs.bzl|g' */BUILD.bazel",
            # Rewrite go_library to use pre-generated .pb.go files with deps
            # authz
            """cat > authz/BUILD.bazel.new << 'EOF'
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(name = "authz_proto", srcs = ["authz.proto"], import_prefix = "github.com/openconfig/gnsi")
cpp_grpc_library(name = "authz_cc_proto", protos = [":authz_proto"])
go_library(name = "authz", srcs = ["authz.pb.go", "authz_grpc.pb.go"], importpath = "github.com/openconfig/gnsi/authz",
    deps = ["@org_golang_google_grpc//:grpc", "@org_golang_google_grpc//codes", "@org_golang_google_grpc//status",
            "@org_golang_google_protobuf//reflect/protoreflect", "@org_golang_google_protobuf//runtime/protoimpl"])
EOF
mv authz/BUILD.bazel.new authz/BUILD.bazel""",
            # certz
            """cat > certz/BUILD.bazel.new << 'EOF'
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(name = "certz_proto", srcs = ["certz.proto"], import_prefix = "github.com/openconfig/gnsi", deps = ["@com_google_protobuf//:any_proto"])
cpp_grpc_library(name = "certz_cc_proto", protos = [":certz_proto"])
go_library(name = "certz", srcs = ["certz.pb.go", "certz_grpc.pb.go"], importpath = "github.com/openconfig/gnsi/certz",
    deps = ["@org_golang_google_grpc//:grpc", "@org_golang_google_grpc//codes", "@org_golang_google_grpc//status",
            "@org_golang_google_protobuf//reflect/protoreflect", "@org_golang_google_protobuf//runtime/protoimpl",
            "@org_golang_google_protobuf//types/known/anypb"])
EOF
mv certz/BUILD.bazel.new certz/BUILD.bazel""",
            # credentialz
            """cat > credentialz/BUILD.bazel.new << 'EOF'
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(name = "credentialz_proto", srcs = ["credentialz.proto"], import_prefix = "github.com/openconfig/gnsi")
cpp_grpc_library(name = "credentialz_cc_proto", protos = [":credentialz_proto"])
go_library(name = "credentialz", srcs = ["credentialz.pb.go", "credentialz_grpc.pb.go"], importpath = "github.com/openconfig/gnsi/credentialz",
    deps = ["@org_golang_google_grpc//:grpc", "@org_golang_google_grpc//codes", "@org_golang_google_grpc//status",
            "@org_golang_google_protobuf//reflect/protoreflect", "@org_golang_google_protobuf//runtime/protoimpl"])
EOF
mv credentialz/BUILD.bazel.new credentialz/BUILD.bazel""",
            # pathz
            """cat > pathz/BUILD.bazel.new << 'EOF'
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(name = "pathz_proto", srcs = ["pathz.proto"], import_prefix = "github.com/openconfig/gnsi",
    deps = ["@com_github_openconfig_gnmi//proto/gnmi:gnmi_proto"])
cpp_grpc_library(name = "pathz_cc_proto", protos = [":pathz_proto"])
go_library(name = "pathz", srcs = ["pathz.pb.go", "pathz_grpc.pb.go"], importpath = "github.com/openconfig/gnsi/pathz",
    deps = ["@com_github_openconfig_gnmi//proto/gnmi:go_default_library",
            "@org_golang_google_grpc//:grpc", "@org_golang_google_grpc//codes", "@org_golang_google_grpc//status",
            "@org_golang_google_protobuf//reflect/protoreflect", "@org_golang_google_protobuf//runtime/protoimpl"])
EOF
mv pathz/BUILD.bazel.new pathz/BUILD.bazel""",
            # acctz
            """cat > acctz/BUILD.bazel.new << 'EOF'
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(name = "acctz_proto", srcs = ["acctz.proto"], import_prefix = "github.com/openconfig/gnsi",
    deps = ["@com_google_protobuf//:any_proto", "@com_google_protobuf//:timestamp_proto"])
cpp_grpc_library(name = "acctz_cc_proto", protos = [":acctz_proto"])
go_library(name = "acctz", srcs = ["acctz.pb.go", "acctz_grpc.pb.go"], importpath = "github.com/openconfig/gnsi/acctz",
    deps = ["@org_golang_google_grpc//:grpc", "@org_golang_google_grpc//codes", "@org_golang_google_grpc//status",
            "@org_golang_google_protobuf//reflect/protoreflect", "@org_golang_google_protobuf//runtime/protoimpl",
            "@org_golang_google_protobuf//types/known/anypb", "@org_golang_google_protobuf//types/known/timestamppb"])
EOF
mv acctz/BUILD.bazel.new acctz/BUILD.bazel""",
        ],
        sum = "h1:Enn5i3m6KsnHeUI+kalB9OH8fADf0oeymd/3Ze0BzME=",
        version = "v1.7.0",
    )

deps = module_extension(implementation = _ext_impl)
