"""Legacy Go dependencies that cannot be migrated to go_deps due to cross-repo visibility issues.

These repositories need custom BUILD files with complex patch_cmds that reference external repos.
This is a known bzlmod limitation: https://github.com/bazelbuild/bazel/issues/19301
"""

load("@gazelle//:deps.bzl", "go_repository")

def _ext_impl(m):
    # msteinert/pam - CGO dependency on PAM library from distroless
    # Custom BUILD file links against @bookworm//libpam0g-dev:libpam0g
    # Needs patches and custom BUILD (build_file_generation = "off")
    go_repository(
        name = "com_github_msteinert_pam",
        importpath = "github.com/msteinert/pam",
        sum = "h1:4XoXKtMCH3+e6GIkW41uxm6B37eYqci/DH3gzSq7ocg=",
        version = "v1.0.0",
        build_file_proto_mode = "disable",
        build_file_generation = "off",
        patches = ["//patches:msteinert_pam.patch"],
        patch_args = ["-p1"],
    )

    # openconfig/gnoi - references @com_github_grpc_grpc//bazel:cc_grpc_library.bzl
    # Cannot use go_deps because of cross-repo dependency visibility
    go_repository(
        name = "com_github_openconfig_gnoi",
        importpath = "github.com/openconfig/gnoi",
        sum = "h1:7u+4jc9kEuaXMYHCLLW2eRO0WC3mElx+0/t/xqRtYJ4=",
        version = "v0.4.1-0.20240320162840-dbdca7782474",
    )

    # openconfig/gnsi - uses pre-generated .pb.go files instead of proto compilation
    # Patch simplifies BUILD files to use pregenerated .pb.go files
    go_repository(
        name = "com_github_openconfig_gnsi",
        build_file_proto_mode = "disable",
        importpath = "github.com/openconfig/gnsi",
        patch_args = ["-p1"],
        patches = ["//patches:github.com-openconfig-gnsi.patch"],
        sum = "h1:Enn5i3m6KsnHeUI+kalB9OH8fADf0oeymd/3Ze0BzME=",
        version = "v1.7.0",
    )

deps = module_extension(implementation = _ext_impl)
