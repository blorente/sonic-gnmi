workspace(name = "sonic-gnmi")

DEBIAN_VERSION = "bullseye"

# TODO BL: remove file when we're done

# GOLANG_VERSION = "1.22.4"
#
# load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
# load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
#
# http_archive(
#     name = "rules_pkg",
#     sha256 = "d20c951960ed77cb7b341c2a59488534e494d5ad1d30c4818c736d57772a9fef",
#     urls = [
#         "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz",
#         "https://github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz",
#     ],
# )
#
# load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
#
# rules_pkg_dependencies()
#
# local_repository(
#     name = "sonic-mgmt-common",
#     path = "../sonic-mgmt-common",
# )
#
# ### Python
# http_archive(
#     name = "rules_python",
#     sha256 = "a3a6e99f497be089f81ec082882e40246bfd435f52f4e82f37e89449b04573f6",
#     strip_prefix = "rules_python-0.10.2",
#     url = "https://github.com/bazelbuild/rules_python/archive/refs/tags/0.10.2.tar.gz",
# )
#
# load("@rules_python//python:repositories.bzl", "python_register_toolchains")
#
# python_register_toolchains(
#     name = "python3_9",
#     python_version = "3.9",
# )
#
# load("@python3_9//:defs.bzl", "interpreter")
# load("@rules_python//python:pip.bzl", "pip_install")
#
# # Creates a central external repo, @pip, that contains Bazel targets for all the
# # third-party packages specified in the requirements.txt file.
# pip_install(
#     name = "pip",
#     python_interpreter_target = interpreter,
#     requirements = "//third_party/pip:requirements.txt",
# )

# ### C++ Rules
# http_archive(
#     name = "rules_foreign_cc",
#     sha256 = "4b33d62cf109bcccf286b30ed7121129cc34cf4f4ed9d8a11f38d9108f40ba74",
#     strip_prefix = "rules_foreign_cc-0.11.1",
#     url = "https://github.com/bazelbuild/rules_foreign_cc/releases/download/0.11.1/rules_foreign_cc-0.11.1.tar.gz",
# )
#
# load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")
#
# rules_foreign_cc_dependencies()
#
# libyangBUILD = """
# load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")
#
# package(default_visibility = ["//visibility:public"])
#
# filegroup(
#     name = "all_srcs",
#     srcs = glob(["**"]),
#     visibility = ["//visibility:public"],
# )
#
# cmake(
#     name = "libyang",
#     lib_source = ":all_srcs",
#     cache_entries = {},
#     data = ["@pcre_archive//:pcre_shared",],
#     # Using shared libs here.  When using static libs the libyang.a archive
#     # contains undefined references to "extension" libraries which libyang
#     # builds but does not install.  This results in linker errors later on
#     # complaining that libyang.a does not define: nacm, metadata, yangdata,
#     # user_yang_types, and user_inet_types.  These are all defined in .a
#     # archives but are abandond in the libyang build directory when the
#     # ENABLE_STATIC option is used.
#     out_shared_libs = ["libyang.so",
#                        "libyang.so.1",
#                        "libyang.so.1.10.17",
#                       ],
#     out_data_dirs = [
#         "lib/libyang1/extensions/",
#         "lib/libyang1/user_types/",
#     ],
#     visibility = ["//visibility:public"],
#     deps = [
#         "@pcre_archive//:pcre_shared",
#     ],
# )
# """
#
# http_archive(
#     name = "com_github_cesnet_libyang",
#     build_file_content = libyangBUILD,
#     patch_args = ["-p1"],
#     patches = [
#         #"//patches:libyang-repo.patch",
#         "//patches:libyang.patch",
#     ],
#     sha256 = "1b736443d2c69b5d7a71ac412655e6edab0647b18f35f7bf504b0a24e06cb046",
#     strip_prefix = "libyang-1.0.225",
#     urls = [
#         "https://github.com/CESNET/libyang/archive/refs/tags/v1.0.225.tar.gz",
#     ],
# )
#
# pcreBUILD = """
# load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")
#
# package(default_visibility = ["//visibility:public"])
#
# filegroup(
#     name = "pcre_srcs",
#     srcs = glob(["**"]),
#     visibility = ["//visibility:public"],
# )
#
# cmake(
#     name = "pcre_static",
#     lib_source = ":pcre_srcs",
#     working_directory = "pcre-8.45/",
#     cache_entries = {
#         "CMAKE_C_FLAGS": "-fPIC",
#         "BUILD_SHARED_LIBS": "off",
#         "PCRE_SUPPORT_UNICODE_PROPERTIES" : "ON",
#     },
#     out_shared_libs = [
#     ],
#     out_static_libs = [
#         "libpcre.a",
#         "libpcrecpp.a",
#         "libpcreposix.a",
#     ],
# )
# cmake(
#     name = "pcre_shared",
#     lib_source = ":pcre_srcs",
#     working_directory = "pcre-8.45/",
#     cache_entries = {
#         "CMAKE_C_FLAGS": "-fPIC",
#         "BUILD_SHARED_LIBS": "ON",
#         "PCRE_SUPPORT_UNICODE_PROPERTIES" : "ON",
#     },
#     out_shared_libs = [
#         "libpcre.so",
#         "libpcrecpp.so",
#         "libpcreposix.so",
#     ],
#     out_static_libs = [
#     ],
# )
# """
#
# http_archive(
#     name = "pcre_archive",
#     build_file_content = pcreBUILD,
#     sha256 = "4dae6fdcd2bb0bb6c37b5f97c33c2be954da743985369cddac3546e3218bffb8",
#     urls = [
#         "https://sourceforge.net/projects/pcre/files/pcre/8.45/pcre-8.45.tar.bz2",
#     ],
# )
#
# http_archive(
#     # This is used in the status.proto in gnoi blackbox
#     name = "googleapis",
#     sha256 = "90c5c5baf3e5844620ad47504e4929aa7f9768cb14ff2897cc7733f97f804f63",
#     strip_prefix = "googleapis-fe771208f21a28e11507352faf03098244d42f0c",
#     urls = [
#         "https://github.com/googleapis/googleapis/archive/fe771208f21a28e11507352faf03098244d42f0c.zip",
#     ],
# )
#
# load("@googleapis//:repository_rules.bzl", "switched_rules_by_language")
#
# switched_rules_by_language(
#     name = "com_google_googleapis_imports",
# )
#
# http_archive(
#     # There is an issue with rules_go v0.48 with a broken common_proto dependency.
#     name = "io_bazel_rules_go",
#     sha256 = "f74c98d6df55217a36859c74b460e774abc0410a47cc100d822be34d5f990f16",
#     urls = [
#         "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.47.1/rules_go-v0.47.1.zip",
#         "https://github.com/bazelbuild/rules_go/releases/download/v0.47.1/rules_go-v0.47.1.zip",
#     ],
# )
#
# http_archive(
#     name = "bazel_gazelle",
#     sha256 = "d76bf7a60fd8b050444090dfa2837a4eaf9829e1165618ee35dceca5cbdf58d5",
#     urls = [
#         "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.37.0/bazel-gazelle-v0.37.0.tar.gz",
#         "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.37.0/bazel-gazelle-v0.37.0.tar.gz",
#     ],
# )
#
# load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
# load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")
#
### gNSI Requires Rules Proto
# http_archive(
#     # v5 of this repo is reserved for migrating away from WORKSPACE to bzlmod
#     name = "rules_proto_grpc",
#     sha256 = "2a0860a336ae836b54671cbbe0710eec17c64ef70c4c5a88ccfd47ea6e3739bd",
#     strip_prefix = "rules_proto_grpc-4.6.0",
#     urls = ["https://github.com/rules-proto-grpc/rules_proto_grpc/releases/download/4.6.0/rules_proto_grpc-4.6.0.tar.gz"],
# )
#
# load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")
#
# rules_proto_grpc_toolchains()
#
# rules_proto_grpc_repos()
#
# load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
#
# rules_proto_dependencies()
#
# rules_proto_toolchains()
#
# load("//:deps.bzl", "go_dependencies")
#
# # gazelle:repository_macro deps.bzl%go_dependencies
# go_dependencies()
#
# go_rules_dependencies()
#
# go_register_toolchains(
#     # nogo = "@//telemetry:gnmi_nogo",
#     version = GOLANG_VERSION,
# )
#
# gazelle_dependencies()

# ### Docker image building
# http_archive(
#     # This should eventually be replaced with rules_oci as it is deprecated.
#     name = "io_bazel_rules_docker",
#     sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
#     urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
# )
#
# load(
#     "@io_bazel_rules_docker//repositories:repositories.bzl",
#     container_repositories = "repositories",
# )
#
# container_repositories()
#
# load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")
#
# container_deps()
#
# load("@io_bazel_rules_docker//container:container.bzl", "container_load")
# load("@io_bazel_rules_docker//contrib:dockerfile_build.bzl", "dockerfile_image")
#
# dockerfile_image(
#     name = "test_image_tar",
#     build_args = {"DEBIAN_VERSION": DEBIAN_VERSION},
#     dockerfile = "//tools/docker:Dockerfile.test",
# )
#
# # Load the test-environment image tarball.
# container_load(
#     name = "test_image",
#     file = "@test_image_tar//image:dockerfile_image.tar",
# )
#
# dockerfile_image(
#     name = "debug_image_tar",
#     build_args = {"DEBIAN_VERSION": DEBIAN_VERSION},
#     dockerfile = "//tools/docker:Dockerfile.debug",
# )
#
# # Load the debug-environment image tarball.
# container_load(
#     name = "debug_image",
#     file = "@debug_image_tar//image:dockerfile_image.tar",
# )
#
# # Load the prod-environment image tarball.
# container_load(
#     name = "telemetry_image",
#     file = "@telemetry_image_tar//image:dockerfile_image.tar",
# )
#
# dockerfile_image(
#     name = "telemetry_image_tar",
#     build_args = {"DEBIAN_VERSION": DEBIAN_VERSION},
#     dockerfile = "//tools/docker:Dockerfile.prod",
# )
#
# # Load the host-environment image tarball.
# container_load(
#     name = "host_image",
#     file = "@host_image_tar//image:dockerfile_image.tar",
# )
#
# dockerfile_image(
#     name = "host_image_tar",
#     build_args = {"DEBIAN_VERSION": DEBIAN_VERSION},
#     dockerfile = "//tools/docker:Dockerfile.host",
# )
#
# buildimageBUILD = """
# load("@rules_pkg//:pkg.bzl", "pkg_tar")
# filegroup(
#     name = "exported_yangs",
#     srcs = glob(["src/sonic-yang-models/yang-models/*.yang"]),
#     visibility = ["//visibility:public"],
# )
# filegroup(
#     name = "exported_yang_templates",
#     srcs = glob(["src/sonic-yang-models/yang-templates/*.yang.j2"]),
#     visibility = ["//visibility:public"],
# )
# genrule(
#     name = "yang-file-export",
#     srcs = [
#         ":exported_yangs",
#         ":exported_yang_templates",
#     ],
#     outs = [
#         "sonic-yangs-export.tar",
#         "sonic-yang-templates-export.tar",
#     ],
#     cmd = "for f in $(locations :exported_yangs); do " +
#           "  tar -r -f $(@D)/sonic-yangs-export.tar -C $$(dirname $$f) `basename $$f`;" +
#           "done; " +
#           "for f in $(locations :exported_yang_templates); do " +
#           "  tar -r -f $(@D)/sonic-yang-templates-export.tar -C $$(dirname $$f) `basename $$f`;" +
#           "done;",
#     visibility = ["//visibility:public"],
# )
#
# pkg_tar(
#     name = "sonic-cfggen",
#     srcs = glob(["src/sonic-config-engine/*"]),
#     mode = "0644",
#     package_dir = "/sonic-config-engine",
#     # strip_prefix = "/testdata",
#     visibility = ["//visibility:public"],
# )
# """
#
# new_git_repository(
#     name = "sonic-buildimage",
#     branch = "master",
#     build_file_content = buildimageBUILD,
#     remote = "https://github.com/sonic-net/sonic-buildimage",
# )
#
# swsscommonBUILD = """
# filegroup(
#     name = "all_srcs",
#     srcs = glob(["common/*.cpp"], exclude=["common/loglevel.cpp", "common/loglevel_util.cpp"]),
#     visibility = ["//visibility:public"],
# )
# filegroup(
#     name = "all_hdrs",
#     srcs = glob(["common/*.h", "common/*.hpp"]),
#     visibility = ["//visibility:public"],
# )
# filegroup(
#     name = "all_luas",
#     srcs = glob(["common/*.lua"]),
#     visibility = ["//visibility:public"],
# )
# filegroup(
#     name = "swig_template",
#     srcs = ["pyext/swsscommon.i"],
#     visibility = ["//visibility:public"],
# )
# # cc_shared_library(
# #     name = "swsscommon_shared",
# #     shared_lib_name = "libswsscommon.so",
# #     deps = [
# #         ":swsscommon_base",
# #     ],
# #     visibility = ["//visibility:public"],
# # )
# cc_library(
#     name = "swsscommon_base",
#     srcs = [
#         ":all_srcs",
#     ],
#     hdrs = [":all_hdrs",],
#     # include_prefix = "swss",
#     # strip_include_prefix = "common",
#     copts = [
#         "-std=c++14",
#         "-I/usr/include/libnl3", # Expected location in the SONiC build container"
#     ],
#     includes = [
#         "sonic-swss-common",
#         "sonic-swss-common/common",
#     ],
#     linkopts = ["-lpthread -lhiredis -lnl-genl-3 -lnl-nf-3 -lnl-route-3 -lnl-3 -lzmq -lboost_serialization -luuid"],
#     # implementation_deps = ["@com_github_cesnet_libyang//:libyang"],
#     deps = [
#         "@com_github_nlohmann_json//:json",
#         "@com_github_cesnet_libyang//:libyang",
#     ],
#     visibility = ["//visibility:public"],
# )
# """
#
# new_git_repository(
#     name = "sonic-swss-common",
#     build_file_content = swsscommonBUILD,
#     commit = "8e24cedf016c73112561eac4c7f6a6fe3b21faf3",
#     remote = "https://github.com/sonic-net/sonic-swss-common",
# )
#
# git_repository(
#     name = "com_github_nlohmann_json",
#     # Current tip of "develop" branch as of Nov-2023.  Using this commit since
#     # the last release is quite old and does not contain bazel support.
#     commit = "6eab7a2b187b10b2494e39c1961750bfd1bda500",
#     remote = "https://github.com/nlohmann/json",
# )
