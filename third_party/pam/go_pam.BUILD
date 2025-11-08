load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "pam",
    srcs = [
        "errors.go",
        "errors_bsd.go",
        "errors_linux.go",
        "transaction.c",
        "transaction.go",
        "transaction_linux.go",
    ],
    cgo = True,
    cdeps = [
        "@_main~_repo_rules~linux_pam//:pam",
    ],
    clinkopts = [
        "-ldl",
    ],
    importpath = "github.com/msteinert/pam",
    visibility = ["//visibility:public"],
)

go_test(
    name = "pam_test",
    srcs = [
        "example_test.go",
        "transaction_linux_test.go",
        "transaction_test.go",
    ],
    embed = [":pam"],
)
