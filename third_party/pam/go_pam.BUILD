load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "pam",
    srcs = [
        "callback.go",
        "transaction.c",
        "transaction.go",
    ],
    cgo = True,
    cdeps = [
        "//third_party/pam:pam",
    ],
    copts = ["-Wall", "-std=c99"],
    clinkopts = ["-ldl"],
    importpath = "github.com/msteinert/pam",
    visibility = ["//visibility:public"],
)

alias(
    name = "go_default_library",
    actual = ":pam",
    visibility = ["//visibility:public"],
)

go_test(
    name = "pam_test",
    srcs = [
        "callback_test.go",
        "example_test.go",
        "transaction_test.go",
    ],
    embed = [":pam"],
)
