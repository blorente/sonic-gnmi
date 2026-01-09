"Repository rule for SONiC libyang packages"

def _sonic_libyang_impl(rctx):
    # Download the runtime .deb package
    rctx.download(
        url = rctx.attr.runtime_url,
        sha256 = rctx.attr.runtime_sha256,
        output = "libyang.deb",
    )

    # Download the dev .deb package
    rctx.download(
        url = rctx.attr.dev_url,
        sha256 = rctx.attr.dev_sha256,
        output = "libyang-dev.deb",
    )

    # Extract the .deb packages (ar archives containing data.tar.xz)
    # First extract the ar archive, then the data tarball
    rctx.execute(["ar", "x", "libyang.deb"])
    rctx.execute(["mkdir", "-p", "runtime"])
    rctx.execute(["tar", "-xf", "data.tar.xz", "-C", "runtime"])

    rctx.execute(["rm", "-f", "debian-binary", "control.tar.xz", "data.tar.xz"])
    rctx.execute(["ar", "x", "libyang-dev.deb"])
    rctx.execute(["mkdir", "-p", "dev"])
    rctx.execute(["tar", "-xf", "data.tar.xz", "-C", "dev"])

    # Clean up
    rctx.execute(["rm", "-f", "debian-binary", "control.tar.xz", "data.tar.xz", "libyang.deb", "libyang-dev.deb"])

    # Copy headers to repo root so #include <libyang/libyang.h> works directly
    # We copy instead of symlink because Bazel's glob doesn't follow directory symlinks
    # This is needed because rules_go CGO doesn't properly use cc_library includes attribute
    rctx.execute(["cp", "-r", "dev/usr/include/libyang", "libyang"])

    # Create BUILD file with cc_library targets
    # Using native cc_library rule which is always available
    build_content = '''
package(default_visibility = ["//visibility:public"])

# Export all files for debugging/inspection
filegroup(
    name = "all_files",
    srcs = glob(["**/*"]),
)

# Headers from libyang-dev
filegroup(
    name = "headers",
    srcs = glob(["dev/usr/include/libyang/*.h"]),
)

# Shared library from libyang runtime
filegroup(
    name = "shared_libs",
    srcs = glob([
        "runtime/usr/lib/x86_64-linux-gnu/libyang.so.*",
    ]),
)

# Extension plugins
filegroup(
    name = "extension_plugins",
    srcs = glob(["runtime/usr/lib/x86_64-linux-gnu/libyang/extensions/*.so"]),
)

# User type plugins
filegroup(
    name = "user_type_plugins",
    srcs = glob(["runtime/usr/lib/x86_64-linux-gnu/libyang/user_types/*.so"]),
)

# Native cc_library for libyang
# Headers are symlinked to repo root so #include <libyang/libyang.h> works
# directly when rules_go CGO adds -iquote to the repo root.
# This workaround is needed because rules_go doesn't properly pass
# the cc_library includes attribute to CGO compilation.
cc_library(
    name = "libyang",
    hdrs = glob(["libyang/*.h"]),
    srcs = glob(["runtime/usr/lib/x86_64-linux-gnu/libyang.so.*"]),
    data = [
        ":shared_libs",
        ":extension_plugins",
        ":user_type_plugins",
    ],
    includes = ["."],
    linkopts = [
        "-Wl,-rpath,/usr/lib/x86_64-linux-gnu",
    ],
)
'''
    rctx.file("BUILD.bazel", build_content)

sonic_libyang = repository_rule(
    implementation = _sonic_libyang_impl,
    attrs = {
        "runtime_url": attr.string(mandatory = True),
        "runtime_sha256": attr.string(mandatory = True),
        "dev_url": attr.string(mandatory = True),
        "dev_sha256": attr.string(mandatory = True),
    },
)
