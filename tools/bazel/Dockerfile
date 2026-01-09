# Minimal Bazel build image based on sonic-slave-bookworm
# sonic-slave-bookworm already includes: Python 3.11, Go 1.21+, most build deps
FROM sonicdev-microsoft.azurecr.io:443/sonic-slave-bookworm:latest

# Make apt-get non-interactive
ENV DEBIAN_FRONTEND=noninteractive

# Install only bazel-specific requirements not already in sonic-slave-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    eatmydata \
    && rm -rf /var/lib/apt/lists/*

# PAM development libraries (needed for authentication)
RUN eatmydata apt-get update && eatmydata apt-get install -y --no-install-recommends \
    libpam0g-dev \
    libpam-dev \
    && rm -rf /var/lib/apt/lists/*

# libusb for crypta_client (attestation)
RUN eatmydata apt-get update && eatmydata apt-get install -y --no-install-recommends \
    libusb-1.0-0-dev \
    && rm -rf /var/lib/apt/lists/*

# swig for generating bindings from sonic-swss-common
RUN eatmydata apt-get update && eatmydata apt-get install -y --no-install-recommends \
    swig \
    && rm -rf /var/lib/apt/lists/*

# Create workspace for additional libraries
RUN mkdir -p /workspace_libs

# Boost 1.74.0 - specific version required for runtime compatibility
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends wget; \
    wget -q -O /workspace_libs/boost_1_74_0.tar.gz https://archives.boost.io/release/1.74.0/source/boost_1_74_0.tar.gz; \
    cd /workspace_libs; \
    tar xzf boost_1_74_0.tar.gz; \
    cd boost_1_74_0; \
    ./bootstrap.sh; \
    ./b2 install; \
    cd /; \
    rm -rf /workspace_libs/boost_1_74_0 /workspace_libs/boost_1_74_0.tar.gz; \
    rm -rf /var/lib/apt/lists/*

# Hiredis - specific version for Debian Bookworm compatibility
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends wget; \
    cd /workspace_libs; \
    wget -q http://ftp.us.debian.org/debian/pool/main/h/hiredis/libhiredis0.14_0.14.1-3_amd64.deb; \
    wget -q http://ftp.us.debian.org/debian/pool/main/h/hiredis/libhiredis-dev_0.14.1-3_amd64.deb; \
    dpkg -i libhiredis0.14_0.14.1-3_amd64.deb; \
    dpkg -i libhiredis-dev_0.14.1-3_amd64.deb; \
    rm -f /workspace_libs/*.deb; \
    rm -rf /var/lib/apt/lists/*

# libpcre 8.45 - required dependency for libyang
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends wget cmake build-essential; \
    cd /workspace_libs; \
    wget -q https://sourceforge.net/projects/pcre/files/pcre/8.45/pcre-8.45.tar.bz2; \
    tar xjf pcre-8.45.tar.bz2; \
    mkdir pcre-8.45/build; \
    cd pcre-8.45/build; \
    cmake .. -DBUILD_SHARED_LIBS=on; \
    make -j$(nproc); \
    make install; \
    cd /; \
    rm -rf /workspace_libs/pcre-8.45 /workspace_libs/pcre-8.45.tar.bz2; \
    rm -rf /var/lib/apt/lists/*

# Temporary directories for Kokoro builds
RUN mkdir -p /tmpfs/tmp && chmod -R a+rw /tmpfs
