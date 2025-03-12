FROM marketplace.gcr.io/google/rbe-ubuntu18-04:latest

RUN apt update
RUN apt install -y apt-utils
RUN apt install -y sudo vim

RUN apt install -y \
    libpam0g-dev \
    libpam-dev
    #
    # Not sure if these pcre libs are needed anymore.  We are building the
    # original libpcre from source as it is required for libyang.  However it
    # also appears that some of the packages installed for swss-common below
    # depend on the newer version pcre-2 so the "apt install" ends up installing
    # some libpcre2 (and pcre3??) packages.
    #
    # libpcre2-dev \
    # libpcre3-dev \
    # libpcre3

# Needed for sonic-swss-common
RUN apt install -y make \
    libtool             \
    m4                  \
    autoconf            \
    dh-exec             \
    debhelper           \
    cmake               \
    pkg-config          \
    libnl-3-dev         \
    libnl-genl-3-dev    \
    libnl-route-3-dev   \
    libnl-nf-3-dev      \
    libgtest-dev        \
    uuid-dev            \
    libzmq3-dev

# Moved into bazel WORKSPACE to build from source
#    nlohmann-json-dev

# Boost is being installed below from source, we are not using the package
# manager because we want to specify exact versions.
#    libboost-dev
#    libboost-all-dev

# Redis library is also installed below
#    libhiredis-dev

# Installing python from the CPython repo source below
#    libpython3-dev

# Swig may be needed once we are generating files from sonic-swss-common in
# sonic-gnmi/swsscommon/
RUN apt install -y swig

# Removed:
#    libpython2.7-dev    \

# Cannot find this package
#RUN apt install -y libgmock-dev

# Bazel is now building and installling libyang, these debs are not longer needed.
# COPY tools/docker/libyang*.deb /
# RUN dpkg -i /libyang*.deb

# Setup the specific version of boost which is also used in the containers to
# run the executables this build container will create.
RUN apt install -y wget
RUN mkdir /workspace_libs
RUN set -eux; \
    wget -q -O /workspace_libs/boost_1_74_0.tar.gz https://archives.boost.io/release/1.74.0/source/boost_1_74_0.tar.gz; \
    cd /workspace_libs; \
    tar xzf boost_1_74_0.tar.gz; \
    cd boost_1_74_0; \
    ./bootstrap.sh; \
    ./b2 install; \
    cd /workspace_libs; \
    rm -rf /workspace_libs/boost_1_74_0;

# Download and install redis client library.
RUN set -eux; \
    cd /workspace_libs; \
    wget -q http://ftp.us.debian.org/debian/pool/main/h/hiredis/libhiredis0.14_0.14.1-1_amd64.deb; \
    wget -q http://ftp.us.debian.org/debian/pool/main/h/hiredis/libhiredis-dev_0.14.1-1_amd64.deb; \
    dpkg -i libhiredis0.14_0.14.1-1_amd64.deb; \
    dpkg -i libhiredis-dev_0.14.1-1_amd64.deb;

# Download and build libpcre, this is a dependency of libyang.
RUN set -eux; \
    cd /workspace_libs; \
    wget -q https://sourceforge.net/projects/pcre/files/pcre/8.45/pcre-8.45.tar.bz2; \
    tar xjf pcre-8.45.tar.bz2; \
    mkdir pcre-8.45/build; \
    cd pcre-8.45/build; \
    cmake .. -DBUILD_SHARED_LIBS=on; \
    make -j`nproc`; \
    make install; \
    cd /workspace_libs; \
    rm -rf pcre-8.45;

# Download and build Python 3.9.2, this is a dependency of sonic-gnmi/sonic_data_client
RUN apt install -y libssl-dev
RUN set -eux; \
    cd /workspace_libs; \
    wget -q https://github.com/python/cpython/archive/refs/tags/v3.9.2.tar.gz; \
    tar xzf v3.9.2.tar.gz; \
    cd cpython-3.9.2; \
    ./configure ; \
    make -j`nproc`; \
    make install; \
    cd /workspace_libs; \
    rm -rf cpython-3.9.2;

# Needed for Kokoro builds, the environment there is a bit different...
RUN mkdir -p /tmpfs/tmp
RUN chmod -R a+r /tmpfs
RUN chmod -R a+w /tmpfs
