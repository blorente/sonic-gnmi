# The content of this folder has been generated

```
$ git clone https://github.com/bazelbuild/bazel-toolchains.git
$ cd bazel-toolchains
$ ./rbe_configs_gen --bazel_version=5.2.0 \
    --toolchain_container=gcr.io/gpins-sonic-swss/sonic-rbe-umf:latest \
    --output_src_root=/usr/local/google/pins/bazel/sonic-telemetry \
    --output_config_path=tools/bazel --exec_os=linux --target_os=linux
```

# How to build the rbe container?
```
$ docker build -t sonic-rbe-umf:latest .
```

# How to upload the rbe container?

```
$ yes | gcloud auth configure-docker
$ docker tag sonic-rbe-umf:latest gcr.io/gpins-sonic-swss/sonic-rbe-umf:latest
$ docker push gcr.io/gpins-sonic-swss/sonic-rbe-umf:latest

```

# How to activate it?

To build in a docker sandbox use the `--config=remote` option, for example:

```
$ bazel build --config=remote //gnmi_server:gnmi_server_test_image
```

# Debugging build within the docker sandbox?

Extra debug information is available when the `--keep_going`,
 `--verbose_failures` and `--sandbox_debug` options, for example:

 ```
$ bazel build --config=remote --keep_going --verbose_failures --sandbox_debug \
        //gnmi_server:gnmi_server_test_image
 ```