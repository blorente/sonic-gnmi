#!/bin/bash

yes | gcloud auth configure-docker
docker build -t sonic-rbe-bookworm:latest .
docker tag sonic-rbe-bookworm:latest gcr.io/gpins-sonic-swss/sonic-rbe-bookworm:latest
docker push gcr.io/gpins-sonic-swss/sonic-rbe-bookworm:latest