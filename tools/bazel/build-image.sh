#!/bin/bash

yes | gcloud auth configure-docker
docker build -t sonic-rbe-umf:latest .
docker tag sonic-rbe-umf:latest gcr.io/gpins-sonic-swss/sonic-rbe-umf:latest
docker push gcr.io/gpins-sonic-swss/sonic-rbe-umf:latest