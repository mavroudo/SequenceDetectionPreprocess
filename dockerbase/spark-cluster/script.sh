#!/bin/bash

if [ "$1" = "build" ]; then
  echo "Creating docker network"
  docker network create --driver=bridge  siesta-net || true
  docker build -t mavroudo/spark-base -f spark-base.Dockerfile .
  docker build -t spark-master -f spark-master.Dockerfile .
  docker build -t spark-worker -f spark-worker.Dockerfile .
fi