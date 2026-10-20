#!/bin/sh
set -eu

docker compose -f docker.yaml up -d --build
docker compose -f docker.yaml ps
