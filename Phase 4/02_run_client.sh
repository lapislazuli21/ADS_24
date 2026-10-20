#!/bin/sh
set -eu

docker compose -f docker.yaml --profile tools run --build --rm client
