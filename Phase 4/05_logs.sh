#!/bin/sh
set -eu

docker compose -f docker.yaml logs --tail=100 loadbalancer wordcount1 wordcount2 wordcount3
