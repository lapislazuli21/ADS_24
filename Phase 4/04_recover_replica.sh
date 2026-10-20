#!/bin/sh
set -eu

replica=${1:-wordcount2}
docker compose -f docker.yaml start "$replica"
docker compose -f docker.yaml ps
