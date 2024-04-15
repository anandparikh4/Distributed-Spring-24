#! /bin/sh
set -x
set -e

DOCKER_CONTAINER_NAME="Database"
until docker exec $DOCKER_CONTAINER_NAME pg_isready; do sleep 1; done

python load_balancer.py &

jobs_array=$(jobs -p)

trap "kill -SIGTERM $jobs_array; wait; exit 0" SIGTERM
trap "kill -SIGINT  $jobs_array; wait; exit 0" SIGINT

wait
