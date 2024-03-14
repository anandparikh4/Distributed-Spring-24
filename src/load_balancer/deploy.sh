#! /bin/bash
set -x
set -e

postgres &
python load_balancer.py &

jobs_array=$(jobs -p | tr '\n' ' ')

trap "kill -SIGTERM $jobs_array; wait; exit 0" SIGTERM
trap "kill -SIGINT  $jobs_array; wait; exit 0" SIGINT

wait
