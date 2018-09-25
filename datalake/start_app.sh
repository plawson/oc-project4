#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo "usage: $0 <title> full|test"
    exit 1
fi

spark-submit --deploy-mode client \
    --conf spark.executor.memory=7g \
    --conf spark.executor.instances=3 \
    --conf spark.driver.memory=5g  \
    ./find_experts.py $1 $2