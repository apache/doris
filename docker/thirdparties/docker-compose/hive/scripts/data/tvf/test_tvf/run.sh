#!/bin/bash

set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

## mkdir and put data to hdfs
hadoop fs -mkdir -p /catalog/tvf/
hadoop fs -put "${CUR_DIR}"/tvf/* /catalog/tvf/
