#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to load generated ssb data set to Doris
# Only for 1 fact table: lineorder
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

CURDIR=${ROOT}
SSB_DATA_DIR=$CURDIR/ssb-data/

usage() {
  echo "
Usage: $0 <options>
  Optional options:
     -c             parallelism to load data of lineorder table, default is 5.

  Eg.
    $0              load data using default value.
    $0 -c 10        load lineorder table data using parallelism 10.     
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'c:' \
  -- "$@")

eval set -- "$OPTS"

PARALLEL=3
HELP=0

if [ $# == 0 ] ; then
    usage
fi

while true; do
    case "$1" in
        -h) HELP=1 ; shift ;;
        -c) PARALLEL=$2 ; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

echo "Parallelism: $PARALLEL"

# check if ssb-data exists
if [[ ! -d $SSB_DATA_DIR/ ]]; then
    echo "$SSB_DATA_DIR does not exist. Run sh gen-ssb-data.sh first."
    exit 1
fi

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! $CMD; then
        echo "$NAME is missing. This script depends on cURL to load data to Doris."
        exit 1
    fi
}

check_prerequest "curl --version" "curl"

# load lineorder
source $CURDIR/doris-cluster.conf

echo "FE_HOST: $FE_HOST"
echo "FE_HTTP_PORT: $FE_HTTP_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

function load()
{
    echo $@
    curl --location-trusted -u $USER:$PASSWORD -H "column_separator:|" -H "columns:lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy" -T $@ http://$FE_HOST:$FE_HTTP_PORT/api/$DB/lineorder/_stream_load
}


# set parallelism
[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1

for ((i=1;i<=$PARALLEL;i++))
do
    echo >&3
done

for file in `ls $SSB_DATA_DIR/lineorder.tbl.*`
do
    read -u3
    {
        load $file
        echo >&3
    }&
done
