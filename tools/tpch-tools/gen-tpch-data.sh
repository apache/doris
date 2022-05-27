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
# This script is used to generate TPC-H data set
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
  cd "$ROOT"
  pwd
)

CURDIR=${ROOT}
TPCH_DBGEN_DIR=$CURDIR/TPC-H_Tools_v3.0.0/dbgen/
TPCH_DATA_DIR=$CURDIR/tpch-data/

usage() {
  echo "
Usage: $0 <options>
  Optional options:
     -s             scale factor, default is 100
     -c             parallelism to generate data of (lineitem, orders, partsupp) table, default is 10

  Eg.
    $0              generate data using default value.
    $0 -s 10        generate data with scale factor 10.
    $0 -s 10 -c 5   generate data with scale factor 10. And using 5 threads to generate data concurrently.
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'hs:c:' \
  -- "$@")

eval set -- "$OPTS"

SCALE_FACTOR=100
PARALLEL=10
HELP=0

if [ $# == 0 ]; then
  usage
fi

while true; do
  case "$1" in
  -h)
    HELP=1
    shift
    ;;
  -s)
    SCALE_FACTOR=$2
    shift 2
    ;;
  -c)
    PARALLEL=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "Internal error"
    exit 1
    ;;
  esac
done

if [[ ${HELP} -eq 1 ]]; then
  usage
  exit
fi

echo "Scale Factor: $SCALE_FACTOR"
echo "Parallelism: $PARALLEL"

# check if dbgen exists
if [[ ! -f $TPCH_DBGEN_DIR/dbgen ]]; then
  echo "$TPCH_DBGEN_DIR/dbgen does not exist. Run build-tpch-dbgen.sh first to build it first."
  exit 1
fi

if [[ -d $TPCH_DATA_DIR/ ]]; then
  echo "$TPCH_DATA_DIR exists. Remove it before generating data"
  exit 1
fi

mkdir $TPCH_DATA_DIR/

# gen data
cd $TPCH_DBGEN_DIR
echo "Begin to generate data for table: region"
$TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T r
echo "Begin to generate data for table: nation"
$TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T n
echo "Begin to generate data for table: supplier"
$TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T s
echo "Begin to generate data for table: part"
$TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T P
echo "Begin to generate data for table: customer"
$TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T c
echo "Begin to generate data for table: partsupp"
for i in $(seq 1 $PARALLEL); do
  {
    $TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T S -C $PARALLEL -S ${i}
  } &
done
wait

echo "Begin to generate data for table: orders"
for i in $(seq 1 $PARALLEL); do
  {
    $TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T O -C $PARALLEL -S ${i}
  } &
done
wait

echo "Begin to generate data for table: lineitem"
for i in $(seq 1 $PARALLEL); do
  {
    $TPCH_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T L -C $PARALLEL -S ${i}
  } &
done
wait

cd -

# move data to $TPCH_DATA_DIR
mv $TPCH_DBGEN_DIR/*.tbl* $TPCH_DATA_DIR/

# check data
du -sh $TPCH_DATA_DIR/*.tbl*
