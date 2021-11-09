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
# This script is used to generate ssb data set
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

CURDIR=${ROOT}
SSB_DBGEN_DIR=$CURDIR/ssb-dbgen/
SSB_DATA_DIR=$CURDIR/ssb-data/

usage() {
  echo "
Usage: $0 <options>
  Optional options:
     -s             scale factor, default is 100
     -c             parallelism to generate data of lineorder table, default is 10

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

if [ $# == 0 ] ; then
    usage
fi

while true; do
    case "$1" in
        -h) HELP=1 ; shift ;;
        -s) SCALE_FACTOR=$2 ; shift 2 ;;
        -c) PARALLEL=$2 ; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

echo "Scale Factor: $SCALE_FACTOR"
echo "Parallelism: $PARALLEL"

# check if dbgen exists
if [[ ! -f $SSB_DBGEN_DIR/dbgen ]]; then
    echo "$SSB_DBGEN_DIR/dbgen does not exist. Run build-ssb-dbgen.sh first to build it first."
    exit 1
fi

if [[ -d $SSB_DATA_DIR/ ]]; then
    echo "$SSB_DATA_DIR exists. Remove it before generating data"
    exit 1
fi

mkdir $SSB_DATA_DIR/

# gen data
cd $SSB_DBGEN_DIR
echo "Begin to generate data for table: customer"
$SSB_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T c
echo "Begin to generate data for table: part"
$SSB_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T p
echo "Begin to generate data for table: supplier"
$SSB_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T s
echo "Begin to generate data for table: date"
$SSB_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T d
echo "Begin to generate data for table: lineorder"
$SSB_DBGEN_DIR/dbgen -f -s $SCALE_FACTOR -T l -C $PARALLEL
cd -

# move data to $SSB_DATA_DIR
mv $SSB_DBGEN_DIR/*.tbl* $SSB_DATA_DIR/

# check data
du -sh $SSB_DATA_DIR/*.tbl*
