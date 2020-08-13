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

# print usage
usage() {
    echo "
Description:
        This script is used to restore the tablets from trash. It supports single mode
    and batch mode.
        In single mode, it will restore just one tablet.
        In batch mode, it will restore all the tablets specified in file. The content 
    of the file is comma-split tablet id and schema hash, like the following:
            12345,11111
            12346,11111
            12347,11111

Usage: $0 <options>
    Optional options:
        -h | --help         print help info
        -b | --backend      backend http service, default: http://127.0.0.1/8040
        -t | --tablet_id    tablet id to restore
        -s | --schema_hash  tablet related schema hash
        -f | --file         file with lines containing comma-split tablet id and schema hash

Examples:
    batch mode:
        sh restore_tablet_tool.sh -b "http://127.0.0.1:8040" -f tablets.txt
        sh restore_tablet_tool.sh --backend "http://127.0.0.1:8040" --file tablets.txt

    single mode:
        sh restore_tablet_tool.sh -b "http://127.0.0.1:8040" -t 12345 -s 11111
        sh restore_tablet_tool.sh --backend "http://127.0.0.1:8040" --tablet_id 12345 --schema_hash 11111
    "
    exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o 'b:t:s:f:' \
  -l 'server:,tablet_id:,schema_hash:,file:,help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

SERVER="http://127.0.0.1/8040"
TABLET_ID=
SCHEMA_HASH=
FILENAME=
BATCH_MODE=false

while true; do
    case "$1" in
        -b|--backend) SERVER=$2 ; shift 2 ;;
        -f|--file) FILENAME=$2 ; BATCH_MODE=true ; shift 2 ;;
        -t|--tablet_id) TABLET_ID=$2 ; shift 2 ;;
        -s|--schema_hash) SCHEMA_HASH=$2 ; shift 2 ;;
        -h|--help) usage ; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

restore_tablet() {
    echo "start to restore tablet id:"$2", schema hash:"$3
    curl -X POST "$1/api/restore_tablet?tablet_id=$2&schema_hash=$3"
    echo -e "\n"
}

if [ $BATCH_MODE = true ] ; then
    lines=`cat $FILENAME`
    for line in $lines
    do
        # split the comma-split line
        # format: tablet_id,schema_hash
        fields=(${line/,/ })
        TABLET_ID=${fields[0]}
        SCHEMA_HASH=${fields[1]}
        restore_tablet $SERVER $TABLET_ID $SCHEMA_HASH
    done
else
    restore_tablet $SERVER $TABLET_ID $SCHEMA_HASH
fi
