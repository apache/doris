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
Usage: $0 <options>
    Optional options:
        -h | --host  BE hostname
        -p | --port  BE http port
        -t | --tablet_id  tablet id to restore
        -s | --schema_hash    tablet related schema hash
        -f | --file   file with lines containing comma-split tablet id and schema hash
    "
    exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o 'h:p:t:s:f:' \
  -l 'host:,port:,tablet_id:,schema_hash:,file:,help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

HOSTNAME=127.0.0.1
HTTPPORT=8040
TABLET_ID=
SCHEMA_HASH=
FILENAME=
USE_FILE=false

while true; do
    case "$1" in
        -h|--host) HOSTNAME=$2 ; shift 2 ;;
        -p|--port) HTTPPORT=$2 ; shift 2 ;;
        -f|--file) FILENAME=$2 ; USE_FILE=true ; shift 2 ;;
        -t|--tablet_id) TABLET_ID=$2 ; shift 2 ;;
        -s|--schema_hash) SCHEMA_HASH=$2 ; shift 2 ;;
        --help) usage ; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

restore_tablet() {
    echo "start to restore tablet id:"$3", schema hash:"$4
    curl -X POST "http://$1:$2/api/restore_tablet?tablet_id=$3&schema_hash=$4"
    echo -e "\n"
}

if [ $USE_FILE = true ] ; then
    lines=`cat $FILENAME`
    for line in $lines
    do
        # split the comma-split line
        # format: tablet_id,schema_hash
        fields=(${line/,/ })
        TABLET_ID=${fields[0]}
        SCHEMA_HASH=${fields[1]}
        restore_tablet $HOSTNAME $HTTPPORT $TABLET_ID $SCHEMA_HASH
    done
else
    restore_tablet $HOSTNAME $HTTPPORT $TABLET_ID $SCHEMA_HASH
fi