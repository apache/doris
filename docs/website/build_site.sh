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

set -eo pipefail

CUR=`dirname "$0"`
CUR=`cd "$CUR"; pwd`

export DORIS_HOME=${CUR}/../../

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --cn               build Chinese Documentation
     --en               build English Documentation
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'cn' \
  -l 'en' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_CN=
BUILD_EN=
HELP=0
if [ $# == 1 ] ; then
    # default
    BUILD_CN=1
    BUILD_EN=0
else
    BUILD_CN=0
    BUILD_EN=0
    while true; do
        case "$1" in
            --cn) BUILD_CN=1 ; shift ;;
            --en) BUILD_EN=1 ; shift ;;
            --help) HELP=1; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi


if [ ${BUILD_CN} -eq 1 ] ; then
    rm -rf ${CUR}/source/Docs/*
    rm -rf ${CUR}/source/resources/
    cp -r ${DORIS_HOME}/docs/documentation/cn/ ${CUR}/source/Docs/
    cp -r ${DORIS_HOME}/docs/resources/ ${CUR}/source/

    make clean && make html
elif [ ${BUILD_EN} -eq 1 ] ; then
    echo "not implemented yet"
    exit 1
fi

