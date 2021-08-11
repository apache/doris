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

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'url:' \
  -l 'installDir:' \
  -l 'mkFeMetadir' \
  -- "$@")

eval set -- "$OPTS"

URL=
DORIS_HOME=
MKDIR_FE_META_DIR=0
while true; do
    case "$1" in
        --url) URL=$2 ; shift 2;;
        --installDir) DORIS_HOME=$2 ; shift 2;;
        --mkFeMetadir) MKDIR_FE_META_DIR=1 ; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [ x"$DORIS_HOME" == x"" ]; then
    echo "--installDir can not empty!"
    exit 1
fi

if [ x"$URL" == x"" ]; then
    echo "--url can not empty!"
    exit 1
fi

if [ ! -d $DORIS_HOME ]; then
    mkdir -p $DORIS_HOME
fi

wget  $URL -O doris-fe.tar.gz --quiet
if [ $? -ne 0 ] ;then exit 1;fi

tar -zxvf doris-fe.tar.gz  -C $DORIS_HOME
if [ $? -ne 0 ] ;then exit 1;fi

if [[ ${MKDIR_FE_META_DIR} -eq 1 ]] && [[ ! -d "$DORIS_HOME/doris-meta" ]]; then
  mkdir $DORIS_HOME/doris-meta
fi
if [ $? -ne 0 ] ;then exit 1;fi