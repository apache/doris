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

source /etc/profile
source ~/.bash_profile

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'url:' \
  -l 'installDir:' \
  -- "$@")

eval set -- "$OPTS"

URL=
DORIS_FE_HOME=
while true; do
    case "$1" in
        --url) URL=$2 ; shift 2;;
        --installDir) DORIS_FE_HOME=$2 ; shift 2;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [ x"$DORIS_FE_HOME" == x"" ]; then
    echo "--installDir can not empty!"
    exit 1
fi

if [ x"$URL" == x"" ]; then
    echo "--url can not empty!"
    exit 1
fi

if [ ! -d $DORIS_FE_HOME ]; then
    mkdir -p $DORIS_FE_HOME
else
    rm -rf $DORIS_FE_HOME/*
fi
if [ $? -ne 0 ] ;then exit 1;fi

cd $DORIS_FE_HOME
wget  $URL -O doris-fe.tar.gz --quiet
if [ $? -ne 0 ] ;then exit 1;fi

mkdir -p $DORIS_FE_HOME/fetmp
if [ $? -ne 0 ] ;then exit 1;fi

tar -zxvf doris-fe.tar.gz --strip-components=1 -C $DORIS_FE_HOME/fetmp
if [ $? -ne 0 ] ;then exit 1;fi

mv $DORIS_FE_HOME/fetmp/fe/* $DORIS_FE_HOME/
if [ $? -ne 0 ] ;then exit 1;fi

rm -rf $DORIS_FE_HOME/fetmp
if [ $? -ne 0 ] ;then exit 1;fi
