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
# This script is used to build ssb-dbgen
# sssb-dbgen's source code is from https://github.com/electrum/ssb-dbgen.git
# Usage: 
#    sh build-ssb-dbgen.sh
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

CURDIR=${ROOT}
SSB_DBGEN_DIR=$CURDIR/ssb-dbgen/

# download ssb-dbgen first
if [[ -d $SSB_DBGEN_DIR ]]; then
    echo "Dir $CURDIR/ssb-dbgen/ already exists. No need to download."
    echo "If you want to download ssb-dbgen again, please delete this dir first."
else
    curl https://palo-cloud-repo-bd.bd.bcebos.com/baidu-doris-release/ssb-dbgen-linux.tar.gz | tar xz -C $CURDIR/
fi

# compile ssb-dbgen
cd $SSB_DBGEN_DIR/ && make
cd -

# check
if [[ -f $CURDIR/ssb-dbgen/dbgen ]]; then
    echo "Build succeed! Run $CURDIR/ssb-dbgen/dbgen -h"
    exit 0
else
    echo "Build failed!"
    exit 1
fi
