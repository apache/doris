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
# This script is used to compile Spark-Doris-Connector
# Usage:
#    sh build.sh
#
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}/../../

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . ${DORIS_HOME}/custom_env.sh
fi

# check maven
MVN_CMD=mvn
if [[ ! -z ${CUSTOM_MVN} ]]; then
    MVN_CMD=${CUSTOM_MVN}
fi
if ! ${MVN_CMD} --version; then
    echo "Error: mvn is not found"
    exit 1
fi
export MVN_CMD

${MVN_CMD} clean package


mkdir -p output/
cp target/doris-spark-1.0.0-SNAPSHOT.jar ./output/

echo "*****************************************"
echo "Successfully build Spark-Doris-Connector"
echo "*****************************************"

exit 0
