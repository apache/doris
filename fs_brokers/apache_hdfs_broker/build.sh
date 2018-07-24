#!/usr/bin/env bash

# Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

# check java version
if [ -z $JAVA_HOME ]; then
    echo "Error: JAVA_HOME is not set."
    exit 1
fi
JAVA=${JAVA_HOME}/bin/java
JAVA_VER=$(${JAVA} -version 2>&1 | sed 's/.* version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q' | cut -f1 -d " ")
if [ $JAVA_VER -lt 18 ]; then
    echo "Error: java version is too old" $JAVA_VER" need jdk 1.8."
    exit 1
fi

export BROKER_HOME=$ROOT

# Every time, build deps
DEPS_DIR=${BROKER_HOME}/deps
cd ${DEPS_DIR} && sh build.sh
cd ${BROKER_HOME}

# export all variable need by other module
export PATH=${DEPS_DIR}/bin:$PATH
ANT_HOME=${DEPS_DIR}/ant
export PATH=${ANT_HOME}/bin:$PATH
ant output
exit
