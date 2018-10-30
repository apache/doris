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

# check DORIS_HOME
if [[ -z ${DORIS_HOME} ]]; then
    echo "Error: DORIS_HOME is not set"
    exit 1
fi

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    source ${DORIS_HOME}/custom_env.sh
fi

# check java version
if [ -z ${JAVA_HOME} ]; then
    echo "Error: JAVA_HOME is not set, use thirdparty/installed/jdk1.8.0_131"
    export JAVA_HOME=${DORIS_HOME}/thirdparty/installed/jdk1.8.0_131
fi

export JAVA=${JAVA_HOME}/bin/java
JAVA_VER=$(${JAVA} -version 2>&1 | sed 's/.* version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q' | cut -f1 -d " ")
if [ $JAVA_VER -lt 18 ]; then
    echo "Error: require JAVA with JDK version at least 1.8"
    exit 1
fi

# check maven
export MVN=mvn
if ! ${MVN} --version; then
    echo "Error: mvn is not found"
    exit 1
fi

# check python
export PYTHON=python
if ! ${PYTHON} --version; then
    export PYTHON=python2.7
    if ! ${PYTHON} --version; then
        echo "Error: python is not found"
        exit
    fi
fi

# set DORIS_THIRDPARTY
if [ -z ${DORIS_THIRDPARTY} ]; then
    export DORIS_THIRDPARTY=${DORIS_HOME}/thirdparty
fi

