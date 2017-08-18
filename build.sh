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

##############################################################
# This script is used to compile Palo.
# Usage:
#    sh build.sh        build both Backend and Frontend.
#    sh build.sh -clean clean previous output and build.
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`
export PALO_HOME=$ROOT

PARALLEL=4

# Check java version
if [ -z $JAVA_HOME ]; then
    echo "Error: JAVA_HOME is not set."
    exit 1
fi
JAVA=${JAVA_HOME}/bin/java
JAVA_VER=$(${JAVA} -version 2>&1 | sed 's/.* version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')
if [[ $JAVA_VER < 18 ]]; then
    echo "Require JAVA with JDK version at least 1.8"
    exit 1
fi

# Check ant
if ! ant -version; then
    echo "ant is not found."
    exit 1
fi

cd ${PALO_HOME}

# Check args
CLEAN_ALL=0
if [ $# -ne 0 ]; then
    if [ "$1"x == "-clean"x ]; then
        CLEAN_ALL=1
    fi
fi

echo "CLEAN_ALL:  "${CLEAN_ALL}

# Clean and build generated code
echo "Build generated code"
cd ${PALO_HOME}/gensrc
if [ ${CLEAN_ALL} -eq 1 ]; then
   make clean
fi 
make
cd ${PALO_HOME}

# Clean and build Backend
echo "Build Backend"
if [ ${CLEAN_ALL} -eq 1 ]; then
    rm ${PALO_HOME}/be/build/ -rf
    rm ${PALO_HOME}/be/output/ -rf
fi
mkdir -p ${PALO_HOME}/be/build/
cd ${PALO_HOME}/be/build/
cmake ../
make -j${PARALLEL}
make install
cd ${PALO_HOME}

# Build docs, should be built before Frontend
echo "Build docs"
cd ${PALO_HOME}/docs
if [ ${CLEAN_ALL} -eq 1 ]; then
    make clean
fi
make
cd ${PALO_HOME}

# Clean and build Frontend
echo "Build Frontend"
cd ${PALO_HOME}/fe
if [ ${CLEAN_ALL} -eq 1 ]; then
    ant clean
fi
ant install
cd ${PALO_HOME}

# Clean and prepare output dir
PALO_OUTPUT=${PALO_HOME}/output/
rm -rf ${PALO_OUTPUT}
mkdir -p ${PALO_OUTPUT}

#Copy Frontend and Backend
cp -R ${PALO_HOME}/fe/output ${PALO_OUTPUT}/fe
cp -R ${PALO_HOME}/be/output ${PALO_OUTPUT}/be

echo "***************************************"
echo "Successfully build Palo."
echo "***************************************"

exit 0
