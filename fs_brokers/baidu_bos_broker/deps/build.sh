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

# This file build all deps

set -e

DEPSDIR=`dirname "$0"`
DEPSDIR=`cd ${DEPSDIR}; pwd`

CURDIR=`pwd`
if [ ! -f ${DEPSDIR}/bin/bce-java-sdk-0.9.1-internal.jar ]
then
    echo "***********************************"
    echo " Downloading dependency libraries "
    echo "***********************************"
    cd ${DEPSDIR}
    # Check out depends
    # extract archive
    tar xzf baidu_bos_broker_java_libraries.tar.gz
    echo "Unpacking dependency libraries...Done "
else
    echo "Depends already exists."
fi

if [ ! -f bin/thrift ];then
	echo "thrift is not found."
	echo "You need to copy thrift binary file from 'thirdparty/installed/bin/thrift' to $CURDIR"
	exit 1
fi

cd ${CURDIR}
