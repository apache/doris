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

####################################################################
# This script is used to build Doris used in Baidu

##############################################################
# the script generates src/be/src/common/version.h and 
# fe/src/main/java/org/apache/doris/common/Version.java which 
# contains the build version based on the git hash or svn revision.
##############################################################

build_version="trunk"

unset LANG
unset LC_CTYPE

user=`whoami`
date=`date +"%a, %d %b %Y %H:%M:%S %Z"`
hostname=`hostname`

cwd=`pwd`

if [ -z ${DORIS_HOME+x} ]
then
    ROOT=`dirname "$0"`
    ROOT=`cd "$ROOT"; pwd`
    DORIS_HOME=${ROOT}/../..
    echo "DORIS_HOME: ${DORIS_HOME}"
fi

if [[ -z ${PALO_TEST_BINARY_DIR} ]]; then
    if [ -e ${DORIS_HOME}/gensrc/build/java/org/apache/doris/common/Version.java \
         -a -e ${DORIS_HOME}/gensrc/build/gen_cpp/version.h ]; then
        exit
    fi
fi

cd ${DORIS_HOME}
if [ -d .svn ]; then
    revision=`svn info | sed -n -e 's/Last Changed Rev: \(.*\)/\1/p'`
    url=`svn info | sed -n -e 's/^URL: \(.*\)/\1/p'`
    if echo ${url} | grep '\/tags\/' > /dev/null
    then
        build_version="`echo ${url} | sed 's/.*_\([0-9-]\+\)_PD_BL.*/\1/g' | sed 's/-/\./g'`"
    fi
elif [ -d .git ]; then
    revision=`git log -1 --pretty=format:"%H"`
    url="git://${hostname}${DORIS_HOME}"
else
    revision="Unknown"
    url="file://${DORIS_HOME}"
fi

cd ${cwd}

build_hash="${url}@${revision}"
build_time="${date}"
build_info="${user}@${hostname}"

VERSION_PACKAGE="${DORIS_HOME}/gensrc/build/java/org/apache/doris/common"
mkdir -p ${VERSION_PACKAGE}
cat >"${VERSION_PACKAGE}/Version.java" <<EOF
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common;

// This is a generated file, DO NOT EDIT IT.
// To change this file, see palo/src/common/version-info
// the file should be placed in src/java/org/apache/doris/common/Version.java

public class Version {

  public static final String PALO_BUILD_VERSION = "${build_version}";
  public static final String PALO_BUILD_HASH = "${build_hash}";
  public static final String PALO_BUILD_TIME = "${build_time}";
  public static final String PALO_BUILD_INFO = "${build_info}";

  public static void main(String[] args) {
    System.out.println("palo_build_version: " + PALO_BUILD_VERSION);
    System.out.println("palo_build_hash: " + PALO_BUILD_HASH);
    System.out.println("palo_build_time: " + PALO_BUILD_TIME);
    System.out.println("palo_build_info: " + PALO_BUILD_INFO);
  }

}
EOF

GEN_CPP_DIR=${DORIS_HOME}/gensrc/build/gen_cpp/
mkdir -p ${GEN_CPP_DIR}
cat >"${GEN_CPP_DIR}/version.h" <<EOF
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This is a generated file, DO NOT EDIT IT.
// To change this file, see bin/gen_build_version.sh
// the file should be placed in src/be/src/common/version.h

#ifndef PALO_GEN_CPP_VERSION_H
#define PALO_GEN_CPP_VERSION_H

namespace doris {

#define PALO_BUILD_VERSION "${build_version}"
#define PALO_BUILD_HASH    "${build_hash}"
#define PALO_BUILD_TIME    "${build_time}"
#define PALO_BUILD_INFO    "${build_info}"

} // namespace doris

#endif
EOF
