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

if [[ -z ${DORIS_TEST_BINARY_DIR} ]]; then
    if [ -e ${DORIS_HOME}/fe/fe-core/target/generated-sources/build/org/apache/doris/common/Version.java \
         -a -e ${DORIS_HOME}/gensrc/build/gen_cpp/version.h ]; then
        exit
    fi
fi

cd ${DORIS_HOME}
if [ -d .svn ]; then
    revision=`svn info | sed -n -e 's/Last Changed Rev: \(.*\)/\1/p'`
    short_revision="${revision}"
    url=`svn info | sed -n -e 's/^URL: \(.*\)/\1/p'`
    if echo ${url} | grep '\/tags\/' > /dev/null
    then
        build_version="`echo ${url} | sed 's/.*_\([0-9-]\+\)_PD_BL.*/\1/g' | sed 's/-/\./g'`"
    fi
elif [ -d .git ]; then
    revision=`git log -1 --pretty=format:"%H"`
    short_revision=`git log -1 --pretty=format:"%h"`
    url="git://${hostname}${DORIS_HOME}"
else
    revision="Unknown"
    short_revision="${revision}"
    url="file://${DORIS_HOME}"
fi

cd ${cwd}

build_hash="${url}@${revision}"
build_short_hash="${short_revision}"
build_time="${date}"
build_info="${user}@${hostname}"

java_cmd=
if [[ (-n "$JAVA_HOME") && (-x "$JAVA_HOME/bin/java") ]]; then
    java_cmd="$JAVA_HOME/bin/java"
else
    echo "JAVA_HOME is not set, or java bin is not found"
    exit -1
fi

java_version_str=`$java_cmd -fullversion 2>&1`
java_version_str=$(echo $java_version_str | sed -e 's/"/\\"/g')

echo "get java cmd: $java_cmd"
echo "get java version: $java_version_str"

VERSION_PACKAGE="${DORIS_HOME}/fe/fe-core/target/generated-sources/build/org/apache/doris/common"
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
// To change this file, see gensrc/script/gen_build_version.sh
// the file should be placed in src/java/org/apache/doris/common/Version.java

public class Version {

  public static final String DORIS_BUILD_VERSION = "${build_version}";
  public static final String DORIS_BUILD_HASH = "${build_hash}";
  public static final String DORIS_BUILD_TIME = "${build_time}";
  public static final String DORIS_BUILD_INFO = "${build_info}";
  public static final String DORIS_JAVA_COMPILE_VERSION = "${java_version_str}";

  public static void main(String[] args) {
    System.out.println("doris_build_version: " + DORIS_BUILD_VERSION);
    System.out.println("doris_build_hash: " + DORIS_BUILD_HASH);
    System.out.println("doris_build_time: " + DORIS_BUILD_TIME);
    System.out.println("doris_build_info: " + DORIS_BUILD_INFO);
    System.out.println("doris_java_compile_version: " + DORIS_JAVA_COMPILE_VERSION);
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
// To change this file, see gensrc/script/gen_build_version.sh
// the file should be placed in gensrc/build/gen_cpp/version.h

#ifndef DORIS_GEN_CPP_VERSION_H
#define DORIS_GEN_CPP_VERSION_H

namespace doris {

#define DORIS_BUILD_VERSION    "${build_version}"
#define DORIS_BUILD_HASH       "${build_hash}"
#define DORIS_BUILD_SHORT_HASH "${build_short_hash}"
#define DORIS_BUILD_TIME       "${build_time}"
#define DORIS_BUILD_INFO       "${build_info}"

} // namespace doris

#endif
EOF
