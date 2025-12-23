#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Determine the current working directory
_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Preserve the calling directory
_CALLING_DIR="$(pwd)"
# Options used during compilation
_COMPILE_JVM_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=1g"

# Installs any application tarball given a URL, the expected tarball name,
# and, optionally, a checkable binary path to determine if the binary has
# already been installed
## Arg1 - URL
## Arg2 - Tarball Name
## Arg3 - Checkable Binary
install_app() {
  local remote_tarball="$1/$2"
  local local_tarball="${_DIR}/build/$2"
  local binary="${_DIR}/build/$3"

  local curl_opts="--silent --show-error -L"
  local wget_opts="--no-verbose"

  if [ -z "$3" -o ! -f "$binary" ]; then
    # check if we already have the tarball
    # check if we have curl installed
    # download application
    [ ! -f "${local_tarball}" ] && [ $(command -v curl) ] && \
      echo "exec: curl ${curl_opts} ${remote_tarball}" 1>&2 && \
      curl ${curl_opts} "${remote_tarball}" > "${local_tarball}"
    # if the file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${local_tarball}" ] && [ $(command -v wget) ] && \
      echo "exec: wget ${wget_opts} ${remote_tarball}" 1>&2 && \
      wget ${wget_opts} -O "${local_tarball}" "${remote_tarball}"
    # if both were unsuccessful, exit
    [ ! -f "${local_tarball}" ] && \
      echo -n "ERROR: Cannot download $2 with cURL or wget; " && \
      echo "please install manually and try again." && \
      exit 2
    cd "${_DIR}/build" && tar -xzf "$2"
  fi
}

# See simple version normalization: http://stackoverflow.com/questions/16989598/bash-comparing-version-numbers
function version { echo "$@" | awk -F. '{ printf("%03d%03d%03d\n", $1,$2,$3); }'; }

# Determine the Maven version from the root pom.xml file and
# install maven under the build/ folder if needed.
install_mvn() {
  local MVN_VERSION=`grep "<maven.version>" "${_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  MVN_BIN="$(command -v mvn)"
  if [ "$MVN_BIN" ]; then
    local MVN_DETECTED_VERSION="$(mvn --version | head -n1 | awk '{print $3}')"
  fi
  if [ $(version $MVN_DETECTED_VERSION) -lt $(version $MVN_VERSION) ]; then
    local APACHE_MIRROR=${APACHE_MIRROR:-'https://www.apache.org/dyn/closer.lua?action=download&filename='}
    if [ $(command -v curl) ]; then
      local TEST_MIRROR_URL="${APACHE_MIRROR}/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz"
      if ! curl -L --output /dev/null --silent --head --fail "$TEST_MIRROR_URL" ; then
        # Fall back to archive.apache.org for older Maven
        echo "Falling back to archive.apache.org to download Maven"
        APACHE_MIRROR="https://archive.apache.org/dist"
      fi
    fi

    mkdir -p build
    install_app \
      "${APACHE_MIRROR}/maven/maven-3/${MVN_VERSION}/binaries" \
      "apache-maven-${MVN_VERSION}-bin.tar.gz" \
      "build/apache-maven-${MVN_VERSION}/bin/mvn"

    MVN_BIN="${_DIR}/build/apache-maven-${MVN_VERSION}/bin/mvn"
  fi
}

install_mvn

# Reset the current working directory
cd "${_CALLING_DIR}"

# Set any `mvn` options if not already present
export MAVEN_OPTS=${MAVEN_OPTS:-"$_COMPILE_JVM_OPTS"}

echo "Using \`mvn\` from path: $MVN_BIN" 1>&2
"${MVN_BIN}" "$@"
