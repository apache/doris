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
# This script run the clang-format to check and fix
# cplusplus source files.
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DORIS_HOME=$(
    cd "${ROOT}/.."
    pwd
)
export DORIS_HOME

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if ! command -v brew &>/dev/null; then
        echo "Error: Homebrew is missing. Please install it first due to we use Homebrew to manage the tools which are needed to build the project."
        exit 1
    fi
    if ! brew list llvm@16 > /dev/null 2>&1; then
        echo "Error: Please install llvm@16 firt due to we use it to format code."
        exit 1
    fi
    export PATH=$(brew --prefix llvm@16)/bin:$PATH
fi

if [[ -z $(command -v clang-format) ]]; then
    echo "clang-format not found, please install clang-format"
    exit 1
fi

CLANG_FORMAT_VERSION=$(clang-format --version | perl -nle 'print $& if m{version \K[0-9]+}')
if [[ ${CLANG_FORMAT_VERSION} -ne 16 ]]; then
    echo "clang-format version is not 16, please install clang-format version 16 or upgrade your clang-format version to 16"
    exit 1
fi

CLANG_FORMAT="${CLANG_FORMAT_BINARY:=$(command -v clang-format)}"

python "${DORIS_HOME}/build-support/run_clang_format.py" "--clang-format-executable" "${CLANG_FORMAT}" "-r" "--style" "file" "--inplace" "true" "--extensions" "c,h,C,H,cpp,hpp,cc,hh,c++,h++,cxx,hxx" "--exclude" "none" "be/src be/test cloud/src cloud/test be/benchmark"
