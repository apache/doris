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

# check DORIS_HOME
export LC_ALL=C

if [[ -z "${DORIS_HOME}" ]]; then
    echo "Error: DORIS_HOME is not set"
    exit 1
fi

# check OS type
if [[ -n "${OSTYPE}" ]]; then
    if [[ "${OSTYPE}" != "linux-gnu" ]] && [[ "${OSTYPE:0:6}" != "darwin" ]]; then
        echo "Error: Unsupported OS type: ${OSTYPE}"
        exit 1
    fi
fi

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if ! command -v brew &>/dev/null; then
        echo "Error: Homebrew is missing. Please install it first due to we use Homebrew to manage the tools which are needed to build the project."
        exit 1
    fi

    cat >"${DORIS_HOME}/custom_env_mac.sh" <<EOF
# This file is generated automatically. PLEASE DO NOT MODIFY IT.

HOMEBREW_REPO_PREFIX="$(brew --prefix)"
CELLARS=(
    automake
    autoconf
    libtool
    pkg-config
    texinfo
    coreutils
    gnu-getopt
    python@3
    cmake
    ninja
    ccache
    bison
    byacc
    gettext
    wget
    pcre
    maven
    llvm@16
    m4
)
for cellar in "\${CELLARS[@]}"; do
    EXPORT_CELLARS="\${HOMEBREW_REPO_PREFIX}/opt/\${cellar}/bin:\${EXPORT_CELLARS}"
done
export PATH="\${EXPORT_CELLARS}:/usr/bin:\${PATH}"

export DORIS_BUILD_PYTHON_VERSION='python3'

export NODE_OPTIONS='--openssl-legacy-provider'
EOF

    DORIS_HOME_ABSOLUATE_PATH="$(
        set -e
        cd "${DORIS_HOME}"
        pwd
    )"
    SOURCE_MAC_ENV_CONTENT="source '${DORIS_HOME_ABSOLUATE_PATH}/custom_env_mac.sh'"
    if [[ ! -f "${DORIS_HOME}/custom_env.sh" ]] ||
        ! grep "${SOURCE_MAC_ENV_CONTENT}" "${DORIS_HOME}/custom_env.sh" &>/dev/null; then
        echo "${SOURCE_MAC_ENV_CONTENT}" >>"${DORIS_HOME}/custom_env.sh"
    fi
fi

# include custom environment variables
if [[ -f "${DORIS_HOME}/custom_env.sh" ]]; then
    # shellcheck disable=1091
    . "${DORIS_HOME}/custom_env.sh"
fi

# set DORIS_THIRDPARTY
if [[ -z "${DORIS_THIRDPARTY}" ]]; then
    export DORIS_THIRDPARTY="${DORIS_HOME}/thirdparty"
fi

# check python
if [[ -z "${DORIS_BUILD_PYTHON_VERSION}" ]]; then
    DORIS_BUILD_PYTHON_VERSION="python"
fi

export PYTHON="${DORIS_BUILD_PYTHON_VERSION}"

if ! ${PYTHON} --version; then
    echo "Error: ${PYTHON} is not found, maybe you should set DORIS_BUILD_PYTHON_VERSION."
    exit 1
fi

if [[ -z "${DORIS_TOOLCHAIN}" ]]; then
    if [[ "$(uname -s)" == 'Darwin' ]]; then
        DORIS_TOOLCHAIN=clang
    else
        DORIS_TOOLCHAIN=clang
    fi
fi

if [[ "${DORIS_TOOLCHAIN}" == "gcc" ]]; then
    # set GCC HOME
    if [[ -z "${DORIS_GCC_HOME}" ]]; then
        DORIS_GCC_HOME="$(dirname "$(command -v gcc)")"/..
        export DORIS_GCC_HOME
    fi

    export CC="${DORIS_GCC_HOME}/bin/gcc"
    export CXX="${DORIS_GCC_HOME}/bin/g++"
    if test -x "${DORIS_GCC_HOME}/bin/ld"; then
        export DORIS_BIN_UTILS="${DORIS_GCC_HOME}/bin/"
    fi
    ENABLE_PCH='OFF'
elif [[ "${DORIS_TOOLCHAIN}" == "clang" ]]; then
    # set CLANG HOME
    if [[ -z "${DORIS_CLANG_HOME}" ]]; then
        DORIS_CLANG_HOME="$(dirname "$(command -v clang)")"/..
        export DORIS_CLANG_HOME
    fi

    export CC="${DORIS_CLANG_HOME}/bin/clang"
    export CXX="${DORIS_CLANG_HOME}/bin/clang++"
    if test -x "${DORIS_CLANG_HOME}/bin/ld.lld"; then
        export DORIS_BIN_UTILS="${DORIS_CLANG_HOME}/bin/"
    fi
    if [[ -f "${DORIS_CLANG_HOME}/bin/llvm-symbolizer" ]]; then
        export ASAN_SYMBOLIZER_PATH="${DORIS_CLANG_HOME}/bin/llvm-symbolizer"
    fi

    covs=()
    while IFS='' read -r line; do covs+=("${line}"); done <<<"$(find "${DORIS_CLANG_HOME}" -name "llvm-cov*")"
    if [[ ${#covs[@]} -ge 1 ]]; then
        LLVM_COV="${covs[0]}"
    else
        LLVM_COV="$(command -v llvm-cov)"
    fi
    export LLVM_COV

    profdatas=()
    while IFS='' read -r line; do profdatas+=("${line}"); done <<<"$(find "${DORIS_CLANG_HOME}" -name "llvm-profdata*")"
    if [[ ${#profdatas[@]} -ge 1 ]]; then
        LLVM_PROFDATA="${profdatas[0]}"
    else
        LLVM_PROFDATA="$(command -v llvm-profdata)"
    fi
    export LLVM_PROFDATA

    if [[ -z "${ENABLE_PCH}" ]]; then
        ENABLE_PCH='ON'
    fi
else
    echo "Error: unknown DORIS_TOOLCHAIN=${DORIS_TOOLCHAIN}, currently only 'gcc' and 'clang' are supported"
    exit 1
fi

export CCACHE_COMPILERCHECK=content
if [[ "${ENABLE_PCH}" == "ON" ]]; then
    export CCACHE_PCH_EXTSUM=true
    export CCACHE_SLOPPINESS="pch_defines,time_macros"
else
    export CCACHE_NOPCH_EXTSUM=true
    export CCACHE_SLOPPINESS="default"
fi

if [[ -z "${DORIS_BIN_UTILS}" ]]; then
    export DORIS_BIN_UTILS='/usr/bin/'
fi

if [[ -z "${DORIS_GCC_HOME}" ]]; then
    DORIS_GCC_HOME="$(dirname "$(command -v gcc)")/.."
    export DORIS_GCC_HOME
fi

if [[ ! -f "${DORIS_GCC_HOME}/bin/gcc" ]]; then
    echo "Error: wrong directory DORIS_GCC_HOME=${DORIS_GCC_HOME}"
    exit 1
fi

# export CLANG COMPATIBLE FLAGS
CLANG_COMPATIBLE_FLAGS="$(echo | "${DORIS_GCC_HOME}/bin/gcc" -Wp,-v -xc++ - -fsyntax-only 2>&1 |
    grep -E '^\s+/' | awk '{print "-I" $1}' | tr '\n' ' ')"
export CLANG_COMPATIBLE_FLAGS

# get jdk version, return version as an Integer.
# 1.8 => 8, 13.0 => 13
function jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'

    if [[ -z "${java_cmd}" ]]; then
        result=no_java
        return 1
    else
        local version
        # remove \r for Cygwin
        version="$("${java_cmd}" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n' | grep version | awk '{print $3}')"
        version="${version//\"/}"
        if [[ "${version}" =~ ^1\. ]]; then
            result="$(echo "${version}" | awk -F '.' '{print $2}')"
        else
            result="$(echo "${version}" | awk -F '.' '{print $1}')"
        fi
    fi
    echo "${result}"
    return 0
}

# check java version
# must use jdk_17
function check_jdk_version() {
    if [[ ! -x "${JAVA}" ]]; then
        echo "The JAVA_HOME environment variable is not defined correctly"
        echo "This environment variable is needed to run this program"
        echo "NB: JAVA_HOME should point to a JDK not a JRE"
        exit 1
    fi

    echo "Check JAVA version"
    java_version="$(
        set -e
        jdk_version "${JAVA}"
    )"
    if [[ "${java_version}" -ne 17 ]]; then
        echo "ERROR: The JAVA version is ${java_version}, it must be JDK-17."
        exit 1
    fi
    return 0
}

# if is called from build-thirdparty.sh, no need to check these tools
if test -z "${DO_NOT_CHECK_JAVA_ENV:-}"; then
    # register keyword is forbidden to use in C++17
    # the C++ code generated by flex that remove register keyword after version 2.6.0
    # so we need check flex version here to avoid compilation failed
    flex_ver="$(flex --version | awk '{print $2}')"
    required_ver="2.6.0"
    if [[ ! "$(printf '%s\n' "${required_ver}" "${flex_ver}" | sort -V | head -n1)" = "${required_ver}" ]]; then
        echo "Error: flex version (${flex_ver}) must be greater than or equal to ${required_ver}"
        exit 1
    fi

    # check java home
    if [[ -z "${JAVA_HOME}" ]]; then
        if [[ -n "${JDK_17}" ]]; then
            echo "Use JDK_17 = ${JDK_17}"
            JAVA="${JDK_17}/bin/java"
            JAVAP="${JDK_17}/bin/javap"
            export JAVA_HOME="${JDK_17}"
        else
            JAVA="$(command -v java)"
            JAVAP="$(command -v javap)"
        fi
        check_jdk_version
    else
        JAVA="${JAVA_HOME}/bin/java"
        JAVAP="${JAVA_HOME}/bin/javap"

        echo "Check JAVA_HOME version"
        java_version="$(
            set -e
            jdk_version "${JAVA}"
        )"
        if [[ "${java_version}" -ne 17 ]]; then
            echo "JAVA_HOME=${JAVA_HOME}. It does not point to JDK-17."
            if [[ -n "${JDK_17}" ]]; then
                echo "Use JDK_17=${JDK_17}."
                JAVA="${JDK_17}/bin/java"
                JAVAP="${JDK_17}/bin/javap"
                export JAVA_HOME="${JDK_17}"
                check_jdk_version
            else
                echo "The 'JDK_17' environment variable is not set."
                echo "ERROR: The JAVA version is ${java_version}, it must be JDK-17."
                exit 1
            fi
        fi
    fi
    export JAVA

    JAVA_VER="$("${JAVAP}" -verbose java.lang.String | grep "major version" | cut -d " " -f5)"
    if [[ "${JAVA_VER}" -lt 52 ]]; then
        echo "Error: require JAVA with JDK version at least 1.8"
        exit 1
    fi

    # check maven
    MVN_CMD='mvn'
    if [[ -n "${CUSTOM_MVN}" ]]; then
        MVN_CMD="${CUSTOM_MVN}"
    fi
    if ! "${MVN_CMD}" --version; then
        echo "Error: mvn is not found"
        exit 1
    fi
    export MVN_CMD
fi

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if ! command -v libtoolize &>/dev/null && command -v glibtoolize &>/dev/null; then
        shopt -s expand_aliases
        alias libtoolize='glibtoolize'
    fi
fi

CMAKE_CMD='cmake'
if [[ -n "${CUSTOM_CMAKE}" ]]; then
    CMAKE_CMD="${CUSTOM_CMAKE}"
fi
if ! "${CMAKE_CMD}" --version; then
    echo "Error: cmake is not found"
    exit 1
fi
export CMAKE_CMD

GENERATOR="Unix Makefiles"
BUILD_SYSTEM="make"
if NINJA_VERSION="$(ninja --version 2>/dev/null)"; then
    echo "ninja ${NINJA_VERSION}"
    GENERATOR="Ninja"
    BUILD_SYSTEM="ninja"
fi

if CCACHE_VERSION="$(ccache --version 2>/dev/null)"; then
    echo "${CCACHE_VERSION}" | head -n 1
    # shellcheck disable=2034
    CMAKE_USE_CCACHE="-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
fi

export GENERATOR
export BUILD_SYSTEM

export PKG_CONFIG_PATH="${DORIS_HOME}/thirdparty/installed/lib64/pkgconfig:${PKG_CONFIG_PATH}"
