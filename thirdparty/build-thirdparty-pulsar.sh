#!/usr/bin/env bash
# shellcheck disable=2034

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

#################################################################################
# This script will
# 1. Check prerequisite libraries. Including:
#    cmake byacc flex automake libtool binutils-dev libiberty-dev bison
# 2. Compile and install all thirdparties which are downloaded
#    using *download-thirdparty.sh*.
#
# This script will run *download-thirdparty.sh* once again
# to check if all thirdparties have been downloaded, unpacked and patched.
#################################################################################

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${curdir}/.."
export TP_DIR="${curdir}"

# include custom environment variables
if [[ -f "${DORIS_HOME}/env.sh" ]]; then
    export BUILD_THIRDPARTY_WIP=1
    . "${DORIS_HOME}/env.sh"
    export BUILD_THIRDPARTY_WIP=
fi

# Check args
usage() {
    echo "
Usage: $0 [options...] [packages...]
  Optional options:
     -j <num>               build thirdparty parallel
     --clean                clean the extracted data
     --continue <package>   continue to build the remaining packages (starts from the specified package)
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o 'hj:' \
    -l 'help,clean,continue:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

KERNEL="$(uname -s)"

if [[ "${KERNEL}" == 'Darwin' ]]; then
    PARALLEL="$(($(sysctl -n hw.logicalcpu) / 4 + 1))"
else
    PARALLEL="$(($(nproc) / 4 + 1))"
fi

while true; do
    case "$1" in
    -j)
        PARALLEL="$2"
        shift 2
        ;;
    -h)
        HELP=1
        shift
        ;;
    --help)
        HELP=1
        shift
        ;;
    --clean)
        CLEAN=1
        shift
        ;;
    --continue)
        CONTINUE=1
        start_package="${2}"
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${CONTINUE}" -eq 1 ]]; then
    if [[ -z "${start_package}" ]] || [[ "${#}" -ne 0 ]]; then
        usage
    fi
fi

read -r -a packages <<<"${@}"

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
    PACKAGES            -- ${packages[*]}
    CONTINUE            -- ${start_package}
"

if [[ ! -f "${TP_DIR}/download-thirdparty.sh" ]]; then
    echo "Download thirdparty script is missing".
    exit 1
fi

if [[ ! -f "${TP_DIR}/vars.sh" ]]; then
    echo "vars.sh is missing".
    exit 1
fi

. "${TP_DIR}/vars.sh"

cd "${TP_DIR}"

if [[ "${CLEAN}" -eq 1 ]] && [[ -d "${TP_SOURCE_DIR}" ]]; then
    echo 'Clean the extracted data ...'
    find "${TP_SOURCE_DIR}" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} \;
    echo 'Success!'
fi

# Download thirdparties.
"${TP_DIR}/download-thirdparty.sh"

export LD_LIBRARY_PATH="${TP_DIR}/installed/lib:${LD_LIBRARY_PATH}"

# toolchain specific warning options and settings
if [[ "${CC}" == *gcc ]]; then
    warning_uninitialized='-Wno-maybe-uninitialized'
    warning_stringop_truncation='-Wno-stringop-truncation'
    warning_class_memaccess='-Wno-class-memaccess'
    warning_array_parameter='-Wno-array-parameter'
    warning_narrowing='-Wno-narrowing'
    warning_dangling_reference='-Wno-dangling-reference'
    boost_toolset='gcc'
elif [[ "${CC}" == *clang ]]; then
    warning_uninitialized='-Wno-uninitialized'
    warning_shadow='-Wno-shadow'
    warning_dangling_gsl='-Wno-dangling-gsl'
    warning_unused_but_set_variable='-Wno-unused-but-set-variable'
    warning_defaulted_function_deleted='-Wno-defaulted-function-deleted'
    warning_reserved_identifier='-Wno-reserved-identifier'
    warning_suggest_override='-Wno-suggest-override -Wno-suggest-destructor-override'
    warning_option_ignored='-Wno-option-ignored'
    warning_narrowing='-Wno-c++11-narrowing'
    boost_toolset='clang'
    libhdfs_cxx17='-std=c++1z'

    test_warning_result="$("${CC}" -xc++ "${warning_unused_but_set_variable}" /dev/null 2>&1 || true)"
    if echo "${test_warning_result}" | grep 'unknown warning option' >/dev/null; then
        warning_unused_but_set_variable=''
    fi
fi

# prepare installed prefix
mkdir -p "${TP_DIR}/installed/lib64"
pushd "${TP_DIR}/installed"/
ln -sf lib64 lib
popd

# Configure the search paths for pkg-config and cmake
export PKG_CONFIG_PATH="${TP_DIR}/installed/lib64/pkgconfig"
export CMAKE_PREFIX_PATH="${TP_DIR}/installed"

echo "PKG_CONFIG_PATH: ${PKG_CONFIG_PATH}"
echo "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}"

check_prerequest() {
    local CMD="$1"
    local NAME="$2"
    if ! eval "${CMD}"; then
        echo "${NAME} is missing"
        exit 1
    else
        echo "${NAME} is found"
    fi
}

# sudo apt-get install cmake
# sudo yum install cmake
check_prerequest "${CMAKE_CMD} --version" "cmake"

# sudo apt-get install byacc
# sudo yum install byacc
check_prerequest "byacc -V" "byacc"

# sudo apt-get install flex
# sudo yum install flex
check_prerequest "flex -V" "flex"

# sudo apt-get install automake
# sudo yum install automake
check_prerequest "automake --version" "automake"

# sudo apt-get install libtool
# sudo yum install libtool
check_prerequest "libtoolize --version" "libtool"

# aclocal_version should equal to automake_version
aclocal_version=$(aclocal --version | sed -n '1p' | awk 'NF>1{print $NF}')
automake_version=$(automake --version | sed -n '1p' | awk 'NF>1{print $NF}')
if [[ "${aclocal_version}" != "${automake_version}" ]]; then
    echo "Error: aclocal version(${aclocal_version}) is not equal to automake version(${automake_version})."
    exit 1
fi

# sudo apt-get install binutils-dev
# sudo yum install binutils-devel
#check_prerequest "locate libbfd.a" "binutils-dev"

# sudo apt-get install libiberty-dev
# no need in centos 7.1
#check_prerequest "locate libiberty.a" "libiberty-dev"

# sudo apt-get install bison
# sudo yum install bison
# necessary only when compiling be
#check_prerequest "bison --version" "bison"

#########################
# build all thirdparties
#########################

# Name of cmake build directory in each thirdpary project.
# Do not use `build`, because many projects contained a file named `BUILD`
# and if the filesystem is not case sensitive, `mkdir` will fail.
BUILD_DIR=doris_build

check_if_source_exist() {
    if [[ -z $1 ]]; then
        echo "dir should specified to check if exist."
        exit 1
    fi

    if [[ ! -d "${TP_SOURCE_DIR}/$1" ]]; then
        echo "${TP_SOURCE_DIR}/$1 does not exist."
        exit 1
    fi
    echo "===== begin build $1"
}

check_if_archive_exist() {
    if [[ -z $1 ]]; then
        echo "archive should specified to check if exist."
        exit 1
    fi

    if [[ ! -f "${TP_SOURCE_DIR}/$1" ]]; then
        echo "${TP_SOURCE_DIR}/$1 does not exist."
        exit 1
    fi
}

remove_all_dylib() {
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        find "${TP_INSTALL_DIR}/lib64" -name "*.dylib" -delete
    fi
}

if [[ -z "${STRIP_TP_LIB}" ]]; then
    if [[ "${KERNEL}" != 'Darwin' ]]; then
        STRIP_TP_LIB='ON'
    else
        STRIP_TP_LIB='OFF'
    fi
fi

if [[ "${STRIP_TP_LIB}" = "ON" ]]; then
    echo "Strip thirdparty libraries"
else
    echo "Do not strip thirdparty libraries"
fi

strip_lib() {
    if [[ "${STRIP_TP_LIB}" = "ON" ]]; then
        if [[ -z $1 ]]; then
            echo "Must specify the library to be stripped."
            exit 1
        fi
        if [[ ! -f "${TP_LIB_DIR}/$1" ]]; then
            echo "Library to be stripped (${TP_LIB_DIR}/$1) does not exist."
            exit 1
        fi
        strip --strip-debug --strip-unneeded "${TP_LIB_DIR}/$1"
    fi
}

build_openssl() {
    MACHINE_TYPE="$(uname -m)"
    OPENSSL_PLATFORM="linux-x86_64"
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        OPENSSL_PLATFORM="darwin64-${MACHINE_TYPE}-cc"
    elif [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        OPENSSL_PLATFORM="linux-aarch64"
    fi

    check_if_source_exist "${OPENSSL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${OPENSSL_SOURCE}"

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        CXXFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        LIBDIR="lib" \
        ./Configure --prefix="${TP_INSTALL_DIR}" --with-rand-seed=devrandom -shared "${OPENSSL_PLATFORM}"
    # NOTE(amos): Never use '&&' to concat commands as it will eat error code
    # See https://mywiki.wooledge.org/BashFAQ/105 for more detail.
    make -j "${PARALLEL}"
    make install_sw
    # NOTE(zc): remove this dynamic library files to make libcurl static link.
    # If I don't remove this files, I don't known how to make libcurl link static library
    if [[ -f "${TP_INSTALL_DIR}/lib64/libcrypto.so" ]]; then
        rm -rf "${TP_INSTALL_DIR}"/lib64/libcrypto.so*
    fi
    if [[ -f "${TP_INSTALL_DIR}/lib64/libssl.so" ]]; then
        rm -rf "${TP_INSTALL_DIR}"/lib64/libssl.so*
    fi
    remove_all_dylib
}

# zlib
build_zlib() {
    check_if_source_exist "${ZLIB_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ZLIB_SOURCE}"

    CFLAGS="-O3 -fPIC" \
        CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}"

    make -j "${PARALLEL}"
    make install

    # minizip
    cd contrib/minizip
    autoreconf --force --install
    ./configure --prefix="${TP_INSTALL_DIR}" --enable-static=yes --enable-shared=no
    make -j "${PARALLEL}"
    make install
}

# zstd
build_zstd() {
    check_if_source_exist "${ZSTD_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ZSTD_SOURCE}/build/cmake"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DBUILD_TESTING=OFF -DZSTD_BUILD_TESTS=OFF -DZSTD_BUILD_STATIC=ON \
        -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_SHARED=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    strip_lib libzstd.a
}

# boost
build_boost() {
    check_if_source_exist "${BOOST_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BOOST_SOURCE}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        cxxflags='-static'
    else
        cxxflags=''
    fi

    CXXFLAGS="${cxxflags}" \
        ./bootstrap.sh --prefix="${TP_INSTALL_DIR}" --with-toolset="${boost_toolset}"
    # -q: Fail at first error
    ./b2 -q link=static runtime-link=static -j "${PARALLEL}" \
        --without-mpi --without-graph --without-graph_parallel --without-python \
        cxxflags="-std=c++17 -g -I${TP_INCLUDE_DIR} -L${TP_LIB_DIR}" install
}

# gtest
build_gtest() {
    check_if_source_exist "${GTEST_SOURCE}"

    cd "${TP_SOURCE_DIR}/${GTEST_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" ../ -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_POSITION_INDEPENDENT_CODE=On
    # -DCMAKE_CXX_FLAGS="$warning_uninitialized"

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    strip_lib libgtest.a
}

# protobuf
build_protobuf() {
    check_if_source_exist "${PROTOBUF_SOURCE}"
    cd "${TP_SOURCE_DIR}/${PROTOBUF_SOURCE}"

    if [[ "${KERNEL}" == 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR}"
    else
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc -Wl,--undefined=pthread_create"
    fi

    mkdir -p cmake/build
    cd cmake/build

    CXXFLAGS="-O2 -I${TP_INCLUDE_DIR}" \
        LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -Dprotobuf_USE_EXTERNAL_GTEST=ON \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -Dprotobuf_BUILD_SHARED_LIBS=OFF \
        -Dprotobuf_BUILD_TESTS=OFF \
        -Dprotobuf_WITH_ZLIB_DEFAULT=ON \
        -Dprotobuf_ABSL_PROVIDER=package \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ../..

    make -j "${PARALLEL}"
    make install
    strip_lib libprotobuf.a
    strip_lib libprotoc.a
}

# snappy
build_snappy() {
    check_if_source_exist "${SNAPPY_SOURCE}"
    cd "${TP_SOURCE_DIR}/${SNAPPY_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    CFLAGS="-O3" CXXFLAGS="-O3" "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_INSTALL_INCLUDEDIR="${TP_INCLUDE_DIR}"/snappy \
        -DSNAPPY_BUILD_TESTS=0 ../

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    #build for libarrow.a
    cp "${TP_INCLUDE_DIR}/snappy/snappy-c.h" "${TP_INCLUDE_DIR}/snappy-c.h"
    cp "${TP_INCLUDE_DIR}/snappy/snappy-sinksource.h" "${TP_INCLUDE_DIR}/snappy-sinksource.h"
    cp "${TP_INCLUDE_DIR}/snappy/snappy-stubs-public.h" "${TP_INCLUDE_DIR}/snappy-stubs-public.h"
    cp "${TP_INCLUDE_DIR}/snappy/snappy.h" "${TP_INCLUDE_DIR}/snappy.h"
}

# curl
build_curl() {
    check_if_source_exist "${CURL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CURL_SOURCE}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        libs='-lcrypto -lssl -lcrypto -ldl -static'
    else
        libs='-lcrypto -lssl -lcrypto -ldl'
    fi

    CPPFLAGS="-I${TP_INCLUDE_DIR} " \
        LDFLAGS="-L${TP_LIB_DIR}" LIBS="${libs}" \
        PKG_CONFIG="pkg-config --static" \
        ./configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static \
        --without-librtmp --with-ssl="${TP_INSTALL_DIR}" --without-libidn2 --disable-ldap --enable-ipv6 \
        --without-libssh2 --without-brotli --without-nghttp2

    make curl_LDFLAGS=-all-static -j "${PARALLEL}"
    make curl_LDFLAGS=-all-static install
    strip_lib libcurl.a
}

# pulsar
build_pulsar() {
    check_if_source_exist "${PULSAR_SOURCE}"

    cd "${TP_SOURCE_DIR}"/"${PULSAR_SOURCE}"

    "${CMAKE_CMD}" -DCMAKE_LIBRARY_PATH="${TP_INSTALL_DIR}"/lib -DCMAKE_INCLUDE_PATH="${TP_INSTALL_DIR}"/include \
        -DPROTOC_PATH="${TP_INSTALL_DIR}"/bin/protoc -DBUILD_TESTS=OFF -DBUILD_PYTHON_WRAPPER=OFF -DBUILD_DYNAMIC_LIB=OFF .
    make -j "${PARALLEL}"

    cp lib/libpulsar.a "${TP_INSTALL_DIR}"/lib/
    cp -r include/pulsar "${TP_INSTALL_DIR}"/include/
}

if [[ "${#packages[@]}" -eq 0 ]]; then
    packages=(
        openssl
        zlib
        zstd
        boost # must before thrift
        gtest
        protobuf # after gtest
        snappy
        curl
        pulsar
    )
fi

for package in "${packages[@]}"; do
    if [[ "${package}" == "${start_package}" ]]; then
        PACKAGE_FOUND=1
    fi
    if [[ "${CONTINUE}" -eq 0 ]] || [[ "${PACKAGE_FOUND}" -eq 1 ]]; then
        command="build_${package}"
        ${command}
    fi
done

echo "Finished to build all thirdparties"
