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

#libbacktrace
build_libbacktrace() {
    check_if_source_exist "${LIBBACKTRACE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LIBBACKTRACE_SOURCE}"

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        CXXFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}"

    make -j "${PARALLEL}"
    make install
}

# libevent
build_libevent() {
    check_if_source_exist "${LIBEVENT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LIBEVENT_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    CFLAGS="-std=c99 -D_BSD_SOURCE -fno-omit-frame-pointer -g -ggdb -O2 -I${TP_INCLUDE_DIR}" \
        CPPLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DEVENT__DISABLE_TESTS=ON \
        -DEVENT__DISABLE_OPENSSL=ON -DEVENT__DISABLE_SAMPLES=ON -DEVENT__DISABLE_REGRESS=ON ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    remove_all_dylib
    strip_lib libevent.a
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

# thrift
build_thrift() {
    check_if_source_exist "${THRIFT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${THRIFT_SOURCE}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        cflags="-I${TP_INCLUDE_DIR}"
        cxxflags="-I${TP_INCLUDE_DIR} ${warning_unused_but_set_variable} -Wno-inconsistent-missing-override"
        ldflags="-L${TP_LIB_DIR} --static"
    else
        cflags="-I${TP_INCLUDE_DIR} -Wno-implicit-function-declaration -Wno-inconsistent-missing-override"
        cxxflags="-I${TP_INCLUDE_DIR} ${warning_unused_but_set_variable} -Wno-inconsistent-missing-override"
        ldflags="-L${TP_LIB_DIR}"
    fi

    # NOTE(amos): libtool discard -static. --static works.
    ./configure CFLAGS="${cflags}" CXXFLAGS="${cxxflags}" LDFLAGS="${ldflags}" LIBS="-lcrypto -ldl -lssl" \
        --prefix="${TP_INSTALL_DIR}" --docdir="${TP_INSTALL_DIR}/doc" --enable-static --disable-shared --disable-tests \
        --disable-tutorial --without-qt4 --without-qt5 --without-csharp --without-erlang --without-nodejs --without-nodets --without-swift \
        --without-lua --without-perl --without-php --without-php_extension --without-dart --without-ruby --without-cl \
        --without-haskell --without-go --without-haxe --without-d --without-python -without-java --without-dotnetcore -without-rs --with-cpp \
        --with-libevent="${TP_INSTALL_DIR}" --with-boost="${TP_INSTALL_DIR}" --with-openssl="${TP_INSTALL_DIR}"

    if [[ -f compiler/cpp/thrifty.hh ]]; then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi

    make -j "${PARALLEL}"
    make install
    strip_lib libthrift.a
    strip_lib libthriftnb.a
}

# protobuf
build_protobuf() {
    check_if_source_exist "${PROTOBUF_SOURCE}"
    cd "${TP_SOURCE_DIR}/${PROTOBUF_SOURCE}"
    rm -fr gmock

    # NOTE(amos): -Wl,--undefined=pthread_create force searching for pthread symbols.
    # See https://stackoverflow.com/a/65348893/1329147 for detailed explanation.
    mkdir gmock
    cd gmock
    tar xf "${TP_SOURCE_DIR}/${GTEST_NAME}"

    mv "${GTEST_SOURCE}" gtest

    cd "${TP_SOURCE_DIR}/${PROTOBUF_SOURCE}"

    ./autogen.sh

    if [[ "${KERNEL}" == 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR}"
    else
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc -Wl,--undefined=pthread_create"
    fi

    CXXFLAGS="-fPIC -O2 -I${TP_INCLUDE_DIR}" \
        LDFLAGS="${ldflags}" \
        ./configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static --with-zlib="${TP_INSTALL_DIR}/include"

    # ATTN: If protoc is not built fully statically the linktime libc may newer than runtime.
    #       This will casue protoc cannot run
    #       If you really need to dynamically link protoc, please set the environment variable DYN_LINK_PROTOC=1

    if [[ "${DYN_LINK_PROTOC:-0}" == "1" || "${KERNEL}" == 'Darwin' ]]; then
        echo "link protoc dynamiclly"
    else
        cd src
        sed -i 's/^AM_LDFLAGS\(.*\)$/AM_LDFLAGS\1 -all-static/' Makefile
        cd -
    fi

    make -j "${PARALLEL}"
    make install
    strip_lib libprotobuf.a
    strip_lib libprotoc.a
}

# gflags
build_gflags() {
    check_if_source_exist "${GFLAGS_SOURCE}"

    cd "${TP_SOURCE_DIR}/${GFLAGS_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=On ../

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# glog
build_glog() {
    check_if_source_exist "${GLOG_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GLOG_SOURCE}"

    # to generate config.guess and config.sub to support aarch64
    rm -rf config.*
    autoreconf -i

    CPPFLAGS="-I${TP_INCLUDE_DIR} -fpermissive -fPIC" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}" --enable-frame-pointers --disable-shared --enable-static

    make -j "${PARALLEL}"
    make install
    strip_lib libglog.a
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

# rapidjson
build_rapidjson() {
    check_if_source_exist "${RAPIDJSON_SOURCE}"
    cd "${TP_SOURCE_DIR}/${RAPIDJSON_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    "${CMAKE_CMD}" ../ -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DRAPIDJSON_BUILD_DOC=OFF \
        -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_TESTS=OFF

    make -j "${PARALLEL}"
    make install
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

# gperftools
build_gperftools() {
    check_if_source_exist "${GPERFTOOLS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GPERFTOOLS_SOURCE}"
    if [[ ! -f configure ]]; then
        ./autogen.sh
    fi

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        LD_LIBRARY_PATH="${TP_LIB_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        LD_LIBRARY_PATH="${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}/gperftools" --disable-shared --enable-static --disable-libunwind --with-pic --enable-frame-pointers

    make -j "${PARALLEL}"
    make install
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

# lz4
build_lz4() {
    check_if_source_exist "${LZ4_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LZ4_SOURCE}"

    # clean old symbolic links
    local old_symbolic_links=('lz4c' 'lz4cat' 'unlz4')
    for link in "${old_symbolic_links[@]}"; do
        rm -f "${TP_INSTALL_DIR}/bin/${link}"
    done

    make -j "${PARALLEL}" install PREFIX="${TP_INSTALL_DIR}" BUILD_SHARED=no INCLUDEDIR="${TP_INCLUDE_DIR}/lz4"
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

# bzip
build_bzip() {
    check_if_source_exist "${BZIP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BZIP_SOURCE}"

    make -j "${PARALLEL}" install PREFIX="${TP_INSTALL_DIR}"
}

# lzo2
build_lzo2() {
    check_if_source_exist "${LZO2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LZO2_SOURCE}"

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static

    make -j "${PARALLEL}"
    make install
    strip_lib liblzo2.a
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

    CPPFLAGS="-I${TP_INCLUDE_DIR} -DNGHTTP2_STATICLIB" \
        LDFLAGS="-L${TP_LIB_DIR}" LIBS="${libs}" \
        PKG_CONFIG="pkg-config --static" \
        ./configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static \
        --without-librtmp --with-ssl="${TP_INSTALL_DIR}" --without-libidn2 --disable-ldap --enable-ipv6 \
        --without-libssh2 --without-brotli

    make curl_LDFLAGS=-all-static -j "${PARALLEL}"
    make curl_LDFLAGS=-all-static install
    strip_lib libcurl.a
}

# re2
build_re2() {
    check_if_source_exist "${RE2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${RE2_SOURCE}"

    "${CMAKE_CMD}" -DCMAKE_BUILD_TYPE=Release -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}"
    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    strip_lib libre2.a
}

# hyperscan
build_hyperscan() {
    check_if_source_exist "${RAGEL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${RAGEL_SOURCE}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        cxxflags='-static'
    else
        cxxflags=''
    fi

    CXXFLAGS="${cxxflags}" \
        ./configure --prefix="${TP_INSTALL_DIR}"
    make install

    check_if_source_exist "${HYPERSCAN_SOURCE}"
    cd "${TP_SOURCE_DIR}/${HYPERSCAN_SOURCE}"

    # We don't need to build tools/hsbench which depends on sqlite3 installed.
    rm -rf "${TP_SOURCE_DIR}/${HYPERSCAN_SOURCE}/tools/hsbench"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    CXXFLAGS="-D_HAS_AUTO_PTR_ETC=0" \
        "${CMAKE_CMD}" -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DBOOST_ROOT="${TP_INSTALL_DIR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DBUILD_EXAMPLES=OFF ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    strip_lib libhs.a
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

# mysql
build_mysql() {
    check_if_source_exist "${MYSQL_SOURCE}"
    check_if_source_exist "${BOOST_SOURCE}"

    cd "${TP_SOURCE_DIR}/${MYSQL_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    if [[ ! -d "${BOOST_SOURCE}" ]]; then
        cp -rf "${TP_SOURCE_DIR}/${BOOST_SOURCE}" ./
    fi

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        cflags='-static -pthread -lrt'
        cxxflags='-static -pthread -lrt'
    else
        cflags='-pthread'
        cxxflags='-pthread'
    fi

    CFLAGS="${cflags}" CXXFLAGS="${cxxflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" ../ -DCMAKE_LINK_SEARCH_END_STATIC=1 \
        -DWITH_BOOST="$(pwd)/${BOOST_SOURCE}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}/mysql" \
        -DWITHOUT_SERVER=1 -DWITH_ZLIB=1 -DZLIB_ROOT="${TP_INSTALL_DIR}" \
        -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O3 -g -fabi-version=2 -fno-omit-frame-pointer -fno-strict-aliasing -std=gnu++11" \
        -DDISABLE_SHARED=1 -DBUILD_SHARED_LIBS=0 -DZLIB_LIBRARY="${TP_INSTALL_DIR}/lib/libz.a" -DENABLE_DTRACE=0
    "${BUILD_SYSTEM}" -j "${PARALLEL}" mysqlclient

    # copy headers manually
    rm -rf ../../../installed/include/mysql/
    mkdir ../../../installed/include/mysql/ -p
    cp -R ./include/* ../../../installed/include/mysql/
    cp -R ../include/* ../../../installed/include/mysql/
    cp ../libbinlogevents/export/binary_log_types.h ../../../installed/include/mysql/
    echo "mysql headers are installed."

    # copy libmysqlclient.a
    cp libmysql/libmysqlclient.a ../../../installed/lib/
    echo "mysql client lib is installed."
    strip_lib libmysqlclient.a
}

#leveldb
build_leveldb() {
    check_if_source_exist "${LEVELDB_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LEVELDB_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    CXXFLAGS="-fPIC" "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DLEVELDB_BUILD_BENCHMARKS=OFF \
        -DLEVELDB_BUILD_TESTS=OFF ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    strip_lib libleveldb.a
}

# brpc
build_brpc() {
    check_if_source_exist "${BRPC_SOURCE}"

    cd "${TP_SOURCE_DIR}/${BRPC_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"
    else
        ldflags="-L${TP_LIB_DIR}"

        # Don't set OPENSSL_ROOT_DIR
        sed '/set(OPENSSL_ROOT_DIR/,/)/ d' ../CMakeLists.txt >../CMakeLists.txt.bak
        mv ../CMakeLists.txt.bak ../CMakeLists.txt
    fi

    # Currently, BRPC can't be built for static libraries only (without .so). Therefore, we should add `-fPIC`
    # to the dependencies which are required by BRPC. Dependencies: zlib, glog, protobuf, leveldb
    LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" -DBUILD_SHARED_LIBS=1 -DWITH_GLOG=ON -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_LIBRARY_PATH="${TP_INSTALL_DIR}/lib64" -DCMAKE_INCLUDE_PATH="${TP_INSTALL_DIR}/include" \
        -DBUILD_BRPC_TOOLS=OFF \
        -DPROTOBUF_PROTOC_EXECUTABLE="${TP_INSTALL_DIR}/bin/protoc" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    remove_all_dylib
    strip_lib libbrpc.a
}

# rocksdb
build_rocksdb() {
    check_if_source_exist "${ROCKSDB_SOURCE}"

    cd "${TP_SOURCE_DIR}/${ROCKSDB_SOURCE}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags='-static-libstdc++ -static-libgcc'
    else
        if [[ "$(uname -m)" != 'x86_64' ]]; then
            ldflags=''
        else
            ldflags="-L${TP_LIB_DIR} -ljemalloc_doris"
        fi
    fi

    # -Wno-range-loop-construct gcc-11
    CFLAGS="-I ${TP_INCLUDE_DIR} -I ${TP_INCLUDE_DIR}/snappy -I ${TP_INCLUDE_DIR}/lz4" \
        CXXFLAGS="-include cstdint -Wno-deprecated-copy ${warning_stringop_truncation} ${warning_shadow} ${warning_dangling_gsl} \
    ${warning_defaulted_function_deleted} ${warning_unused_but_set_variable} -Wno-pessimizing-move -Wno-range-loop-construct" \
        LDFLAGS="${ldflags}" \
        PORTABLE=1 make USE_RTTI=1 -j "${PARALLEL}" static_lib
    cp librocksdb.a ../../installed/lib/librocksdb.a
    cp -r include/rocksdb ../../installed/include/
    strip_lib librocksdb.a
}

# cyrus_sasl
build_cyrus_sasl() {
    check_if_source_exist "${CYRUS_SASL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CYRUS_SASL_SOURCE}"

    CFLAGS="-fPIC -Wno-implicit-function-declaration" \
        CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        LIBS="-lcrypto" \
        ./configure --prefix="${TP_INSTALL_DIR}" --enable-static --enable-shared=no --with-openssl="${TP_INSTALL_DIR}" --with-pic --enable-gssapi="${TP_INSTALL_DIR}" --with-gss_impl=mit --with-dblib=none

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        make -j "${PARALLEL}"
        make install
    else
        make -j "${PARALLEL}"
        make framedir="${TP_INCLUDE_DIR}/sasl" install
    fi
}

# librdkafka
build_librdkafka() {
    check_if_source_exist "${LIBRDKAFKA_SOURCE}"

    cd "${TP_SOURCE_DIR}/${LIBRDKAFKA_SOURCE}"

    # NOTE(amos): librdkafka uses a weird autoconf variant (mklove) which doesn't allow extending PKG_CONFIG with spaces in cmd.
    # As a result, we use a patch to hard code "--static" into PKG_CONFIG instead.
    # PKG_CONFIG="pkg-config --static"

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR} -lssl -lcrypto -lzstd -lz -lsasl2 \
        -lgssapi_krb5 -lkrb5 -lkrb5support -lk5crypto -lcom_err -lresolv" \
        ./configure --prefix="${TP_INSTALL_DIR}" --enable-static --enable-sasl --disable-c11threads

    make -j "${PARALLEL}"
    make install

    remove_all_dylib
    strip_lib librdkafka.a
    strip_lib librdkafka++.a
}

# libunixodbc
build_libunixodbc() {
    check_if_source_exist "${ODBC_SOURCE}"

    cd "${TP_SOURCE_DIR}/${ODBC_SOURCE}"

    CFLAGS="-I${TP_INCLUDE_DIR} -Wno-int-conversion -Wno-implicit-function-declaration" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}" --with-included-ltdl --enable-static=yes --enable-shared=no

    make -j "${PARALLEL}"
    make install
}

# flatbuffers
build_flatbuffers() {
    check_if_source_exist "${FLATBUFFERS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${FLATBUFFERS_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags='-static-libstdc++ -static-libgcc'
    else
        ldflags=''
    fi

    LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DFLATBUFFERS_CXX_FLAGS="${warning_class_memaccess} ${warning_unused_but_set_variable}" \
        -DFLATBUFFERS_BUILD_TESTS=OFF \
        ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"

    cp flatc ../../../installed/bin/flatc
    cp -r ../include/flatbuffers ../../../installed/include/flatbuffers
    cp libflatbuffers.a ../../../installed/lib/libflatbuffers.a
}

# arrow
build_arrow() {
    check_if_source_exist "${ARROW_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ARROW_SOURCE}/cpp"

    mkdir -p release
    cd release

    export ARROW_BROTLI_URL="${TP_SOURCE_DIR}/${BROTLI_NAME}"
    export ARROW_GLOG_URL="${TP_SOURCE_DIR}/${GLOG_NAME}"
    export ARROW_LZ4_URL="${TP_SOURCE_DIR}/${LZ4_NAME}"
    export ARROW_FLATBUFFERS_URL="${TP_SOURCE_DIR}/${FLATBUFFERS_NAME}"
    export ARROW_ZSTD_URL="${TP_SOURCE_DIR}/${ZSTD_NAME}"
    export ARROW_JEMALLOC_URL="${TP_SOURCE_DIR}/${JEMALLOC_ARROW_NAME}"
    export ARROW_Thrift_URL="${TP_SOURCE_DIR}/${THRIFT_NAME}"
    export ARROW_SNAPPY_URL="${TP_SOURCE_DIR}/${SNAPPY_NAME}"
    export ARROW_ZLIB_URL="${TP_SOURCE_DIR}/${ZLIB_NAME}"
    export ARROW_XSIMD_URL="${TP_SOURCE_DIR}/${XSIMD_NAME}"
    export ARROW_ORC_URL="${TP_SOURCE_DIR}/${ORC_NAME}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"
    else
        ldflags="-L${TP_LIB_DIR}"
    fi

    LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" -DARROW_PARQUET=ON -DARROW_IPC=ON -DARROW_BUILD_SHARED=OFF \
        -DARROW_BUILD_STATIC=ON -DARROW_WITH_BROTLI=ON -DARROW_WITH_LZ4=ON -DARROW_USE_GLOG=ON \
        -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_JSON=ON \
        -DARROW_WITH_UTF8PROC=OFF -DARROW_WITH_RE2=ON -DARROW_ORC=ON \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DBoost_USE_STATIC_RUNTIME=ON \
        -DARROW_GFLAGS_USE_SHARED=OFF \
        -Dgflags_ROOT="${TP_INSTALL_DIR}" \
        -DGLOG_ROOT="${TP_INSTALL_DIR}" \
        -DRE2_ROOT="${TP_INSTALL_DIR}" \
        -DZLIB_LIBRARY="${TP_INSTALL_DIR}/lib/libz.a" -DZLIB_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DRapidJSON_ROOT="${TP_INSTALL_DIR}" \
        -DORC_ROOT="${TP_INSTALL_DIR}" \
        -DBrotli_SOURCE=BUNDLED \
        -DLZ4_LIB="${TP_INSTALL_DIR}/lib/liblz4.a" -DLZ4_INCLUDE_DIR="${TP_INSTALL_DIR}/include/lz4" \
        -DLz4_SOURCE=SYSTEM \
        -DZSTD_LIB="${TP_INSTALL_DIR}/lib/libzstd.a" -DZSTD_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -Dzstd_SOURCE=SYSTEM \
        -DSnappy_LIB="${TP_INSTALL_DIR}/lib/libsnappy.a" -DSnappy_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DSnappy_SOURCE=SYSTEM \
        -DBOOST_ROOT="${TP_INSTALL_DIR}" --no-warn-unused-cli \
        -DThrift_ROOT="${TP_INSTALL_DIR}" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    #copy dep libs
    cp -rf ./jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/libjemalloc_pic.a "${TP_INSTALL_DIR}/lib64/libjemalloc.a"
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlienc-static.a "${TP_INSTALL_DIR}/lib64/libbrotlienc.a"
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlidec-static.a "${TP_INSTALL_DIR}/lib64/libbrotlidec.a"
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlicommon-static.a "${TP_INSTALL_DIR}/lib64/libbrotlicommon.a"
    strip_lib libarrow.a
    strip_lib libjemalloc.a
    strip_lib libparquet.a
}

# abseil
build_abseil() {
    check_if_source_exist "${ABSEIL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ABSEIL_SOURCE}"

    LDFLAGS="-L${TP_LIB_DIR}" \
        "${CMAKE_CMD}" -B "${BUILD_DIR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DABSL_ENABLE_INSTALL=ON \
        -DBUILD_DEPS=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_CXX_STANDARD=11

    cmake --build "${BUILD_DIR}" -j "${PARALLEL}"
    cmake --install "${BUILD_DIR}" --prefix "${TP_INSTALL_DIR}"
}

# s2
build_s2() {
    check_if_source_exist "${S2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${S2_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    LDFLAGS="-L${TP_LIB_DIR}" \
        ${CMAKE_CMD} -G "${GENERATOR}" -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_GFLAGS=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_LIBRARY_PATH="${TP_INSTALL_DIR}" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    strip_lib libs2.a
}

# bitshuffle
build_bitshuffle() {
    check_if_source_exist "${BITSHUFFLE_SOURCE}"
    local ld="${DORIS_BIN_UTILS}/ld"
    local ar="${DORIS_BIN_UTILS}/ar"

    if [[ ! -f "${ld}" ]]; then ld="$(command -v ld)"; fi
    if [[ ! -f "${ar}" ]]; then ar="$(command -v ar)"; fi

    cd "${TP_SOURCE_DIR}/${BITSHUFFLE_SOURCE}"
    PREFIX="${TP_INSTALL_DIR}"

    # This library has significant optimizations when built with AVX2/AVX512. However,
    # we still need to support non-AVX2-capable hardware. So, we build it three times,
    # with the flag AVX2, AVX512 each and once without, and use some linker tricks to
    # suffix the AVX2 symbols with '_avx2', AVX512 symbols with '_avx512'
    arches=('default' 'avx2' 'avx512')
    MACHINE_TYPE="$(uname -m)"
    # Becuase aarch64 don't support avx2, disable it.
    if [[ "${MACHINE_TYPE}" == "aarch64" || "${MACHINE_TYPE}" == 'arm64' ]]; then
        arches=('default')
    fi

    to_link=""
    for arch in "${arches[@]}"; do
        arch_flag=""
        if [[ "${arch}" == "avx2" ]]; then
            arch_flag="-mavx2"
        fi
        if [[ "${arch}" == "avx512" ]]; then
            arch_flag="-mavx512bw -mavx512f"
        fi
        tmp_obj="bitshuffle_${arch}_tmp.o"
        dst_obj="bitshuffle_${arch}.o"
        "${CC}" ${EXTRA_CFLAGS:+${EXTRA_CFLAGS}} ${arch_flag:+${arch_flag}} -std=c99 "-I${PREFIX}/include/lz4" -O3 -DNDEBUG -c \
            "src/bitshuffle_core.c" \
            "src/bitshuffle.c" \
            "src/iochain.c"
        # Merge the object files together to produce a combined .o file.
        "${ld}" -r -o "${tmp_obj}" bitshuffle_core.o bitshuffle.o iochain.o
        # For the AVX2 symbols, suffix them.
        if [[ "${arch}" == "avx2" ]] || [[ "${arch}" == "avx512" ]]; then
            local nm="${DORIS_BIN_UTILS}/nm"
            local objcopy="${DORIS_BIN_UTILS}/objcopy"

            if [[ ! -f "${nm}" ]]; then nm="$(command -v nm)"; fi
            if [[ ! -f "${objcopy}" ]]; then
                if ! objcopy="$(command -v objcopy)"; then
                    objcopy="${TP_INSTALL_DIR}/binutils/bin/objcopy"
                fi
            fi

            # Create a mapping file with '<old_sym> <suffixed_sym>' on each line.
            "${nm}" --defined-only --extern-only "${tmp_obj}" | while read -r addr type sym; do
                echo "${sym} ${sym}_${arch}"
            done >renames.txt
            "${objcopy}" --redefine-syms=renames.txt "${tmp_obj}" "${dst_obj}"
        else
            mv "${tmp_obj}" "${dst_obj}"
        fi
        to_link="${to_link} ${dst_obj}"
    done
    local links
    read -r -a links <<<"${to_link}"
    rm -f libbitshuffle.a
    "${ar}" rs libbitshuffle.a "${links[@]}"
    mkdir -p "${PREFIX}/include/bitshuffle"
    cp libbitshuffle.a "${PREFIX}"/lib/
    cp "${TP_SOURCE_DIR}/${BITSHUFFLE_SOURCE}/src/bitshuffle.h" "${PREFIX}/include/bitshuffle/bitshuffle.h"
    cp "${TP_SOURCE_DIR}/${BITSHUFFLE_SOURCE}/src/bitshuffle_core.h" "${PREFIX}/include/bitshuffle/bitshuffle_core.h"
}

# croaring bitmap
build_croaringbitmap() {
    avx_flag=''
    if [[ -n "${USE_AVX2}" && "${USE_AVX2}" -eq 0 ]]; then
        echo "set USE_AVX2=${USE_AVX2} to FORCE disable AVX2 in croaringbitmap"
        avx_flag="-DROARING_DISABLE_AVX=ON"
    fi

    check_if_source_exist "${CROARINGBITMAP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CROARINGBITMAP_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"
    else
        ldflags="-L${TP_LIB_DIR}"
    fi

    CXXFLAGS="-O3" \
        LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" ${avx_flag:+${avx_flag}} -DROARING_BUILD_STATIC=ON -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DENABLE_ROARING_TESTS=OFF ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# fmt
build_fmt() {
    check_if_source_exist "${FMT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${FMT_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    "${CMAKE_CMD}" -G "${GENERATOR}" -DBUILD_SHARED_LIBS=FALSE -DFMT_TEST=OFF -DFMT_DOC=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..
    "${BUILD_SYSTEM}" -j"${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# parallel_hashmap
build_parallel_hashmap() {
    check_if_source_exist "${PARALLEL_HASHMAP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${PARALLEL_HASHMAP_SOURCE}"
    cp -r parallel_hashmap "${TP_INSTALL_DIR}/include/"
}

# pdqsort
build_pdqsort() {
    check_if_archive_exist "${PDQSORT_FILE}"
    cd "${TP_SOURCE_DIR}"
    cp "${PDQSORT_FILE}" "${TP_INSTALL_DIR}/include/"
}

# libdivide
build_libdivide() {
    check_if_source_exist "${LIBDIVIDE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LIBDIVIDE_SOURCE}"
    cp -r libdivide.h "${TP_INSTALL_DIR}/include/"
}

#orc
build_orc() {
    check_if_source_exist "${ORC_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ORC_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    CXXFLAGS="-O3 -Wno-array-bounds ${warning_reserved_identifier} ${warning_suggest_override}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" ../ -DBUILD_JAVA=OFF \
        -DPROTOBUF_HOME="${TP_INSTALL_DIR}" \
        -DSNAPPY_HOME="${TP_INSTALL_DIR}" \
        -DLZ4_HOME="${TP_INSTALL_DIR}" \
        -DLZ4_INCLUDE_DIR="${TP_INSTALL_DIR}/include/lz4" \
        -DZLIB_HOME="${TP_INSTALL_DIR}" \
        -DZSTD_HOME="${TP_INSTALL_DIR}" \
        -DZSTD_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DBUILD_LIBHDFSPP=OFF \
        -DBUILD_CPP_TESTS=OFF \
        -DSTOP_BUILD_ON_WARNING=OFF \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}"

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    strip_lib liborc.a
}

#cctz
build_cctz() {
    check_if_source_exist "${CCTZ_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CCTZ_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DBUILD_TESTING=OFF ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
}

# all js and csss related
build_js_and_css() {
    check_if_source_exist "${DATATABLES_SOURCE}"
    check_if_source_exist 'Bootstrap-3.3.7'
    check_if_source_exist 'jQuery-3.6.0'

    mkdir -p "${TP_INSTALL_DIR}/webroot"
    cd "${TP_SOURCE_DIR}"
    cp -r "${DATATABLES_SOURCE}" "${TP_INSTALL_DIR}/webroot/"
    cp -r Bootstrap-3.3.7 "${TP_INSTALL_DIR}/webroot/"
    cp -r jQuery-3.6.0 "${TP_INSTALL_DIR}/webroot/"
    cp bootstrap-table.min.js "${TP_INSTALL_DIR}/webroot/Bootstrap-3.3.7/js"
    cp bootstrap-table.min.css "${TP_INSTALL_DIR}/webroot/Bootstrap-3.3.7/css"
}

build_tsan_header() {
    cd "${TP_SOURCE_DIR}"
    if [[ ! -f "${TSAN_HEADER_FILE}" ]]; then
        echo "${TSAN_HEADER_FILE} should exist."
        exit 1
    fi

    mkdir -p "${TP_INSTALL_DIR}/include/sanitizer"
    cp "${TSAN_HEADER_FILE}" "${TP_INSTALL_DIR}/include/sanitizer/"
}

# aws_sdk
build_aws_sdk() {
    check_if_source_exist "${AWS_SDK_SOURCE}"
    cd "${TP_SOURCE_DIR}/${AWS_SDK_SOURCE}"

    rm -rf "${BUILD_DIR}"

    # -Wno-nonnull gcc-11
    "${CMAKE_CMD}" -G "${GENERATOR}" -B"${BUILD_DIR}" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF \
        -DCURL_LIBRARY_RELEASE="${TP_INSTALL_DIR}/lib/libcurl.a" -DZLIB_LIBRARY_RELEASE="${TP_INSTALL_DIR}/lib/libz.a" \
        -DBUILD_ONLY="core;s3;s3-crt;transfer" \
        -DCMAKE_CXX_FLAGS="-Wno-nonnull -Wno-deprecated-declarations ${warning_dangling_reference}" -DCPP_STANDARD=17

    cd "${BUILD_DIR}"

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    strip_lib libaws-cpp-sdk-s3-crt.a
    strip_lib libaws-cpp-sdk-s3.a
    strip_lib libaws-cpp-sdk-core.a
    strip_lib libs2n.a
    strip_lib libaws-crt-cpp.a
    strip_lib libaws-c-http.a
    strip_lib libaws-c-common.a
    strip_lib libaws-c-auth.a
    strip_lib libaws-c-io.a
    strip_lib libaws-c-mqtt.a
    strip_lib libaws-c-s3.a
    strip_lib libaws-c-event-stream.a
    strip_lib libaws-c-cal.a
    strip_lib libaws-cpp-sdk-transfer.a
    strip_lib libaws-checksums.a
    strip_lib libaws-c-compression.a
}

# lzma
build_lzma() {
    if [[ ! -x "$(command -v autopoint)" ]]; then
        echo "autopoint is required by $0, install it first"
        return 255
    fi

    check_if_source_exist "${LZMA_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LZMA_SOURCE}"

    export ACLOCAL_PATH='/usr/share/aclocal'

    sh autogen.sh

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    ../configure --prefix="${TP_INSTALL_DIR}" --enable-shared=no --with-pic

    make -j "${PARALLEL}"
    make install
    strip_lib liblzma.a
}

# xml2
build_xml2() {
    if [[ ! -x "$(command -v pkg-config)" ]]; then
        echo "pkg-config is required by $0, install it first"
        return 255
    fi

    check_if_source_exist "${XML2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${XML2_SOURCE}"

    export ACLOCAL_PATH='/usr/share/aclocal'

    sed '/(libtoolize/,/}/d' autogen.sh | bash
    make distclean

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    CPPLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ../configure --prefix="${TP_INSTALL_DIR}" --enable-shared=no --with-pic --with-python=no --with-lzma="${TP_INSTALL_DIR}"

    make -j "${PARALLEL}"
    make install
    strip_lib libxml2.a
}

# idn
build_idn() {
    check_if_source_exist "${IDN_SOURCE}"
    cd "${TP_SOURCE_DIR}/${IDN_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    ../configure --prefix="${TP_INSTALL_DIR}" --enable-shared=no --with-pic

    make -j "${PARALLEL}"
    make install
}

# gsasl
build_gsasl() {
    check_if_source_exist "${GSASL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GSASL_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    KRB5_CONFIG="${TP_INSTALL_DIR}/bin/krb5-config" \
        CFLAGS="-I${TP_INCLUDE_DIR} -Wno-implicit-function-declaration" \
        ../configure --prefix="${TP_INSTALL_DIR}" --with-gssapi-impl=mit --enable-shared=no --with-pic --with-libidn-prefix="${TP_INSTALL_DIR}"

    make -j "${PARALLEL}"
    make install
}

# krb5
build_krb5() {
    check_if_source_exist "${KRB5_SOURCE}"
    cd "${TP_SOURCE_DIR}/${KRB5_SOURCE}/src"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    if [[ "${KERNEL}" == 'Darwin' ]]; then
        with_crypto_impl='--with-crypto-impl=openssl'
    fi

    CFLAGS="-fcommon -fPIC -I${TP_INSTALL_DIR}/include" LDFLAGS="-L${TP_INSTALL_DIR}/lib" \
        ../configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static \
        --without-keyutils ${with_crypto_impl:+${with_crypto_impl}}

    make -j "${PARALLEL}"
    make install
}

# hdfs3
build_hdfs3() {
    check_if_source_exist "${HDFS3_SOURCE}"
    cd "${TP_SOURCE_DIR}/${HDFS3_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf ./*

    if [[ "$(uname -m)" == "x86_64" ]]; then
        SSE_OPTION='-DENABLE_SSE=ON'
    else
        SSE_OPTION='-DENABLE_SSE=OFF'
    fi
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_TEST=OFF "${SSE_OPTION}" \
        -DProtobuf_PROTOC_EXECUTABLE="${TP_INSTALL_DIR}/bin/protoc" \
        -DProtobuf_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DProtobuf_LIBRARIES="${TP_INSTALL_DIR}/lib/libprotoc.a" \
        -DKERBEROS_INCLUDE_DIRS="${TP_INSTALL_DIR}/include" \
        -DKERBEROS_LIBRARIES="${TP_INSTALL_DIR}/lib/libkrb5.a" \
        -DGSASL_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DGSASL_LIBRARIES="${TP_INSTALL_DIR}/lib/libgsasl.a" \
        -DCMAKE_CXX_FLAGS='-include cstdint' \
        ..

    make CXXFLAGS="${libhdfs_cxx17}" -j "${PARALLEL}"
    make install
    strip_lib libhdfs3.a
}

# jemalloc
build_jemalloc() {
    check_if_source_exist "${JEMALLOC_DORIS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${JEMALLOC_DORIS_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    cflags='-O3 -fno-omit-frame-pointer -fPIC -g'
    CFLAGS="${cflags}" ../configure --prefix="${TP_INSTALL_DIR}" --with-jemalloc-prefix=je --enable-prof --disable-cxx --disable-libdl --disable-shared

    make -j "${PARALLEL}"
    make install
    mv "${TP_INSTALL_DIR}"/lib/libjemalloc.a "${TP_INSTALL_DIR}"/lib/libjemalloc_doris.a
}

# libunwind
build_libunwind() {
    # There are two major variants of libunwind. libunwind on Linux
    # (https://www.nongnu.org/libunwind/) provides unw_backtrace, and
    # Apache/LLVM libunwind (notably used on Apple platforms) doesn't
    if [[ "${KERNEL}" != 'Darwin' ]]; then
        check_if_source_exist "${LIBUNWIND_SOURCE}"
        cd "${TP_SOURCE_DIR}/${LIBUNWIND_SOURCE}"

        mkdir -p "${BUILD_DIR}"
        cd "${BUILD_DIR}"

        # We should enable optimizations (otherwise it will be too slow in debug)
        # and disable sanitizers (otherwise infinite loop may happen)
        # close exceptions and rtti can improve the operating efficiency of the program
        # LIBUNWIND_NO_HEAP: https://reviews.llvm.org/D11897
        # LIBUNWIND_IS_NATIVE_ONLY: https://lists.llvm.org/pipermail/cfe-commits/Week-of-Mon-20160523/159802.html
        # -nostdinc++ only required for gcc compilation
        cflags="-I${TP_INCLUDE_DIR} -std=c99 -D_LIBUNWIND_NO_HEAP=1 -D_DEBUG -D_LIBUNWIND_IS_NATIVE_ONLY -O3 -fno-exceptions -funwind-tables -fno-sanitize=all -nostdinc++ -fno-rtti"
        CFLAGS="${cflags}" LDFLAGS="-L${TP_LIB_DIR} -llzma" ../configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static

        make -j "${PARALLEL}"
        make install
    fi
}

# benchmark
build_benchmark() {
    check_if_source_exist "${BENCHMARK_SOURCE}"

    cd "${TP_SOURCE_DIR}/${BENCHMARK_SOURCE}"

    cmake -E make_directory "build"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        cxxflags='-lresolv -pthread -lrt'
    else
        cxxflags='-lresolv -pthread'
    fi

    # NOTE(amos): -DHAVE_STD_REGEX=1 avoid runtime checks as it will fail when compiling with non-standard toolchain
    CXXFLAGS="${cxxflags}" cmake -E chdir "build" \
        cmake ../ -DBENCHMARK_ENABLE_GTEST_TESTS=OFF -DBENCHMARK_ENABLE_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DHAVE_STD_REGEX=1
    cmake --build "build" --config Release

    mkdir -p "${TP_INCLUDE_DIR}/benchmark"
    cp "${TP_SOURCE_DIR}/${BENCHMARK_SOURCE}/include/benchmark/benchmark.h" "${TP_INCLUDE_DIR}/benchmark/"
    cp "${TP_SOURCE_DIR}/${BENCHMARK_SOURCE}/include/benchmark/export.h" "${TP_INCLUDE_DIR}/benchmark/"
    cp "${TP_SOURCE_DIR}/${BENCHMARK_SOURCE}/build/src/libbenchmark.a" "${TP_LIB_DIR}"
}

# simdjson
build_simdjson() {
    check_if_source_exist "${SIMDJSON_SOURCE}"
    cd "${TP_SOURCE_DIR}/${SIMDJSON_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    CXXFLAGS="-O3" CFLAGS="-O3" \
        "${CMAKE_CMD}" -DSIMDJSON_EXCEPTIONS=OFF \
        -DSIMDJSON_DEVELOPER_MODE=OFF -DSIMDJSON_BUILD_STATIC=ON \
        -DSIMDJSON_JUST_LIBRARY=ON -DSIMDJSON_ENABLE_THREADS=ON ..
    "${CMAKE_CMD}" --build . --config Release

    cp "${TP_SOURCE_DIR}/${SIMDJSON_SOURCE}/${BUILD_DIR}/libsimdjson.a" "${TP_INSTALL_DIR}/lib64"
    cp -r "${TP_SOURCE_DIR}/${SIMDJSON_SOURCE}/include"/* "${TP_INCLUDE_DIR}/"
}

# nlohmann_json
build_nlohmann_json() {
    check_if_source_exist "${NLOHMANN_JSON_SOURCE}"
    cd "${TP_SOURCE_DIR}/${NLOHMANN_JSON_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" -DJSON_BuildTests=OFF ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# opentelemetry
build_opentelemetry() {
    check_if_source_exist "${OPENTELEMETRY_SOURCE}"
    cd "${TP_SOURCE_DIR}/${OPENTELEMETRY_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" -DBUILD_TESTING=OFF \
        -DWITH_OTLP=ON -DWITH_OTLP_GRPC=OFF -DWITH_OTLP_HTTP=ON -DWITH_ZIPKIN=ON -DWITH_EXAMPLES=OFF ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    strip_lib libopentelemetry_exporter_zipkin_trace.a
    strip_lib libopentelemetry_trace.a
    strip_lib libopentelemetry_proto.a
    strip_lib libopentelemetry_resources.a
    strip_lib libopentelemetry_exporter_ostream_span.a
    strip_lib libopentelemetry_http_client_curl.a
    strip_lib libopentelemetry_exporter_otlp_http_client.a
}

# sse2neon
build_sse2neon() {
    check_if_source_exist "${SSE2NEON_SOURCE}"
    cd "${TP_SOURCE_DIR}/${SSE2NEON_SOURCE}"
    cp sse2neon.h "${TP_INSTALL_DIR}/include/"
}

# xxhash
build_xxhash() {
    check_if_source_exist "${XXHASH_SOURCE}"
    cd "${TP_SOURCE_DIR}/${XXHASH_SOURCE}"

    make -j "${PARALLEL}"
    cp -r ./*.h "${TP_INSTALL_DIR}/include/"
    cp libxxhash.a "${TP_INSTALL_DIR}/lib64"
}

build_binutils() {
    check_if_source_exist "${BINUTILS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BINUTILS_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    ../configure --prefix="${TP_INSTALL_DIR}/binutils" --includedir="${TP_INCLUDE_DIR}" --libdir="${TP_LIB_DIR}" \
        --enable-install-libiberty --without-msgpack
    make -j "${PARALLEL}"
    make install-bfd install-libiberty install-binutils
}

build_gettext() {
    check_if_source_exist "${GETTEXT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GETTEXT_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    ../gettext-runtime/configure --prefix="${TP_INSTALL_DIR}" --disable-java
    cd intl
    make -j "${PARALLEL}"
    make install

    remove_all_dylib
}

# concurrentqueue
build_concurrentqueue() {
    check_if_source_exist "${CONCURRENTQUEUE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CONCURRENTQUEUE_SOURCE}"
    cp ./*.h "${TP_INSTALL_DIR}/include/"
}

# fast_float
build_fast_float() {
    check_if_source_exist "${FAST_FLOAT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${FAST_FLOAT_SOURCE}"
    cp -r ./include/fast_float "${TP_INSTALL_DIR}/include/"
}

# hadoop_libs
build_hadoop_libs() {
    check_if_source_exist "${HADOOP_LIBS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${HADOOP_LIBS_SOURCE}"
    echo "THIRDPARTY_INSTALLED=${TP_INSTALL_DIR}" >env.sh
    ./build.sh

    mkdir -p "${TP_INSTALL_DIR}/include/hadoop_hdfs/"
    mkdir -p "${TP_INSTALL_DIR}/lib/hadoop_hdfs/"
    cp -r ./hadoop-dist/target/hadoop-libhdfs-3.3.4/* "${TP_INSTALL_DIR}/lib/hadoop_hdfs/"
    cp -r ./hadoop-dist/target/hadoop-libhdfs-3.3.4/include/hdfs.h "${TP_INSTALL_DIR}/include/hadoop_hdfs/"
    rm -rf "${TP_INSTALL_DIR}/lib/hadoop_hdfs/native/*.a"
    find ./hadoop-dist/target/hadoop-3.3.4/lib/native/ -type f ! -name '*.a' -exec cp {} "${TP_INSTALL_DIR}/lib/hadoop_hdfs/native/" \;
    find ./hadoop-dist/target/hadoop-3.3.4/lib/native/ -type l -exec cp -P {} "${TP_INSTALL_DIR}/lib/hadoop_hdfs/native/" \;
}

# dragonbox
build_dragonbox() {
    check_if_source_exist "${DRAGONBOX_SOURCE}"
    cd "${TP_SOURCE_DIR}/${DRAGONBOX_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DDRAGONBOX_INSTALL_TO_CHARS=ON ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

if [[ "${#packages[@]}" -eq 0 ]]; then
    packages=(
        libunixodbc
        openssl
        libevent
        zlib
        lz4
        bzip
        lzo2
        zstd
        boost # must before thrift
        protobuf
        gflags
        gtest
        glog
        rapidjson
        snappy
        gperftools
        curl
        re2
        hyperscan
        thrift
        leveldb
        brpc
        jemalloc
        rocksdb
        krb5 # before cyrus_sasl
        cyrus_sasl
        librdkafka
        flatbuffers
        orc
        arrow
        abseil
        s2
        bitshuffle
        croaringbitmap
        fmt
        parallel_hashmap
        pdqsort
        libdivide
        cctz
        tsan_header
        mysql
        aws_sdk
        js_and_css
        lzma
        xml2
        idn
        gsasl
        hdfs3
        benchmark
        simdjson
        nlohmann_json
        opentelemetry
        libbacktrace
        sse2neon
        xxhash
        concurrentqueue
        fast_float
        libunwind
        dragonbox
    )
    if [[ "$(uname -s)" == 'Darwin' ]]; then
        read -r -a packages <<<"binutils gettext ${packages[*]}"
    elif [[ "$(uname -s)" == 'Linux' ]]; then
        read -r -a packages <<<"${packages[*]} hadoop_libs"
    fi
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
