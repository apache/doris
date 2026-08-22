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

# The shebang above only takes effect when this script is executed directly.
# `sh build-thirdparty.sh` hands it to /bin/sh instead, which is dash on Debian
# and Ubuntu and parses none of the `[[ ]]`, arrays and here-strings this script
# is built on. It does not stop at the first of them either, it keeps going and
# runs a mangled version of the script. Re-exec under bash so that the way the
# script was invoked cannot decide whether the build works. Keep this block
# POSIX, it has to be parsed by the shell that is about to be replaced.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

TP_CXX_STANDARD=20

export DORIS_HOME="${curdir}/.."
export TP_DIR="${curdir}"

# include custom environment variables
if [[ -f "${DORIS_HOME}/env.sh" ]]; then
    export DO_NOT_CHECK_JAVA_ENV=1
    . "${DORIS_HOME}/env.sh"
    export DO_NOT_CHECK_JAVA_ENV=
fi

# Optional ccache for the cmake-based packages. A full third-party build is cold every
# time, so a warm ccache turns a rebuild triggered by one changed package into a few
# minutes instead of hours - that is what this is for, and CI is where it pays off.
#
# CMake initialises CMAKE_<LANG>_COMPILER_LAUNCHER from the environment variables of the
# same name, so this needs no change to the cmake invocations below. It is also why this
# does not go through CC/CXX: "ccache <compiler>" would land the compiler name in
# CMAKE_<LANG>_FLAGS and leak into whatever the package exports. Autotools packages are
# deliberately left alone. Off by default, since prefixing the compiler changes how every
# package configures itself.
if [[ "${ENABLE_THIRDPARTY_CCACHE:-OFF}" == "ON" ]]; then
    if ! command -v ccache &>/dev/null; then
        echo "ENABLE_THIRDPARTY_CCACHE=ON, but ccache is not in PATH" >&2
        exit 1
    fi
    export CMAKE_C_COMPILER_LAUNCHER='ccache'
    export CMAKE_CXX_COMPILER_LAUNCHER='ccache'
    echo "ccache is enabled for the cmake-based third-party packages"
fi

# Check args
usage() {
    echo "
Usage: $0 [options...] [packages...]
  Optional options:
     -j <num>               build thirdparty parallel
     --clean                clean the extracted data
     --continue <package>   continue to build the remaining packages (starts from the specified package)

  Environment variables:
     ENABLE_THIRDPARTY_CCACHE=ON          compile the cmake-based packages through ccache
     DISABLE_THIRDPARTY_BUILD_AZURE=ON    skip the azure-sdk-for-cpp package
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

BUILD_AZURE="ON"

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

# Whether the third-party tree carries azure and whether Doris links it are two
# separate questions. env.sh's DISABLE_BUILD_AZURE answers the second one for BE and
# the cloud meta-service, and it still defaults to ON on aarch64 and macOS, where the
# published prebuilt archives predate azure. Those archives have to grow the libraries
# before anything can link them, so the build does not read that switch;
# DISABLE_THIRDPARTY_BUILD_AZURE opts the build itself out.
if [[ "$(echo "${DISABLE_THIRDPARTY_BUILD_AZURE}" | tr '[:lower:]' '[:upper:]')" == "ON" ]]; then
    BUILD_AZURE='OFF'
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
prepare_arrow_paimon_download_packages "${packages[@]}"
bash "${TP_DIR}/download-thirdparty.sh" "${ARROW_PAIMON_DOWNLOAD_PACKAGES[@]}"

export LD_LIBRARY_PATH="${TP_DIR}/installed/lib:${LD_LIBRARY_PATH}"

# toolchain specific warning options and settings
if [[ "${CC}" == *gcc ]]; then
    warning_uninitialized='-Wno-maybe-uninitialized'
    warning_stringop_truncation='-Wno-stringop-truncation'
    warning_class_memaccess='-Wno-class-memaccess'
    warning_array_parameter='-Wno-array-parameter'
    warning_narrowing='-Wno-narrowing'
    warning_dangling_reference='-Wno-dangling-reference'

    gcc_major_version=$("${CC}" -dumpversion | cut -d. -f1)
    if [[ "${gcc_major_version}" -ge 15 ]]; then
        warning_deprecated_literal_operator='-Wno-deprecated-literal-operator'
    fi

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
export PKG_CONFIG_LIBDIR="${TP_DIR}/installed/lib64/pkgconfig"
export CMAKE_PREFIX_PATH="${TP_DIR}/installed"

echo "PKG_CONFIG_LIBDIR: ${PKG_CONFIG_LIBDIR}"
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
        "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DEVENT__DISABLE_TESTS=ON \
        -DEVENT__DISABLE_SAMPLES=ON -DEVENT__DISABLE_REGRESS=ON ..

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

    if [[ "${KERNEL}" == 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR}"
    else
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc -Wl,--undefined=pthread_create"
    fi

    mkdir -p cmake/build
    cd cmake/build

    CXXFLAGS="-O2 -I${TP_INCLUDE_DIR}" \
        LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -Dprotobuf_USE_EXTERNAL_GTEST=ON \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -Dprotobuf_BUILD_SHARED_LIBS=OFF \
        -Dprotobuf_BUILD_TESTS=OFF \
        -DZLIB_LIBRARY="${TP_LIB_DIR}/libz.a" \
        -Dprotobuf_ABSL_PROVIDER=package \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ../..

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

    "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=On ../

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# glog
build_glog() {
    check_if_source_exist "${GLOG_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GLOG_SOURCE}"

    if [[ "${GLOG_SOURCE}" == "glog-0.4.0" ]]; then
        # to generate config.guess and config.sub to support aarch64
        rm -rf config.*
        autoreconf -i

        CPPFLAGS="-I${TP_INCLUDE_DIR} -fpermissive -fPIC" \
            LDFLAGS="-L${TP_LIB_DIR}" \
            ./configure --prefix="${TP_INSTALL_DIR}" --enable-frame-pointers --disable-shared --enable-static

        make -j "${PARALLEL}"
        make install
    elif [[ "${GLOG_SOURCE}" == "glog-0.6.0" ]]; then
        LDFLAGS="-L${TP_LIB_DIR}" \
            "${CMAKE_CMD}" -S . -B build -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DWITH_UNWIND=OFF \
            -DBUILD_SHARED_LIBS=OFF \
            -DWITH_TLS=OFF

        "${CMAKE_CMD}" --build build --target install
    fi

    strip_lib libglog.a
}

# gtest
build_gtest() {
    check_if_source_exist "${GTEST_SOURCE}"

    cd "${TP_SOURCE_DIR}/${GTEST_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" ../ -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_POSITION_INDEPENDENT_CODE=On
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

    "${CMAKE_CMD}" ../ -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DRAPIDJSON_BUILD_DOC=OFF \
        -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_TESTS=OFF

    make -j "${PARALLEL}"
    make install
}

# snappy
build_snappy() {
    check_if_source_exist "${SNAPPY_SOURCE}"
    cd "${TP_SOURCE_DIR}/${SNAPPY_SOURCE}"

    # Enable RTTI for snappy (required by Doris BE for SnappySlicesSource inheritance)
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        sed -i '' 's/-fno-rtti/-frtti/g' CMakeLists.txt
    else
        sed -i 's/-fno-rtti/-frtti/g' CMakeLists.txt
    fi

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    CFLAGS="-O3" CXXFLAGS="-O3" "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_INSTALL_INCLUDEDIR="${TP_INCLUDE_DIR}"/snappy \
        -DSNAPPY_BUILD_TESTS=OFF \
        -DSNAPPY_BUILD_BENCHMARKS=OFF ../

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

# crc32c
build_crc32c() {
    check_if_source_exist "${CRC32C_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CRC32C_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCRC32C_BUILD_TESTS=0 -DCRC32C_BUILD_BENCHMARKS=0 -DCRC32C_USE_GLOG=OFF \
        -DBUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}" all install
}

# zstd
build_zstd() {
    check_if_source_exist "${ZSTD_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ZSTD_SOURCE}/build/cmake"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DBUILD_TESTING=OFF -DZSTD_BUILD_TESTS=OFF -DZSTD_BUILD_STATIC=ON \
        -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_SHARED=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    strip_lib libzstd.a
}

# bzip
build_bzip() {
    check_if_source_exist "${BZIP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BZIP_SOURCE}"

    make -j "${PARALLEL}" install PREFIX="${TP_INSTALL_DIR}" CFLAGS="-fPIC"
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

# brotli
build_brotli() {
    check_if_source_exist "${BROTLI_SOURCE}"
    # brotli has been builded in build_arrow, so just copy headers
    cp -r "${TP_SOURCE_DIR}/${BROTLI_SOURCE}/c/include/brotli" "${TP_INCLUDE_DIR}/"
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

# re2
build_re2() {
    check_if_source_exist "${RE2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${RE2_SOURCE}"

    "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_BUILD_TYPE=Release \
        -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}"
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
        "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_BUILD_TYPE=RelWithDebInfo \
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
        cflags='-static -pthread -lrt -std=gnu89'
        cxxflags='-static -pthread -lrt'
    else
        cflags='-pthread -std=gnu89'
        cxxflags='-pthread'
    fi

    CFLAGS="${cflags}" CXXFLAGS="${cxxflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" ../ -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_LINK_SEARCH_END_STATIC=1 \
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

    CXXFLAGS="-fPIC" "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DLEVELDB_BUILD_BENCHMARKS=OFF \
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
    # If BUILD_SHARED_LIBS=OFF, on centos 5.4 will error: `undefined reference to `google::FlagRegisterer`, no error on MacOS.
    # If glog is compiled before gflags, the above error will not exist, this works in glog 0.4,
    # but glog 0.6 enforces dependency on gflags.
    # glog must be enabled, otherwise error: `flag 'v' was defined more than once` (in files 'glog-0.6.0/src/vlog_is_on.cc' and 'brpc-1.6.0/src/butil/logging.cc')
    LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" -DBUILD_SHARED_LIBS=ON -DWITH_GLOG=ON -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
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

    CFLAGS="-fPIC -std=gnu89 -Wno-implicit-function-declaration" \
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
build_odbc() {
    check_if_source_exist "${ODBC_SOURCE}"

    cd "${TP_SOURCE_DIR}/${ODBC_SOURCE}"

    CFLAGS="-I${TP_INCLUDE_DIR} -Wno-int-conversion -std=gnu89 -Wno-implicit-function-declaration" \
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
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DFLATBUFFERS_CXX_FLAGS="${warning_class_memaccess} ${warning_unused_but_set_variable}" \
        -DFLATBUFFERS_BUILD_TESTS=OFF \
        ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"

    cp flatc ../../../installed/bin/flatc
    rm -rf ../../../installed/include/flatbuffers
    cp -r ../include/flatbuffers ../../../installed/include/flatbuffers
    cp libflatbuffers.a ../../../installed/lib/libflatbuffers.a
}

# c-ares
build_cares() {
    check_if_source_exist "${CARES_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CARES_SOURCE}"

    mkdir -p build
    cd build
    cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCARES_STATIC=ON \
        -DCARES_SHARED=OFF \
        -DCARES_STATIC_PIC=ON \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..
    make
    make install
}

# grpc
build_grpc() {
    check_if_source_exist "${GRPC_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GRPC_SOURCE}"

    mkdir -p cmake/build
    cd cmake/build

    "${CMAKE_CMD}" -DgRPC_INSTALL=ON \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DgRPC_BUILD_TESTS=OFF \
        -Dgrpc_csharp_plugin=OFF \
        -Dgrpc_node_plugin=OFF \
        -Dgrpc_objective_c_plugin=OFF \
        -Dgrpc_php_plugin=OFF \
        -Dgrpc_python_plugin=OFF \
        -Dgrpc_ruby_plugin=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DgRPC_CARES_PROVIDER=package \
        -Dc-ares_DIR="${TP_INSTALL_DIR}" \
        -DgRPC_ABSL_PROVIDER=package \
        -Dabsl_DIR="${TP_INSTALL_DIR}" \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DProtobuf_DIR="${TP_INSTALL_DIR}" \
        -DgRPC_RE2_PROVIDER=package \
        -Dre2_DIR:STRING="${TP_INSTALL_DIR}" \
        -DgRPC_SSL_PROVIDER=package \
        -DOPENSSL_ROOT_DIR="${TP_INSTALL_DIR}" \
        -DgRPC_ZLIB_PROVIDER=package \
        -DZLIB_ROOT="${TP_INSTALL_DIR}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        ../..

    make -j "${PARALLEL}"
    make install

    # for grpc > v1.55, cmake 2.22 does not support find_dependency, delete this line after cmake version upgrade.
    # sed -i 's/find_dependency/find_package/g' "${TP_INSTALL_DIR}"/lib64/cmake/grpc/gRPCConfig.cmake
}

# arrow
build_arrow() {
    check_if_source_exist "${ARROW_SOURCE}"
    invalidate_arrow_prebuilt_marker "${TP_INSTALL_DIR}"
    cd "${TP_SOURCE_DIR}/${ARROW_SOURCE}/cpp"

    mkdir -p release
    cd release

    export ARROW_BROTLI_URL="${TP_SOURCE_DIR}/${BROTLI_NAME}"
    export ARROW_GLOG_URL="${TP_SOURCE_DIR}/${GLOG_NAME}"
    export ARROW_LZ4_URL="${TP_SOURCE_DIR}/${LZ4_NAME}"
    export ARROW_FLATBUFFERS_URL="${TP_SOURCE_DIR}/${FLATBUFFERS_NAME}"
    export ARROW_ZSTD_URL="${TP_SOURCE_DIR}/${ZSTD_NAME}"
    export ARROW_Thrift_URL="${TP_SOURCE_DIR}/${THRIFT_NAME}"
    export ARROW_SNAPPY_URL="${TP_SOURCE_DIR}/${SNAPPY_NAME}"
    export ARROW_ZLIB_URL="${TP_SOURCE_DIR}/${ZLIB_NAME}"
    export ARROW_XSIMD_URL="${TP_SOURCE_DIR}/${XSIMD_NAME}"
    export ARROW_ORC_URL="${TP_SOURCE_DIR}/${ORC_NAME}"
    export ARROW_GRPC_URL="${TP_SOURCE_DIR}/${GRPC_NAME}"
    export ARROW_PROTOBUF_URL="${TP_SOURCE_DIR}/${PROTOBUF_NAME}"

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"
    else
        ldflags="-L${TP_LIB_DIR}"
    fi

    LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_CXX_STANDARD="${TP_CXX_STANDARD}" \
        -G "${GENERATOR}" -DARROW_PARQUET=ON -DARROW_IPC=ON -DARROW_BUILD_SHARED=OFF \
        -DARROW_BUILD_STATIC=ON -DARROW_WITH_BROTLI=ON -DARROW_WITH_LZ4=ON -DARROW_USE_GLOG=ON \
        -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_JSON=ON \
        -DARROW_WITH_UTF8PROC=OFF -DARROW_WITH_RE2=ON -DARROW_ORC=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_DATASET=ON \
        -DARROW_ACERO=ON \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DARROW_WITH_GRPC=ON \
        -DgRPC_SOURCE=SYSTEM \
        -DgRPC_ROOT="${TP_INSTALL_DIR}" \
        -DARROW_WITH_PROTOBUF=ON \
        -DProtobuf_SOURCE=SYSTEM \
        -DProtobuf_LIB="${TP_INSTALL_DIR}/lib/libprotoc.a" -DProtobuf_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DARROW_FLIGHT=ON \
        -DARROW_FLIGHT_SQL=ON \
        -DBoost_USE_STATIC_RUNTIME=ON \
        -DARROW_GFLAGS_USE_SHARED=OFF \
        -Dgflags_ROOT="${TP_INSTALL_DIR}" \
        -Dglog_ROOT="${TP_INSTALL_DIR}" \
        -Dre2_ROOT="${TP_INSTALL_DIR}" \
        -DZLIB_SOURCE=SYSTEM \
        -DZLIB_LIBRARY="${TP_INSTALL_DIR}/lib/libz.a" -DZLIB_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DRapidJSON_SOURCE=SYSTEM \
        -DRapidJSON_ROOT="${TP_INSTALL_DIR}" \
        -Dorc_ROOT="${TP_INSTALL_DIR}" \
        -Dxsimd_SOURCE=BUNDLED \
        -DBrotli_SOURCE=BUNDLED \
        -DARROW_LZ4_USE_SHARED=OFF \
        -DLZ4_ROOT="${TP_INSTALL_DIR};${TP_INSTALL_DIR}/include/lz4" \
        -DLZ4_LIB="${TP_INSTALL_DIR}/lib/liblz4.a" -DLZ4_INCLUDE_DIR="${TP_INSTALL_DIR}/include/lz4" \
        -DLz4_SOURCE=SYSTEM \
        -DARROW_ZSTD_USE_SHARED=OFF \
        -DZSTD_LIB="${TP_INSTALL_DIR}/lib/libzstd.a" -DZSTD_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -Dzstd_SOURCE=SYSTEM \
        -DSnappy_LIB="${TP_INSTALL_DIR}/lib/libsnappy.a" -DSnappy_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DSnappy_SOURCE=SYSTEM \
        -DBoost_ROOT="${TP_INSTALL_DIR}" --no-warn-unused-cli \
        -DARROW_JEMALLOC=OFF -DARROW_MIMALLOC=OFF \
        -DJEMALLOC_HOME="${TP_INSTALL_DIR}" \
        -DARROW_THRIFT_USE_SHARED=OFF \
        -DThrift_SOURCE=SYSTEM \
        -DThrift_ROOT="${TP_INSTALL_DIR}" ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    #copy dep libs
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlienc-static.a "${TP_INSTALL_DIR}/lib64/libbrotlienc.a"
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlidec-static.a "${TP_INSTALL_DIR}/lib64/libbrotlidec.a"
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlicommon-static.a "${TP_INSTALL_DIR}/lib64/libbrotlicommon.a"
    strip_lib libarrow.a
    strip_lib libarrow_compute.a
    strip_lib libparquet.a
    strip_lib libarrow_dataset.a
    strip_lib libarrow_acero.a

    publish_arrow_prebuilt_marker "${TP_INSTALL_DIR}"
}

# arrow-adbc
# Produces three artifacts from one source tree:
#   libadbc_driver_manager.a  -- statically linked into doris_be
#   libadbc_driver_jni.so     -- loaded by the FE adbc connector
#   libadbc_driver_sqlite.so  -- BE unit tests only, not shipped
# and installs a fourth that is not built here:
#   libadbc_driver_flightsql.so -- prebuilt, adbc tests only, not shipped
build_arrow_adbc() {
    check_if_source_exist "${ARROW_ADBC_SOURCE}"

    local adbc_src="${TP_SOURCE_DIR}/${ARROW_ADBC_SOURCE}"

    # The SQLite driver needs a SQLite3 development package, which Doris does not
    # ship and most build hosts lack. arrow-adbc vendors the amalgamation, so build
    # it here into a scratch prefix and hand the paths to FindSQLite3. It ends up
    # statically inside libadbc_driver_sqlite.so, so nothing sqlite is installed.
    local sqlite_host="${adbc_src}/c/${BUILD_DIR}-sqlite-host"
    rm -rf "${sqlite_host}"
    mkdir -p "${sqlite_host}/include" "${sqlite_host}/lib"
    "${CC}" -O2 -fPIC -DSQLITE_ENABLE_COLUMN_METADATA=1 \
        -c "${adbc_src}/c/vendor/sqlite3/sqlite3.c" -o "${sqlite_host}/sqlite3.o"
    ar rcs "${sqlite_host}/lib/libsqlite3.a" "${sqlite_host}/sqlite3.o"
    cp -f "${adbc_src}/c/vendor/sqlite3/sqlite3.h" "${sqlite_host}/include/"

    # (1) driver manager + sqlite driver
    cd "${adbc_src}/c"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DADBC_DRIVER_MANAGER=ON \
        -DADBC_DRIVER_SQLITE=ON \
        -DADBC_BUILD_STATIC=ON \
        -DADBC_BUILD_SHARED=ON \
        -DADBC_BUILD_TESTS=OFF \
        -DADBC_USE_CCACHE=OFF \
        -DSQLite3_INCLUDE_DIR="${sqlite_host}/include" \
        -DSQLite3_LIBRARY="${sqlite_host}/lib/libsqlite3.a" \
        ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    # (2) JNI bridge for the FE. Must come after (1): it links the driver manager.
    #     Built directly rather than through upstream's java/CMakeLists.txt, which
    #     shells out to Maven just to generate the JNI header; that header is
    #     checked in as a patch instead (see thirdparty/patches). Also not taken
    #     from the Maven jar: the prebuilt binary there requires GLIBC_2.34 and
    #     GLIBCXX_3.4.31, which excludes CentOS 7/8, Rocky 8, Ubuntu 20.04 and more.
    #     Only jni.h is needed, so any JDK will do.
    if [[ ! -f "${JAVA_HOME}/include/jni.h" ]]; then
        echo "arrow-adbc: JAVA_HOME must point at a JDK (no ${JAVA_HOME}/include/jni.h)"
        exit 1
    fi
    local jni_md_dir='linux'
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        jni_md_dir='darwin'
    fi

    "${CXX}" -std="c++${TP_CXX_STANDARD}" -O2 -fPIC -shared \
        -I"${JAVA_HOME}/include" \
        -I"${JAVA_HOME}/include/${jni_md_dir}" \
        -I"${adbc_src}/java/driver/jni/doris_generated" \
        -I"${TP_INCLUDE_DIR}" \
        "${adbc_src}/java/driver/jni/src/main/cpp/jni_wrapper.cc" \
        -o "${TP_INSTALL_DIR}/lib64/libadbc_driver_jni.so" \
        "${TP_INSTALL_DIR}/lib64/libadbc_driver_manager.a"

    # (3) Flight SQL driver. Not built: upstream implements it in Go, so it comes
    #     prebuilt out of the release wheel that download-thirdparty.sh unpacked
    #     (see vars.sh). Nothing links against it; the FE and BE dlopen it at run
    #     time, and only the adbc tests ask for it.
    if [[ -n "${ARROW_ADBC_FLIGHTSQL_SOURCE}" ]]; then
        check_if_source_exist "${ARROW_ADBC_FLIGHTSQL_SOURCE}"
        cp -f "${TP_SOURCE_DIR}/${ARROW_ADBC_FLIGHTSQL_SOURCE}/libadbc_driver_flightsql.so" \
            "${TP_INSTALL_DIR}/lib64/libadbc_driver_flightsql.so"
    fi

    rm -rf "${sqlite_host}"
}

# abseil
build_abseil() {
    check_if_source_exist "${ABSEIL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ABSEIL_SOURCE}"

    LDFLAGS="-L${TP_LIB_DIR}" \
        "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -B "${BUILD_DIR}" -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DABSL_ENABLE_INSTALL=ON \
        -DBUILD_DEPS=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DABSL_PROPAGATE_CXX_STD=ON \
        -DBUILD_SHARED_LIBS=OFF

    "${CMAKE_CMD}" --build "${BUILD_DIR}" -j "${PARALLEL}"
    "${CMAKE_CMD}" --install "${BUILD_DIR}" --prefix "${TP_INSTALL_DIR}"
}

# s2
build_s2() {
    check_if_source_exist "${S2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${S2_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    LDFLAGS="-L${TP_LIB_DIR}" \
        ${CMAKE_CMD} -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -G "${GENERATOR}" -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
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
    MACHINE_OS=$(uname -s)

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
        arches=('default' 'neon')
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
        if [[ "${MACHINE_OS}" != "Darwin" ]] && [[ "${arch}" == "neon" ]]; then
            arch_flag="-march=armv8-a+crc"
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
        if [[ "${MACHINE_OS}" != "Darwin" ]] && { [[ "${arch}" == "avx2" ]] || [[ "${arch}" == "avx512" ]] || [[ "${arch}" == "neon" ]]; }; then
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
        "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -G "${GENERATOR}" ${avx_flag:+${avx_flag}} -DROARING_BUILD_STATIC=ON -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
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

    "${CMAKE_CMD}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
     -G "${GENERATOR}" -DBUILD_SHARED_LIBS=FALSE -DFMT_TEST=OFF -DFMT_DOC=OFF -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..
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

# timsort
build_timsort() {
    check_if_archive_exist "${TIMSORT_FILE}"
    cd "${TP_SOURCE_DIR}"
    mkdir -p "${TP_INSTALL_DIR}/include/gfx"
    cp "${TIMSORT_FILE}" "${TP_INSTALL_DIR}/include/gfx/"
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
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
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

    # -Wno-elaborated-enum-base to make C++20 on MacOS happy
    "${CMAKE_CMD}" -G "${GENERATOR}" \
    -DCMAKE_CXX_FLAGS="$CMAKE_CXX_FLAGS -Wno-elaborated-enum-base" \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_TOOLS=OFF \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DBUILD_TESTING=OFF ..
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
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF \
        -DCURL_LIBRARY_RELEASE="${TP_INSTALL_DIR}/lib/libcurl.a" -DZLIB_LIBRARY_RELEASE="${TP_INSTALL_DIR}/lib/libz.a" \
        -DBUILD_ONLY="core;s3;s3-crt;transfer;identity-management;sts;kinesis" \
        -DCMAKE_CXX_FLAGS="-Wno-nonnull -Wno-deprecated-literal-operator ${warning_deprecated_literal_operator} -Wno-deprecated-declarations ${warning_dangling_reference}" -DCPP_STANDARD=17

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
    strip_lib libaws-cpp-sdk-identity-management.a
    strip_lib libaws-cpp-sdk-sts.a
    strip_lib libaws-cpp-sdk-kinesis.a
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

    CFLAGS="-fcommon -fPIC -I${TP_INSTALL_DIR}/include -std=gnu89" LDFLAGS="-L${TP_INSTALL_DIR}/lib" \
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
    "${CMAKE_CMD}" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
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
build_jemalloc_doris() {
    check_if_source_exist "${JEMALLOC_DORIS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${JEMALLOC_DORIS_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    cflags='-O3 -fno-omit-frame-pointer -fPIC -g'
    # Build jemalloc --with-lg-page=16 in order to make the wheel work on both 4k and 64k page arm64 systems.
    # Jemalloc compiled on a system with page size 4K can only run on a system with the same page size 4K.
    # If it is run on a system with page size > 4K, an error `unsupported system page size`.
    # Jemalloc compiled on a system with page size 64K can run on a system with page size < 64K,
    # but this will waste more memory. Jemalloc does not support dynamic adaptation to the page size of the system.
    # The reason is that jemalloc will perform some optimizations based on the page size when compiling.
    if [[ "${MACHINE_TYPE}" == "aarch64" || "${MACHINE_TYPE}" == 'arm64' ]]; then
        WITH_LG_PAGE='--with-lg-page=16'
    else
        WITH_LG_PAGE=''
    fi

    # It is not easy to remove `with-jemalloc-prefix`, which may affect the compatibility between third-party and old version codes.
    # Also, will building failed on Mac, it said can't find mallctl symbol. because jemalloc's default prefix on macOS is "je_", not "".
    # Maybe can use alias instead of overwrite.
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        # Doris does not build GNU libunwind on macOS, and Apple/LLVM libunwind does not provide
        # jemalloc's required unw_backtrace symbol. Keep macOS on its original profiler backtrace
        # path instead of forcing a Linux-only libunwind configuration.
        CFLAGS="${cflags}" \
            ../configure --prefix="${TP_INSTALL_DIR}" \
            --with-install-suffix="_doris" "${WITH_LG_PAGE}" \
            --with-jemalloc-prefix=je --enable-prof \
            --disable-cxx --disable-libdl --disable-shared
    else
        CPPFLAGS="-I${TP_INCLUDE_DIR}" CFLAGS="${cflags}" LDFLAGS="-L${TP_LIB_DIR}" \
            LIBS="-llzma -lz" \
            ../configure --prefix="${TP_INSTALL_DIR}" \
            --with-install-suffix="_doris" "${WITH_LG_PAGE}" \
            --with-jemalloc-prefix=je --enable-prof --enable-prof-libunwind \
            --disable-prof-libgcc --disable-cxx --disable-libdl --disable-shared

        # The stack trace API redirects dl_iterate_phdr to a PHDR cache. On glibc platforms,
        # jemalloc heap profiling must not silently fall back to libgcc's _Unwind_Backtrace path,
        # because that path can re-enter the loader-lock implementation while a sampled target
        # thread is interrupted.
        if ! grep -qE "result: prof-libunwind +: 1$" config.log; then
            echo "ERROR: jemalloc prof-libunwind is not enabled; refusing libgcc-backed heap profiles." >&2
            grep -E "result: prof-(libunwind|libgcc|gcc) +:" config.log >&2 || true
            exit 1
        fi
        if grep -qE "result: prof-libgcc +: 1$" config.log; then
            echo "ERROR: jemalloc prof-libgcc is enabled; heap profiling must use libunwind only." >&2
            grep -E "result: prof-(libunwind|libgcc|gcc) +:" config.log >&2 || true
            exit 1
        fi
    fi

    make -j "${PARALLEL}"
    make install
    mv "${TP_INCLUDE_DIR}/jemalloc/jemalloc_doris.h" "${TP_INCLUDE_DIR}/jemalloc/jemalloc.h"
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
        cflags="-I${TP_INCLUDE_DIR} -std=c99 -D_LIBUNWIND_NO_HEAP=1 -D_DEBUG -D_LIBUNWIND_IS_NATIVE_ONLY -O3 -fno-exceptions -funwind-tables -fno-sanitize=all -nostdinc++ -fno-rtti -Wno-error=incompatible-pointer-types"
        CFLAGS="${cflags}" LDFLAGS="-L${TP_LIB_DIR} -llzma" ../configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static

        make -j "${PARALLEL}"
        make install
    fi
}

# benchmark
build_benchmark() {
    check_if_source_exist "${BENCHMARK_SOURCE}"

    cd "${TP_SOURCE_DIR}/${BENCHMARK_SOURCE}"

    "${CMAKE_CMD}" -E make_directory "build"

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
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
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

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" -DJSON_BuildTests=OFF ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
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
        --enable-install-libiberty --without-msgpack -with-system-zlib
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

# hadoop_libs_3_4
build_hadoop_libs_3_4() {
    check_if_source_exist "${HADOOP_LIBS_3_4_SOURCE}"
    cd "${TP_SOURCE_DIR}/${HADOOP_LIBS_3_4_SOURCE}"
    echo "THIRDPARTY_INSTALLED=${TP_INSTALL_DIR}" >env.sh
    ./build.sh

    rm -rf "${TP_INSTALL_DIR}/include/hadoop_hdfs_3_4/"
    rm -rf "${TP_INSTALL_DIR}/lib/hadoop_hdfs_3_4/"
    mkdir -p "${TP_INSTALL_DIR}/include/hadoop_hdfs_3_4/"
    mkdir -p "${TP_INSTALL_DIR}/lib/hadoop_hdfs_3_4/"
    cp -r ./hadoop-dist/target/hadoop-libhdfs-3.4.2/* "${TP_INSTALL_DIR}/lib/hadoop_hdfs_3_4/"
    cp -r ./hadoop-dist/target/hadoop-libhdfs-3.4.2/include/hdfs.h "${TP_INSTALL_DIR}/include/hadoop_hdfs_3_4/"
    rm -rf "${TP_INSTALL_DIR}/lib/hadoop_hdfs_3_4/native/*.a"
    find ./hadoop-dist/target/hadoop-3.4.2/lib/native/ -type f ! -name '*.a' -exec cp {} "${TP_INSTALL_DIR}/lib/hadoop_hdfs_3_4/native/" \;
    find ./hadoop-dist/target/hadoop-3.4.2/lib/native/ -type l -exec cp -P {} "${TP_INSTALL_DIR}/lib/hadoop_hdfs_3_4/native/" \;

    # 3.3.6.6 installed this same layout under hadoop_hdfs/, and that prefix is what
    # branch-3.0, branch-3.1, the cloud module and anything outside this tree still
    # include and link. Only 3.4.2.4 is built now, so point the old name at it rather
    # than ship a second 182MB copy per platform. Relative target, so the prebuilt
    # archive stays relocatable - the same shape as the lib -> lib64 link the install
    # prefix is set up with.
    #
    # No trailing slash: `rm -rf link/` deletes what the link points at on BSD rm and
    # does nothing on GNU rm, while `rm -rf link` removes just the link everywhere.
    # The removal has to run first - `ln -s` against an existing real directory would
    # land inside it instead of replacing it.
    rm -rf "${TP_INSTALL_DIR}/include/hadoop_hdfs" "${TP_INSTALL_DIR}/lib/hadoop_hdfs"
    ln -sfn hadoop_hdfs_3_4 "${TP_INSTALL_DIR}/include/hadoop_hdfs"
    ln -sfn hadoop_hdfs_3_4 "${TP_INSTALL_DIR}/lib/hadoop_hdfs"
}

# AvxToNeon
build_avx2neon() {
    check_if_source_exist "${AVX2NEON_SOURCE}"
    cd "${TP_SOURCE_DIR}/${AVX2NEON_SOURCE}"
    mkdir -p "${TP_INSTALL_DIR}/include/avx2neon/"
    cp -r ./* "${TP_INSTALL_DIR}/include/avx2neon/"
}

# libdeflate
build_libdeflate() {
    check_if_source_exist "${LIBDEFLATE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LIBDEFLATE_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# streamvbyte
build_streamvbyte() {
    check_if_source_exist "${STREAMVBYTE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${STREAMVBYTE_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# jsoncpp
build_jsoncpp() {
    check_if_source_exist "${JSONCPP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${JSONCPP_SOURCE}"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DJSONCPP_WITH_TESTS=OFF -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# libuuid
build_libuuid() {
    check_if_source_exist "${LIBUUID_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LIBUUID_SOURCE}"
    CC=gcc ./configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static
    make -j "${PARALLEL}" CFLAGS="-fPIC"
    make install
}

# ali_sdk
build_ali_sdk() {
    build_jsoncpp
    build_libuuid
    check_if_source_exist "${ALI_SDK_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ALI_SDK_SOURCE}"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        CXXFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DBUILD_PRODUCT=core -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DTP_INSTALL_DIR="${TP_INSTALL_DIR}" ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# base64
build_base64() {
    check_if_source_exist "${BASE64_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BASE64_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release ..
    MACHINE_TYPE="$(uname -m)"
    if [[ "${MACHINE_TYPE}" == "aarch64" || "${MACHINE_TYPE}" == 'arm64' ]]; then
        CFLAGS="--target=aarch64-linux-gnu -march=armv8-a+crc" NEON64_CFLAGS=" "
    else
        AVX2_CFLAGS=-mavx2 SSSE3_CFLAGS=-mssse3 SSE41_CFLAGS=-msse4.1 SSE42_CFLAGS=-msse4.2 AVX_CFLAGS=-mavx
    fi
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# azure blob storage
build_azure() {
    if [[ "${BUILD_AZURE}" == "OFF" ]]; then
        echo "Skip build azure"
        return
    fi

    check_if_source_exist "${AZURE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${AZURE_SOURCE}"
    azure_dir="$(pwd)"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    # We need use openssl 1.1.1n, which is already carried in vcpkg-custom-ports
    AZURE_PORTS="vcpkg-custom-ports"
    AZURE_MANIFEST_DIR="."

    local azure_machine_type
    local vcpkg_arch
    azure_machine_type="$(uname -m)"
    case "${azure_machine_type}" in
    aarch64 | arm64)
        vcpkg_arch='arm64'
        ;;
    x86_64 | amd64)
        vcpkg_arch='x64'
        ;;
    *)
        echo "azure: unsupported machine type ${azure_machine_type}" >&2
        exit 1
        ;;
    esac

    # vcpkg builds every port twice, debug and release, and installs both. Doris only
    # ever links the release halves, and VCPKG_BUILD_TYPE - the only supported way to
    # ask for release only - can be set from a triplet file, so shadow the built-in
    # triplet with our own. Naming the file after the built-in triplet is what makes
    # the overlay take precedence.
    local vcpkg_triplet
    local vcpkg_triplet_dir="${PWD}/doris-vcpkg-triplets"
    mkdir -p "${vcpkg_triplet_dir}"
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        local vcpkg_osx_arch='x86_64'
        if [[ "${vcpkg_arch}" == 'arm64' ]]; then vcpkg_osx_arch='arm64'; fi
        vcpkg_triplet="${vcpkg_arch}-osx"
        cat >"${vcpkg_triplet_dir}/${vcpkg_triplet}.cmake" <<EOF
set(VCPKG_TARGET_ARCHITECTURE ${vcpkg_arch})
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES ${vcpkg_osx_arch})
set(VCPKG_BUILD_TYPE release)
EOF
        if [[ -n "${MACOSX_DEPLOYMENT_TARGET}" ]]; then
            echo "set(VCPKG_OSX_DEPLOYMENT_TARGET \"${MACOSX_DEPLOYMENT_TARGET}\")" \
                >>"${vcpkg_triplet_dir}/${vcpkg_triplet}.cmake"
        fi
    else
        vcpkg_triplet="${vcpkg_arch}-linux"
        cat >"${vcpkg_triplet_dir}/${vcpkg_triplet}.cmake" <<EOF
set(VCPKG_TARGET_ARCHITECTURE ${vcpkg_arch})
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_BUILD_TYPE release)
EOF
    fi

    # vcpkg ships no prebuilt cmake/ninja/curl for aarch64 Linux, so it has to reuse
    # the ones already on PATH.
    if [[ "${vcpkg_arch}" == 'arm64' && "${KERNEL}" != 'Darwin' ]]; then
        export VCPKG_FORCE_SYSTEM_BINARIES=1
    fi

    # libcrypto.a needs dlopen/dlsym/dlclose/dlerror, and with clang find_library may
    # not turn up libdl, so ask for it explicitly. Apple has no libdl - those symbols
    # live in libSystem - and -ldl would fail the link there.
    local azure_link_flags=()
    if [[ "${KERNEL}" != 'Darwin' ]]; then
        azure_link_flags=(-DCMAKE_EXE_LINKER_FLAGS="-ldl" -DCMAKE_SHARED_LINKER_FLAGS="-ldl")
    fi

    # vcpkg fetches the sources of curl, libxml2, openssl and zlib from their upstream
    # hosts while cmake configures, and none of that goes through download-thirdparty.sh
    # and its mirror. Keep those tarballs outside "${BUILD_DIR}", which was wiped above,
    # so a retry - or a later run in the same tree - only fetches what the attempt
    # before it missed. "thirdparty/src*" is already gitignored.
    VCPKG_DOWNLOADS="${TP_SOURCE_DIR}/vcpkg-downloads"
    export VCPKG_DOWNLOADS
    mkdir -p "${VCPKG_DOWNLOADS}"

    # vcpkg's own retry is three attempts inside one second, which rides out nothing:
    # in apache/doris run 32032837067 all three jobs died on
    # github.com/madler/zlib/archive/v1.3.1.tar.gz answering 429 (500 on macOS arm64),
    # and because azure is the last package this script builds, each of them threw away
    # a finished tree over one file. So wait out a rate limit window instead. Only a
    # download is worth waiting on - a port that will not compile, or a bad option,
    # fails identically every time - so the configure is retried on nothing else, and
    # the ports that did build come back from vcpkg's binary cache, which lives outside
    # this directory. cmake is piped through tee rather than redirected so that a
    # vcpkg install that takes twenty minutes still shows progress; this script runs
    # under `set -o pipefail`, so the status tested below is cmake's, not tee's.
    #
    # DISABLE_AMQP and DISABLE_AZURE_CORE_OPENTELEMETRY are already the patched
    # defaults; passing them here keeps the reason visible from the build script.
    local azure_attempt
    local azure_attempts=5
    local azure_backoff
    local azure_log="${PWD}/doris-azure-configure.log"
    for ((azure_attempt = 1; azure_attempt <= azure_attempts; azure_attempt++)); do
        if "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DCMAKE_CXX_FLAGS="-Wno-maybe-uninitialized" \
            "${azure_link_flags[@]}" \
            -DVCPKG_TARGET_TRIPLET="${vcpkg_triplet}" \
            -DVCPKG_OVERLAY_TRIPLETS="${vcpkg_triplet_dir}" \
            -DDISABLE_RUST_IN_BUILD=ON -DDISABLE_AMQP=ON -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON \
            -DBUILD_TESTING=OFF -DBUILD_SAMPLES=OFF -DBUILD_PERFORMANCE_TESTS=OFF \
            -DVCPKG_MANIFEST_MODE=ON -DVCPKG_OVERLAY_PORTS="${azure_dir}/${AZURE_PORTS}" -DVCPKG_MANIFEST_DIR="${azure_dir}/${AZURE_MANIFEST_DIR}" -DWARNINGS_AS_ERRORS=FALSE -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release .. 2>&1 | tee "${azure_log}"; then
            break
        fi

        if ! grep -qE 'Download failed, halting portfile|error: curl: \(' "${azure_log}"; then
            echo "azure: cmake configure failed, and not on a download - see above" >&2
            exit 1
        fi

        if [[ "${azure_attempt}" -eq "${azure_attempts}" ]]; then
            echo "azure: vcpkg could not download its sources in ${azure_attempts} attempts" >&2
            exit 1
        fi

        # A configure that died inside the vcpkg toolchain file leaves a cache with no
        # compiler in it, and cmake would report that instead of running vcpkg again.
        rm -rf CMakeCache.txt CMakeFiles

        azure_backoff=$((azure_attempt * 120))
        echo "azure: vcpkg could not download a source, retrying in ${azure_backoff}s" \
            "(attempt $((azure_attempt + 1)) of ${azure_attempts})" >&2
        sleep "${azure_backoff}"
    done
    rm -f "${azure_log}"

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# dragonbox
build_dragonbox() {
    check_if_source_exist "${DRAGONBOX_SOURCE}"
    cd "${TP_SOURCE_DIR}/${DRAGONBOX_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DDRAGONBOX_INSTALL_TO_CHARS=ON ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

# icu
build_icu() {
    check_if_source_exist "${ICU_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ICU_SOURCE}/icu4c/source"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    ../configure --prefix="${TP_INSTALL_DIR}" \
        --enable-static \
        --disable-shared \
        --enable-release \
        --disable-tests \
        --disable-samples \
        --disable-fuzzer

    make -j "${PARALLEL}"
    make install
}

# jindofs
build_jindofs() {
    check_if_source_exist "${JINDOFS_SOURCE}"

    rm -rf "${TP_INSTALL_DIR}/jindofs_libs/"
    mkdir -p "${TP_INSTALL_DIR}/jindofs_libs/"
    cp -r ${TP_SOURCE_DIR}/${JINDOFS_SOURCE}/* "${TP_INSTALL_DIR}/jindofs_libs/"
}

# juicefs
build_juicefs() {
    check_if_archive_exist "${JUICEFS_NAME}"

    rm -rf "${TP_INSTALL_DIR}/juicefs_libs/"
    mkdir -p "${TP_INSTALL_DIR}/juicefs_libs/"
    cp -r "${TP_SOURCE_DIR}/${JUICEFS_NAME}" "${TP_INSTALL_DIR}/juicefs_libs/"
}

# pugixml
build_pugixml() {
    check_if_source_exist "${PUGIXML_SOURCE}"
    cd "${TP_SOURCE_DIR}/${PUGIXML_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" -G "${GENERATOR}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    cp "${TP_SOURCE_DIR}/${PUGIXML_SOURCE}/src/pugixml.hpp" "${TP_INSTALL_DIR}/include/"
    cp "${TP_SOURCE_DIR}/${PUGIXML_SOURCE}/src/pugiconfig.hpp" "${TP_INSTALL_DIR}/include/"
}

# paimon-cpp
build_paimon_cpp() {
    check_if_source_exist "${PAIMON_CPP_SOURCE}"
    require_arrow_prebuilt_for_paimon "${TP_INSTALL_DIR}"
    invalidate_paimon_prebuilt_marker "${TP_INSTALL_DIR}"
    cd "${TP_SOURCE_DIR}/${PAIMON_CPP_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    # Darwin doesn't build GNU libunwind in this script, so don't force -lunwind there.
    local paimon_linker_flags="-L${TP_LIB_DIR} -lbrotlienc -lbrotlidec -lbrotlicommon -llzma"
    if [[ "${KERNEL}" != 'Darwin' ]]; then
        paimon_linker_flags="${paimon_linker_flags} -lunwind"
    fi

    CXXFLAGS="-Wno-nontrivial-memcall" \
    "${CMAKE_CMD}" -C "${TP_DIR}/paimon-cpp-cache.cmake" \
        -G "${GENERATOR}" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_CXX_STANDARD="${TP_CXX_STANDARD}" \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DPAIMON_BUILD_SHARED=OFF \
        -DPAIMON_BUILD_STATIC=ON \
        -DPAIMON_BUILD_TESTS=OFF \
        -DPAIMON_ENABLE_ORC=ON \
        -DPAIMON_ENABLE_AVRO=OFF \
        -DPAIMON_ENABLE_LANCE=OFF \
        -DPAIMON_ENABLE_JINDO=OFF \
        -DPAIMON_ENABLE_LUMINA=OFF \
        -DPAIMON_ENABLE_LUCENE=OFF \
        -DCMAKE_EXE_LINKER_FLAGS="${paimon_linker_flags}" \
        -DCMAKE_SHARED_LINKER_FLAGS="${paimon_linker_flags}" \
        ..
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install

    # Install paimon-cpp internal dependencies with renamed versions
    # These libraries are built but not installed by default
    echo "Installing paimon-cpp internal dependencies..."

    # Arrow deps: When PAIMON_USE_EXTERNAL_ARROW=ON (Plan B), paimon-cpp
    # reuses Doris's Arrow and does NOT build arrow_ep, so the paimon_deps
    # directory is not needed.  When building its own Arrow (legacy), copy
    # arrow artefacts into an isolated directory to avoid clashing with Doris.
    local paimon_deps_dir="${TP_INSTALL_DIR}/paimon-cpp/lib64/paimon_deps"
    if [ -d "arrow_ep-install/lib" ]; then
        mkdir -p "${paimon_deps_dir}"
        for paimon_arrow_dep in \
            libarrow.a \
            libarrow_compute.a \
            libarrow_filesystem.a \
            libarrow_dataset.a \
            libarrow_acero.a \
            libparquet.a; do
            if [ -f "arrow_ep-install/lib/${paimon_arrow_dep}" ]; then
                cp -v "arrow_ep-install/lib/${paimon_arrow_dep}" "${paimon_deps_dir}/${paimon_arrow_dep}"
            fi
        done
    else
        echo "  arrow_ep-install not found (PAIMON_USE_EXTERNAL_ARROW=ON?) – skipping paimon_deps Arrow copy"
    fi

    # Install roaring_bitmap, renamed to avoid conflict with Doris's croaringbitmap
    if [ -f "release/libroaring_bitmap.a" ]; then
        cp -v "release/libroaring_bitmap.a" "${TP_INSTALL_DIR}/lib64/libroaring_bitmap_paimon.a"
    fi

    # Install xxhash, renamed to avoid conflict with Doris's xxhash
    if [ -f "release/libxxhash.a" ]; then
        cp -v "release/libxxhash.a" "${TP_INSTALL_DIR}/lib64/libxxhash_paimon.a"
    fi

    # Install fmt v11 (from fmt_ep-install directory, renamed to avoid conflict with Doris's fmt v7)
    if [ -f "fmt_ep-install/lib/libfmt.a" ]; then
        cp -v "fmt_ep-install/lib/libfmt.a" "${TP_INSTALL_DIR}/lib64/libfmt_paimon.a"
    fi

    # Install tbb (from tbb_ep-install directory, renamed to avoid conflict with Doris's tbb)
    if [ -f "tbb_ep-install/lib/libtbb.a" ]; then
        cp -v "tbb_ep-install/lib/libtbb.a" "${TP_INSTALL_DIR}/lib64/libtbb_paimon.a"
    fi

    echo "Paimon-cpp internal dependencies installed successfully"
    publish_paimon_prebuilt_marker "${TP_INSTALL_DIR}"
}

# lance-c
build_lance_c() {
    check_if_source_exist "${LANCE_C_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LANCE_C_SOURCE}"

    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"

    local cargo_bin="${LANCE_C_CARGO:-${CARGO:-cargo}}"
    if ! command -v "${cargo_bin}" >/dev/null 2>&1; then
        echo "cargo is required to build lance-c. Install Rust 1.91.0 or set LANCE_C_CARGO."
        exit 1
    fi
    if [[ ! -x "${TP_INSTALL_DIR}/bin/protoc" ]]; then
        echo "protoc is required to build lance-c. Build protobuf first."
        exit 1
    fi

    local required_rust_version="1.91.0"
    local cargo_env=(
        "CARGO_BUILD_JOBS=${PARALLEL}"
        "CARGO_TARGET_DIR=${PWD}/${BUILD_DIR}"
        "PROTOC=${TP_INSTALL_DIR}/bin/protoc"
    )
    if command -v rustup >/dev/null 2>&1 && [[ -z "${RUSTUP_TOOLCHAIN}" ]]; then
        if ! rustup toolchain list | grep -Eq '^1\.91\.0([[:space:]-]|$)'; then
            rustup toolchain install "${required_rust_version}" --profile minimal
        fi
        cargo_env+=("RUSTUP_TOOLCHAIN=${required_rust_version}")
    fi

    local cargo_version
    if ! cargo_version="$(env "${cargo_env[@]}" "${cargo_bin}" --version | awk '{print $2}')"; then
        echo "failed to get cargo version for lance-c. Install Rust ${required_rust_version} or set LANCE_C_CARGO/RUSTUP_TOOLCHAIN."
        exit 1
    fi
    if [[ "${cargo_version}" != "${required_rust_version}" ]]; then
        echo "lance-c requires Rust/Cargo ${required_rust_version}, but found ${cargo_version}."
        echo "Install Rust ${required_rust_version} or set LANCE_C_CARGO/RUSTUP_TOOLCHAIN."
        exit 1
    fi

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        cargo_env+=("CFLAGS=${CFLAGS:-} -std=gnu17")
    fi

    local cargo_args=(build --release --locked)
    if [[ "$(echo "${LANCE_C_CARGO_OFFLINE}" | tr '[:lower:]' '[:upper:]')" == "ON" ]]; then
        cargo_args+=(--offline)
    fi
    env "${cargo_env[@]}" "${cargo_bin}" "${cargo_args[@]}"

    mkdir -p "${TP_INSTALL_DIR}/include" "${TP_INSTALL_DIR}/lib64"
    rm -rf "${TP_INSTALL_DIR}/include/lance"
    cp -av include/lance "${TP_INSTALL_DIR}/include/"
    cp -v "${BUILD_DIR}/release/liblance_c.a" "${TP_INSTALL_DIR}/lib64/"

    if [[ "${STRIP_TP_LIB}" = "ON" && "${KERNEL}" != 'Darwin' ]]; then
        strip --strip-debug --strip-unneeded "${TP_INSTALL_DIR}/lib64/liblance_c.a"
    fi
}

if [[ "${#packages[@]}" -eq 0 ]]; then
    packages=(
        jindofs
        juicefs
        odbc
        openssl
        libevent
        zlib
        crc32c
        lz4
        bzip
        lzo2
        zstd
        boost # must before thrift
        abseil
        gflags
        gtest
        glog
        protobuf # after gtest
        rapidjson
        snappy
        gperftools
        curl
        re2
        hyperscan
        thrift
        leveldb
        brpc
        lzma
        libunwind
        jemalloc_doris
        rocksdb
        krb5 # before cyrus_sasl
        cyrus_sasl
        librdkafka
        flatbuffers
        orc
        cares
        grpc # after cares, protobuf
        arrow
        arrow_adbc
        lance_c
        s2
        bitshuffle
        croaringbitmap
        fmt
        parallel_hashmap
        pdqsort
        timsort
        libdivide
        cctz
        tsan_header
        mysql
        aws_sdk
        js_and_css
        xml2
        idn
        gsasl
        hdfs3
        benchmark
        simdjson
        nlohmann_json
        libbacktrace
        sse2neon
        xxhash
        concurrentqueue
        fast_float
        avx2neon
        libdeflate
        streamvbyte
        ali_sdk
        base64
        azure
        brotli
        icu
        pugixml
        paimon_cpp
    )
    if [[ "$(uname -s)" == 'Darwin' ]]; then
        read -r -a packages <<<"binutils gettext ${packages[*]}"
    fi
    # hadoop_libs_3_4 runs last on every platform: its native build links against
    # what the packages above install into ${TP_INSTALL_DIR}.
    read -r -a packages <<<"${packages[*]} hadoop_libs_3_4"
fi

# Map a package name to its source directory variable(s) and remove them to free disk space.
# This is called after each package is built and installed successfully.
cleanup_package_source() {
    local pkg="$1"
    local src_var
    local src_dir

    # Map package name to the uppercase *_SOURCE variable name
    case "${pkg}" in
        libevent)        src_var="LIBEVENT_SOURCE" ;;
        openssl)         src_var="OPENSSL_SOURCE" ;;
        thrift)          src_var="THRIFT_SOURCE" ;;
        protobuf)        src_var="PROTOBUF_SOURCE" ;;
        gflags)          src_var="GFLAGS_SOURCE" ;;
        glog)            src_var="GLOG_SOURCE" ;;
        gtest)           src_var="GTEST_SOURCE" ;;
        rapidjson)       src_var="RAPIDJSON_SOURCE" ;;
        snappy)          src_var="SNAPPY_SOURCE" ;;
        gperftools)      src_var="GPERFTOOLS_SOURCE" ;;
        zlib)            src_var="ZLIB_SOURCE" ;;
        crc32c)          src_var="CRC32C_SOURCE" ;;
        lz4)             src_var="LZ4_SOURCE" ;;
        bzip)            src_var="BZIP_SOURCE" ;;
        lzo2)            src_var="LZO2_SOURCE" ;;
        zstd)            src_var="ZSTD_SOURCE" ;;
        #boost)           src_var="BOOST_SOURCE" ;; // boost is used for mysql later
        abseil)          src_var="ABSEIL_SOURCE" ;;
        curl)            src_var="CURL_SOURCE" ;;
        re2)             src_var="RE2_SOURCE" ;;
        hyperscan)
            # hyperscan also builds ragel, clean both
            if [[ -n "${RAGEL_SOURCE}" && -d "${TP_SOURCE_DIR}/${RAGEL_SOURCE}" ]]; then
                echo "Cleaning up source: ${RAGEL_SOURCE}"
                rm -rf "${TP_SOURCE_DIR}/${RAGEL_SOURCE}"
            fi
            src_var="HYPERSCAN_SOURCE"
            ;;
        mysql)           src_var="MYSQL_SOURCE" ;;
        odbc)            src_var="ODBC_SOURCE" ;;
        leveldb)         src_var="LEVELDB_SOURCE" ;;
        brpc)            src_var="BRPC_SOURCE" ;;
        rocksdb)         src_var="ROCKSDB_SOURCE" ;;
        cyrus_sasl)      src_var="CYRUS_SASL_SOURCE" ;;
        librdkafka)      src_var="LIBRDKAFKA_SOURCE" ;;
        flatbuffers)     src_var="FLATBUFFERS_SOURCE" ;;
        arrow)           src_var="ARROW_SOURCE" ;;
        arrow_adbc)
            # arrow_adbc also unpacks the prebuilt flightsql driver, clean both
            if [[ -n "${ARROW_ADBC_FLIGHTSQL_SOURCE}" && -d "${TP_SOURCE_DIR}/${ARROW_ADBC_FLIGHTSQL_SOURCE}" ]]; then
                echo "Cleaning up source: ${ARROW_ADBC_FLIGHTSQL_SOURCE}"
                rm -rf "${TP_SOURCE_DIR}/${ARROW_ADBC_FLIGHTSQL_SOURCE}" \
                    "${TP_SOURCE_DIR}/${ARROW_ADBC_FLIGHTSQL_SOURCE}"-*.dist-info
            fi
            src_var="ARROW_ADBC_SOURCE"
            ;;
        brotli)          src_var="BROTLI_SOURCE" ;;
        cares)           src_var="CARES_SOURCE" ;;
        grpc)            src_var="GRPC_SOURCE" ;;
        s2)              src_var="S2_SOURCE" ;;
        bitshuffle)      src_var="BITSHUFFLE_SOURCE" ;;
        croaringbitmap)  src_var="CROARINGBITMAP_SOURCE" ;;
        fmt)             src_var="FMT_SOURCE" ;;
        parallel_hashmap) src_var="PARALLEL_HASHMAP_SOURCE" ;;
        orc)             src_var="ORC_SOURCE" ;;
        cctz)            src_var="CCTZ_SOURCE" ;;
        jemalloc_doris)  src_var="JEMALLOC_DORIS_SOURCE" ;;
        libunwind)       src_var="LIBUNWIND_SOURCE" ;;
        benchmark)       src_var="BENCHMARK_SOURCE" ;;
        simdjson)        src_var="SIMDJSON_SOURCE" ;;
        nlohmann_json)   src_var="NLOHMANN_JSON_SOURCE" ;;
        libbacktrace)    src_var="LIBBACKTRACE_SOURCE" ;;
        sse2neon)        src_var="SSE2NEON_SOURCE" ;;
        xxhash)          src_var="XXHASH_SOURCE" ;;
        concurrentqueue) src_var="CONCURRENTQUEUE_SOURCE" ;;
        fast_float)      src_var="FAST_FLOAT_SOURCE" ;;
        hadoop_libs_3_4) src_var="HADOOP_LIBS_3_4_SOURCE" ;;
        avx2neon)        src_var="AVX2NEON_SOURCE" ;;
        libdeflate)      src_var="LIBDEFLATE_SOURCE" ;;
        streamvbyte)     src_var="STREAMVBYTE_SOURCE" ;;
        ali_sdk)
            # ali_sdk internally builds jsoncpp and libuuid, clean all three
            for dep_var in JSONCPP_SOURCE LIBUUID_SOURCE ALI_SDK_SOURCE; do
                dep_dir="${!dep_var}"
                if [[ -n "${dep_dir}" && -d "${TP_SOURCE_DIR}/${dep_dir}" ]]; then
                    echo "Cleaning up source: ${dep_dir}"
                    rm -rf "${TP_SOURCE_DIR}/${dep_dir}"
                fi
            done
            return
            ;;
        base64)          src_var="BASE64_SOURCE" ;;
        azure)           src_var="AZURE_SOURCE" ;;
        dragonbox)       src_var="DRAGONBOX_SOURCE" ;;
        icu)             src_var="ICU_SOURCE" ;;
        jindofs)         src_var="JINDOFS_SOURCE" ;;
        juicefs)         src_var="JUICEFS_SOURCE" ;;
        pugixml)         src_var="PUGIXML_SOURCE" ;;
        paimon_cpp)      src_var="PAIMON_CPP_SOURCE" ;;
        lance_c)         src_var="LANCE_C_SOURCE" ;;
        aws_sdk)         src_var="AWS_SDK_SOURCE" ;;
        lzma)            src_var="LZMA_SOURCE" ;;
        xml2)            src_var="XML2_SOURCE" ;;
        idn)             src_var="IDN_SOURCE" ;;
        gsasl)           src_var="GSASL_SOURCE" ;;
        krb5)            src_var="KRB5_SOURCE" ;;
        hdfs3)           src_var="HDFS3_SOURCE" ;;
        libdivide)       src_var="LIBDIVIDE_SOURCE" ;;
        binutils)        src_var="BINUTILS_SOURCE" ;;
        gettext)         src_var="GETTEXT_SOURCE" ;;
        # Header-only files, skip cleanup
        pdqsort|timsort|tsan_header|js_and_css)
            return
            ;;
        *)
            echo "Warning: no source mapping for package '${pkg}', skipping cleanup"
            return
            ;;
    esac

    src_dir="${!src_var}"
    if [[ -n "${src_dir}" && -d "${TP_SOURCE_DIR}/${src_dir}" ]]; then
        echo "Cleaning up source: ${src_dir}"
        rm -rf "${TP_SOURCE_DIR}/${src_dir}"
    fi
}

for package in "${packages[@]}"; do
    if [[ "${package}" == "${start_package}" ]]; then
        PACKAGE_FOUND=1
    fi
    if [[ "${CONTINUE}" -eq 0 ]] || [[ "${PACKAGE_FOUND}" -eq 1 ]]; then
        command="build_${package}"
        ${command}
        cd "${TP_DIR}"
        cleanup_package_source "${package}"
        echo "debug after clean: ${package}"
        df -h
        du -sh "${TP_DIR}"
    fi
done

echo "Finished to build all thirdparties"
