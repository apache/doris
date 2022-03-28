#!/bin/bash
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

set -e
################################################################
# This script will download all thirdparties and java libraries
# which are defined in *vars.sh*, unpack patch them if necessary.
# You can run this script multi-times.
# Things will only be downloaded, unpacked and patched once.
################################################################
curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

if [[ -z "${DORIS_HOME}" ]]; then
    DORIS_HOME=$curdir/..
fi

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . ${DORIS_HOME}/custom_env.sh
fi

if [[ -z "${TP_DIR}" ]]; then
    TP_DIR=$curdir
fi

if [ ! -f ${TP_DIR}/vars.sh ]; then
    echo "vars.sh is missing".
    exit 1
fi
. ${TP_DIR}/vars.sh

mkdir -p ${TP_DIR}/src

md5sum_bin=md5sum
if ! command -v ${md5sum_bin} >/dev/null 2>&1; then
    echo "Warn: md5sum is not installed"
    md5sum_bin=""
fi

md5sum_func() {
    local FILENAME=$1
    local DESC_DIR=$2
    local MD5SUM=$3

    if [ "$md5sum_bin" == "" ]; then
       return 0
    else
       md5=`md5sum "$DESC_DIR/$FILENAME"`
       if [ "$md5" != "$MD5SUM  $DESC_DIR/$FILENAME" ]; then
           echo "$DESC_DIR/$FILENAME md5sum check failed!"
           echo -e "except-md5 $MD5SUM \nactual-md5 $md5"
           return 1
       fi
    fi

    return 0
}

# return 0 if download succeed.
# return 1 if not.
download_func() {
    local FILENAME=$1
    local DOWNLOAD_URL=$2
    local DESC_DIR=$3
    local MD5SUM=$4

    if [ -z "$FILENAME" ]; then
        echo "Error: No file name specified to download"
        exit 1
    fi
    if [ -z "$DOWNLOAD_URL" ]; then
        echo "Error: No download url specified for $FILENAME"
        exit 1
    fi
    if [ -z "$DESC_DIR" ]; then
        echo "Error: No dest dir specified for $FILENAME"
        exit 1
    fi


    local STATUS=1
    for attemp in 1 2; do
        if [ -r "$DESC_DIR/$FILENAME" ]; then
            if md5sum_func $FILENAME $DESC_DIR $MD5SUM; then
                echo "Archive $FILENAME already exist."
                STATUS=0
                break;
            fi
            echo "Archive $FILENAME will be removed and download again."
            rm -f "$DESC_DIR/$FILENAME"
        else
            echo "Downloading $FILENAME from $DOWNLOAD_URL to $DESC_DIR"
            wget --no-check-certificate -q $DOWNLOAD_URL -O $DESC_DIR/$FILENAME
            if [ "$?"x == "0"x ]; then
                if md5sum_func $FILENAME $DESC_DIR $MD5SUM; then
                    STATUS=0
                    echo "Success to download $FILENAME"
                    break;
                fi
                echo "Archive $FILENAME will be removed and download again."
                rm -f "$DESC_DIR/$FILENAME"
            else
                echo "Failed to download $FILENAME. attemp: $attemp"
            fi
        fi
    done

    if [ $STATUS -ne 0 ]; then
        echo "Failed to download $FILENAME"
    fi
    return $STATUS
}

# download thirdparty archives
echo "===== Downloading thirdparty archives..."
for TP_ARCH in ${TP_ARCHIVES[*]}
do
    NAME=$TP_ARCH"_NAME"
    MD5SUM=$TP_ARCH"_MD5SUM"
    if test "x$REPOSITORY_URL" = x; then
        URL=$TP_ARCH"_DOWNLOAD"
        download_func ${!NAME} ${!URL} $TP_SOURCE_DIR ${!MD5SUM}
        if [ "$?"x != "0"x ]; then
            echo "Failed to download ${!NAME}"
            exit 1
        fi
    else
        URL="${REPOSITORY_URL}/${!NAME}"
        download_func ${!NAME} ${URL} $TP_SOURCE_DIR ${!MD5SUM}
        if [ "$?x" != "0x" ]; then
            #try to download from home
            URL=$TP_ARCH"_DOWNLOAD"
            download_func ${!NAME} ${!URL} $TP_SOURCE_DIR ${!MD5SUM}
            if [ "$?"x != "0"x ]; then
                echo "Failed to download ${!NAME}"
                exit 1 # download failed again exit.
            fi
        fi
    fi
done
echo "===== Downloading thirdparty archives...done"

# check if all tp archievs exists
echo "===== Checking all thirdpart archives..."
for TP_ARCH in ${TP_ARCHIVES[*]}
do
    NAME=$TP_ARCH"_NAME"
    if [ ! -r $TP_SOURCE_DIR/${!NAME} ]; then
        echo "Failed to fetch ${!NAME}"
        exit 1
    fi
done
echo "===== Checking all thirdpart archives...done"

# unpacking thirdpart archives
echo "===== Unpacking all thirdparty archives..."
TAR_CMD="tar"
UNZIP_CMD="unzip"
SUFFIX_TGZ="\.(tar\.gz|tgz)$"
SUFFIX_XZ="\.tar\.xz$"
SUFFIX_ZIP="\.zip$"
SUFFIX_BZ2="\.tar\.bz2$"
for TP_ARCH in ${TP_ARCHIVES[*]}
do
    NAME=$TP_ARCH"_NAME"
    SOURCE=$TP_ARCH"_SOURCE"

    if [ -z "${!SOURCE}" ]; then
        continue
    fi

    if [ ! -d $TP_SOURCE_DIR/${!SOURCE} ]; then
        if [[ "${!NAME}" =~ $SUFFIX_TGZ  ]]; then
            echo "$TP_SOURCE_DIR/${!NAME}"
            echo "$TP_SOURCE_DIR/${!SOURCE}"
            if ! $TAR_CMD xzf "$TP_SOURCE_DIR/${!NAME}" -C "$TP_SOURCE_DIR/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ $SUFFIX_XZ ]]; then
            echo "$TP_SOURCE_DIR/${!NAME}"
            echo "$TP_SOURCE_DIR/${!SOURCE}"
            if ! $TAR_CMD xJf "$TP_SOURCE_DIR/${!NAME}" -C "$TP_SOURCE_DIR/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ $SUFFIX_ZIP ]]; then
            if ! $UNZIP_CMD -o -qq "$TP_SOURCE_DIR/${!NAME}" -d "$TP_SOURCE_DIR/"; then
                echo "Failed to unzip ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ $SUFFIX_BZ2 ]]; then
            if ! $TAR_CMD xf "$TP_SOURCE_DIR/${!NAME}" -C "$TP_SOURCE_DIR/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        fi
    else
        echo "${!SOURCE} already unpacked."
    fi
done
echo "===== Unpacking all thirdparty archives...done"

echo "===== Patching thirdparty archives..."

###################################################################################
# PATCHED_MARK is a empty file which will be created in some thirdparty source dir
# only after that thirdparty source is patched.
# This is to avoid duplicated patch.
###################################################################################
PATCHED_MARK="patched_mark"

# glog patch
cd $TP_SOURCE_DIR/$GLOG_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p1 < $TP_PATCH_DIR/glog-0.4.0.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $GLOG_SOURCE"

# gtest patch
cd $TP_SOURCE_DIR/$GTEST_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p1 < $TP_PATCH_DIR/googletest-release-1.11.0.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $GTEST_SOURCE"

# mysql patch
cd $TP_SOURCE_DIR/$MYSQL_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p1 < $TP_PATCH_DIR/mysql-server-mysql-5.7.18.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $MYSQL_SOURCE"

# libevent patch
cd $TP_SOURCE_DIR/$LIBEVENT_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p1 < $TP_PATCH_DIR/libevent.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $LIBEVENT_SOURCE"

# s2 patch to disable shared library
cd $TP_SOURCE_DIR/$S2_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p1 < $TP_PATCH_DIR/s2geometry-0.9.0.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $S2_SOURCE"

# hdfs3 patch to fix compile error
cd $TP_SOURCE_DIR/$HDFS3_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p1 < $TP_PATCH_DIR/libhdfs3-master.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $HDFS3_SOURCE"

# rocksdb patch to fix compile error
if [ $ROCKSDB_SOURCE == "rocksdb-5.14.2" ]; then
    cd $TP_SOURCE_DIR/$ROCKSDB_SOURCE
    if [ ! -f $PATCHED_MARK ]; then
        patch -p1 < $TP_PATCH_DIR/rocksdb-5.14.2.patch
        touch $PATCHED_MARK
    fi
    cd -
fi
echo "Finished patching $ROCKSDB_SOURCE"

# patch librdkafka to avoid crash
if [ $LIBRDKAFKA_SOURCE = "librdkafka-1.8.2" ]; then
    cd $TP_SOURCE_DIR/$LIBRDKAFKA_SOURCE
    if [ ! -f $PATCHED_MARK ]; then
        patch -p0 < $TP_PATCH_DIR/librdkafka-1.8.2.patch
        touch $PATCHED_MARK
    fi
    cd -
fi
echo "Finished patching $LIBRDKAFKA_SOURCE"

cd $TP_SOURCE_DIR/$AWS_SDK_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    if [ $AWS_SDK_SOURCE == "aws-sdk-cpp-1.9.211" ]; then
        wget --no-check-certificate -q https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/aws-crt-cpp-1.9.211.tar.gz -O aws-crt-cpp-1.9.211.tar.gz
        ret="$?"
        if [ $ret -eq 0 ] ; then
            tar xzf aws-crt-cpp-1.9.211.tar.gz
        else
            bash ./prefetch_crt_dependency.sh
        fi
    else
        bash ./prefetch_crt_dependency.sh
    fi
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $AWS_SDK_SOURCE"


