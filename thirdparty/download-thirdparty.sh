#!/bin/bash

# Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

export PALO_HOME=$curdir/../
source $curdir/vars.sh

mkdir -p $TP_DIR/src
mkdir -p $TP_DIR/installed
mkdir -p $TP_DIR/installed/jar/

download() {
    local FILENAME=$1
    local DOWNLOAD_URL=$2
    local DESC_DIR=$3

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


    SUCCESS=0
    for attemp in 1 2; do
        if [ -r "$DESC_DIR/$FILENAME" ]; then
            echo "Archive $FILENAME already exist."
            SUCCESS=1
            break;
        else
            echo "Downloading $FILENAME from $DOWNLOAD_URL to $DESC_DIR"
            wget --no-check-certificate $DOWNLOAD_URL -O $DESC_DIR/$FILENAME
            if [ "$?"x == "0"x ]; then
                SUCCESS=1
                echo "Success to download $FILENAME"
                break;
            else
                echo "Failed to download $FILENAME. attemp: $attemp"
            fi
        fi
    done

    if [ $SUCCESS -ne 1 ]; then
        echo "Failed to download $FILENAME"
        exit 1
    fi
}

# download thirdparty archives
echo "===== Downloading thirdparty archives..."
for TP_ARCH in ${TP_ARCHIVES[*]}
do
    URL=$TP_ARCH"_DOWNLOAD"
    NAME=$TP_ARCH"_NAME"
    download ${!NAME} ${!URL} $TP_SOURCE_DIR
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
for TP_ARCH in ${TP_ARCHIVES[*]}
do
    NAME=$TP_ARCH"_NAME"
    SOURCE=$TP_ARCH"_SOURCE"

    if [ -z "${!SOURCE}" ]; then
        continue
    fi

    if [ ! -d $TP_SOURCE_DIR/${!SOURCE} ]; then
        if [[ "${!NAME}" =~ \.(tar\.gz|tgz)$  ]]; then
            echo "$TP_SOURCE_DIR/${!NAME}"
            echo "$TP_SOURCE_DIR/${!SOURCE}"
            if ! $TAR_CMD xzf "$TP_SOURCE_DIR/${!NAME}" -C "$TP_SOURCE_DIR/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ \.zip$ ]]; then
            if ! $UNZIP_CMD "$TP_SOURCE_DIR/${!NAME}" -d "$TP_SOURCE_DIR/"; then
                echo "Failed to unzip ${!NAME}"
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
    patch -p1 < $TP_PATCH_DIR/glog-0.3.3-vlog-double-lock-bug.patch
    patch -p1 < $TP_PATCH_DIR/glog-0.3.3-for-palo2.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $GLOG_SOURCE"

# ncurses patch
cd $TP_SOURCE_DIR/$NCURSES_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p0 < $TP_PATCH_DIR/ncurses-6.0.patch 
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $NCURSES_SOURCE"

# re2 patch
cd $TP_SOURCE_DIR/$RE2_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p0 < $TP_PATCH_DIR/re2-2017-05-01.patch 
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $RE2_SOURCE"

# mysql patch
cd $TP_SOURCE_DIR/$MYSQL_SOURCE
if [ ! -f $PATCHED_MARK ]; then
    patch -p0 < $TP_PATCH_DIR/mysql-5.7.18.patch
    touch $PATCHED_MARK
fi
cd -
echo "Finished patching $MYSQL_SOURCE"

##########################
# Download java libraries
##########################

# create download dir first
mkdir -p $TP_JAR_DIR

echo "===== Downloading java libraries..."
for TP_JAR in ${TP_JARS[*]}
do
    URL=$TP_JAR"_DOWNLOAD"
    NAME=$TP_JAR"_NAME"
    download ${!NAME} ${!URL} $TP_JAR_DIR

    if [ "$TP_JAR"x == "KUDU"x ];then
        # unzip kudu
        tar xzf $TP_JAR_DIR/$KUDU_NAME -C $TP_JAR_DIR/
    fi
done
echo "===== Downloading java libraries...done"

# check if all java libraries exist
echo "===== Checking all java libraries..."
for TP_JAR in ${TP_JARS[*]}
do
    NAME=$TP_JAR"_NAME"
    if [ ! -r $TP_JAR_DIR/${!NAME} ]; then
        echo "Failed to fetch ${!NAME}"
        exit 1
    fi
done
echo "===== Checking all java libraries...done"
echo "===== Finish downloading all thirdparties"
