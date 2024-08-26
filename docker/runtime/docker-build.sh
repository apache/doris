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

# get parameters from command line
# convert tags
# build fe, be, ms
# find cpu arch
# download binary tar package.
# extract archive
#

usage() {
    echo "$0 <options>
    -v                    specify version to build.
    Eg:
    $0 -v x.x.x           build the "x.x.x" version and the binary will download from https://doris.apache.org/zh-CN/download/ to download binary."
}

version=

OPTS="$(getopt -n "$0" -o 'v:' -- "$*")"
eval set -- "${OPTS}"
while true; do
    case "$1" in
        -v|--version)
        version="$2"
        shift 2
        ;;
        --)
        shift
        break
        ;;
        *)
        usage
        shift
        exit 1
        ;;

    esac
done

version=$(echo $version | sed 's/\s//g')

if [[ "x$version" == "x" ]]; then
    usage
    exit 1
fi

ARCH=$(uname -m)

url_arch=
sub_path=
if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
    url_arch="arm64"
    sub_path="arm64"
else
    url_arch="x64"
    sub_path="amd64"
fi

if [[ "$url_arch" == "x64" ]]; then
    avx=$(cat /proc/cpuinfo | grep avx2 | wc -l)
    if [[ $avx -le 0 ]]; then
        url_arch=$url_arch"-noavx2"
    fi
fi

URL="https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-${version}-bin-${url_arch}.tar.gz"

echo "wget the archive binary in current directory!"
wget $URL

echo "extract archive"

tar zxf "apache-doris-${version}-bin-${url_arch}.tar.gz"

echo "distribute binary to corresponding path."
mkdir -p fe/resource/${sub_path}/apache-doris-${version}-bin-${url_arch}
mkdir -p be/resource/${sub_path}/apache-doris-${version}-bin-${url_arch}
mkdir -p ms/resource/${sub_path}/apache-doris-${version}-bin-${url_arch}
mkdir -p broker/resource/${sub_path}/apache-doris-${version}-bin-${url_arch}

mv -f apache-doris-${version}-bin-${url_arch}/fe fe/resource/$sub_path/apache-doris-${version}-bin-${url_arch}/
mv -f apache-doris-${version}-bin-${url_arch}/be be/resource/$sub_path/apache-doris-${version}-bin-${url_arch}/
mv -f apache-doris-${version}-bin-${url_arch}/ms ms/resource/$sub_path/apache-doris-${version}-bin-${url_arch}/
mv -f apache-doris-${version}-bin-${url_arch}/extensions broker/resource/$sub_path/apache-doris-${version}-bin-${url_arch}/

echo "docker build base iamge,tag=selectdb/base:latest"
cd base-image/ && docker build -t selectdb/base:latest -f Dockerfile_base .
cd -

echo "docker build fe image,tag=doris.fe:${version}"
cd fe/ && docker build -t doris.fe:${version} -f Dockerfile --build-arg DORIS_VERSION=${version} .
cd -

echo "docker build be image,tag=doris.be:${version}"
cd be/ && docker build -t doris.be:${version} -f Dockerfile --build-arg DORIS_VERSION=${version} .
cd -

echo "docker build ms image,tag=doris.ms:${version}"
cd ms/ && docker build -t doris.ms:${version} -f Dockerfile --build-arg DORIS_VERSION=${version} .
cd -

echo "docker build broker image,tag=doris.broker:${version}"
cd broker/ && docker build -t doris.broker:${version} -f Dockerfile --build-arg DORIS_VERSION=${version} .
