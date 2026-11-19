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

set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

. /etc/os-release
case "${VERSION_ID}" in
    22.04)
        apt_sources=/etc/apt/sources.list
        apt_library=libapt-pkg6.0
        ssl_library=libssl3
        mysql_client=mysql-client-core-8.0
        apt_minimum=2.4.15ubuntu0.1
        ssl_minimum=3.0.2-0ubuntu1.30
        ;;
    26.04)
        apt_sources=/etc/apt/sources.list.d/ubuntu.sources
        apt_library=libapt-pkg7.0
        ssl_library=libssl3t64
        mysql_client=mysql-client-core
        apt_minimum=3.2.1ubuntu0.1
        ssl_minimum=3.5.5-1ubuntu3.7
        ;;
    *) echo "Unsupported Ubuntu runtime: ${VERSION_ID}" >&2; exit 1 ;;
esac

sed -i 's|http://|https://|g' "${apt_sources}"
# Resolute's OpenSSL-based APT cannot discover the bootstrap bundle before
# ca-certificates creates the default OpenSSL certificate paths.
apt_options=(-o Acquire::https::CaInfo=/etc/ssl/certs/ca-certificates.crt)
apt-get "${apt_options[@]}" update --error-on=any
# Patch the package manager before the remaining downloads (CVE-2026-109518).
apt-get "${apt_options[@]}" install -y --only-upgrade apt "${apt_library}"
apt-get "${apt_options[@]}" upgrade -y --with-new-pkgs
# Component bases use this default set. Test images pass their own runtime or
# build-only packages so the same patch policy does not add unused tooling.
if [[ $# -eq 0 ]]; then
    set -- curl fontconfig hostname iproute2 iputils-ping jq libstdc++6 \
        "${mysql_client}" net-tools numactl openssl procps tzdata util-linux wget
fi
# Accept the core-client name from either supported Ubuntu release so test
# images and explicit Jammy overrides use the same installer.
packages=()
for package in "$@"; do
    case "${package}" in
        mysql-client-core|mysql-client-core-8.0) packages+=("${mysql_client}") ;;
        *) packages+=("${package}") ;;
    esac
done
apt-get "${apt_options[@]}" install -y --no-install-recommends ca-certificates "${ssl_library}" "${packages[@]}"

dpkg --compare-versions "$(dpkg-query -W -f='${Version}' apt)" ge "${apt_minimum}"
dpkg --compare-versions "$(dpkg-query -W -f='${Version}' "${ssl_library}")" ge "${ssl_minimum}"
mkdir -p /usr/share/doris
dpkg-query -W -f='${Package}\t${Version}\n' | sort > /usr/share/doris/runtime-packages.tsv
apt-get clean
rm -rf /var/lib/apt/lists/*
