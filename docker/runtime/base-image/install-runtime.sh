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

sed -i 's|http://|https://|g' /etc/apt/sources.list
apt-get update
# Patch the package manager before the remaining downloads (CVE-2026-109518).
apt-get install -y --only-upgrade apt libapt-pkg6.0
apt-get upgrade -y --with-new-pkgs
# Component bases use this default set. Test images pass their own runtime or
# build-only packages so the same patch policy does not add unused tooling.
if [[ $# -eq 0 ]]; then
    set -- curl fontconfig hostname iproute2 iputils-ping jq libstdc++6 \
        mysql-client-core-8.0 net-tools numactl openssl procps tzdata util-linux wget
fi
apt-get install -y --no-install-recommends ca-certificates libssl3 "$@"

dpkg --compare-versions "$(dpkg-query -W -f='${Version}' apt)" ge 2.4.15ubuntu0.1
dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libssl3)" ge 3.0.2-0ubuntu1.30
mkdir -p /usr/share/doris
dpkg-query -W -f='${Package}\t${Version}\n' | sort > /usr/share/doris/runtime-packages.tsv
apt-get clean
rm -rf /var/lib/apt/lists/*
