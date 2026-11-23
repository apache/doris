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

# Keep builds independent of subscription repositories exposed by a RHEL host.
repositories=(--disablerepo='*' --enablerepo=ubi-9-baseos-rpms --enablerepo=ubi-9-appstream-rpms)
microdnf "${repositories[@]}" update -y
microdnf "${repositories[@]}" install -y --setopt=install_weak_deps=0 \
    bash ca-certificates curl-minimal findutils fontconfig gawk glibc-langpack-en \
    grep gzip hostname iproute iputils jq libstdc++ mysql net-tools numactl \
    openssl openssl-libs procps-ng sed tar tzdata util-linux wget which

# UBI Minimal keeps tzdata in the RPM database but removes its zoneinfo files.
# Installing an already-current RPM does not restore them; BE needs the files.
microdnf "${repositories[@]}" reinstall -y tzdata
ln -snf /usr/share/zoneinfo/UTC /etc/localtime
test -f /usr/share/zoneinfo/UTC

mkdir -p /usr/share/doris
rpm -qa --qf '%{NAME}\t%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}\n' \
    | sort > /usr/share/doris/runtime-packages.tsv
microdnf clean all
rm -rf /var/cache/yum /var/cache/dnf
