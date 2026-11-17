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
#
set -euo pipefail
umask 077
fixture_dir=${1:?Usage: generate-tls-fixture.sh OUTPUT_DIRECTORY}
mkdir -p "$fixture_dir"
cd "$fixture_dir"

# The prebuilt BE dependency directory can shadow the runnable system CLI with an
# OpenSSL binary whose shared libraries were removed for static linking.
openssl_bin=$(command -v openssl)
if ! "$openssl_bin" version >/dev/null 2>&1; then
    openssl_bin=$(command -p -v openssl)
fi

# Test-only keys are regenerated per invocation; none is distributed with the fixture.
for authority in ca untrusted-ca; do
    "$openssl_bin" req -x509 -newkey rsa:2048 -nodes -days 2 -sha256 \
        -keyout "$authority.key" -out "$authority.pem" \
        -subj "/CN=Doris Test $authority" \
        -addext 'basicConstraints=critical,CA:TRUE' \
        -addext 'keyUsage=critical,keyCertSign,cRLSign'
done

for peer in fe be dns-fe wrong-name untrusted; do
    authority=ca
    san='DNS:localhost,IP:127.0.0.1'
    if [[ "$peer" == untrusted ]]; then
        authority=untrusted-ca
    elif [[ "$peer" == wrong-name ]]; then
        san='DNS:wrong.invalid'
    elif [[ "$peer" == dns-fe ]]; then
        # A DNS-only identity detects clients that accidentally validate the resolved IP instead.
        san='DNS:localhost'
    fi
    "$openssl_bin" req -new -newkey rsa:2048 -nodes -sha256 \
        -keyout "$peer.key" -out "$peer.csr" -subj "/CN=$peer"
    cat > "$peer.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=$san
EOF
    "$openssl_bin" x509 -req -days 2 -sha256 -in "$peer.csr" \
        -CA "$authority.pem" -CAkey "$authority.key" -CAcreateserial \
        -extfile "$peer.ext" -out "$peer.pem"
done
