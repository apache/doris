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

# Source inside the identity-checked test checkout before running CloudUT.
# The caller retains ownership of the test command, timeout and result collection.
source /root/doris/regression-test/pipeline/common/ci-policy.sh cloudut
export ASAN_OPTIONS="$CI_CLOUDUT_ASAN_OPTIONS" LIBHDFS_OPTS="$CI_CLOUDUT_LIBHDFS_OPTS"
CI_CLOUDUT_TEST_PRELOAD=
if [[ "${CLOUDUT_RUNTIME_IMAGE:?}" == "$CI_FDB_ENTROPY_COMPAT_IMAGE" ]]; then
    fdb_entropy_dir=$(mktemp -d /tmp/cloudut-fdb-entropy.XXXXXX)
    fdb_entropy_so="$fdb_entropy_dir/libcloudut-fdb-entropy.so"
    fdb_entropy_source=/root/doris/cloud/test/fdb_getentropy_compat.c
    "${CC:?}" -shared -fPIC -O2 -Wall -Wextra -Werror -Wl,-z,defs \
        -o "$fdb_entropy_so" "$fdb_entropy_source"
    nm -D --defined-only "$fdb_entropy_so" | awk '$2=="T" && $3=="getentropy" {print $3}' | grep -Fx getentropy
    sha256sum "$fdb_entropy_source" > /validation/cloudut-fdb-entropy-source.txt
    sha256sum "$fdb_entropy_so" > /validation/cloudut-fdb-entropy-library.txt
    # The temporary library lives in this build's disposable container.
    CI_CLOUDUT_TEST_PRELOAD="$fdb_entropy_so${LD_PRELOAD:+:$LD_PRELOAD}"
fi
