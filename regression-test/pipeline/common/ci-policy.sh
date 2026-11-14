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

# Source after the checkout/artifact identity check, before starting a lane.
# Usage: source /path/to/doris/regression-test/pipeline/common/ci-policy.sh PROFILE
ci_load_source_policy() {
    local profile=$1 policy_dir was_allexport=false
    case "$profile" in
        compile|feut|beut|cloudut|p0|cloud_p0|vault_p0|nonConcurrent|external) ;;
        *) echo "Unsupported source CI policy: $profile" >&2; return 1 ;;
    esac
    policy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || return 1
    # Missing source policy is a rollout error. Never fall back to private flags.
    [[ -f "$policy_dir/$profile/conf/ci.env" ]] || return 1
    [[ $- != *a* ]] || was_allexport=true
    set -a
    source "$policy_dir/$profile/conf/ci.env"
    local policy_rc=$?
    if [[ "$was_allexport" == false ]]; then set +a; fi
    return "$policy_rc"
}
ci_load_source_policy "$1"
