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

# Build Step: Command Line
: <<EOF
#!/bin/bash

# Execute step even if some of the previous steps failed


if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/clean.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance
    bash -x clean.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/clean.sh" && exit 1
fi
EOF

#####################################################################################
## clean.sh content ##

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

# shellcheck source=/dev/null
# stop_doris
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
stop_doris
