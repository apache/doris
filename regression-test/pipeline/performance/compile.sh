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

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/compile.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x compile.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/compile.sh" && exit 1
fi
EOF

#####################################################################################
## compile.sh content ##

if ${DEBUG:-false}; then
    pr_num_from_trigger="28431"
    commit_id_from_trigger="b052225cd0a180b4576319b5bd6331218dd0d3fe"
    target_branch="master"
fi
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 2; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 2; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 2; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 2; fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

merge_pr_to_target_branch_latest() {
    local pr_num_from_trigger="$1"
    local target_branch="$2"
    echo "INFO: merge pull request into ${target_branch}"
    if [[ -z "${teamcity_build_checkoutDir}" ]]; then
        echo "ERROR: env teamcity_build_checkoutDir not set" && return 1
    fi
    cd "${teamcity_build_checkoutDir}" || return 1
    git reset --hard
    git fetch origin "${target_branch}"
    git checkout "${target_branch}"
    git reset --hard origin/"${target_branch}"
    git pull origin "${target_branch}"
    git submodule update --init be/src/clucene
    git submodule update --init be/src/apache-orc
    local target_branch_commit_id
    target_branch_commit_id=$(git rev-parse HEAD)
    git config user.email "ci@selectdb.com"
    git config user.name "ci"
    echo "git fetch origin refs/pull/${pr_num_from_trigger}/head"
    git fetch origin "refs/pull/${pr_num_from_trigger}/head"
    git merge --no-edit --allow-unrelated-histories FETCH_HEAD
    echo "INFO: merge refs/pull/${pr_num_from_trigger}/head into master: ${target_branch_commit_id}"
    CONFLICTS=$(git ls-files -u | wc -l)
    if [[ "${CONFLICTS}" -gt 0 ]]; then
        echo "ERROR: merge refs/pull/${pr_num_from_trigger}/head into master failed. Aborting"
        git merge --abort
        return 1
    fi
}

if [[ "${target_branch}" == "master" ]]; then
    REMOTE_CCACHE='/mnt/remote_ccache_master'
    docker_image="apache/doris:build-env-ldb-toolchain-0.19-latest"
elif [[ "${target_branch}" == "branch-2.0" ]]; then
    docker_image="apache/doris:build-env-for-2.0"
    REMOTE_CCACHE='/mnt/remote_ccache_branch_2'
elif [[ "${target_branch}" == "branch-1.2-lts" ]]; then
    REMOTE_CCACHE='/mnt/remote_ccache_master'
    docker_image="apache/doris:build-env-for-1.2"
else
    REMOTE_CCACHE='/mnt/remote_ccache_master'
    docker_image="apache/doris:build-env-ldb-toolchain-latest"
fi
if ${merge_target_branch_latest:-true}; then
    if ! merge_pr_to_target_branch_latest "${pr_num_from_trigger}" "${target_branch}"; then
        exit 1
    fi
else
    echo "INFO: skip merge_pr_to_target_branch_latest"
fi
mount_swapfile=""
if [[ -f /root/swapfile ]]; then mount_swapfile="-v /root/swapfile:/swapfile --memory-swap -1"; fi
git_storage_path=$(grep storage "${teamcity_build_checkoutDir}"/.git/config | rev | cut -d ' ' -f 1 | rev | awk -F '/lfs' '{print $1}')
sudo docker container prune -f
sudo docker image prune -f
sudo docker pull "${docker_image}"
docker_name=doris-compile-"${commit_id_from_trigger}"
if sudo docker ps -a --no-trunc | grep "${docker_name}"; then
    sudo docker stop "${docker_name}"
    sudo docker rm "${docker_name}"
fi
rm -f custom_env.sh
cp "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/custom_env.sh .
if [[ "${target_branch}" == "master" ]]; then
    echo "export JAVA_HOME=/usr/lib/jvm/jdk-17.0.2" >>custom_env.sh
fi
rm -rf "${teamcity_build_checkoutDir}"/output
set -x
# shellcheck disable=SC2086
sudo docker run -i --rm \
    --name "${docker_name}" \
    -e TZ=Asia/Shanghai \
    ${mount_swapfile} \
    -v /etc/localtime:/etc/localtime:ro \
    -v "${HOME}"/.m2:/root/.m2 \
    -v "${HOME}"/.npm:/root/.npm \
    -v /mnt/ccache/.ccache:/root/.ccache \
    -v "${REMOTE_CCACHE}":/root/ccache \
    -v "${git_storage_path}":/root/git \
    -v "${teamcity_build_checkoutDir}":/root/doris \
    "${docker_image}" \
    /bin/bash -c "mkdir -p ${git_storage_path} \
                    && cp -r /root/git/* ${git_storage_path}/ \
                    && cd /root/doris \
                    && export CCACHE_LOGFILE=/tmp/cache.debug \
                    && export CCACHE_REMOTE_STORAGE=file:///root/ccache \
                    && export EXTRA_CXX_FLAGS=-O3 \
                    && export USE_JEMALLOC='ON' \
                    && export ENABLE_PCH=OFF \
                    && export CUSTOM_NPM_REGISTRY=https://registry.npmjs.org \
                    && bash build.sh --fe --be --clean 2>&1 | tee build.log"
set +x
set -x
succ_symble="Successfully build Doris"
if [[ -d output ]] && grep "${succ_symble}" "${teamcity_build_checkoutDir}"/build.log; then
    echo "INFO: ${succ_symble}"
else
    echo -e "ERROR: BUILD FAILED"
    exit 1
fi
