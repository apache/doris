#!/usr/bin/env bash

########################### Teamcity Build Step: Command Line #######################
: <<EOF
#!/bin/bash

set -x
pwd
rm -rf ../.old/*

export teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
export commit_id_from_checkout="%build.vcs.number%"
export target_branch='%teamcity.pullRequest.target.branch%'

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
    # target_branch_commit_id=$(git rev-parse HEAD)
    git config user.email "ci@selectdb.com"
    git config user.name "ci"
    echo "git fetch origin refs/pull/${pr_num_from_trigger}/head"
    git fetch origin "refs/pull/${pr_num_from_trigger}/head"
    git merge --no-edit --allow-unrelated-histories FETCH_HEAD
    echo "INFO: merge refs/pull/${pr_num_from_trigger}/head into master: $(git rev-parse HEAD)"
    # CONFLICTS=$(git ls-files -u | wc -l)
    if [[ $(git ls-files -u | wc -l) -gt 0 ]]; then
        echo "ERROR: merge refs/pull/${pr_num_from_trigger}/head into master failed. Aborting"
        git merge --abort
        return 1
    fi
}
# It should not merge, otherwise binary and cases may mismatch!
# if ! merge_pr_to_target_branch_latest "${pr_num_from_trigger}" "${target_branch}";then return 1; fi
export PATH=/usr/local/software/apache-maven-3.6.3/bin:${PATH}
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/cloud_p0/prepare.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/cloud_p0/
    bash prepare.sh
else
    echo "Build Step file missing: regression-test/pipeline/cloud_p0/prepare.sh" && exit 1
fi
EOF
#####################################################################################
## prepare.sh content ##

if ${DEBUG:-false}; then
    pr_num_from_trigger=${pr_num_from_debug:-"30772"}
    commit_id_from_trigger=${commit_id_from_debug:-"8a0077c2cfc492894d9ff68916e7e131f9a99b65"}
    commit_id_from_checkout=${commit_id_from_debug:-"8a0077c2cfc492894d9ff68916e7e131f9a99b65"} # teamcity checkout commit id
    target_branch="master"
fi

# shellcheck source=/dev/null
# stop_doris, clean_fdb, install_fdb, install_java
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh
# shellcheck source=/dev/null
# check_oss_file_exist
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh

echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_checkout}" ]]; then echo "ERROR: env commit_id_from_checkout not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi
if [[ -z "${cos_ak}" || -z "${cos_sk}" ]]; then echo "ERROR: env cos_ak or cos_sk not set" && exit 1; fi
if [[ -z "${oss_ak}" || -z "${oss_sk}" ]]; then echo "ERROR: env oss_ak or oss_sk not set." && exit 1; fi

echo "#### 1. check if need run"
if [[ "${commit_id_from_trigger}" != "${commit_id_from_checkout}" ]]; then
    echo -e "从触发流水线 -> 流水线开始跑，这个时间段中如果有新commit，
这时候流水线 checkout 出来的 commit 就不是触发时的传过来的 commit 了，
这种情况不需要跑，预期 pr owner 会重新触发。"
    echo -e "ERROR: PR(${pr_num_from_trigger}),
    the commit_id_from_checkout
    ${commit_id_from_checkout}
    not equail to the commit_id_from_trigger
    ${commit_id_from_trigger}
    commit_id_from_trigger is outdate"
    exit 1
fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi
if [[ "${target_branch}" == "master" ]]; then
    echo "INFO: PR target branch ${target_branch}"
    install_java
else
    echo "WARNING: PR target branch ${target_branch} is NOT in (master), skip pipeline."
    bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export skip_pipeline=true"
    exit 0
fi

# shellcheck source=/dev/null
# _get_pr_changed_files file_changed_performance
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
if _get_pr_changed_files "${pr_num_from_trigger}"; then
    if ! file_changed_cloud_p0; then
        bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export skip_pipeline=true"
        exit 0
    fi
fi

echo "#### 2. check if tpch depending files exist"
set -x
if ! [[ -d "${teamcity_build_checkoutDir}"/regression-test/pipeline/cloud_p0/ &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh ]]; then
    echo "ERROR: depending files missing" && exit 1
fi

echo "#### 3. try to kill old doris process"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
stop_doris

echo "#### 4. prepare fundationdb"
install_fdb
clean_fdb "cloud_instance_0"

echo "#### 5. check if binary package ready"
export OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile_result"}"
if ! check_oss_file_exist "${pr_num_from_trigger}_${commit_id_from_trigger}.tar.gz"; then return 1; fi
