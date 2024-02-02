#!/usr/bin/env bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

set -x
pwd
rm -rf ../.old/*
set +x

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
    pull_request_num="30772"
    commit_id_from_trigger="8a0077c2cfc492894d9ff68916e7e131f9a99b65"
    commit_id="8a0077c2cfc492894d9ff68916e7e131f9a99b65" # teamcity checkout commit id
    target_branch="master"
fi

echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pull_request_num}" ]]; then echo "ERROR: env pull_request_num not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id}" ]]; then echo "ERROR: env commit_id not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi

commit_id_from_checkout=${commit_id}

echo "#### 1. check if need run"
if [[ "${commit_id_from_trigger}" != "${commit_id_from_checkout}" ]]; then
    echo -e "从触发流水线 -> 流水线开始跑，这个时间段中如果有新commit，
这时候流水线 checkout 出来的 commit 就不是触发时的传过来的 commit 了，
这种情况不需要跑，预期 pr owner 会重新触发。"
    echo -e "ERROR: PR(${pull_request_num}),
    the lastest commit id
    ${commit_id_from_checkout}
    not equail to the commit_id_from_trigger
    ${commit_id_from_trigger}
    commit_id_from_trigger is outdate"
    exit 1
fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi
if [[ "${target_branch}" == "master" || "${target_branch}" == "branch-2.0" ]]; then
    echo "INFO: PR target branch ${target_branch} is in (master, branch-2.0)"
else
    echo "WARNING: PR target branch ${target_branch} is NOT in (master, branch-2.0), skip pipeline."
    bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export skip_pipeline=true"
    exit 0
fi

# shellcheck source=/dev/null
# _get_pr_changed_files file_changed_performance
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
if _get_pr_changed_files "${pull_request_num}"; then
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

# shellcheck source=/dev/null
# stop_doris, clean_fdb, install_fdb
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

echo "#### 3. try to kill old doris process"
stop_doris

echo "#### 4. try to clean fdb"
clean_fdb

echo "#### 5. install fdb"
install_fdb

echo "#### 6. check if binary package ready"
# shellcheck source=/dev/null
# check_oss_file_exist
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh
export OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile_result"}"
if ! check_oss_file_exist "${pull_request_num}_${commit_id_from_trigger}.tar.gz"; then return 1; fi
