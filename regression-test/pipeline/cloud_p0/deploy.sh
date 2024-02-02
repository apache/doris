#!/usr/bin/env bash

############################### Build Step: Command Line ############################
: <<EOF
#!/bin/bash
export DEBUG=true

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/cloud_p0/deploy.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/cloud_p0
    bash -x deploy.sh
else
    echo "Build Step file missing: regression-test/pipeline/cloud_p0/deploy.sh" && exit 1
fi
EOF
#####################################################################################

########################## deploy.sh content ########################################
# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

# shellcheck source=/dev/null
# upload_doris_log_to_oss
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh
# shellcheck source=/dev/null
# stop_doris, install_fdb, clean_fdb, print_doris_conf,
# start_doris_fe, get_doris_conf_value, start_doris_be,
# print_doris_fe_log, print_doris_be_log, archive_doris_logs
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

if ${DEBUG:-false}; then
    pull_request_num="30772"
    commit_id="8a0077c2cfc492894d9ff68916e7e131f9a99b65"
    target_branch="master"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pull_request_num}" ]]; then echo "ERROR: env pull_request_num not set" && exit 1; fi
if [[ -z "${commit_id}" ]]; then echo "ERROR: env commit_id not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi

echo "#### Deploy Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0
(
    echo "#### 1. try to kill old doris process"
    stop_doris
    install_fdb
    clean_fdb

    echo "#### 2. setup warehouse"
    #TODO
    if ! create_warehouse; then exit 1; fi
    if ! warehouse_add_fe; then exit 1; fi
    if ! warehouse_add_be; then exit 1; fi

    set -e
    echo "#### 2. copy conf from regression-test/pipeline/cloud_p0/conf/ and modify"
    cp -rf "${DORIS_HOME}"/ms/ "${DORIS_HOME}"/recycler/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/cloud_p0/conf/fe_custom.conf "${DORIS_HOME}"/fe/conf/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/cloud_p0/conf/be_custom.conf "${DORIS_HOME}"/be/conf/
    fdb_cluster="$(cat /etc/foundationdb/fdb.cluster)"
    sed -i "s/^fdb_cluster = .*/fdb_cluster = ${fdb_cluster}/" "${DORIS_HOME}"/ms/conf/doris_cloud.conf
    sed -i "s/^fdb_cluster = .*/fdb_cluster = ${fdb_cluster}/" "${DORIS_HOME}"/recycler/conf/doris_cloud.conf
    sed -i "s/^brpc_listen_port = .*/fbrpc_listen_port = 6000/" "${DORIS_HOME}"/recycler/conf/doris_cloud.conf
    print_doris_conf

    echo "#### 3. start Doris"
    # meta_dir=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe_custom.conf meta_dir)
    # storage_root_path=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be_custom.conf storage_root_path)
    # mkdir -p "${meta_dir}"
    # mkdir -p "${storage_root_path}"

    if ! start_doris_ms; then exit 1; fi
    if ! start_doris_fe; then exit 1; fi
    if ! start_doris_recycler; then exit 1; fi
    if ! start_doris_be; then exit 1; fi




    echo "#### 4. reset session variables"
    if ! reset_doris_session_variables; then exit 1; fi
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    stop_doris
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pull_request_num}_${commit_id}_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
#####################################################################################
