#!/usr/bin/env bash

########################### Teamcity Build Step: Command Line #######################
: <<EOF
#!/bin/bash

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/nonConcurrent/deploy.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/nonConcurrent
    bash -x deploy.sh
else
    echo "Build Step file missing: regression-test/pipeline/nonConcurrent/deploy.sh" && exit 1
fi
EOF
#####################################################################################

########################## deploy.sh content ########################################
# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

# shellcheck source=/dev/null
# upload_doris_log_to_oss, download_oss_file
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh
# shellcheck source=/dev/null
# stop_doris, install_fdb, clean_fdb, print_doris_conf,
# start_doris_fe, get_doris_conf_value, start_doris_be,
# print_doris_fe_log, print_doris_be_log, archive_doris_logs
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

if ${DEBUG:-false}; then
    pr_num_from_trigger=${pr_num_from_debug:-"30772"}
    commit_id_from_trigger=${commit_id_from_debug:-"8a0077c2cfc492894d9ff68916e7e131f9a99b65"}
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
# if [[ -z "${oss_ak}" || -z "${oss_sk}" ]]; then echo "ERROR: env oss_ak or oss_sk not set." && exit 1; fi

echo "#### Deploy Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0
(
    echo "#### 1. download doris binary"
    cd "${teamcity_build_checkoutDir}"
    export OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/selectdb_compile_result"}"
    if download_oss_file "${pr_num_from_trigger}_${commit_id_from_trigger}.tar.gz"; then
        rm -rf "${teamcity_build_checkoutDir}"/output
        tar -I pigz -xf "${pr_num_from_trigger}_${commit_id_from_trigger}.tar.gz"
    else exit 1; fi

    echo "#### 2. try to kill old doris process"
    stop_doris

    set -e
    echo "#### 3. copy conf from regression-test/pipeline/nonConcurrent/conf/ and modify"
    # try to remove dir of ms and recycler, otherwise it will cause 'stop MS grace fail' and 'stop RECYCLER grace fail'
    rm -rf "${DORIS_HOME}"/ms/ "${DORIS_HOME}"/recycler/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/nonConcurrent/conf/fe_custom.conf "${DORIS_HOME}"/fe/conf/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/nonConcurrent/conf/be_custom.conf "${DORIS_HOME}"/be/conf/
    print_doris_conf

    echo "#### 4. start Doris"
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-17-*' | sed -n '1p')"
    export JAVA_HOME
    if ! prepare_java_udf; then exit 1; fi
    if ! start_doris_fe; then exit 1; fi
    if ! start_doris_be; then exit 1; fi
    if ! add_doris_be_to_fe; then exit 1; fi
    if ! deploy_doris_sql_converter; then exit 1; else
        set_session_variable sql_converter_service_url "http://127.0.0.1:${doris_sql_converter_port:-5001}/api/v1/convert"
    fi

    echo "#### 5. set session variables"
    if ! reset_doris_session_variables; then exit 1; fi
    session_variables_file="${teamcity_build_checkoutDir}/regression-test/pipeline/nonConcurrent/conf/session_variables.sql"
    echo -e "\n\ntuned session variables:\n$(cat "${session_variables_file}")\n\n"
    set_doris_session_variables_from_file "${session_variables_file}"
    # record session variables
    set +x
    show_session_variables &>"${DORIS_HOME}"/session_variables
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    stop_doris
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pr_num_from_trigger}_${commit_id_from_trigger}_$(date +%Y%m%d%H%M%S)_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
#####################################################################################
