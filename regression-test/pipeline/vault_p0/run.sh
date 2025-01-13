#!/usr/bin/env bash

########################### Teamcity Build Step: Command Line #######################
: <<EOF
#!/bin/bash
export PATH=/usr/local/software/jdk1.8.0_131/bin:/usr/local/software/apache-maven-3.6.3/bin:${PATH}
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/vault_p0/run.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/vault_p0/
    bash -x run.sh
else
    echo "Build Step file missing: regression-test/pipeline/vault_p0/run.sh" && exit 1
fi
EOF
############################# run.sh content ########################################
# shellcheck source=/dev/null
# _monitor_regression_log, print_running_pipeline_tasks
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh
# shellcheck source=/dev/null
# create_an_issue_comment
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
# shellcheck source=/dev/null
# upload_doris_log_to_oss
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh
# shellcheck source=/dev/null
# reporting_build_problem, reporting_messages_error
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/teamcity-utils.sh

if ${DEBUG:-false}; then
    pr_num_from_trigger=${pr_num_from_debug:-"30772"}
    commit_id_from_trigger=${commit_id_from_debug:-"8a0077c2cfc492894d9ff68916e7e131f9a99b65"}
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${s3SourceAk}" || -z "${s3SourceSk}" ]]; then echo "ERROR: env s3SourceAk or s3SourceSk not set" && exit 1; fi
if [[ -z "${hwYunAk}" || -z "${hwYunSk}" ]]; then echo "WARNING: env hwYunAk or hwYunSk not set"; fi
if [[ -z "${txYunAk}" || -z "${txYunSk}" ]]; then echo "WARNING: env txYunAk or txYunSk not set"; fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

echo "#### Run vault_p0 test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0
need_collect_log=false

# monitoring the log files in "${DORIS_HOME}"/regression-test/log/ for keyword 'Reach limit of connections'
_monitor_regression_log &

# shellcheck disable=SC2317
run() {
    set -e
    shopt -s inherit_errexit

    cd "${teamcity_build_checkoutDir}" || return 1
    {
        echo # add a new line to prevent two config items from being combined, which will cause the error "No signature of method"
        echo "ak='${s3SourceAk}'"
        echo "sk='${s3SourceSk}'"
        echo "hwYunAk='${hwYunAk:-}'"
        echo "hwYunSk='${hwYunSk:-}'"
        echo "txYunAk='${txYunAk:-}'"
        echo "txYunSk='${txYunSk:-}'"
    } >>"${teamcity_build_checkoutDir}"/regression-test/pipeline/vault_p0/conf/regression-conf-custom.groovy
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/vault_p0/conf/regression-conf-custom.groovy \
        "${teamcity_build_checkoutDir}"/regression-test/conf/

    # start minio docker to run case test_rountine_load
    sed -i "s/^CONTAINER_UID=\"doris--\"/CONTAINER_UID=\"doris-external--\"/" "${teamcity_build_checkoutDir}"/docker/thirdparties/custom_settings.env
    if bash "${teamcity_build_checkoutDir}"/docker/thirdparties/run-thirdparties-docker.sh -c minio; then echo; else echo "ERROR: start minio docker failed"; fi

    # used to set up HDFS docker
    docker_compose_hdfs_yaml='
version: "3"

services:
 namenode:
  image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
  environment:
   - CLUSTER_NAME=test
  container_name: hadoop3-namenode
  ports:
   - "9870:9870"
  expose:
   - "9870"
  healthcheck:
   test: [ "CMD", "curl", "http://localhost:9870/" ]
   interval: 5s
   timeout: 120s
   retries: 120
  network_mode: "host"

 datanode:
  image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
  ports:
   - "9864:9864"
  container_name: hadoop3-datanode
  expose:
   - "9864"
  healthcheck:
   test: [ "CMD", "curl", "http://localhost:9864" ]
   interval: 5s
   timeout: 60s
   retries: 120
  network_mode: "host"
'
    if echo "${docker_compose_hdfs_yaml}" >docker-compose.yaml && docker-compose up -d; then echo; else echo "ERROR: start hdfs docker failed"; fi
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    if "${teamcity_build_checkoutDir}"/run-regression-test.sh \
        --teamcity \
        --run \
        --times "${repeat_times_from_trigger:-1}" \
        -parallel 10 \
        -suiteParallel 10 \
        -actionParallel 10 \
        -runNonConcurrent true; then
        echo
    else
        bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export need_collect_log=true"
        # regression 测试跑完后输出的汇总信息，Test 1961 suites, failed 1 suites, fatal 0 scripts, skipped 0 scripts
        # 如果 test_suites>0 && failed_suites<=3  && fatal_scripts=0，就把返回状态码改为正常的0，让teamcity根据跑case的情况去判断成功还是失败
        # 这样预期能够快速 mute 不稳定的 case
        summary=$(
            grep -aoE 'Test ([0-9]+) suites, failed ([0-9]+) suites, fatal ([0-9]+) scripts, skipped ([0-9]+) scripts' \
                "${DORIS_HOME}"/regression-test/log/doris-regression-test.*.log
        )
        set -x
        test_suites=$(echo "${summary}" | cut -d ' ' -f 2)
        failed_suites=$(echo "${summary}" | cut -d ' ' -f 5)
        fatal_scripts=$(echo "${summary}" | cut -d ' ' -f 8)
        if [[ ${test_suites} -gt 0 && ${failed_suites} -le ${failed_suites_threshold:=100} && ${fatal_scripts} -eq 0 ]]; then
            echo "INFO: regression test result meet (test_suites>0 && failed_suites<=${failed_suites_threshold} && fatal_scripts=0)"
        else
            return 1
        fi
    fi
}
export -f run
# 设置超时时间（以分为单位）
timeout_minutes=$((${repeat_times_from_trigger:-1} * ${BUILD_TIMEOUT_MINUTES:-180}))m
timeout "${timeout_minutes}" bash -cx run
exit_flag="$?"
if print_running_pipeline_tasks; then :; fi
# shellcheck source=/dev/null
source "$(cd "${teamcity_build_checkoutDir}" && bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]] || ${need_collect_log}; then
    check_if_need_gcore "${exit_flag}"
    if core_file_name=$(archive_doris_coredump "${pr_num_from_trigger}_${commit_id_from_trigger}_$(date +%Y%m%d%H%M%S)_doris_coredump.tar.gz"); then
        reporting_build_problem "coredump"
        print_doris_fe_log
        print_doris_be_log
    fi
    stop_doris
    if log_file_name=$(archive_doris_logs "${pr_num_from_trigger}_${commit_id_from_trigger}_$(date +%Y%m%d%H%M%S)_doris_logs.tar.gz"); then
        if log_info="$(upload_doris_log_to_oss "${log_file_name}")"; then
            reporting_messages_error "${log_info##*logs.tar.gz to }"
        fi
    fi
    if core_info="$(upload_doris_log_to_oss "${core_file_name}")"; then reporting_messages_error "${core_info##*coredump.tar.gz to }"; fi
fi

exit "${exit_flag}"
