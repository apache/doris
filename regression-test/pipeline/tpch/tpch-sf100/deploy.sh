#!/bin/bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/tpch/tpch-sf100/deploy.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x deploy.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/deploy.sh" && exit 1
fi
EOF

## deploy.sh content ##

# download_oss_file
source ../../common/oss-utils.sh
# start_doris_fe, get_doris_conf_value, start_doris_be, stop_doris,
# print_doris_fe_log, print_doris_be_log, archive_doris_logs
source ../../common/doris-utils.sh

echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_id}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_id or commit_id not set"
    exit 1
fi
if ${DEBUG:-false}; then
    pull_request_id="26344"
    commit_id="97ee15f75e88f5af6de308d948361eaa7c261602"
    commit_id="14eb608b2d17710c7921642b3112c07472919650"
fi

echo "#### Deploy Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0
need_backup_doris_logs=false

echo "#### 1. try to kill old doris process and remove old doris binary"
stop_doris && rm -rf output

echo "#### 2. download doris binary tar ball"
cd "${teamcity_build_checkoutDir}" || exit 1
if download_oss_file "${pull_request_id:-}_${commit_id:-}.tar.gz"; then
    tar -I pigz -xf "${pull_request_id:-}_${commit_id:-}.tar.gz"
    if [[ -d output && -d output/fe && -d output/be ]]; then
        echo "INFO: be version: $(./output/be/lib/doris_be --version)"
        rm -rf "${pull_request_id}_${commit_id}.tar.gz"
    fi
else
    echo "ERROR: download compiled binary failed" && exit 1
fi

echo "#### 3. copy conf from regression-test/pipeline/tpch/tpch-sf100/conf/"
rm -f "${DORIS_HOME}"/fe/conf/fe_custom.conf "${DORIS_HOME}"/be/conf/be_custom.conf
if [[ -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/fe.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/be.conf ]]; then
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/fe.conf "${DORIS_HOME}"/fe/conf/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/be.conf "${DORIS_HOME}"/be/conf/
else
    echo "ERROR: doris conf file missing in ${teamcity_build_checkoutDir}/regression-test/pipeline/tpch/tpch-sf100/conf/"
    exit 1
fi

echo "#### 4. start Doris"
meta_dir=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf meta_dir)
storage_root_path=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be.conf storage_root_path)
mkdir -p "${meta_dir}"
mkdir -p "${storage_root_path}"
if ! start_doris_fe; then
    echo "WARNING: Start doris fe failed at first time"
    print_doris_fe_log
    echo "WARNING: delete meta_dir and storage_root_path, then retry"
    rm -rf "${meta_dir:?}/"*
    rm -rf "${storage_root_path:?}/"*
    if ! start_doris_fe; then
        need_backup_doris_logs=true
        exit_flag=1
    fi
fi
if ! start_doris_be; then
    echo "WARNING: Start doris be failed at first time"
    print_doris_be_log
    echo "WARNING: delete storage_root_path, then retry"
    rm -rf "${storage_root_path:?}/"*
    if ! start_doris_be; then
        need_backup_doris_logs=true
        exit_flag=1
    fi
fi
if ! add_doris_be_to_fe; then
    need_backup_doris_logs=true
    exit_flag=1
else
    # wait 10s for doris totally started, otherwize may encounter the error below,
    # ERROR 1105 (HY000) at line 102: errCode = 2, detailMessage = Failed to find enough backend, please check the replication num,replication tag and storage medium.
    sleep 10s
fi

echo "#### 5. set session variables"
echo "TODO"

echo "#### 6. check if need backup doris logs"
if ${need_backup_doris_logs}; then
    print_doris_fe_log
    print_doris_be_log
    archive_doris_logs "${pull_request_id}_${commit_id}_doris_logs.tar.gz"
    upload_doris_log_to_oss "${pull_request_id}_${commit_id}_doris_logs.tar.gz"
fi

exit "${exit_flag}"
