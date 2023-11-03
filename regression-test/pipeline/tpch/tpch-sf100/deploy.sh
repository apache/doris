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
# start_doris_fe, get_doris_conf_value, start_doris_be
source ../../common/doris-utils.sh
set -e
shopt -s inherit_errexit

echo "#### Deploy Doris ####"
echo "#### 1. download doris binary tar ball"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
cd "${teamcity_build_checkoutDir}"
####DEBUG####
pull_request_id="26344"
commit_id="97ee15f75e88f5af6de308d948361eaa7c261602"
####DEBUG####
if download_oss_file "${pull_request_id:-}_${commit_id:-}.tar.gz"; then
    tar -I pigz -xf "${pull_request_id:-}_${commit_id:-}.tar.gz"
    if [[ -d output && -d output/fe && -d output/be ]]; then
        echo "INFO: be version: $(./output/be/lib/doris_be --version)"
        DORIS_HOME="${teamcity_build_checkoutDir}/output"
        export DORIS_HOME
        rm -rf "${pull_request_id}_${commit_id}.tar.gz"
    fi
fi
echo "#### 2. try to kill old doris process"
if pgrep -fia doris; then pgrep -fi doris | xargs kill -9; fi

echo "#### 3. copy conf from regression-test/pipeline/tpch/tpch-sf100/conf/"
rm -f output/fe/conf/fe_custom.conf output/be/conf/be_custom.conf
cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/fe.conf "${DORIS_HOME}"/fe/conf/
cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/be.conf "${DORIS_HOME}"/be/conf/

echo "#### 4. start Doris"
meta_dir=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf meta_dir)
storage_root_path=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be.conf storage_root_path)
mkdir -p "${meta_dir}"
mkdir -p "${storage_root_path}"
if ! start_doris_fe; then
    echo "WARNING: --------------------${DORIS_HOME}/fe/log/fe.out--------------------"
    cat "${DORIS_HOME}"/fe/log/fe.out
    echo "WARNING: ----------------------------------------"
    echo "WARNING: delete meta_dir and storage_root_path, then retry"
    rm -rf "${meta_dir:?}/"*
    rm -rf "${storage_root_path:?}/"*
    start_doris_fe
fi
if ! start_doris_be; then
    echo "WARNING: --------------------${DORIS_HOME}/be/log/be.out--------------------"
    cat "${DORIS_HOME}"/be/log/be.out
    echo "WARNING: ----------------------------------------"
    echo "WARNING: delete storage_root_path, then retry"
    rm -rf "${storage_root_path:?}/"*
    start_doris_be
fi
add_doris_be_to_fe

echo "#### 5. set session variables"
echo "TODO"
