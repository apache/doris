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
if download_oss_file "${pull_request_id:-}_${commit_id:-}.tar.gz"; then
    tar -I pigz -xf "${pull_request_id:-}_${commit_id:-}.tar.gz"
    if [[ -d output && -d output/fe && -d output/be ]]; then
        echo "INFO: be version: $(./output/be/lib/doris_be --version)"
        doris_home="$(pwd)/output"
        rm -rf "${pull_request_id}_${commit_id}.tar.gz"
    fi
fi
echo "#### 2. try to kill old doris process"
if pgrep -fia doris; then pgrep -fi doris | xargs kill -9; fi

echo "#### 3. copy conf from regression-test/pipeline/tpch/tpch-sf100/conf/"
rm -f output/fe/conf/fe_custom.conf output/be/conf/be_custom.conf
cp -f conf/fe.conf "${doris_home}"/fe/conf/
cp -f conf/be.conf "${doris_home}"/be/conf/

echo "#### 4. start Doris"
if ! start_doris_fe "${doris_home}"; then
    echo "WARNING: --------------------${doris_home}/fe/log/fe.out--------------------"
    cat "${doris_home}"/fe/log/fe.out
    echo "WARNING: ----------------------------------------"
    echo "WARNING: delete meta_dir and storage_root_path, then retry"
    meta_dir=$(get_doris_conf_value "${doris_home}"/fe/conf/fe.conf meta_dir)
    storage_root_path=$(get_doris_conf_value "${doris_home}"/be/conf/be.conf storage_root_path)
    rm -rf "${meta_dir:?}/"*
    rm -rf "${storage_root_path:?}/"*
    start_doris_fe "${doris_home}"
fi
if ! start_doris_be "${doris_home}"; then
    echo "WARNING: --------------------${doris_home}/be/log/be.out--------------------"
    cat "${doris_home}"/be/log/be.out
    echo "WARNING: ----------------------------------------"
    echo "WARNING: delete storage_root_path, then retry"
    storage_root_path=$(get_doris_conf_value "${doris_home}"/be/conf/be.conf storage_root_path)
    rm -rf "${storage_root_path:?}/"*
    start_doris_be "${doris_home}"
fi
add_doris_be_to_fe "${doris_home}"

echo "#### 5. set session variables"
echo "TODO"
