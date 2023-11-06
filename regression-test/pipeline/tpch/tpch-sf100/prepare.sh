#!/bin/bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/tpch/tpch-sf100/prepare.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x prepare.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/prepare.sh" && exit 1
fi
EOF

## run.sh content ##

echo "#### Check env"
if [[ -z "${commit_id_from_trigger}" || -z ${commit_id:-} || -z ${pull_request_id:-} ]]; then
    echo "ERROR: env commit_id_from_trigger or commit_id or pull_request_id not set" && exit 1
else
    commit_id_from_checkout=${commit_id}
fi
if ${DEBUG:-false}; then commit_id_from_trigger=${commit_id}; fi

echo "Prepare to run tpch sf100 test"

echo "#### 1. check if need run"
if [[ "${commit_id_from_trigger}" != "${commit_id_from_checkout}" ]]; then
    echo -e "目前是在 clickbench 流水线 compile 完后触发本 tpch 流水线的，
有可能 pr 在 clickbench 流水线还在跑的时候新提交了commit，
这时候 tpch 流水线 checkout 出来的 commit 就不是触发时的传过来的 commit了，
这种情况不需要跑"
    echo -e "ERROR: PR(${pull_request_id}),
    the lastest commit id
    ${commit_id_from_checkout}
    not equail to the commit_id_from_trigger
    ${commit_id_from_trigger}
    commit_id_from_trigger is outdate"
    exit 1
fi

if ! [[ -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/fe.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/conf/be.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/deploy.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/run.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/load-tpch-data.sh &&
    -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/create-tpch-tables.sh &&
    -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh ]]; then
    echo "ERROR: depending files missing" && exit 1
fi
