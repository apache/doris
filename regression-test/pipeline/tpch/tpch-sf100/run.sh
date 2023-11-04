#!/bin/bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/tpch/tpch-sf100/run.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x run.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/run.sh" && exit 1
fi
EOF

## deploy.sh content ##
# check_tpch_table_rows, stop_doris
source ../../common/doris-utils.sh
# create_an_issue_comment
source ../../common/github-utils.sh
set -e
shopt -s inherit_errexit

echo "#### Run tpch-sf100 test on Doris ####"
echo "## 1. check if need to load data"
SF="100" # SCALE FACTOR
if ${DEBUG:-false}; then
    SF="1"
fi
TPCH_DATA_DIR="/data/tpch/sf_${SF}"                                               # no / at the end
TPCH_DATA_DIR_LINK="${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/tpch-data # no / at the end
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
db_name="tpch_sf${SF}"
sed -i "s|^export DB=.*$|export DB='${db_name}'|g" \
    "${teamcity_build_checkoutDir}"/tools/tpch-tools/conf/doris-cluster.conf
if ! check_tpch_table_rows "${db_name}" "${SF}"; then
    echo "INFO: need to load tpch-sf${SF} data"
    # prepare data
    if [[ ! -d ${TPCH_DATA_DIR} ]]; then
        mkdir -p "${TPCH_DATA_DIR}"
        (
            cd "${TPCH_DATA_DIR}"
            declare -A table_file_count
            table_file_count=(['region']=1 ['nation']=1 ['supplier']=1 ['customer']=1 ['part']=1 ['partsupp']=10 ['orders']=10 ['lineitem']=10)
            for table_name in ${!table_file_count[*]}; do
                if [[ ${table_file_count[${table_name}]} -eq 1 ]]; then
                    url="https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/tpch/sf${SF}/${table_name}.tbl"
                    if ! wget --continue -t3 "${url}"; then echo "ERROR: wget --continue ${url}" && return 1; fi
                elif [[ ${table_file_count[${table_name}]} -eq 10 ]]; then
                    for i in {1..10}; do
                        url="https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/tpch/sf${SF}/${table_name}.tbl.${i}"
                        if ! wget --continue -t3 "${url}"; then echo "ERROR: wget --continue ${url}" && return 1; fi
                    done
                fi
            done
        )
    fi
    # create table and load data
    sed -i "s|^SCALE_FACTOR=[0-9]\+$|SCALE_FACTOR=${SF}|g" "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/create-tpch-tables.sh
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/create-tpch-tables.sh
    rm -rf "${TPCH_DATA_DIR_LINK}"
    ln -s "${TPCH_DATA_DIR}" "${TPCH_DATA_DIR_LINK}"
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/load-tpch-data.sh -c 10
fi

echo "## 2. run tpch-sf${SF} query"
sed -i "s|^SCALE_FACTOR=[0-9]\+$|SCALE_FACTOR=${SF}|g" "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh
bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
line_begin=$((line_end - 24))

comment_body="Tpch sf${SF} test resutl on commit ${commit_id:-}
run tpch-sf${SF} query with default conf and session variables
$(sed -n "${line_begin},${line_end}" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

echo "## 3. run tpch-sf${SF} query with runtime_filter_mode=off"
set_session_variable runtime_filter_mode off
bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
line_begin=$((line_end - 24))
comment_body="run tpch-sf${SF} query with default conf and set session variables runtime_filter_mode=off
$(sed -n "${line_begin},${line_end}" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

echo "## 4. comment result on tpch"
create_an_issue_comment "${pull_request_id:-}" "${comment_body}"

stop_doris
