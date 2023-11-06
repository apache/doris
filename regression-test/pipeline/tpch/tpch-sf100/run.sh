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

## run.sh content ##

# check_tpch_table_rows, stop_doris, set_session_variable
source ../../common/doris-utils.sh
# create_an_issue_comment
source ../../common/github-utils.sh

echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_id}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_id or commit_id not set"
    exit 1
fi

echo "#### Run tpch-sf100 test on Doris ####"
exit_flag=0

(
    set -e
    shopt -s inherit_errexit

    echo "#### 1. check if need to load data"
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
                cd "${TPCH_DATA_DIR}" || exit 1
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
        if ! check_tpch_table_rows "${db_name}" "${SF}"; then
            exit 1
        fi
    fi

    echo "#### 2. run tpch-sf${SF} query"
    sed -i "s|^SCALE_FACTOR=[0-9]\+$|SCALE_FACTOR=${SF}|g" "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
    line_begin=$((line_end - 23))
    comment_body="Tpch sf${SF} test resutl on commit ${commit_id:-}

run tpch-sf${SF} query with default conf and session variables
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

    echo "#### 3. run tpch-sf${SF} query with runtime_filter_mode=off"
    set_session_variable runtime_filter_mode off
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
    line_begin=$((line_end - 23))
    comment_body="${comment_body}

run tpch-sf${SF} query with default conf and set session variable runtime_filter_mode=off
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

    echo "#### 4. comment result on tpch"
    comment_body=$(echo "${comment_body}" | sed -e ':a;N;$!ba;s/\n/\\n/g') # 将所有的换行符替换为\n
    comment_body='Tpch sf1 test resutl on commit 63363d994c1692ec988cb521e30d34e0418d1a2c\n\nrun tpch-sf1 query with default conf and session variables\nq1  382  204  203  203\nq2  428  203  211  203\nq3  126  102  103  102\nq4  117  102  85  85\nq5  180  158  155  155\nq6  44  43  40  40\nq7  198  153  157  153\nq8  197  209  188  188\nq9  300  271  247  247\nq10  119  123  118  118\nq11  124  117  129  117\nq12  80  69  69  69\nq13  110  108  96  96\nq14  45  65  44  44\nq15  81  65  68  65\nq16  117  231  109  109\nq17  122  109  112  109\nq18  185  218  185  185\nq19  66  58  60  58\nq20  115  120  124  120\nq21  265  250  266  250\nq22  66  61  64  61\nTotal cold run time: 3467 ms\nTotal hot run time: 2777 ms\n\nrun tpch-sf1 query with default conf and set session variable runtime_filter_mode=off\nq1  160  177  156  156\nq2  124  132  133  132\nq3  97  92  100  92\nq4  76  76  69  69\nq5  138  145  144  144\nq6  40  41  32  32\nq7  146  130  133  130\nq8  161  165  152  152\nq9  244  226  207  207\nq10  100  101  108  101\nq11  96  101  108  101\nq12  62  68  68  68\nq13  114  108  103  103\nq14  39  38  42  38\nq15  59  54  59  54\nq16  83  97  83  83\nq17  98  90  92  90\nq18  178  176  226  176\nq19  59  54  60  54\nq20  112  125  110  110\nq21  247  226  235  226\nq22  69  59  60  59\nTotal cold run time: 2502 ms\nTotal hot run time: 2377 ms'
    create_an_issue_comment "${pull_request_id:-}" "${comment_body}"

    stop_doris
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    print_doris_fe_log
    print_doris_be_log
    archive_doris_logs "${pull_request_id}_${commit_id}_doris_logs.tar.gz"
    upload_doris_log_to_oss "${pull_request_id}_${commit_id}_doris_logs.tar.gz"
fi

exit "${exit_flag}"
