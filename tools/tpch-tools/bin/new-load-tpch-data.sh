#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to load generated TPC-H data set into Doris.
# for table lineitem, orders, partsupp, they will be loading in parallel
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
TPCH_DATA_DIR="${CURDIR}/tpch-data"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
    -c             parallelism to load data of lineitem, orders, partsupp, default is 5.
    -x              use transaction id. multi times of loading with the same id won't load duplicate data.

  Eg.
    $0              load data using default value.
    $0 -c 10        load lineitem, orders, partsupp table data using parallelism 10.
    $0 -x blabla    use transaction id \"blabla\".
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hc:x:' \
    -- "$@")

eval set -- "${OPTS}"

PARALLEL=5
HELP=0
TXN_ID=""

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -c)
        PARALLEL=$2
        shift 2
        ;;
    -x)
        TXN_ID=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Parallelism: ${PARALLEL}"

run_sql() {
    echo "$*" >&2
    mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$*"
}

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! ${CMD}; then
        echo "${NAME} is missing. This script depends on cURL to load data to Doris."
        exit 1
    fi
}

check_prerequest "curl --version" "curl"

# load tables
source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "Parallelism: ${PARALLEL}"
echo "FE_HOST: ${FE_HOST}"
echo "FE_HTTP_PORT: ${FE_HTTP_PORT}"
echo "USER: ${USER}"
echo "DB: ${DB}"

urlHeader="http"
if [[ "a${ENABLE_MTLS}" == "atrue" ]] && \
   [[ -n "${CERT_PATH}" ]] && \
   [[ -n "${KEY_PATH}" ]] && \
   [[ -n "${CACERT_PATH}" ]]; then
    export mysqlMTLSInfo="--ssl-mode=VERIFY_CA --tls-version=TLSv1.2 --ssl-ca=${CACERT_PATH} --ssl-cert=${CERT_PATH} --ssl-key=${KEY_PATH}"
    export curlMTLSInfo="--cert ${CERT_PATH} --key ${KEY_PATH} --cacert ${CACERT_PATH}"
    urlHeader="https"
fi

if [[ "a${ENABLE_PROFILE}" == "atrue" ]]; then
    run_sql "set global enable_profile=true;"
else
    run_sql "set global enable_profile=false;"
fi

RESULT_CSV="load_time_summary.csv"
echo -ne "object\t" | tee -a "${RESULT_CSV}"
echo -ne "OpType\t" | tee -a "${RESULT_CSV}"
echo -ne "ms\t" | tee -a "${RESULT_CSV}"
echo "" | tee -a "${RESULT_CSV}"

declare -A table_columns=(
    ["region"]="r_regionkey, r_name, r_comment, temp"
    ["nation"]="n_nationkey, n_name, n_regionkey, n_comment, temp"
    ["supplier"]="s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, temp"
    ["customer"]="c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp"
    ["part"]="p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp"
    ["partsupp"]="ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, temp"
    ["orders"]="o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp"
    ["lineitem"]="l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment, temp"
)

wait_for_s3_load() {
    local label=$1
    local tbl=$2
    echo "Waiting for load $label to finish..."
    while true; do
        state=$(run_sql "SHOW LOAD WHERE Label = '${label}' ORDER BY CreateTime DESC LIMIT 1;" | awk '{print $3}' |tail -n 1)
        if [[ "$state" == "FINISHED" ]]; then
            echo "Load $label finished."
            startTime=$(run_sql "SHOW LOAD WHERE Label = '${label}' ORDER BY CreateTime DESC LIMIT 1\G;" |awk 'NR==10'|awk -F 'CreateTime: ' '{print $2}')
            finishTime=$(run_sql "SHOW LOAD WHERE Label = '${label}' ORDER BY CreateTime DESC LIMIT 1\G;" |awk 'NR==14'|awk -F 'LoadFinishTime: ' '{print $2}')
            start_seconds=$(date -d "$startTime" +%s)
            end_seconds=$(date -d "$finishTime" +%s)
            timeCost=$((end_seconds - start_seconds))
            echo -ne "${tbl}\t" | tee -a "${RESULT_CSV}"
            echo -ne "S3Load\t" | tee -a "${RESULT_CSV}"
            echo -ne "${timeCost}\t" | tee -a "${RESULT_CSV}"
            echo "" | tee -a "${RESULT_CSV}"
            break
        elif [[ "$state" == "CANCELLED" ]]; then
            echo "Load $label cancelled."
            exit 1
        fi
        sleep 5
    done
}

function do_stream_load() {
    local table="$1"
    local columns="$2"
    local file="${3:-no}"
    if echo "partsupp|orders|lineitems"|grep -gw "${table}" ] ;then
        local FILE_ID="${file//*./}"
    fi

    if [[ -z ${TXN_ID} ]]; then
        ret=$(curl -s --location-trusted -u "${USER}":"${PASSWORD}" -H "Expect: 100-continue" -H "column_separator:|" \
            -H "columns: ${columns}" \
            -T "${file}" ${urlHeader}://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/${table}/_stream_load \
            ${curlMTLSInfo})
    else
        ret=$(curl -s --location-trusted -u "${USER}":"${PASSWORD}" -H "Expect: 100-continue" -H "column_separator:|" \
            -H "label:${TXN_ID}_${table}_${FILE_ID}" \
            -H "columns: ${columns}" \
            -T "${file}" ${urlHeader}://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/${table}/_stream_load \
            ${curlMTLSInfo})
    fi

    loadCost=-9999999
    if [[ $(echo "${ret}" | jq ".Status") == '"Success"' ]]; then
        echo "----loaded ${file}"
        loadCost=$(echo ${ret}|awk '{print $25}'|awk -F ','  '{print $1}')
    else
        echo -e "\033[31m----load ${file} FAIL...\n${ret}\033[0m"
    fi
    echo -ne "${${file}}\t" | tee -a "${RESULT_CSV}"
    echo -ne "StreamLoad\t" | tee -a "${RESULT_CSV}"
    echo -ne "${loadCost}\t" | tee -a "${RESULT_CSV}"
    echo "" | tee -a "${RESULT_CSV}"
}

if [[ "$ENABLE_S3_LOAD" == true ]]; then
  echo "======== Using S3 Load mode ========"
  for table_name in ${!table_columns[*]}; do
    label="${table_name}_$(date +%s)"
    echo "Loading table ${table_name} via S3..."
    s3file="${table_name}.tbl.gz"
    if [ "$table_name" == "lineitem" ];then
        s3file="lineitem.tbl.*"
    elif [ "$table_name" == "orders" ];then
        s3file="orders.tbl.*"
    elif [ "$table_name" == "partsupp" ];then
        s3file="partsupp.tbl.*"
    fi
    run_sql "
    LOAD LABEL ${label}
    (
        DATA INFILE(\"s3://${S3_BUCKET}/${S3_PATH_PREFIX}/${s3file}\")
        INTO TABLE ${table_name}
        COLUMNS TERMINATED BY '|'
        (${table_columns[$table_name]})
        PROPERTIES(\"skip_lines\" = \"0\")
    )
    WITH S3 (
        \"AWS_ENDPOINT\" = \"${S3_ENDPOINT}\",
        \"AWS_REGION\" = \"${S3_REGION}\",
        \"AWS_ACCESS_KEY\" = \"${S3_ACCESS_KEY}\",
        \"AWS_SECRET_KEY\" = \"${S3_SECRET_KEY}\"
    )
    PROPERTIES (\"timeout\" = \"3600\", \"max_filter_ratio\" = \"0.1\");
    "
    wait_for_s3_load "${label}" "${table_name}"
  done
else
    # check if tpch-data exists
    if [[ ! -d "${TPCH_DATA_DIR}"/ ]]; then
        echo "${TPCH_DATA_DIR} does not exist. Run sh gen-tpch-data.sh first."
        exit 1
    fi
    # start load
    start_time=$(date +%s)
    echo "Start time: $(date)"
    do_stream_load "region" "$table_columns[region]" "${TPCH_DATA_DIR}"/region.tbl
    do_stream_load "nation" "$table_columns[nation]" "${TPCH_DATA_DIR}"/nation.tbl
    do_stream_load "supplier" "$table_columns[supplier]" "${TPCH_DATA_DIR}"/supplier.tbl
    do_stream_load "customer" "$table_columns[customer]" "${TPCH_DATA_DIR}"/customer.tbl
    do_stream_load "part" "$table_columns[part]" "${TPCH_DATA_DIR}"/part.tbl

    # set parallelism

    # 以PID为名, 防止创建命名管道时与已有文件重名，从而失败
    fifo="/tmp/$$.fifo"
    # 创建命名管道
    mkfifo "${fifo}"
    # 以读写方式打开命名管道，文件标识符fd为3，fd可取除0，1，2，5外0-9中的任意数字
    exec 3<>"${fifo}"
    # 删除文件, 也可不删除, 不影响后面操作
    rm -rf "${fifo}"

    # 在fd3中放置$PARALLEL个空行作为令牌
    for ((i = 1; i <= PARALLEL; i++)); do
        echo >&3
    done

    date
    for file in "${TPCH_DATA_DIR}"/lineitem.tbl*; do
        # 领取令牌, 即从fd3中读取行, 每次一行
        # 对管道，读一行便少一行，每次只能读取一行
        # 所有行读取完毕, 执行挂起, 直到管道再次有可读行
        # 因此实现了进程数量控制
        read -r -u3

        # 要批量执行的命令放在大括号内, 后台运行
        {
            do_stream_load "lineitem" "$table_columns[lineitem]" "${file}"
            echo "----loaded ${file}"
            sleep 2
            # 归还令牌, 即进程结束后，再写入一行，使挂起的循环继续执行
            echo >&3
        } &
    done

    date
    for file in "${TPCH_DATA_DIR}"/orders.tbl*; do
        read -r -u3
        {
            do_stream_load "orders" "$table_columns[orders]" "${file}"
            echo "----loaded ${file}"
            sleep 2
            echo >&3
        } &
    done

    date
    for file in "${TPCH_DATA_DIR}"/partsupp.tbl*; do
        read -r -u3
        {
            do_stream_load "partsupp" "$table_columns[partsupp]" "${file}"
            echo "----loaded ${file}"
            sleep 2
            echo >&3
        } &
    done

    # 等待所有的后台子进程结束
    wait
    # 删除文件标识符
    exec 3>&-
fi

end_time=$(date +%s)
echo "End time: $(date)"

echo "Finish load tpch data, Time taken: $((end_time - start_time)) seconds"
echo '============================================'

start=$(date +%s)
run_sql "analyze database ${DB} with full with sync;"
end=$(date +%s)
totalTime=$((end - start))
echo "analyze database ${DB} with full with sync total time: ${totalTime} s"
echo -ne "analyzeDB\t" | tee -a "${RESULT_CSV}"
echo -ne "Analyze\t" | tee -a "${RESULT_CSV}"
echo -ne "${totalTime}\t" | tee -a "${RESULT_CSV}"
echo "" | tee -a "${RESULT_CSV}"
echo '============================================'
