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
source $CURDIR/../conf/cluster.conf
TPCH_DATA_DIR="${DATA_SAVE_DIR}"
if [ "_${TPCH_DATA_DIR}" == "_tpch-data" ];then
    TPCH_DATA_DIR="$CURDIR/../bin/${DATA_SAVE_DIR}"
fi
echo "TPCH_DATA_DIR: ${TPCH_DATA_DIR}"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     -c             parallelism to load data of lineitem, orders, partsupp, default is 5.

  Eg.
    $0              load data using default value.
    $0 -c 10        load lineitem, orders, partsupp table data using parallelism 10.     
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hc:' \
    -- "$@")

eval set -- "${OPTS}"

PARALLEL=5
HELP=0

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

# check if tpch-data exists
if [[ ! -d "${TPCH_DATA_DIR}"/ ]]; then
    echo "${TPCH_DATA_DIR} does not exist. Run sh gen-tpch-data.sh first."
    exit 1
fi

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
source "${CURDIR}/../conf/cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "FE_HOST: ${FE_HOST}"
echo "FE_HTTP_PORT: ${FE_HTTP_PORT}"
echo "USER: ${USER}"
echo "DB: ${DB}"

touch $CURDIR/../tpch_load_result.csv
truncate -s0 $CURDIR/../tpch_load_result.csv

clt=""
exec_clt=""
if [ -z "${PASSWORD}" ];then
    clt="mysql -h${FE_HOST} -u${USER} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB "
else
    clt="mysql -h${FE_HOST} -u${USER} -p${PASSWORD} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -p${PASSWORD} -P$FE_QUERY_PORT -D$DB "
fi

run_sql() {
    echo "$*"
    $clt -e "$*"
}

function load_region() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: r_regionkey, r_name, r_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/region/_stream_load
}
function load_nation() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: n_nationkey, n_name, n_regionkey, n_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/nation/_stream_load
}
function load_supplier() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/supplier/_stream_load
}
function load_customer() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/customer/_stream_load
}
function load_part() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/part/_stream_load
}
function load_partsupp() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/partsupp/_stream_load
}
function load_orders() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/orders/_stream_load
}
function load_lineitem() {
    echo "$*"
    curl --location-trusted -u "${USER}":"${PASSWORD}" -H "column_separator:|" \
        -H "columns: l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp" \
        -T "$*" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/lineitem/_stream_load
}

run_sql "truncate table lineitem;"
run_sql "truncate table orders;"
run_sql "truncate table partsupp;"
run_sql "truncate table part;"
run_sql "truncate table customer;"
run_sql "truncate table supplier;"
run_sql "truncate table nation;"
run_sql "truncate table region;"

# start load
start_time=$(date +%s)
echo "Start time: $(date)"

region_start=$(date +%s.%N)
load_region "${TPCH_DATA_DIR}"/region.tbl
region_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$region_end-$region_start" | bc)
echo -n "region_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

nation_start=$(date +%s.%N)
load_nation "${TPCH_DATA_DIR}"/nation.tbl
nation_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$nation_end-$nation_start" | bc)
echo -n "nation_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

supplier_start=$(date +%s.%N)
load_supplier "${TPCH_DATA_DIR}"/supplier.tbl
supplier_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$supplier_end-$supplier_start" | bc)
echo -n "supplier_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

customer_start=$(date +%s.%N)
load_customer "${TPCH_DATA_DIR}"/customer.tbl
customer_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$customer_end-$customer_start" | bc)
echo -n "customer_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

part_start=$(date +%s.%N)
load_part "${TPCH_DATA_DIR}"/part.tbl
part_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$part_end-$part_start" | bc)
echo -n "part_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

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
lineitem_start=$(date +%s.%N)
for file in "${TPCH_DATA_DIR}"/lineitem.tbl*; do
    # 领取令牌, 即从fd3中读取行, 每次一行
    # 对管道，读一行便少一行，每次只能读取一行
    # 所有行读取完毕, 执行挂起, 直到管道再次有可读行
    # 因此实现了进程数量控制
    read -r -u3

    # 要批量执行的命令放在大括号内, 后台运行
    {
        load_lineitem "${file}"
        echo "----loaded ${file}"
        sleep 2
        # 归还令牌, 即进程结束后，再写入一行，使挂起的循环继续执行
        echo >&3
    } &
done
lineitem_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$lineitem_end-$lineitem_start" | bc)
echo -n "lineitem_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

date
orders_start=$(date +%s.%N)
for file in "${TPCH_DATA_DIR}"/orders.tbl*; do
    read -r -u3
    {
        load_orders "${file}"
        echo "----loaded ${file}"
        sleep 2
        echo >&3
    } &
done
orders_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$orders_end-$orders_start" | bc)
echo -n "orders_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

date
partsupp_start=$(date +%s.%N)
for file in "${TPCH_DATA_DIR}"/partsupp.tbl*; do
    read -r -u3
    {
        load_partsupp "${file}"
        echo "----loaded ${file}"
        sleep 2
        echo >&3
    } &
done
partsupp_end=$(date +%s.%N)
cost_time=$(echo "scale=4;$partsupp_end-$partsupp_start" | bc)
echo -n "partsupp_stream_load:" | tee -a $CURDIR/../tpch_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../tpch_load_result.csv
echo "" | tee -a $CURDIR/../tpch_load_result.csv

# 等待所有的后台子进程结束
wait
# 删除文件标识符
exec 3>&-

end_time=$(date +%s)
echo "End time: $(date)"

echo "Finish load tpch data, Time taken: $((end_time - start_time)) seconds"
