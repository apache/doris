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
# This script is used to load generated ssb data set to Doris
# Only for 1 fact table: lineorder
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
SSB_DATA_DIR="${CURDIR}/ssb-data/"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
    -c              parallelism to load data of lineorder table, default is 5.
    -x              use transaction id. multi times of loading with the same id won't load duplicate data.

  Eg.
    $0              load data using default value.
    $0 -c 10        load lineorder table data using parallelism 10.
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

# check if ssb-data exists
if [[ ! -d ${SSB_DATA_DIR}/ ]]; then
    echo "${SSB_DATA_DIR} does not exist. Run sh gen-ssb-data.sh first."
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

run_sql() {
    sql="$*"
    echo "${sql}"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$@"
}

load_lineitem_flat() {
    # Loading data in batches by year.
    local flat_con_idx=0
    for con in 'lo_orderdate<19930101' 'lo_orderdate>=19930101 and lo_orderdate<19940101' 'lo_orderdate>=19940101 and lo_orderdate<19950101' 'lo_orderdate>=19950101 and lo_orderdate<19960101' 'lo_orderdate>=19960101 and lo_orderdate<19970101' 'lo_orderdate>=19970101 and lo_orderdate<19980101' 'lo_orderdate>=19980101'; do
        echo -e "\n${con}"
        flat_con_idx=$((flat_con_idx + 1))

        if [[ -z ${TXN_ID} ]]; then
            run_sql "
INSERT INTO lineorder_flat
SELECT
    LO_ORDERDATE,
    LO_ORDERKEY,
    LO_LINENUMBER,
    LO_CUSTKEY,
    LO_PARTKEY,
    LO_SUPPKEY,
    LO_ORDERPRIORITY,
    LO_SHIPPRIORITY,
    LO_QUANTITY,
    LO_EXTENDEDPRICE,
    LO_ORDTOTALPRICE,
    LO_DISCOUNT,
    LO_REVENUE,
    LO_SUPPLYCOST,
    LO_TAX,
    LO_COMMITDATE,
    LO_SHIPMODE,
    C_NAME,
    C_ADDRESS,
    C_CITY,
    C_NATION,
    C_REGION,
    C_PHONE,
    C_MKTSEGMENT,
    S_NAME,
    S_ADDRESS,
    S_CITY,
    S_NATION,
    S_REGION,
    S_PHONE,
    P_NAME,
    P_MFGR,
    P_CATEGORY,
    P_BRAND,
    P_COLOR,
    P_TYPE,
    P_SIZE,
    P_CONTAINER
FROM (
    SELECT
        lo_orderkey,
        lo_linenumber,
        lo_custkey,
        lo_partkey,
        lo_suppkey,
        lo_orderdate,
        lo_orderpriority,
        lo_shippriority,
        lo_quantity,
        lo_extendedprice,
        lo_ordtotalprice,
        lo_discount,
        lo_revenue,
        lo_supplycost,
        lo_tax,
        lo_commitdate,
        lo_shipmode
    FROM lineorder
    WHERE ${con}
) l
INNER JOIN customer c
ON (c.c_custkey = l.lo_custkey)
INNER JOIN supplier s
ON (s.s_suppkey = l.lo_suppkey)
INNER JOIN part p
ON (p.p_partkey = l.lo_partkey);
"
        else
            run_sql "
INSERT INTO lineorder_flat
WITH LABEL \`${TXN_ID}_flat_${flat_con_idx}\`
SELECT
    LO_ORDERDATE,
    LO_ORDERKEY,
    LO_LINENUMBER,
    LO_CUSTKEY,
    LO_PARTKEY,
    LO_SUPPKEY,
    LO_ORDERPRIORITY,
    LO_SHIPPRIORITY,
    LO_QUANTITY,
    LO_EXTENDEDPRICE,
    LO_ORDTOTALPRICE,
    LO_DISCOUNT,
    LO_REVENUE,
    LO_SUPPLYCOST,
    LO_TAX,
    LO_COMMITDATE,
    LO_SHIPMODE,
    C_NAME,
    C_ADDRESS,
    C_CITY,
    C_NATION,
    C_REGION,
    C_PHONE,
    C_MKTSEGMENT,
    S_NAME,
    S_ADDRESS,
    S_CITY,
    S_NATION,
    S_REGION,
    S_PHONE,
    P_NAME,
    P_MFGR,
    P_CATEGORY,
    P_BRAND,
    P_COLOR,
    P_TYPE,
    P_SIZE,
    P_CONTAINER
FROM (
    SELECT
        lo_orderkey,
        lo_linenumber,
        lo_custkey,
        lo_partkey,
        lo_suppkey,
        lo_orderdate,
        lo_orderpriority,
        lo_shippriority,
        lo_quantity,
        lo_extendedprice,
        lo_ordtotalprice,
        lo_discount,
        lo_revenue,
        lo_supplycost,
        lo_tax,
        lo_commitdate,
        lo_shipmode
    FROM lineorder
    WHERE ${con}
) l
INNER JOIN customer c
ON (c.c_custkey = l.lo_custkey)
INNER JOIN supplier s
ON (s.s_suppkey = l.lo_suppkey)
INNER JOIN part p
ON (p.p_partkey = l.lo_partkey);
"
        fi
    done
}

check_prerequest "curl --version" "curl"

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "FE_HOST: ${FE_HOST}"
echo "FE_HTTP_PORT: ${FE_HTTP_PORT}"
echo "USER: ${USER}"
echo "DB: ${DB}"

start_time=$(date +%s)
echo "Start time: $(date)"
echo "==========Start to load data into ssb tables=========="

echo 'Loading data for table: part'
if [[ -z ${TXN_ID} ]]; then
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "column_separator:|" \
        -H "columns:p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy" \
        -T "${SSB_DATA_DIR}"/part.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/part/_stream_load
else
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "label:${TXN_ID}_part" -H "column_separator:|" \
        -H "columns:p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy" \
        -T "${SSB_DATA_DIR}"/part.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/part/_stream_load
fi

echo 'Loading data for table: dates'
if [[ -z ${TXN_ID} ]]; then
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "column_separator:|" \
        -H "columns:d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy" \
        -T "${SSB_DATA_DIR}"/date.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/dates/_stream_load
else
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "label:${TXN_ID}_date" -H "column_separator:|" \
        -H "columns:d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy" \
        -T "${SSB_DATA_DIR}"/date.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/dates/_stream_load
fi

echo 'Loading data for table: supplier'
if [[ -z ${TXN_ID} ]]; then
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "column_separator:|" \
        -H "columns:s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy" \
        -T "${SSB_DATA_DIR}"/supplier.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/supplier/_stream_load
else
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "label:${TXN_ID}_supplier" -H "column_separator:|" \
        -H "columns:s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy" \
        -T "${SSB_DATA_DIR}"/supplier.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/supplier/_stream_load
fi

echo 'Loading data for table: customer'
if [[ -z ${TXN_ID} ]]; then
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "column_separator:|" \
        -H "columns:c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use" \
        -T "${SSB_DATA_DIR}"/customer.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/customer/_stream_load
else
    curl --location-trusted -u "${USER}":"${PASSWORD}" \
        -H "label:${TXN_ID}_customer" -H "column_separator:|" \
        -H "columns:c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use" \
        -T "${SSB_DATA_DIR}"/customer.tbl http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/customer/_stream_load
fi

echo "Loading data for table: lineorder, with ${PARALLEL} parallel"
function load() {
    echo "$@"
    # shellcheck disable=SC2016,SC2124
    local FILE_ID="${@//*./}"
    if [[ -z ${TXN_ID} ]]; then
        curl --location-trusted -u "${USER}":"${PASSWORD}" \
            -H "column_separator:|" \
            -H "columns:lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy" \
            -T "$@" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/lineorder/_stream_load
    else
        curl --location-trusted -u "${USER}":"${PASSWORD}" \
            -H "label:${TXN_ID}_lineorder_${FILE_ID}" -H "column_separator:|" \
            -H "columns:lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy" \
            -T "$@" http://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/lineorder/_stream_load
    fi
}

# set parallelism
[[ -e /tmp/fd1 ]] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1

for ((i = 1; i <= PARALLEL; i++)); do
    echo >&3
done

date
for file in "${SSB_DATA_DIR}"/lineorder.tbl.*; do
    read -r -u3
    {
        load "${file}"
        echo >&3
    } &
done

# wait for child thread finished
wait
date

echo "==========Start to insert data into ssb flat table=========="
date
load_lineitem_flat
date
echo '============================================'

end_time=$(date +%s)
echo "End time: $(date)"

echo "Finish load ssb data, Time taken: $((end_time - start_time)) seconds"

start=$(date +%s)
run_sql "analyze database ${DB} with full with sync;"
end=$(date +%s)
totalTime=$((end - start))
echo "analyze database ${DB} with full with sync total time: ${totalTime} s"
echo '============================================'
