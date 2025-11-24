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
    echo "${sql}" >&2
    mysql $mysqlMTLSInfo -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$@"
}

get_and_save_profile() {
    local runName=$1  # e.g. q1.1_cold / q1.1_hot1
    local queryId
    queryId=$(curl --user ${USER}:${PASSWORD} ${urlHeader}://${FE_HOST}:${FE_HTTP_PORT}/rest/v1/query_profile ${curlMTLSInfo}|jq -r '.data.rows[0]["Profile ID"]')
    if [[ -z "${queryId}" ]]; then
        echo "Fail to find query_id for ${runName}"
        return
    fi
    echo "${runName} -> query_id=${queryId}"

    # Get profile
    local profileFile="${PROFILE_DIR}/${runName}_${queryId}.profile"
    curl -s ${curlMTLSInfo} -u "${USER}:${PASSWORD}" "${urlHeader}://${FE_HOST}:${FE_HTTP_PORT}/api/profile/text?query_id=${queryId}" -o "${profileFile}"
    echo "profile saved: ${profileFile}"
}

load_lineitem_flat() {
    # Loading data in batches by year.
    local flat_con_idx=0
    for con in 'lo_orderdate<19930101' 'lo_orderdate>=19930101 and lo_orderdate<19940101' 'lo_orderdate>=19940101 and lo_orderdate<19950101' 'lo_orderdate>=19950101 and lo_orderdate<19960101' 'lo_orderdate>=19960101 and lo_orderdate<19970101' 'lo_orderdate>=19970101 and lo_orderdate<19980101' 'lo_orderdate>=19980101'; do
        echo -e "\n${con}"
        flat_con_idx=$((flat_con_idx + 1))
        insertStart=$(date +%s%N)
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
        insertEnd=$(date +%s%N)
        insertCost=$(((insertEnd - insertStart) / 1000000))
        inertsql=lineorder_flat_${flat_con_idx}
        get_and_save_profile ${inertsql}
	    echo -ne "${inertsql}\t" | tee -a "${RESULT_CSV}"
        echo -ne "InsertInto\t" | tee -a "${RESULT_CSV}"
        echo -ne "${insertCost}\t" | tee -a "${RESULT_CSV}"
        echo "" | tee -a "${RESULT_CSV}"
    done
}

check_prerequest "curl --version" "curl"

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD}

PROFILE_DIR="load_profiles"
if [[ -d "${PROFILE_DIR}" ]]; then
    rm -r "${PROFILE_DIR}"
fi
mkdir -p "${PROFILE_DIR}"

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

wait_for_load() {
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

start_time=$(date +%s%N)
echo "Start time: $(date)"

if [[ "$ENABLE_S3_LOAD" == true ]]; then
  echo "======== Using S3 Load mode ========"
  tables=("part" "dates" "supplier" "customer" "lineorder")
  declare -A table_columns
  table_columns[part]="p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy"
  table_columns[dates]="d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy"
  table_columns[supplier]="s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy"
  table_columns[customer]="c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use"
  table_columns[lineorder]="lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy"
  for tbl in "${tables[@]}"; do
    label="${tbl}_$(date +%s)"
    echo "Loading table ${tbl} via S3..."
    s3file=$tbl
    if [ "$tbl" == "dates" ];then
       s3file="date"
    fi

    run_sql "
    LOAD LABEL ${label}
    (
        DATA INFILE(\"s3://${S3_BUCKET}/${S3_PATH_PREFIX}/${s3file}.tbl*\")
        INTO TABLE ${tbl}
        COLUMNS TERMINATED BY '|'
        (${table_columns[$tbl]})
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

    wait_for_load "${label}" "${tbl}"
  done

else
    # check if ssb-data exists
    if [[ ! -d ${SSB_DATA_DIR}/ ]]; then
        echo "${SSB_DATA_DIR} does not exist. Run sh gen-ssb-data.sh first."
        exit 1
    fi
    echo "==========Start to stream load data into ssb tables=========="

    declare -A stream_load_tables=(
        ["part"]="p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy"
        ["dates"]="d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy"
        ["supplier"]="s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy"
        ["customer"]="c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use"
    )

    for tbl in part dates supplier customer; do
        echo "Loading data for table: ${tbl}"
        tblfile=$tbl
        if [[ "$tbl" == "dates" ]];then 
            tblfile="date"
        fi

        if [[ -z ${TXN_ID} ]]; then
            res=$(curl --location-trusted -u "${USER}":"${PASSWORD}" \
                -H "Expect: 100-continue" -H "column_separator:|" \
                -H "columns:${stream_load_tables[$tbl]}" \
                -T "${SSB_DATA_DIR}/${tblfile}.tbl" ${urlHeader}://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/${tbl}/_stream_load \
                ${curlMTLSInfo})
        else
            res=$(curl --location-trusted -u "${USER}":"${PASSWORD}" \
                -H "Expect: 100-continue" -H "label:${TXN_ID}_${tbl}" -H "column_separator:|" \
                -H "columns:${stream_load_tables[$tbl]}" \
                -T "${SSB_DATA_DIR}/${tblfile}.tbl" ${urlHeader}://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/${tbl}/_stream_load \
                ${curlMTLSInfo})
        fi
        echo ${res}
        loadCost=-9999999
        if [[ $(echo "${res}" | jq ".Status") == '"Success"' ]]; then
            echo "----loaded ${file}"
            loadCost=$(echo ${res}|awk '{print $25}'|awk -F ','  '{print $1}')
        else
            echo -e "\033[31m----load ${file} FAIL...\n${res}\033[0m"
        fi
	    echo -ne "${tbl}\t" | tee -a "${RESULT_CSV}"
        echo -ne "StreamLoad\t" | tee -a "${RESULT_CSV}"
        echo -ne "${loadCost}\t" | tee -a "${RESULT_CSV}"
        echo "" | tee -a "${RESULT_CSV}"
    done

    echo "Loading data for table: lineorder, with ${PARALLEL} parallel"
    function load() {
        echo "$@"
        
        local FILE_ID="${@//*./}"
        if [[ -z ${TXN_ID} ]]; then
            res=$(curl --location-trusted -u "${USER}":"${PASSWORD}" \
                -H "Expect: 100-continue" -H "column_separator:|" \
                -H "columns:lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy" \
                -T "$@" ${urlHeader}://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/lineorder/_stream_load \
                ${curlMTLSInfo})
        else
            res=$(curl --location-trusted -u "${USER}":"${PASSWORD}" \
                -H "Expect: 100-continue" -H "label:${TXN_ID}_lineorder_${FILE_ID}" -H "column_separator:|" \
                -H "columns:lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy" \
                -T "$@" ${urlHeader}://"${FE_HOST}":"${FE_HTTP_PORT}"/api/"${DB}"/lineorder/_stream_load \
                ${curlMTLSInfo})
        fi
        echo ${res}
        loadCost=-9999999
        status=$(echo ${res}|awk '{print $11}'|awk -F '"'  '{print $2}')
        if [ $status == "Success" ];then
            loadCost=$(echo ${res}|awk '{print $25}'|awk -F ','  '{print $1}')
        fi
	    echo "lineorder_${FILE_ID} $res"
	    echo -ne "lineorder_${FILE_ID}\t" | tee -a "${RESULT_CSV}"
        echo -ne "StreamLoad\t" | tee -a "${RESULT_CSV}"
        echo -ne "${loadCost}\t" | tee -a "${RESULT_CSV}"
        echo "" | tee -a "${RESULT_CSV}"
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
fi

echo "==========Start to insert data into ssb flat table=========="
date
load_lineitem_flat
date
echo '============================================'

end_time=$(date +%s%N)
echo "End time: $(date)"

echo "Finish load ssb data, Time taken: $((end_time - start_time)) seconds"

start=$(date +%s%N)
run_sql "analyze database ${DB} with full with sync;"
end=$(date +%s%N)
totalTime=$(((end - start) / 1000000))
echo "analyze database ${DB} with full with sync total time: ${totalTime} ms"

echo -ne "analyzeDB\t" | tee -a "${RESULT_CSV}"
echo -ne "Analyze\t" | tee -a "${RESULT_CSV}"
echo -ne "${totalTime}\t" | tee -a "${RESULT_CSV}"
echo "" | tee -a "${RESULT_CSV}"
echo '============================================'

