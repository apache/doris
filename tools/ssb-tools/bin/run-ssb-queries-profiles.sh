#!/usr/bin/env bash
set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(cd "${ROOT}" && pwd)
CURDIR=${ROOT}
QUERIES_DIR=${CURDIR}/../ssb-queries

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD:-}

echo "FE_HOST: ${FE_HOST:='127.0.0.1'}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT:='9030'}"
echo "FE_HTTP_PORT: ${FE_HTTP_PORT:='8030'}"
echo "USER: ${USER:='root'}"
echo "DB: ${DB:='ssb'}"

urlHeader="http"
# 支持 mTLS 场景
if [[ "a${ENABLE_MTLS}" == "atrue" ]] && \
   [[ -n "${CERT_PATH}" ]] && \
   [[ -n "${KEY_PATH}" ]] && \
   [[ -n "${CACERT_PATH}" ]]; then
    export mysqlMTLSInfo="--ssl-mode=VERIFY_CA --tls-version=TLSv1.2 --ssl-ca=${CACERT_PATH} --ssl-cert=${CERT_PATH} --ssl-key=${KEY_PATH}"
    export curlMTLSInfo="--cert ${CERT_PATH} --key ${KEY_PATH} --cacert ${CACERT_PATH}"
    urlHeader="https"
fi

run_sql() {
    echo "$@"
    mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${USER}" -D"${DB}" -e "$@"
}

PROFILE_DIR="profiles"
mkdir -p "${PROFILE_DIR}"
touch result.csv

cold_run_sum=0
best_hot_run_sum=0

# =========================
# 新增函数：获取最新 query_id 并下载 profile
# =========================
get_and_save_profile() {
    local runName=$1  # e.g. q1.1_cold / q1.1_hot1
    local queryId
    queryId=$(curl --user root: ${urlHeader}://${FE_HOST}:${FE_HTTP_PORT}/rest/v1/query_profile ${curlMTLSInfo}|jq -r '.data.rows[0]["Profile ID"]')
    if [[ -z "${queryId}" ]]; then
        echo "❌ 未找到 query_id for ${runName}"
        return
    fi
    echo "✅ ${runName} -> query_id=${queryId}"

    # 获取 profile
    local profileFile="${PROFILE_DIR}/${runName}_${queryId}.profile"
    curl -s ${curlMTLSInfo} -u "${USER}:" "${urlHeader}://${FE_HOST}:${FE_HTTP_PORT}/api/profile/text?query_id=${queryId}" -o "${profileFile}"
    echo "profile saved: ${profileFile}"
}

# =========================
# 主循环逻辑
# =========================
cold_run_sum=0
best_hot_run_sum=0

for i in '1.1' '1.2' '1.3' '2.1' '2.2' '2.3' '3.1' '3.2' '3.3' '3.4' '4.1' '4.2' '4.3'; do
    echo -ne "q${i}\t" | tee -a result.csv
    for runType in cold hot1 hot2; do
        start=$(date +%s%3N)
        if ! output=$(mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments \
            <"${QUERIES_DIR}/q${i}.sql" 2>&1); then
            printf "Error: Failed to execute query q%s (%s run). Output:\n%s\n" "${i}" "${runType}" "${output}" >&2
            continue
        fi
        end=$(date +%s%3N)
        cost=$((end - start))
        echo -ne "${cost}\t" | tee -a result.csv

        if [ ${runType} == "cold" ];then
                cold=${cost}
        fi
        if [ ${runType} == "hot1" ];then
                hot1=${cost}
        fi
        if [ ${runType} == "hot2" ];then
                hot2=${cost}
        fi
        #每次执行完一个查询，立即下载 profile
        get_and_save_profile "q${i}_${runType}"
    done
    cold_run_sum=$((cold_run_sum + cold))
    if [[ ${hot1} -lt ${hot2} ]]; then
        best_hot_run_sum=$((best_hot_run_sum + hot1))
        echo -ne "${hot1}" | tee -a result.csv
        echo "" | tee -a result.csv
    else
        best_hot_run_sum=$((best_hot_run_sum + hot2))
        echo -ne "${hot2}" | tee -a result.csv
        echo "" | tee -a result.csv
    fi
    #echo "" | tee -a result.csv
done

echo "✅ All queries finished. Profiles saved under: ${PROFILE_DIR}"

