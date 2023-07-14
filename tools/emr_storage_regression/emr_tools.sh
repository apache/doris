#!/bin/bash

set -eo pipefail

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]            
     --case             regression case runner: ping, data_set
     --profile          cloud credential profile
     --ak               cloud access key
     --sk               cloud secret key
     --endpoint         cloud endpoint
     --region           cloud region
     --service          cloud optional service provider: ali, tx, hw
     --host             doris mysql cli host, example: 127.0.0.1
     --user             doris username, example: user
     --port             doris port, example: 9030
  Example:
    sh emr_tools.sh --case ping --endpoint dlf-vpc.cn-beijing.aliyuncs.com --region cn-beijing  --service ali --ak ak --sk sk
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'case:' \
    -l 'profile:' \
    -l 'ak:' \
    -l 'sk:' \
    -l 'endpoint:' \
    -l 'region:' \
    -l 'service:' \
    -l 'host:' \
    -l 'user:' \
    -l 'port:' \
    -o 'h' \
    -- "$@")"; then
    usage
fi
eval set -- "${OPTS}"

while true; do
    case "$1" in
    --profile)
        PROFILE="$2"
        # can use custom profile: sh emr_tools.sh --profile default_emr_env.sh
        if [[ -n "${PROFILE}" ]]; then
          # shellcheck source="$(pwd)/default_emr_env.sh"
          source "${PROFILE}"
        fi
        shift 2
        break
        ;;
    --case)
        CASE="$2"
        shift 2
        ;;
    --ak)
        AK="$2"
        shift 2
        ;;
    --sk)
        SK="$2"
        shift 2
        ;;
    --endpoint)
        ENDPOINT="$2"
        shift 2
        ;;
    --region)
        REGION="$2"
        shift 2
        ;;
    --service)
        SERVICE="$2"
        shift 2
        ;;
    --host)
        HOST="$2"
        shift 2
        ;;
    --user)
        USER="$2"
        shift 2
        ;;
    --port)
        PORT="$2"
        shift 2
        ;;
    -h)
        usage
        shift
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "$1"
        echo "Internal error"
        exit 1
        ;;
    esac
done

export FE_HOST=${HOST}
export USER=${USER}
export FE_QUERY_PORT=${PORT}

if [[ ${CASE} == 'ping' ]]; then
  if [[ ${SERVICE} == 'hw' ]]; then
    HMS_META_URI="thrift://node-master1rjzj.mrs-dr8h.com:9083,thrift://node-master2rksk.mrs-dr8h.com:9083"
    BEELINE_URI="jdbc:hive2://192.168.0.8:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;hive.server2.proxy.user=hive"
    HMS_WAREHOUSE=obs://datalake-bench/user
  elif [[ ${SERVICE} == 'ali' ]]; then
    HMS_META_URI="thrift://172.16.1.162:9083"
    HMS_WAREHOUSE=oss://benchmark-oss/user
  else
    # [[ ${SERVICE} == 'tx' ]];
    HMS_META_URI="thrift://172.21.0.32:7004"
    HMS_WAREHOUSE=cosn://datalake-bench-cos-1308700295/user
  fi
  sh ping_test/ping_poc.sh "${ENDPOINT}" "${REGION}" "${SERVICE}" "${AK}" "${SK}" "${HMS_META_URI}" "${HMS_WAREHOUSE}" "${BEELINE_URI}"
elif [[ ${CASE} == 'data_set' ]]; then
  if [[ ${SERVICE} == 'tx' ]]; then
      BUCKET=cosn://datalake-bench-cos-1308700295
  elif [[ ${SERVICE} == 'ali' ]]; then
      BUCKET=oss://benchmark-oss
  fi
  # gen table for spark
  if ! sh stardard_set/gen_spark_create_sql.sh "${BUCKET}" obj; then
    echo "Fail to generate spark obj table for test set"
    exit 1
  fi
  if ! sh stardard_set/gen_spark_create_sql.sh hdfs:///benchmark-hdfs hdfs; then
    echo "Fail to generate spark hdfs table for test set, import hdfs data first"
    exit 1
  fi
  
  # FE_HOST=172.16.1.163
  # USER=root
  # PORT=9035
  TYPE=hdfs sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" hms_hdfs
  TYPE=hdfs sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" iceberg_hms
  if [[ ${SERVICE} == 'tx' ]]; then
    sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" hms_cos 
    sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" iceberg_hms_cos
  elif [[ ${SERVICE} == 'ali' ]]; then
    sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" hms_oss
    sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" iceberg_hms_oss
  fi
fi
