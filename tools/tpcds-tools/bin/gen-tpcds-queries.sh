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
# This script is used to generate TPC-DS tables
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
TPCDS_DSQGEN_DIR="${CURDIR}/DSGen-software-code-3.2.0rc1/tools"
TPCDS_QUERIE_DIR="${CURDIR}/../queries"

usage() {
    echo "
This script is used to generate TPC-DS 99 queries of different scale,
Usage: $0 <options>
  Optional options:
    -s             scale factor, default is 100
  Eg.
    $0 -s 1        generate tpcds queries with scale factor 1.
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hs:' \
    -- "$@")

eval set -- "${OPTS}"
HELP=0
SCALE=100

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -s)
        SCALE=$2
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

# check if dsqgen exists
if [[ ! -f ${TPCDS_DSQGEN_DIR}/dsqgen ]]; then
    echo "${TPCDS_DSQGEN_DIR}/dsqgen does not exist. Run build-tpcds-dbgen.sh to build it first."
    exit 1
fi

if [[ -d ${TPCDS_QUERIE_DIR}/ ]]; then
    echo "${TPCDS_QUERIE_DIR} exists. Remove it before re-generating"
    exit 1
else
    mkdir "${TPCDS_QUERIE_DIR}"
fi

cd "${TPCDS_DSQGEN_DIR}"
for i in {1..99}; do
    sed -i '/define _END/d' "../query_templates/query${i}.tpl"
    echo 'define _END = "";' >>"../query_templates/query${i}.tpl"
done

# generate query
"${TPCDS_DSQGEN_DIR}"/dsqgen \
    -DIRECTORY ../query_templates/ \
    -INPUT ../query_templates/templates.lst \
    -DIALECT netezza \
    -QUALIFY Y \
    -SCALE "${SCALE}"

if [[ -f query_0.sql ]]; then
    i=1
    IFS=';'
    query_strs=$(cat query_0.sql)
    of="${TPCDS_QUERIE_DIR}/tpcds_queries.sql"
    for q in ${query_strs}; do
        if [[ ${i} -eq 2 ]]; then
            echo "${q/from catalog_sales))/from catalog_sales) t)};" >>"${of}"
        elif [[ ${i} -eq 5 ]]; then
            q="${q// 14 days/ interval 14 day}" &&
                q="${q//\'store\' || s_store_id as id/concat(\'store\', s_store_id) id}" &&
                q="${q//\'catalog_page\' || cp_catalog_page_id as id/concat(\'catalog_page\', cp_catalog_page_id) id}" &&
                q="${q//\'web_site\' || web_site_id as id/concat(\'web_site\', web_site_id) id}" &&
                echo "${q};" >>"${of}"
        elif [[ ${i} -eq 12 ]] || [[ ${i} -eq 21 ]] || [[ ${i} -eq 22 ]] || [[ ${i} -eq 44 ]] || [[ ${i} -eq 81 ]] || [[ ${i} -eq 102 ]]; then
            q="${q//30 days/interval 30 day}" && echo "${q};" >>"${of}"
        elif [[ ${i} -eq 14 ]]; then
            # q="${q//and d3.d_year between 1999 AND 1999 + 2)/and d3.d_year between 1999 AND 1999 + 2) t}" && echo "${q};" >>"${of}"
            q="${q//where i_brand_id = brand_id/t where i_brand_id = brand_id}" && echo "${q};" >>"${of}"
        elif [[ ${i} -eq 17 ]] || [[ ${i} -eq 40 ]] || [[ ${i} -eq 86 ]] || [[ ${i} -eq 98 ]] || [[ ${i} -eq 99 ]]; then
            q="${q//60 days)/interval 60 day)}" && echo "${q};" >>"${of}"
        elif [[ ${i} -eq 24 ]]; then
            q="${q//c_customer_sk))/c_customer_sk) t)}" &&
                q="${q//best_ss_customer))/best_ss_customer)) t2}" &&
                echo "${q};" >>"${of}"
        elif [[ ${i} -eq 25 ]]; then
            q="${q//c_customer_sk))/c_customer_sk) t)}" &&
                q="${q//c_first_name)/c_first_name) t2}" &&
                echo "${q};" >>"${of}"
        elif [[ ${i} -eq 35 ]] || [[ ${i} -eq 96 ]]; then
            q="${q//90 days)/interval 90 day)}" && echo "${q};" >>"${of}"
        elif [[ ${i} -eq 53 ]]; then
            q="${q//order by 1/ t order by 1}" && echo "${q};" >>"${of}"
        elif [[ ${i} -eq 70 ]]; then
            v=$(echo "${q}" | grep "||" | grep -o "'.*'" | sed -n '1p')
            smc1=$(echo "${v//\'/}" | awk -F"|" '{print $1}')
            smc2=$(echo "${v//\'/}" | awk -F"|" '{print $5}')
            origin="'${smc1% }' || ',' || '${smc2# }'"
            q="${q//${origin}/concat(concat(\'${smc1}\', \',\'), \'${smc2}\')}" && echo "${q};" >>"${of}"
        elif [[ ${i} -eq 84 ]]; then
            q="${q//30 days/interval 30 day}" &&
                q="${q//\'store\' || store_id/concat(\'store\', store_id)}" &&
                q="${q//\'catalog_page\' || catalog_page_id/concat(\'catalog_page\', catalog_page_id)}" &&
                q="${q//\'web_site\' || web_site_id/concat(\'web_site\', web_site_id)}" &&
                echo "${q};" >>"${of}"
        elif [[ ${i} -eq 88 ]]; then
            q="${q//coalesce(c_last_name,\'\') || \', \' || coalesce(c_first_name,\'\')/concat(concat(coalesce(c_last_name,\'\'), \',\'), coalesce(c_first_name,\'\'))}" &&
                echo "${q};" >>"${of}"
        else
            echo "${q};" >>"${of}"
        fi
        i=$((i + 1))
    done
    echo -e "\033[32m
tpcds queries generated in:
    ${TPCDS_QUERIE_DIR}/tpcds_queries.sql
\033[0m"
else
    echo -e "\033[31m ERROR: tpcds queries generate failed \033[0m" && exit 1
fi
cd - >/dev/null
