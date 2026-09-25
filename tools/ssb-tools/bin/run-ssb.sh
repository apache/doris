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

set -Eeuo pipefail
SSB_ROOT=$(cd "$(dirname "$0")/.." && pwd)

usage() {
    cat <<EOF
Usage: $0 [-s 1|100|1000] [-c CONFIG] [-d DATABASE] [--queries-only]
          [--mode both|ssb|flat] [--result-dir DIRECTORY]

Create SSB tables, generate and import data through the Trino SSB connector,
build lineorder_flat, collect statistics, and run the existing benchmark queries.
No .tbl files or separate Trino server are needed.

Requires mysql and the SSB plugin installed on every Doris FE and BE.
The default connection configuration is conf/doris-cluster.conf; scale defaults to 1.
Preparation requires a new database. Use --queries-only to benchmark existing data.
EOF
}

scale=1
config="${SSB_ROOT}/conf/doris-cluster.conf"
database=''
queries_only=0
mode=both
result_dir="${SSB_ROOT}/results/$(date +%Y%m%d-%H%M%S)-$$"
options=$(getopt -o hs:c:d: -l help,queries-only,mode:,result-dir: -- "$@")
eval set -- "${options}"
while true; do
    case "$1" in
        -h | --help)
            usage
            exit 0
            ;;
        -s)
            scale=$2
            shift 2
            ;;
        -c)
            config=$2
            shift 2
            ;;
        -d)
            database=$2
            shift 2
            ;;
        --queries-only)
            queries_only=1
            shift
            ;;
        --mode)
            mode=$2
            shift 2
            ;;
        --result-dir)
            result_dir=$2
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            usage >&2
            exit 1
            ;;
    esac
done
if [[ $# -ne 0 || ! ${scale} =~ ^(1|100|1000)$ || ! ${mode} =~ ^(both|ssb|flat)$ ]]; then
    usage >&2
    exit 1
fi
# shellcheck source=../conf/doris-cluster.conf
source "${config}"
database=${database:-${DB}}
if [[ ! ${database} =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo 'The database name must contain only letters, digits and underscores.' >&2
    exit 1
fi
export MYSQL_PWD=${PASSWORD:-}
mysql_args=(--protocol=tcp --host="${FE_HOST}" --port="${FE_QUERY_PORT}" --user="${USER}"
    --batch --raw --skip-column-names --comments --skip-force)
command -v mysql >/dev/null
mkdir -p "$(dirname "${result_dir}")"
# Refuse to overwrite an earlier run's measurements or query results.
mkdir "${result_dir}"
result_dir=$(cd "${result_dir}" && pwd)
trap 'echo "SSB failed at line ${LINENO}. Logs: ${result_dir}. Prepared tables are preserved." >&2' ERR

run_sql() {
    local sql=$1
    shift
    mysql "${mysql_args[@]}" "$@" --execute "${sql}"
}

insert_sql() {
    # A successful INSERT can otherwise return COMMITTED before its rows are visible.
    # Preserve the diagnostic and abort rather than benchmark incomplete data or retry a write.
    local output status=0
    output=$(
        set -e
        run_sql "SET enable_insert_strict=true; SET insert_max_filter_ratio=0;
        SET insert_timeout=14400; SET insert_visible_timeout_ms=600000; $1" -vvv 2>&1
    ) || status=$?
    printf '%s\n' "${output}" >>"${result_dir}/prepare.log"
    if [[ ${status} -ne 0 ]]; then
        printf '%s\n' "${output}" >&2
        return "${status}"
    fi
    if [[ ${output} == *COMMITTED* ]]; then
        echo 'INSERT is committed but not yet visible; inspect prepare.log before continuing.' >&2
        return 1
    fi
}

echo "SSB SF${scale}: ${FE_HOST}:${FE_QUERY_PORT}/${database}; results: ${result_dir}"
if [[ ${queries_only} -eq 0 ]]; then
    echo 'Creating database and Trino SSB catalog'
    # CREATE DATABASE is deliberately not IF NOT EXISTS: a retry must never append
    # the same generated data to these DUPLICATE KEY tables, even after a partial run.
    run_sql "CREATE DATABASE \`${database}\`;" >>"${result_dir}/prepare.log"
    run_sql "CREATE CATALOG \`ssb_gen_${database}\` PROPERTIES (
        'type'='trino-connector', 'trino.connector.name'='ssb');" >>"${result_dir}/prepare.log"
    mysql_args+=(--database="${database}")
    mysql "${mysql_args[@]}" <"${SSB_ROOT}/ddl/create-ssb-tables-sf${scale}.sql" \
        >>"${result_dir}/prepare.log"

    for table in customer part supplier dates lineorder; do
        echo "Generating and importing ${table}"
        # Use target column names explicitly: source column order must not affect the import.
        # The backticks in sed quote SQL identifiers, not shell commands.
        # shellcheck disable=SC2016
        columns=$(
            set -e
            run_sql "DESC \`${table}\`;" | cut -f1 | sed 's/.*/`&`/' | paste -sd,
        )
        insert_sql "INSERT INTO \`${table}\` (${columns})
            SELECT ${columns} FROM \`ssb_gen_${database}\`.\`sf${scale}\`.\`${table}\`;"
    done

    if [[ ${mode} != ssb ]]; then
        echo 'Building lineorder_flat'
        mysql "${mysql_args[@]}" <"${SSB_ROOT}/ddl/create-ssb-flat-tables-sf${scale}.sql" \
            >>"${result_dir}/prepare.log"
        # Keep the existing loader's year-sized joins to bound each INSERT's memory use.
        for year in 1992 1993 1994 1995 1996 1997 1998; do
            flat_sql=$(sed "s/@YEAR@/${year}/g; s/@NEXT_YEAR@/$((year + 1))/g" \
                "${SSB_ROOT}/ddl/insert-ssb-flat.sql")
            insert_sql "${flat_sql}"
        done
    fi
    echo 'Collecting statistics'
    run_sql "ANALYZE DATABASE \`${database}\` WITH FULL WITH SYNC;" >>"${result_dir}/prepare.log"
else
    mysql_args+=(--database="${database}")
fi

# Repeated runs should execute the query while still benefiting from warm data caches.
mysql_args+=(--init-command='SET enable_sql_cache=false, enable_query_cache=false')
run_sql 'SELECT VERSION(); SHOW VARIABLES; SHOW TABLE STATUS;' >"${result_dir}/environment.txt"
printf 'suite,query,cold_ms,hot1_ms,hot2_ms,best_hot_ms\n' >"${result_dir}/result.csv"
for suite in ssb ssb-flat; do
    if [[ (${mode} == ssb && ${suite} == ssb-flat) || (${mode} == flat && ${suite} == ssb) ]]; then
        continue
    fi
    mkdir "${result_dir}/${suite}"
    cold_total=0
    hot_total=0
    for query in 1.1 1.2 1.3 2.1 2.2 2.3 3.1 3.2 3.3 3.4 4.1 4.2 4.3; do
        times=()
        for run in cold hot1 hot2; do
            start=$(date +%s%3N)
            mysql "${mysql_args[@]}" <"${SSB_ROOT}/${suite}-queries/q${query}.sql" \
                >"${result_dir}/${suite}/q${query}.${run}.out" \
                2>"${result_dir}/${suite}/q${query}.${run}.err"
            end=$(date +%s%3N)
            times+=("$((end - start))")
        done
        best=$((times[1] < times[2] ? times[1] : times[2]))
        printf '%s,q%s,%s,%s,%s,%s\n' "${suite}" "${query}" "${times[@]}" "${best}" |
            tee -a "${result_dir}/result.csv"
        cold_total=$((cold_total + times[0]))
        hot_total=$((hot_total + best))
    done
    echo "${suite}: total cold ${cold_total} ms, total best hot ${hot_total} ms"
done
echo "SSB completed. Results: ${result_dir}/result.csv"
