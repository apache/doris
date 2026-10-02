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
BENCHMARK_ROOT=$(cd "$(dirname "$0")" && pwd)
benchmark=$1
shift
case "${benchmark}" in
    ssb | tpch | tpcds) ;;
    *)
        echo "Unknown benchmark: ${benchmark}" >&2
        exit 1
        ;;
esac
SUITE_ROOT="${BENCHMARK_ROOT}/../${benchmark}-tools"
benchmark_name=${benchmark^^}

usage() {
    cat <<EOF
Usage: run-${benchmark}.sh [-s SCALE] [-c CONFIG] [-d DATABASE] [--queries-only]
                         [--result-dir DIRECTORY] [--mode both|ssb|flat]

Create tables, generate and import data through the Trino ${benchmark} connector,
collect statistics, and run every existing query three times. No data files or
separate Trino server are needed. Prepare lineorder_flat (SSB), lineitem_flat
(TPCH), or store/catalog/web_sales_flat (TPCDS). Existing query SQL is preserved.

SCALE: 1, 100, 1000 (also 10000 for TPCH/TPCDS). Default: 1.
--mode is only supported by SSB; its default is both.
Requires mysql and the generator plugin installed on every Doris FE and BE.
The default connection configuration is conf/doris-cluster.conf.
Preparation requires a new database. Use --queries-only against completed data.
EOF
}

scale=1
config="${SUITE_ROOT}/conf/doris-cluster.conf"
database=''
queries_only=0
mode=''
result_dir="${SUITE_ROOT}/results/$(date +%Y%m%d-%H%M%S)-$$"
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
if [[ $# -ne 0 || ! ${scale} =~ ^(1|100|1000|10000)$ ]] ||
    [[ ${benchmark} == ssb && ${scale} == 10000 ]] ||
    [[ ${benchmark} != ssb && -n ${mode} ]] ||
    [[ -n ${mode} && ! ${mode} =~ ^(both|ssb|flat)$ ]]; then
    usage >&2
    exit 1
fi
mode=${mode:-both}
ddl="${SUITE_ROOT}/ddl/create-${benchmark}-tables-sf${scale}.sql"
flat_tables=()
flat_keys=()
# Each list is the base tables in the existing DDL; TPCH's revenue0 is a view.
# The benchmark name is validated above.
# shellcheck disable=SC2249
case "${benchmark}" in
    ssb)
        tables=(customer part supplier dates lineorder)
        suites=(ssb ssb-flat)
        query_ids=(1.1 1.2 1.3 2.1 2.2 2.3 3.1 3.2 3.3 3.4 4.1 4.2 4.3)
        generator_properties="'trino.connector.name'='ssb'"
        ;;
    tpch)
        tables=(region nation supplier customer part partsupp orders lineitem)
        flat_tables=(lineitem)
        flat_keys=(l_orderkey)
        suites=(tpch)
        mapfile -t query_ids < <(seq 1 22)
        # Match the existing Doris DDL, including exact two-decimal monetary values.
        generator_properties="'trino.connector.name'='tpch',
            'trino.tpch.column-naming'='STANDARD',
            'trino.tpch.double-type-mapping'='DECIMAL',
            'trino.tpch.splits-per-node'='10'"
        ;;
    tpcds)
        tables=(call_center catalog_page catalog_returns catalog_sales customer_address
            customer_demographics customer date_dim household_demographics income_band
            inventory item promotion reason ship_mode store_returns store_sales store
            time_dim warehouse web_page web_returns web_sales web_site)
        flat_tables=(store_sales catalog_sales web_sales)
        flat_keys=(ss_ticket_number cs_order_number ws_order_number)
        suites=(tpcds)
        query_ids=()
        for ((query = 1; query <= 99; query++)); do
            query_ids+=("${query}")
            # The existing suite splits these four templates into two statements.
            case "${query}" in
                14 | 23 | 24 | 39) query_ids+=("${query}_1") ;;
                *) ;;
            esac
        done
        generator_properties="'trino.connector.name'='tpcds', 'trino.tpcds.split-count'='10'"
        ;;
esac

query_path() {
    # The benchmark name is validated above.
    # shellcheck disable=SC2249
    case "${benchmark}" in
        ssb) printf '%s/%s-queries/q%s.sql' "${SUITE_ROOT}" "$1" "$2" ;;
        tpch) printf '%s/queries/q%s.sql' "${SUITE_ROOT}" "$2" ;;
        tpcds) printf '%s/queries/sf%s/query%s.sql' "${SUITE_ROOT}" "${scale}" "$2" ;;
    esac
}

# Fail before creating a database if a requested suite is incomplete.
for suite in "${suites[@]}"; do
    for query in "${query_ids[@]}"; do
        test -r "$(query_path "${suite}" "${query}")"
    done
done
test -r "${ddl}"
for table in "${flat_tables[@]}"; do
    test -r "${SUITE_ROOT}/ddl/select-${table//_/-}-flat.sql"
done
# shellcheck disable=SC1090
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
trap 'echo "${benchmark_name} failed at line ${LINENO}. Logs: ${result_dir}. Prepared tables are preserved." >&2' ERR

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

echo "${benchmark_name} SF${scale}: ${FE_HOST}:${FE_QUERY_PORT}/${database}; results: ${result_dir}"
if [[ ${queries_only} -eq 0 ]]; then
    echo "Creating database and Trino ${benchmark_name} catalog"
    # CREATE DATABASE is deliberately not IF NOT EXISTS: a retry must never append
    # the same generated data to these DUPLICATE KEY tables, even after a partial run.
    run_sql "CREATE DATABASE \`${database}\`;" >>"${result_dir}/prepare.log"
    run_sql "CREATE CATALOG \`${benchmark}_gen_${database}\` PROPERTIES (
        'type'='trino-connector', ${generator_properties});" >>"${result_dir}/prepare.log"
    mysql_args+=(--database="${database}")
    mysql "${mysql_args[@]}" <"${ddl}" \
        >>"${result_dir}/prepare.log"

    for table in "${tables[@]}"; do
        echo "Generating and importing ${table}"
        # Use target column names explicitly: source column order must not affect the import.
        # The backticks in sed quote SQL identifiers, not shell commands.
        # shellcheck disable=SC2016
        columns=$(
            set -e
            run_sql "DESC \`${table}\`;" | cut -f1 | sed 's/.*/`&`/' | paste -sd,
        )
        source_columns=${columns}
        if [[ ${benchmark} == tpcds && ${table} == promotion ]]; then
            # Trino 435's generator misspells this column. Keep Doris's existing
            # schema and map the source name without relying on source column order.
            source_columns=${columns//p_response_target/p_response_targe}
        fi
        insert_sql "INSERT INTO \`${table}\` (${columns})
            SELECT ${source_columns} FROM \`${benchmark}_gen_${database}\`.\`sf${scale}\`.\`${table}\`;"
    done

    if [[ ${benchmark} == ssb && ${mode} != ssb ]]; then
        echo 'Building lineorder_flat'
        mysql "${mysql_args[@]}" <"${SUITE_ROOT}/ddl/create-ssb-flat-tables-sf${scale}.sql" \
            >>"${result_dir}/prepare.log"
        # Keep the existing loader's year-sized joins to bound each INSERT's memory use.
        for year in 1992 1993 1994 1995 1996 1997 1998; do
            flat_sql=$(sed "s/@YEAR@/${year}/g; s/@NEXT_YEAR@/$((year + 1))/g" \
                "${SUITE_ROOT}/ddl/insert-ssb-flat.sql")
            insert_sql "${flat_sql}"
        done
    fi
    echo 'Collecting statistics'
    run_sql "ANALYZE DATABASE \`${database}\` WITH FULL WITH SYNC;" >>"${result_dir}/prepare.log"

    if [[ ${benchmark} != ssb ]]; then
        # Each LEFT JOIN is many-to-one, preserving facts with NULL dimension keys.
        # Create the empty schema separately so every data write uses insert_sql's
        # strict loading and VISIBLE check, including wide-table preparation.
        printf 'table\trows\n' >"${result_dir}/flat-row-counts.tsv"
        for index in "${!flat_tables[@]}"; do
            table=${flat_tables[index]}
            echo "Building ${table}_flat"
            flat_sql=$(cat "${SUITE_ROOT}/ddl/select-${table//_/-}-flat.sql")
            run_sql "CREATE TABLE \`${table}_flat\`
                DISTRIBUTED BY HASH(\`${flat_keys[index]}\`) BUCKETS AUTO
                PROPERTIES ('replication_num'='1') AS ${flat_sql} WHERE 1=0;" \
                >>"${result_dir}/prepare.log"
            insert_sql "INSERT INTO \`${table}_flat\` ${flat_sql};"
            counts=$(run_sql "SELECT (SELECT COUNT(*) FROM \`${table}\`),
                (SELECT COUNT(*) FROM \`${table}_flat\`);")
            IFS=$'\t' read -r fact_count flat_count <<<"${counts}"
            if [[ ${fact_count} != "${flat_count}" ]]; then
                echo "${table}_flat has ${flat_count} rows; expected ${fact_count}." >&2
                exit 1
            fi
            printf '%s\t%s\n' "${table}_flat" "${flat_count}" >>"${result_dir}/flat-row-counts.tsv"
            # Wide tables repeat dimension attributes on every fact. Sample them
            # instead of rescanning all denormalized rows for every column.
            run_sql "ANALYZE TABLE \`${table}_flat\` WITH SAMPLE ROWS 100000 WITH SYNC;" \
                >>"${result_dir}/prepare.log"
        done
    fi
else
    mysql_args+=(--database="${database}")
fi

# Repeated runs should execute the query while still benefiting from warm data caches.
mysql_args+=(--init-command='SET enable_sql_cache=false, enable_query_cache=false')
run_sql 'SELECT VERSION(); SHOW VARIABLES; SHOW TABLE STATUS;' >"${result_dir}/environment.txt"
printf 'suite,query,cold_ms,hot1_ms,hot2_ms,best_hot_ms\n' >"${result_dir}/result.csv"
for suite in "${suites[@]}"; do
    if [[ (${mode} == ssb && ${suite} == ssb-flat) || (${mode} == flat && ${suite} == ssb) ]]; then
        continue
    fi
    mkdir "${result_dir}/${suite}"
    cold_total=0
    hot_total=0
    for query in "${query_ids[@]}"; do
        times=()
        for run in cold hot1 hot2; do
            start=$(date +%s%3N)
            mysql "${mysql_args[@]}" <"$(query_path "${suite}" "${query}")" \
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
echo "${benchmark_name} completed. Results: ${result_dir}/result.csv"
