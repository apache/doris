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

_usage() {
    echo "
This script is used to collect INFO from the scene of users to help us simulate a better.
INFO contains:
    1. table statistics
    2. SQL profile
    3. SQL explain

Usage: $0 <options>
  Optional options:
    -h|--host                           [alternative] doris host, default is '127.0.0.1'
    -p|--port                           [alternative] doris query port, default is '9030'
    --http_port                         [alternative] doris http port, default is '8030'
    -u|--user                           [alternative] doris user, default is 'root'
    -w|--password                       [alternative] doris user password, default is ''
    -t|--table                          [alternative] target table name
    -d|--database                       [required] target database name
    -q|--query_dir                      [required] dir of query files
    "
    exit 1
}

_parse_option() {
    OPTS=$(getopt \
        -n "$0" \
        -o 'h:p:u:w:d:t:q:' \
        -l 'help,host:,port:,user:,password:,database:,table:,query_dir:,http_port:' \
        -- "$@")
    eval set -- "${OPTS}"
    HOST="127.0.0.1"
    PORT="9030"
    HTTP_PORT="8030"
    USER="root"
    PASSWORD=
    DB=
    TABLE=
    QUERY_DIR=
    while true; do
        case "$1" in
        --help)
            _usage
            ;;
        -h | --host)
            HOST=${2:-${HOST}}
            shift 2
            ;;
        -p | --port)
            PORT=${2:-${PORT}}
            shift 2
            ;;
        --http_port)
            HTTP_PORT=${2:-${HTTP_PORT}}
            shift 2
            ;;
        -u | --user)
            USER=${2:-${USER}}
            shift 2
            ;;
        -w | --password)
            PASSWORD=${2:-${PASSWORD}}
            shift 2
            ;;
        -d | --datebase)
            DB=${2:-${DB}}
            shift 2
            ;;
        -t | --table)
            TABLE=${2:-${TABLE}}
            shift 2
            ;;
        -q | --query_dir)
            QUERY_DIR=${2:-${QUERY_DIR}}
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error!"
            return 1
            ;;
        esac
    done

    if [[ -z "${DB}" ]]; then echo -e "ERROR: datebase not specified \n\n" && _usage; fi
    if [[ -z "${QUERY_DIR}" ]]; then echo -e "ERROR: query_dir not specified \n\n" && _usage; fi

}

doris_client() {
    usage="usage:
    doris_client [DB_NAME] SQL"
    if [[ -z "$1" ]]; then echo "${usage}" && return 1; fi
    if [[ -z "$2" ]]; then db="information_schema" && sql="$1"; else db="$1" && sql="$2"; fi
    if ret=$(
        env MYSQL_PWD="${PASSWORD}" \
            mysql \
            -h"${HOST}" \
            -u"${USER}" \
            -P"${PORT:-}" \
            -D"${db}" \
            -e"${sql}"
    ); then
        echo "${ret}"
        return 0
    else
        return 1
    fi
}

_collect_table_statistics() {
    echo "----------collect table statistics----------"
    if [[ -n "${TABLE}" ]]; then
        f="${DB}-${TABLE}.table_stats"
        echo "---- analyze table ${DB}.${TABLE}, and collect statistics into file ${od}/${f}"
        doris_client "${DB}" "analyze table ${TABLE} with sync;"
        doris_client "${DB}" "show table stats ${TABLE};" >"${od}/${f}"
        return
    fi

    tables=$(doris_client "${DB}" "show tables;" | sed -n '2,$p')
    for table in ${tables}; do
        f="${DB}-${table}.table_stats"
        echo "---- analyze table ${DB}.${table}, and collect statistics into file ${od}/${f}"
        doris_client "${DB}" "analyze table ${table} with sync;"
        doris_client "${DB}" "show table stats ${table};" >"${od}/${f}"
    done

}

_collect_query_statistics() {
    echo "----------collect query statistics----------"
    local origin_enable_profile
    origin_enable_profile=$(doris_client "select @@enable_profile;" | sed -n '2p')
    doris_client "set global enable_profile=1"

    IFS=';'
    for f in "${QUERY_DIR}"/*.sql; do
        for query in $(sed "/^--/d" "${f}"); do
            echo "${query}"
            echo "---- get the plan of ${f} into file ${od}/$(basename "${f}").plan"
            doris_client "${DB}" "explain verbose ${query}" >"${od}/$(basename "${f}")".plan

            echo "---- get the profile of ${f} into file ${od}/$(basename "${f}").profile"
            doris_client "${DB}" "${query}" >/dev/null
            query_id=$(doris_client "show query profile '/'" | sed -n 2p | cut -f1)
            curl -s -u "${USER}:${PASSWORD}" "${HOST}:${HTTP_PORT}/rest/v1/query_profile/${query_id}" -o "${od}/$(basename "${f}").profile"
            sed -i "s|&nbsp;| |g;s|</br>|\n|g" "${od}/$(basename "${f}").profile"
        done
    done

    doris_client "set global enable_profile=${origin_enable_profile}"
}

_pack_the_statistics() {
    echo "----------pack the collected statistics----------"
    tar -zcvf "${od}.tar.gz" "${od}" && rm -rf "${od}"
    if curl -s -T "${od}.tar.gz" justtmp-bj-1308700295.cos.ap-beijing.myqcloud.com/"${od}.tar.gz"; then
        echo -e "\033[32m upload success, you can download it from:
        wget https://justtmp-bj-1308700295.cos.ap-beijing.myqcloud.com/${od}.tar.gz \033[0m"
    else
        echo "tar file of collected statistics: ${od}.tar.gz"
    fi
}

main() {
    if [[ $# == 0 ]]; then
        _usage
    else
        _parse_option "$@"
    fi

    ts=$(date '+%Y%m%d%H%M%S')
    od=$(pwd)/${ts}-customer-scene-collected-statistics
    mkdir -p "${od}"

    # 1. 搜集目标表的统计信息，参考 https://doris.apache.org/zh-CN/docs/dev/query-acceleration/statistics/
    _collect_table_statistics

    # 2. 搜集查询语句的plan和profile
    _collect_query_statistics

    # 3. 把搜集到的信息打包并尝试上传到COS
    _pack_the_statistics
}

# main -h 10.16.10.6 -p 59035 --http_port 58035 -d regression_test_ssb_sf0_1_p1 -q /mnt/disk1/lidongyang/doris/regression-test/suites/ssb_sf1_p2/sql
main "$@"
