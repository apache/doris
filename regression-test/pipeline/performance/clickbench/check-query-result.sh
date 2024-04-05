#!/bin/bash
# set -x
set +e

if [[ ! -d query-result-actual ]]; then mkdir query-result-actual; fi

QUERY_NUM=1
while read -r query; do
    echo "query${QUERY_NUM}:  ${query}"
    mysql -h"${host:-127.0.0.1}" -P"${query_port:-9030}" -uroot -D"${db_name:-clickbench}" -e"${query}" >"query-result-actual/doris-q${QUERY_NUM}.result"
    QUERY_NUM=$((QUERY_NUM + 1))
done <queries-sort.sql

is_ok=true
for i in {1..43}; do
    if ! diff -w <(tail -n +2 "query-result-target/doris-q${i}.result") <(tail -n +2 "query-result-actual/doris-q${i}.result") >/dev/null; then
        if [[ "${i}" == 4 ]] && [[ $(sed -n '2p' "query-result-actual/doris-q${i}.result") == '2.528953029'* ]]; then
            is_ok=true
        else
            is_ok=false
            echo "ERROR: query_${i} result is error"
            echo "**** target result **********************************************"
            cat "query-result-target/doris-q${i}.result"
            echo "**** actual result **********************************************"
            cat "query-result-actual/doris-q${i}.result"
            echo "*****************************************************************"
            # break
        fi
    fi
done

if ${is_ok}; then exit 0; else exit 1; fi
