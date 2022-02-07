#!/bin/bash

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
# This file is copied from
# https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/performance-comparison/compare.sh
# and modified by Doris.

set -exu
set -o pipefail
trap "exit" INT TERM
# The watchdog is in the separate process group, so we have to kill it separately
# if the script terminates earlier.
trap 'kill $(jobs -pr) ${watchdog_pid:-} ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# TODO: We should put these to Dockerfile
yum install mysql -y
yum install net-tools -y
yum install python3-pip -y
pip3 install PyMysql -i https://pypi.tuna.tsinghua.edu.cn/simple
pip3 install scipy -i https://pypi.tuna.tsinghua.edu.cn/simple
rpm --import https://repo.clickhouse.com/CLICKHOUSE-KEY.GPG
yum-config-manager --add-repo https://mirrors.tuna.tsinghua.edu.cn/clickhouse/rpm/stable/x86_64 ||:
yum install clickhouse-server clickhouse-client -y
yum -y install epel-release
yum -y install p7zip p7zip-plugins
yum install parallel -y

# Set python output encoding so that we can print queries with Chinese letters.
export PYTHONIOENCODING=utf-8

export IP=`ifconfig |grep inet | grep -v 127.0.0.1 | head -1 |  awk '{print $2}'`

function kill_doris
{
    while pkill -9 java; do echo . ; sleep 1 ; done
    while pkill -9 palo_be; do echo . ; sleep 1 ; done
    echo all killed
}

kill_doris

function clear_doris
{
    for dir in "left" "right"; do
        rm $dir/fe/doris-meta/* -rf
        rm $dir/be/storage/* -rf
    done
}

# New docker will be assigned a different ip, so we clear doris firstly.
clear_doris

function wait_for_server
{
    pid=`cat $1/fe/bin/fe.pid`
    query_port=$(grep query_port $1/fe/conf/fe.conf  | awk {'print $3'})
    for _ in {1..120}
    do
        if mysql -h$IP -P$query_port -uroot -e "select 1" || ! kill -0 "$pid"
        then
            break
        fi
        sleep 1
    done

    if ! mysql -h$IP -P$query_port -uroot -e "select 1"
    then
        echo "Cannot connect to doris server at $query_port"
        return 1
    fi

    if ! kill -0 "$pid"
    then
        echo "Server pid '$pid' is not running"
        return 1
    fi
}

function configure_audit {
    for dir in "left" "right"; do
        unzip -o -d $dir/plugin $dir/plugin/auditloader.zip
        rm $dir/plugin/auditloader.zip -f
        query_port=$(grep query_port $dir/fe/conf/fe.conf  | awk {'print $3'})
        http_port=$(grep http_port $dir/fe/conf/fe.conf  | awk {'print $3'})
        sed -i "s/frontend_host_port=127.0.0.1:8030/http_port=$IP:$query_port/g" $dir/plugin/plugin.conf
        sed -i "s/max_batch_interval_sec=60/max_batch_interval_sec=10/g" $dir/plugin/plugin.conf
	mysql -h$IP -P$query_port -uroot -e "DROP DATABASE IF EXISTS doris_audit_db__" ||:
	mysql -h$IP -P$query_port -uroot -e "CREATE DATABASE doris_audit_db__"
	mysql -h$IP -P$query_port -uroot -e "CREATE TABLE doris_audit_db__.doris_audit_tbl__
(
    query_id varchar(48),
    time datetime not null,
    client_ip varchar(32),
    user varchar(64),
    db varchar(96),
    state varchar(8),
    query_time bigint,
    scan_bytes bigint,
    scan_rows bigint,
    return_rows bigint,
    stmt_id int,
    is_query tinyint,
    frontend_ip varchar(32),
    cpu_time_ms bigint,
    sql_hash varchar(50),
    peak_memory_bytes bigint,
    stmt varchar(5000)
) engine=OLAP
duplicate key(query_id, time, client_ip)
partition by range(time) ()
distributed by hash(query_id) buckets 1
properties(
    \"dynamic_partition.time_unit\" = \"DAY\",
    \"dynamic_partition.start\" = \"-30\",
    \"dynamic_partition.end\" = \"3\",
    \"dynamic_partition.prefix\" = \"p\",
    \"dynamic_partition.buckets\" = \"1\",
    \"dynamic_partition.enable\" = \"true\",
    \"replication_num\" = \"1\"
)"
        plugin_dir=$(pwd)
	plugin_dir="${plugin_dir}/$dir/plugin"
        mysql -h$IP -P$query_port -uroot -e "UNINSTALL PLUGIN AuditLoader" ||:
        mysql -h$IP -P$query_port -uroot -e "INSTALL PLUGIN FROM \"$plugin_dir\""
        mysql -h$IP -P$query_port -uroot -e "SHOW PLUGINS"
    done

    restart
}

function configure
{
    set -m # Spawn temporary in its own process groups
    sed -i 's/http_port = 8030/http_port = 8130/g' right/fe/conf/fe.conf
    sed -i 's/rpc_port = 9020/rpc_port = 9120/g' right/fe/conf/fe.conf
    sed -i 's/query_port = 9030/query_port = 9130/g' right/fe/conf/fe.conf
    sed -i 's/edit_log_port = 9010/edit_log_port = 9110/g' right/fe/conf/fe.conf

    sed -i 's/be_port = 9060/be_port = 9160/g' right/be/conf/be.conf
    sed -i 's/webserver_port = 8040/webserver_port = 8140/g' right/be/conf/be.conf
    sed -i 's/heartbeat_service_port = 9050/heartbeat_service_port = 9150/g' right/be/conf/be.conf
    sed -i 's/brpc_port = 8060/brpc_port = 8160/g' right/be/conf/be.conf

    priority_networks=`echo $IP | awk -F '.' '{print$1"."$2"."$3"."0"/24"}'`
    echo "priority_networks = $priority_networks" >>left/fe/conf/fe.conf
    echo "priority_networks = $priority_networks" >>left/be/conf/be.conf
    echo "priority_networks = $priority_networks" >>right/fe/conf/fe.conf
    echo "priority_networks = $priority_networks" >>right/be/conf/be.conf

    for dir in "left" "right"; do
	cd $dir
	./fe/bin/start_fe.sh --daemon
        cd -
        wait_for_server $dir

        cd $dir
        netstat -nlp
        ifconfig
        query_port=$(grep query_port fe/conf/fe.conf  | awk {'print $3'})
        be_port=$(grep heartbeat_service_port be/conf/be.conf  | awk {'print $3'})
        mysql -h$IP -P$query_port -uroot -e "ALTER SYSTEM ADD BACKEND \"$IP:$be_port\"" ||:
        ./be/bin/start_be.sh --daemon
        sleep 20
        mysql -h$IP -P $query_port -uroot -e "SHOW PROC '/backends'"
	mysql -h$IP -P $query_port -uroot -e "DROP DATABASE IF EXISTS tests"
	mysql -h$IP -P $query_port -uroot -e "CREATE DATABASE tests"
	cd -
    done

    # generate data_10m data_20 table
    # Idealy, each case should generates data itself, however, it takes tens of seconds to ingest data to doris
    for dir in "left" "right"; do
	for number in 20 10000000; do
            query_port=$(grep query_port $dir/fe/conf/fe.conf  | awk {'print $3'})
            be_web_port=$(grep webserver_port $dir/be/conf/be.conf | awk {'print $3'})
            mysql -h$IP -P$query_port -uroot -e "DROP TABLE IF EXISTS tests.numbers_$number"

            mysql -h$IP -P$query_port -uroot -e "CREATE TABLE tests.numbers_${number}(number BIGINT) ENGINE = olap DUPLICATE KEY(number) DISTRIBUTED BY HASH(number)
            PROPERTIES (
                \"storage_medium\" = \"HDD\",
                \"replication_num\" = \"1\"
            )"

	    :>numbers.csv
            seq 0 $number >>numbers.csv
            # TODO error handle
	    label=`uuidgen`
	    curl -u root: -H "label:$label" -T ./numbers.csv http://$IP:$be_web_port/api/tests/numbers_$number/_stream_load
        done
    done

    # configure_audit
}

function restart
{
    kill_doris
    set -m # Spawn servers in their own process groups

    for dir in "left" "right"; do
	cd $dir
	./fe/bin/start_fe.sh --daemon
	./be/bin/start_be.sh --daemon
	cd -
    done

    set +m

    wait_for_server left
    echo left ok

    wait_for_server right
    echo right ok

    sleep 20
}

function run_tests
{
    # Just check that the script runs at all
    "$script_dir/perf.py" --help > /dev/null

    # Find the directory with test files.
    if [ -v CHPC_TEST_PATH ]
    then
        # Use the explicitly set path to directory with test files.
        test_prefix="$CHPC_TEST_PATH"
    elif [ "$PR_TO_TEST" == "0" ]
    then
        # When testing commits from master, use the older test files. This
        # allows the tests to pass even when we add new functions and tests for
        # them, that are not supported in the old revision.
        test_prefix=left/performance
    else
        # For PRs, use newer test files so we can test these changes.
        test_prefix=right/performance
    fi

    # Determine which tests to run.
    if [ -v CHPC_TEST_GREP ]
    then
        # Run only explicitly specified tests, if any.
        # shellcheck disable=SC2010
        test_files=($(ls "$test_prefix" | grep "$CHPC_TEST_GREP" | xargs -I{} -n1 readlink -f "$test_prefix/{}"))
    elif [ "$PR_TO_TEST" -ne 0 ] \
        && [ "$(wc -l < changed-test-definitions.txt)" -gt 0 ] \
        && [ "$(wc -l < other-changed-files.txt)" -eq 0 ]
    then
        # If only the perf tests were changed in the PR, we will run only these
        # tests. The lists of changed files are prepared in entrypoint.sh because
        # it has the repository.
        test_files=($(sed "s/tests\/performance/${test_prefix//\//\\/}/" changed-test-definitions.txt))
    else
        # The default -- run all tests found in the test dir.
        test_files=($(ls "$test_prefix"/*.xml))
    fi

    # We split perf tests into multiple checks to make them faster
    if [ -v CHPC_TEST_RUN_BY_HASH_TOTAL ]; then
        # filter tests array in bash https://stackoverflow.com/a/40375567
        for index in "${!test_files[@]}"; do
            # sorry for this, just calculating hash(test_name) % total_tests_group == my_test_group_num
            test_hash_result=$(echo test_files[$index] | perl -ne 'use Digest::MD5 qw(md5); print unpack('Q', md5($_)) % $ENV{CHPC_TEST_RUN_BY_HASH_TOTAL} == $ENV{CHPC_TEST_RUN_BY_HASH_NUM};')
            # BTW, for some reason when hash(test_name) % total_tests_group != my_test_group_num perl outputs nothing, not zero
            if [ "$test_hash_result" != "1" ]; then
                # deleting element from array
                unset -v 'test_files[$index]'
            fi
        done
        # to have sequential indexes...
        test_files=("${test_files[@]}")
    fi

    # For PRs w/o changes in test definitons, test only a subset of queries,
    # and run them less times. If the corresponding environment variables are
    # already set, keep those values.
    #
    # NOTE: too high CHPC_RUNS/CHPC_MAX_QUERIES may hit internal CI timeout.
    # NOTE: Currently we disabled complete run even for master branch
    #if [ "$PR_TO_TEST" -ne 0 ] && [ "$(wc -l < changed-test-definitions.txt)" -eq 0 ]
    #then
    #    CHPC_RUNS=${CHPC_RUNS:-7}
    #    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-10}
    #else
    #    CHPC_RUNS=${CHPC_RUNS:-13}
    #    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-0}
    #fi

    CHPC_RUNS=${CHPC_RUNS:-7}
    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-10}

    export CHPC_RUNS
    export CHPC_MAX_QUERIES

    # Determine which concurrent benchmarks to run. For now, the only test
    # we run as a concurrent benchmark is 'website'. Run it as benchmark if we
    # are also going to run it as a normal test.
    for test in ${test_files[@]}; do echo "$test"; done | sed -n '/website/p' > benchmarks-to-run.txt

    # Delete old report files.
    for x in {test-times,wall-clock-times}.tsv
    do
        rm -v "$x" ||:
        touch "$x"
    done

    # Randomize test order. BTW, it's not an array no more.
    test_files=$(for f in ${test_files[@]}; do echo "$f"; done | sort -R)

    # Run the tests.
    total_tests=$(echo "$test_files" | wc -w)
    current_test=0
    test_name="<none>"

    LEFT_SERVER_PORT=$(grep query_port left/fe/conf/fe.conf  | awk {'print $3'})
    RIGHT_SERVER_PORT=$(grep query_port right/fe/conf/fe.conf  | awk {'print $3'})
    for test in $test_files
    do
        echo "$current_test of $total_tests tests complete" > status.txt
        # Check that both servers are alive, and restart them if they die.
        mysql -h$IP -P$LEFT_SERVER_PORT -e "select 1" \
            || { echo $test_name >> left-server-died.log ; restart ; }
        mysql -h$IP -P$RIGHT_SERVER_PORT -e "select 1" \
            || { echo $test_name >> right-server-died.log ; restart ; }

        test_name=$(basename "$test" ".xml")
        echo test "$test_name"

        # Don't profile if we're past the time limit.
        # Use awk because bash doesn't support floating point arithmetic.

        (
            set +x
            argv=(
                --host "$IP" "$IP"
                --port "$LEFT_SERVER_PORT" "$RIGHT_SERVER_PORT"
                --runs "$CHPC_RUNS"

                "$test"
            )
            TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
            # one more subshell to suppress trace output for "set +x"
            (
                time "$script_dir/perf.py" "${argv[@]}" > "$test_name-raw.tsv" 2> "$test_name-err.log"
            ) 2>>wall-clock-times.tsv >/dev/null \
                || echo "Test $test_name failed with error code $?" >> "$test_name-err.log"
        ) 2>/dev/null

        current_test=$((current_test + 1))
    done

    wait
}

# TODO: We use clickhouse here
# Build and analyze randomization distribution for all queries.
function analyze_queries
{
rm -v analyze-commands.txt analyze-errors.log all-queries.tsv unstable-queries.tsv ./*-report.tsv raw-queries.tsv ||:
rm -rf analyze ||:
mkdir analyze analyze/tmp ||:

clickhouse start

# Split the raw test output into files suitable for analysis.
# To debug calculations only for a particular test, substitute a suitable
# wildcard here, e.g. `for test_file in modulo-raw.tsv`.
for test_file in *-raw.tsv
do
    test_name=$(basename "$test_file" "-raw.tsv")
    sed -n "s/^query\t/$test_name\t/p" < "$test_file" >> "analyze/query-runs.tsv"
    sed -n "s/^profile\t/$test_name\t/p" < "$test_file" >> "analyze/query-profiles.tsv"
    sed -n "s/^client-time\t/$test_name\t/p" < "$test_file" >> "analyze/client-times.tsv"
    sed -n "s/^report-threshold\t/$test_name\t/p" < "$test_file" >> "analyze/report-thresholds.tsv"
    sed -n "s/^skipped\t/$test_name\t/p" < "$test_file" >> "analyze/skipped-tests.tsv"
    sed -n "s/^display-name\t/$test_name\t/p" < "$test_file" >> "analyze/query-display-names.tsv"
    sed -n "s/^short\t/$test_name\t/p" < "$test_file" >> "analyze/marked-short-queries.tsv"
    sed -n "s/^partial\t/$test_name\t/p" < "$test_file" >> "analyze/partial-queries.tsv"
done

# for each query run, prepare array of metrics from query log
clickhouse-local --query "
create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

-- Separately process 'partial' queries which we could only run on the new server
-- because they use new functions. We can't make normal stats for them, but still
-- have to show some stats so that the PR author can tweak them.
create view partial_queries as select test, query_index
    from file('analyze/partial-queries.tsv', TSV,
        'test text, query_index int, servers Array(int)');

create table partial_query_times engine File(TSVWithNamesAndTypes,
        'analyze/partial-query-times.tsv')
    as select test, query_index, stddevPop(time) time_stddev, median(time) time_median
    from query_runs
    where (test, query_index) in partial_queries
    group by test, query_index
    ;

-- Filter out tests that don't have an even number of runs, to avoid breaking
-- the further calculations. This may happen if there was an error during the
-- test runs, e.g. the server died. It will be reported in test errors, so we
-- don't have to report it again.
create view broken_queries as
    select test, query_index
    from query_runs
    group by test, query_index
    having count(*) % 2 != 0
    ;

-- This is for statistical processing with eqmed.sql
create table query_run_metrics_for_stats engine File(
        TSV, -- do not add header -- will parse with grep
        'analyze/query-run-metrics-for-stats.tsv')
    as select test, query_index, 0 run, version,
        -- For debugging, add a filter for a particular metric like this:
        -- arrayFilter(m, n -> n = 'client_time', metric_values, metric_names)
        --     metric_values
        -- Note that further reporting may break, because the metric names are
        -- not filtered.
        [time]
    from query_runs
    where (test, query_index) not in broken_queries
    order by test, query_index, run, version
    ;

-- This is the list of metric names, so that we can join them back after
-- statistical processing.
create table query_run_metric_names engine File(TSV, 'analyze/query-run-metric-names.tsv')
    as select ['time'] limit 1
    ;
" 2> >(tee -a analyze/errors.log 1>&2)

# This is a lateral join in bash... please forgive me.
# We don't have arrayPermute(), so I have to make random permutations with
# `order by rand`, and it becomes really slow if I do it for more than one
# query. We also don't have lateral joins. So I just put all runs of each
# query into a separate file, and then compute randomization distribution
# for each file. I do this in parallel using GNU parallel.
( set +x # do not bloat the log
IFS=$'\n'
for prefix in $(cut -f1,2 "analyze/query-run-metrics-for-stats.tsv" | sort | uniq)
do
    file="analyze/tmp/${prefix//	/_}.tsv"
    grep "^$prefix	" "analyze/query-run-metrics-for-stats.tsv" > "$file" &
    printf "%s\0\n" \
        "clickhouse-local \
            --file \"$file\" \
            --structure 'test text, query text, run int, version UInt8, metrics Array(float)' \
            --query \"$(cat "$script_dir/eqmed.sql")\" \
            >> \"analyze/query-metric-stats.tsv\"" \
            2>> analyze/errors.log \
        >> analyze/commands.txt
done
wait
unset IFS
)

# The comparison script might be bound to one NUMA node for better test
# stability, and the calculation runs out of memory because of this. Use
# all nodes.
# Use less jobs to avoid OOM. Some queries can consume 8+ GB of memory.
jobs_count=$(($(grep -c ^processor /proc/cpuinfo) / 3))
parallel --jobs  $jobs_count --joblog analyze/parallel-log.txt --null < analyze/commands.txt 2>> analyze/errors.log

clickhouse-local --query "
-- Join the metric names back to the metric statistics we've calculated, and make
-- a denormalized table of them -- statistics for all metrics for all queries.
-- The WITH, ARRAY JOIN and CROSS JOIN do not like each other:
--  https://github.com/ClickHouse/ClickHouse/issues/11868
--  https://github.com/ClickHouse/ClickHouse/issues/11757
-- Because of this, we make a view with arrays first, and then apply all the
-- array joins.
create view query_metric_stat_arrays as
    with (select * from file('analyze/query-run-metric-names.tsv',
        TSV, 'n Array(String)')) as metric_name
    select test, query_index, metric_name, left, right, diff, stat_threshold
    from file('analyze/query-metric-stats.tsv', TSV, 'left Array(float),
        right Array(float), diff Array(float), stat_threshold Array(float),
        test text, query_index int') reports
    order by test, query_index, metric_name
    ;

create view query_metric_stats as
    select * from file('analyze/query-metric-stats-denorm.tsv',
        TSVWithNamesAndTypes,
        'test text, query_index int, metric_name text, left float, right float,
            diff float, stat_threshold float')
    ;

create table query_metric_stats_denorm engine File(TSVWithNamesAndTypes,
        'analyze/query-metric-stats-denorm.tsv')
    as select test, query_index, metric_name, left, right, diff, stat_threshold
    from query_metric_stat_arrays
    left array join metric_name, left, right, diff, stat_threshold
    order by test, query_index, metric_name
    ;
" 2> >(tee -a analyze/errors.log 1>&2)


touch analyze/historical-thresholds.tsv

}

# Analyze results
function report
{
rm -r report ||:
mkdir report report/tmp ||:

rm ./*.{rep,svg} test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv run-errors.tsv ||:

cat analyze/errors.log >> report/errors.log ||:
cat profile-errors.log >> report/errors.log ||:

clickhouse-local --query "
create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create view partial_query_times as select * from
    file('analyze/partial-query-times.tsv', TSVWithNamesAndTypes,
        'test text, query_index int, time_stddev float, time_median double')
    ;

-- Report for partial queries that we could only run on the new server (e.g.
-- queries with new functions added in the tested PR).
create table partial_queries_report engine File(TSV, 'report/partial-queries-report.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select toDecimal64(time_median, 3) time,
        toDecimal64(time_stddev / time_median, 3) relative_time_stddev,
        test, query_index, query_display_name
    from partial_query_times
    join query_display_names using (test, query_index)
    order by test, query_index
    ;

create view query_metric_stats as
    select * from file('analyze/query-metric-stats-denorm.tsv',
        TSVWithNamesAndTypes,
        'test text, query_index int, metric_name text, left float, right float,
            diff float, stat_threshold float')
    ;

create table report_thresholds engine File(TSVWithNamesAndTypes, 'report/thresholds.tsv')
    as select
        query_display_names.test test, query_display_names.query_index query_index,
        ceil(greatest(0.1, historical_thresholds.max_diff,
            test_thresholds.report_threshold), 2) changed_threshold,
        ceil(greatest(0.2, historical_thresholds.max_stat_threshold,
            test_thresholds.report_threshold + 0.1), 2) unstable_threshold,
        query_display_names.query_display_name query_display_name
    from query_display_names
    left join file('analyze/historical-thresholds.tsv', TSV,
        'test text, query_index int, max_diff float, max_stat_threshold float,
            query_display_name text') historical_thresholds
    on query_display_names.test = historical_thresholds.test
        and query_display_names.query_index = historical_thresholds.query_index
        and query_display_names.query_display_name = historical_thresholds.query_display_name
    left join file('analyze/report-thresholds.tsv', TSV,
        'test text, report_threshold float') test_thresholds
    on query_display_names.test = test_thresholds.test
    ;
-- Main statistics for queries -- query time as reported in query log.
create table queries engine File(TSVWithNamesAndTypes, 'report/queries.tsv')
    as select
        -- It is important to have a non-strict inequality with stat_threshold
        -- here. The randomization distribution is actually discrete, and when
        -- the number of runs is small, the quantile we need (e.g. 0.99) turns
        -- out to be the maximum value of the distribution. We can also hit this
        -- maximum possible value with our test run, and this obviously means
        -- that we have observed the difference to the best precision possible
        -- for the given number of runs. If we use a strict equality here, we
        -- will miss such cases. This happened in the wild and lead to some
        -- uncaught regressions, because for the default 7 runs we do for PRs,
        -- the randomization distribution has only 16 values, so the max quantile
        -- is actually 0.9375.
        abs(diff) > changed_threshold        and abs(diff) >= stat_threshold as changed_fail,
        abs(diff) > changed_threshold - 0.05 and abs(diff) >= stat_threshold as changed_show,

        not changed_fail and stat_threshold > unstable_threshold as unstable_fail,
        not changed_show and stat_threshold > unstable_threshold - 0.05 as unstable_show,

        left, right, diff, stat_threshold,
        query_metric_stats.test test, query_metric_stats.query_index query_index,
        query_display_names.query_display_name query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    left join report_thresholds
        on query_display_names.test = report_thresholds.test
            and query_display_names.query_index = report_thresholds.query_index
            and query_display_names.query_display_name = report_thresholds.query_display_name
    -- 'server_time' is rounded down to ms, which might be bad for very short queries.
    -- Use 'client_time' instead.
    where metric_name = 'time'
    order by test, query_index, metric_name
    ;

create table changed_perf_report engine File(TSV, 'report/changed-perf.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        toDecimal64(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select
        toDecimal64(left, 3), toDecimal64(right, 3), times_change_str,
        toDecimal64(diff, 3), toDecimal64(stat_threshold, 3),
        changed_fail, test, query_index, query_display_name
    from queries where changed_show order by abs(diff) desc;

create table unstable_queries_report engine File(TSV, 'report/unstable-queries.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select
        toDecimal64(left, 3), toDecimal64(right, 3), toDecimal64(diff, 3),
        toDecimal64(stat_threshold, 3), unstable_fail, test, query_index, query_display_name
    from queries where unstable_show order by stat_threshold desc;


create view test_speedup as
    select
        test,
        exp2(avg(log2(left / right))) times_speedup,
        count(*) queries,
        unstable + changed bad,
        sum(changed_show) changed,
        sum(unstable_show) unstable
    from queries
    group by test
    order by times_speedup desc
    ;

create view total_speedup as
    select
        'Total' test,
        exp2(avg(log2(times_speedup))) times_speedup,
        sum(queries) queries,
        unstable + changed bad,
        sum(changed) changed,
        sum(unstable) unstable
    from test_speedup
    ;

create table test_perf_changes_report engine File(TSV, 'report/test-perf-changes.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        (times_speedup >= 1
            ? '-' || toString(toDecimal64(times_speedup, 3)) || 'x'
            : '+' || toString(toDecimal64(1 / times_speedup, 3)) || 'x')
        as times_speedup_str
    select test, times_speedup_str, queries, bad, changed, unstable
    -- Not sure what's the precedence of UNION ALL vs WHERE & ORDER BY, hence all
    -- the braces.
    from (
        (
            select * from total_speedup
        ) union all (
            select * from test_speedup
            where
                (times_speedup >= 1 ? times_speedup : (1 / times_speedup)) >= 1.005
                or bad
        )
    )
    order by test = 'Total' desc, times_speedup desc
    ;


create view total_client_time_per_query as select *
    from file('analyze/client-times.tsv', TSV,
        'test text, query_index int, client float, server float');

create table slow_on_client_report engine File(TSV, 'report/slow-on-client.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select client, server, toDecimal64(client/server, 3) p,
        test, query_display_name
    from total_client_time_per_query left join query_display_names using (test, query_index)
    where p > toDecimal64(1.02, 3) order by p desc;

create table wall_clock_time_per_test engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

create table test_time engine Memory as
    select test, sum(client) total_client_time,
        max(client) query_max,
        min(client) query_min,
        count(*) queries
    from total_client_time_per_query full join queries using (test, query_index)
    group by test;

create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

--
-- Guess the number of query runs used for this test. The number is required to
-- calculate and check the average query run time in the report.
-- We have to be careful, because we will encounter:
--  1) partial queries which run only on one server
--  2) short queries which run for a much higher number of times
--  3) some errors that make query run for a different number of times on a
--     particular server.
--
create view test_runs as
    select test,
        -- Default to 7 runs if there are only 'short' queries in the test, and
        -- we can't determine the number of runs.
        if((ceil(medianOrDefaultIf(t.runs, not short), 0) as r) != 0, r, 7) runs
    from (
        select
            -- The query id is the same for both servers, so no need to divide here.
            uniqExact(query_id) runs,
            (test, query_index) in
                (select * from file('analyze/marked-short-queries.tsv', TSV,
                    'test text, query_index int'))
            as short,
            test, query_index
        from query_runs
        group by test, query_index
        ) t
    group by test
    ;

create view test_times_view as
    select
        wall_clock_time_per_test.test test,
        real,
        total_client_time,
        queries,
        query_max,
        real / if(queries > 0, queries, 1) avg_real_per_query,
        query_min,
        runs
    from test_time
        -- wall clock times are also measured for skipped tests, so don't
        -- do full join
        left join wall_clock_time_per_test
            on wall_clock_time_per_test.test = test_time.test
        full join test_runs
            on test_runs.test = test_time.test
    ;

-- WITH TOTALS doesn't work with INSERT SELECT, so we have to jump through these
-- hoops: https://github.com/ClickHouse/ClickHouse/issues/15227
create view test_times_view_total as
    select
        'Total' test,
        sum(real),
        sum(total_client_time),
        sum(queries),
        max(query_max),
        sum(real) / if(sum(queries) > 0, sum(queries), 1) avg_real_per_query,
        min(query_min),
        -- Totaling the number of runs doesn't make sense, but use the max so
        -- that the reporting script doesn't complain about queries being too
        -- long.
        max(runs)
    from test_times_view
    ;

create table test_times_report engine File(TSV, 'report/test-times.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select
        test,
        toDecimal64(real, 3),
        toDecimal64(total_client_time, 3),
        queries,
        toDecimal64(query_max, 3),
        toDecimal64(avg_real_per_query, 3),
        toDecimal64(query_min, 3),
        runs
    from (
        select * from test_times_view
        union all
        select * from test_times_view_total
        )
    order by test = 'Total' desc, avg_real_per_query desc
    ;

-- report for all queries page, only main metric
create table all_tests_report engine File(TSV, 'report/all-queries.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        toDecimal64(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select changed_fail, unstable_fail,
        toDecimal64(left, 3), toDecimal64(right, 3), times_change_str,
        toDecimal64(isFinite(diff) ? diff : 0, 3),
        toDecimal64(isFinite(stat_threshold) ? stat_threshold : 0, 3),
        test, query_index, query_display_name
    from queries order by test, query_index;


-- Report of queries that have inconsistent 'short' markings:
-- 1) have short duration, but are not marked as 'short'
-- 2) the reverse -- marked 'short' but take too long.
-- The threshold for 2) is significantly larger than the threshold for 1), to
-- avoid jitter.
create view shortness
    as select
        (test, query_index) in
            (select * from file('analyze/marked-short-queries.tsv', TSV,
            'test text, query_index int'))
            as marked_short,
        time, test, query_index, query_display_name
    from (
            select right time, test, query_index from queries
            union all
            select time_median, test, query_index from partial_query_times
        ) times
        left join query_display_names
            on times.test = query_display_names.test
                and times.query_index = query_display_names.query_index
    ;

create table inconsistent_short_marking_report
    engine File(TSV, 'report/unexpected-query-duration.tsv')
    as select
        multiIf(marked_short and time > 0.1, '\"short\" queries must run faster than 0.02 s',
                not marked_short and time < 0.02, '\"normal\" queries must run longer than 0.1 s',
                '') problem,
        marked_short, time,
        test, query_index, query_display_name
    from shortness
    where problem != ''
    ;


--------------------------------------------------------------------------------
-- various compatibility data formats follow, not related to the main report

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select 0 short, changed_fail, unstable_fail, left, right, diff,
        stat_threshold, test, query_display_name query
    from queries
    ;

-- new report for all queries with all metrics (no page yet)
create table all_query_metrics_tsv engine File(TSV, 'report/all-query-metrics.tsv') as
    select metric_name, left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold, test, query_index, query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    order by test, query_index;
" 2> >(tee -a report/errors.log 1>&2)

# Prepare source data for metrics and flamegraphs for queries that were profiled
# by perf.py.
for version in {right,left}
do
    rm -rf data
    clickhouse-local --query "
create view query_profiles as
    with 0 as left, 1 as right
    select * from file('analyze/query-profiles.tsv', TSV,
        'test text, query_index int, query_id text, version UInt8, time float')
    where version = $version
    ;

create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create table unstable_query_runs engine File(TSVWithNamesAndTypes,
        'unstable-query-runs.$version.rep') as
    select query_profiles.test test, query_profiles.query_index query_index,
        query_display_name, query_id
    from query_profiles
    left join query_display_names on
        query_profiles.test = query_display_names.test
        and query_profiles.query_index = query_display_names.query_index
    ;

create table unstable_run_metrics engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics.$version.rep') as
    select test, query_index, query_id, time
    from query_runs
    join unstable_query_runs using (query_id)
    ;

create table unstable_run_metrics_2 engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics-2.$version.rep') as
    select
        test, query_index, query_id,
        v, n
    from (
        select
            test, query_index, query_id,
            ['time'] n,
            [time] v
        from query_runs
        join unstable_query_runs using (query_id)
    )
    array join v, n;

" 2> >(tee -a report/errors.log 1>&2) &
done
wait

# Create per-query flamegraphs
touch report/query-files.txt
IFS=$'\n'
for version in {right,left}
do
    for query in $(cut -d'	' -f1-4 "report/stacks.$version.tsv" | sort | uniq)
    do
        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')
        echo "$query_file" >> report/query-files.txt

        # Build separate .svg flamegraph for each query.
        # -F is somewhat unsafe because it might match not the beginning of the
        # string, but this is unlikely and escaping the query for grep is a pain.
        grep -F "$query	" "report/stacks.$version.tsv" \
            | cut -f 5- \
            | sed 's/\t/ /g' \
            | tee "report/tmp/$query_file.stacks.$version.tsv" \
            | ~/fg/flamegraph.pl --hash > "$query_file.$version.svg" &
    done
done
wait
unset IFS

# Create differential flamegraphs.
while IFS= read -r query_file
do
    ~/fg/difffolded.pl "report/tmp/$query_file.stacks.left.tsv" \
            "report/tmp/$query_file.stacks.right.tsv" \
        | tee "report/tmp/$query_file.stacks.diff.tsv" \
        | ~/fg/flamegraph.pl > "$query_file.diff.svg" &
done < report/query-files.txt
wait

# Create per-query files with metrics. Note that the key is different from flamegraphs.
IFS=$'\n'
for version in {right,left}
do
    for query in $(cut -d'	' -f1-3 "report/metric-deviation.$version.tsv" | sort | uniq)
    do
        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')

        # Ditto the above comment about -F.
        grep -F "$query	" "report/metric-deviation.$version.tsv" \
            | cut -f4- > "$query_file.$version.metrics.rep" &
    done
done
wait
unset IFS

# Prefer to grep for clickhouse_driver exception messages, but if there are none,
# just show a couple of lines from the log.
for log in *-err.log
do
    test=$(basename "$log" "-err.log")
    {
        # The second grep is a heuristic for error messages like
        # "socket.timeout: timed out".
        grep -h -m2 -i '\(Exception\|Error\):[^:]' "$log" \
            || grep -h -m2 -i '^[^ ]\+: ' "$log" \
            || head -2 "$log"
    } | sed "s/^/$test\t/" >> run-errors.tsv ||:
done
}

case "$stage" in
"")
    ;&
"configure")
    time configure
    ;&
"run_tests")
    # Ignore the errors to collect the log and build at least some report, anyway
    time run_tests ||:
    ;&
"analyze_queries")
    time analyze_queries ||:
    ;&
"report")
    time report ||:
    ;&
"report_html")
    time "$script_dir/report.py" --report=all-queries > all-queries.html 2> >(tee -a report/errors.log 1>&2) ||:
    time "$script_dir/report.py" > report.html
    ;&
esac

7z a '-x!*/tmp' /output/output.7z ./*.{log,tsv,html,txt,rep,columns} \
    {right,left}/performance \
    report analyze benchmark \
    ./*.core.dmp ./*.core

# Print some final debug info to help debug Weirdness, of which there is plenty.
jobs
pstree -apgT
