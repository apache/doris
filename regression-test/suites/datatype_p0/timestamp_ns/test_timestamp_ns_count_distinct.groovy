// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_timestamp_ns_count_distinct") {
    sql "set enable_aggregate_function_null_v2=true"
    sql "set enable_distinct_streaming_aggregation=true"
    sql "set enable_bucketed_hash_agg=true"
    sql "set be_number_for_test=1"
    sql "set parallel_pipeline_task_num=1"
    sql "set bucketed_agg_min_input_rows=0"
    sql "set bucketed_agg_max_group_keys=0"
    sql "set bucketed_agg_high_card_threshold=1.0"

    sql "drop table if exists timestamp_ns_count_distinct"
    sql """
        create table timestamp_ns_count_distinct (
            id int,
            dt timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 2
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_count_distinct values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.000000001'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """

    qt_count "select count(*), count(dt) from timestamp_ns_count_distinct"
    explain {
        sql """
            select count(distinct dt), multi_distinct_count(dt), approx_count_distinct(dt)
            from timestamp_ns_count_distinct
        """
        contains("BUCKETED AGGREGATE")
    }
    qt_count_distinct """
        select count(distinct dt), multi_distinct_count(dt), approx_count_distinct(dt)
        from timestamp_ns_count_distinct
    """
}
