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

suite("test_runtime_filter_outer_join_nullable_side") {
    sql "drop table if exists rf_outer_join_nullable_a"
    sql "drop table if exists rf_outer_join_nullable_b"
    sql "drop table if exists rf_outer_join_nullable_c"

    sql """
        create table rf_outer_join_nullable_a (
            pk int
        )
        duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1")
    """

    sql """
        create table rf_outer_join_nullable_b (
            pk int
        )
        duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1")
    """

    sql """
        create table rf_outer_join_nullable_c (
            pk int
        )
        duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1")
    """

    sql "insert into rf_outer_join_nullable_a values (1)"
    sql "insert into rf_outer_join_nullable_b values (1)"
    sql "insert into rf_outer_join_nullable_c values (0)"
    sql "sync"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set disable_join_reorder=true"
    sql "set runtime_filter_mode='GLOBAL'"
    sql "set runtime_filter_type='IN_OR_BLOOM_FILTER'"
    sql "set runtime_filter_wait_infinitely=true"
    sql "set enable_runtime_filter_prune=false"
    sql "set parallel_pipeline_task_num=1"

    def query = """
        select coalesce(b.pk, 0) as k, count(*) as cnt
        from rf_outer_join_nullable_a a
        left join rf_outer_join_nullable_b b on a.pk = b.pk
        inner join rf_outer_join_nullable_c c on coalesce(b.pk, 0) = c.pk
        group by 1
        order by 1
    """

    qt_shape """
        explain shape plan
        ${query}
    """

    qt_result query
}
