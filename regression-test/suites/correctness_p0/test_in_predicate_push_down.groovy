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

suite("test_in_predicate_push_down") {
    sql """
        drop table if exists tbl_test_in_predicate_push_down_t1;
    """

    sql """
        drop table if exists tbl_test_in_predicate_push_down_t2;
    """

    sql """
        CREATE TABLE tbl_test_in_predicate_push_down_t1 (id int, value int)
           UNIQUE KEY(`id`)
           DISTRIBUTED BY HASH(`id`) BUCKETS 1
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "enable_unique_key_merge_on_write" = "false",
               "disable_auto_compaction" = "true"
           );
    """

    sql """
        CREATE TABLE tbl_test_in_predicate_push_down_t2 (id int, value int)
           DUPLICATE KEY(`id`)
           DISTRIBUTED BY HASH(`id`) BUCKETS 1
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1"
           );
    """

    sql """
        insert into tbl_test_in_predicate_push_down_t1 values(1, 1);
    """
    sql """
        insert into tbl_test_in_predicate_push_down_t1 values(1, null);
    """
    sql """
        insert into tbl_test_in_predicate_push_down_t1 values(2, 2);
    """
    sql """
        insert into tbl_test_in_predicate_push_down_t1 values(2, null);
    """
    sql """
        insert into tbl_test_in_predicate_push_down_t1 values
            (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10),
            (11, 11), (12, 12);
    """

    sql """
        insert into tbl_test_in_predicate_push_down_t2 values
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4);
    """

    sql " analyze table tbl_test_in_predicate_push_down_t1 with full with sync; "
    sql " analyze table tbl_test_in_predicate_push_down_t2 with full with sync; "

    sql " set max_pushdown_conditions_per_column = 2; "
    sql " set runtime_filter_type = 'IN'; "
    sql " set runtime_filter_max_in_num = 1024; "
    sql " set runtime_filter_wait_time_ms = 10000; "

    qt_select """
        select
            *
        from tbl_test_in_predicate_push_down_t1 t1, tbl_test_in_predicate_push_down_t2 t2
        where t1.value = t2.value
        order by 1, 2, 3, 4;
    """
}
