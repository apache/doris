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

suite("aggregate_strategies") {

    def test_aggregate_strategies = { tableName, bucketNum ->
        sql "SET enable_fallback_to_original_planner=true"

        sql "drop table if exists $tableName"
        sql """CREATE TABLE `$tableName` (
          `id` int(11) NOT NULL,
          `name` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS $bucketNum
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );"""


        // insert 10 rows, with duplicate
        sql "insert into $tableName select number, concat('name_', number) from numbers('number'='5')"
        sql "insert into $tableName select number, concat('name_', number) from numbers('number'='5')"


        sql "SET enable_nereids_planner=true"
        sql "SET enable_fallback_to_original_planner=false"

        order_qt_count_all "select count(ALL *) from $tableName"
        order_qt_count_all "select count(*) from $tableName"
        order_qt_count_all_group_by "select count(*) from $tableName group by id"
        order_qt_count_all_group_by_2 "select count(*) from $tableName group by id, name"
        order_qt_count_2_all_group_by_2 "select count(*), count(*) from $tableName group by id, name"
        order_qt_count_sum "select sum(id), count(name) from $tableName"
        order_qt_group_select_same "select id from $tableName group by id"
        order_qt_group_select_difference "select count(name) from $tableName group by id"

        order_qt_count_distinct "select count(distinct id) from $tableName"

        /*
         * should not use streaming, there has some bug in be will compute wrong result.
         *
         * the case is:
         * ```
         * CREATE TABLE `n` (
         *   `id` bigint NOT NULL
         * ) ENGINE=OLAP
         * DUPLICATE KEY(`id`)
         * COMMENT 'OLAP'
         * DISTRIBUTED BY HASH(`id`) BUCKETS 1
         * PROPERTIES (
         * "replication_allocation" = "tag.location.default: 1",
         * "in_memory" = "false",
         * "storage_format" = "V2",
         * "disable_auto_compaction" = "false"
         * );
         *
         * insert into n select number from numbers('number'='10000000');
         * insert into n select number from numbers('number'='10000000');
         * ```
         *
         * when open streaming aggregate, the result is 19999800, but the correct result is 10000000
         */
        explain {
            sql """
            select
                count(distinct id)
                from $tableName
            """

            notContains "STREAMING"
        }

        // test multi_distinct
        test {
            sql """select
                count(distinct name)
                from $tableName"""
            result([[5L]])
        }

        // test four phase distinct
        test {
            sql """select
                count(distinct id)
                from $tableName"""
            result([[5L]])
        }

        order_qt_count_distinct_group_by "select count(distinct id) from $tableName group by name"
        order_qt_count_distinct_group_by_select_key "select name, count(distinct id) from $tableName group by name"
        order_qt_count_distinct_muilti "select count(distinct id, name) from $tableName"
        order_qt_count_distinct_muilti_group_by "select count(distinct id, name) from $tableName group by name"
        order_qt_count_distinct_muilti_group_by_select_key "select name, count(distinct id, name) from $tableName group by name"

        order_qt_count_distinct_sum_distinct_same "select max(distinct id), sum(distinct id) from $tableName"

        // explain plan select /*+SET_VAR(disable_nereids_rules='THREE_PHASE_AGGREGATE_WITH_DISTINCT,TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI')*/ max(distinct id), sum(distinct id) from test_bucket1_table;

        order_qt_count_distinct_sum_distinct_same "select max(distinct id), sum(distinct id) from $tableName"
        order_qt_count_distinct_sum_distinct_difference "select count(distinct name), sum(distinct id) from $tableName"

        order_qt_count_distinct_sum_distinct_group_by """
        select count(distinct name), sum(distinct id)
        from $tableName group by name"""

        order_qt_count_distinct_sum_distinct_group_by_select_key """
        select name, count(distinct name), sum(distinct id)
        from $tableName group by name"""

        order_qt_group_by_all_group_by """
        select id
        from (
            select id, name
            from $tableName
            group by id, name
        )a
        group by id"""

        order_qt_group_by_partial_group_by """
        select id
        from (
            select id, name
            from $tableName
            group by name, id
        )a
        group by id"""

        order_qt_group_by_count_distinct_sum_distinct """
        select c, c from (select count(distinct id) as c, sum(distinct id) as s
        from $tableName)a group by c, s"""

        order_qt_group_by_count_distinct """
        select c
        from (
        select count(distinct id) as c
            from $tableName
        )a
        group by c"""


        test {
            sql "select count(distinct id, name), count(distinct id) from $tableName"
            exception "The query contains multi count distinct or sum distinct, each can't have multi columns"
        }
    }

    test_aggregate_strategies('test_bucket1_table', 1)
    test_aggregate_strategies('test_bucket10_table', 10)

    test {
        sql """select
                /*+SET_VAR(disable_nereids_rules='TWO_PHASE_AGGREGATE_WITH_DISTINCT')*/
                count(distinct number)
                from numbers('number' = '10000')"""
        result([[10000L]])
    }

    test {
        sql """select
                /*+SET_VAR(disable_nereids_rules='THREE_PHASE_AGGREGATE_WITH_DISTINCT')*/
                count(distinct number)
                from numbers('number' = '10000')"""
        result([[10000L]])
    }

    test {
        sql """select
                /*+SET_VAR(disable_nereids_rules='TWO_PHASE_AGGREGATE_WITH_DISTINCT')*/
                count(distinct number)
                from numbers('number' = '10000')"""
        result([[10000L]])
    }

    test {
        sql """select
                /*+SET_VAR(disable_nereids_rules='THREE_PHASE_AGGREGATE_WITH_DISTINCT')*/
                count(distinct number)
                from numbers('number' = '10000')"""
        result([[10000L]])
    }

    qt_sql_distinct_same_col """SELECT COUNT(DISTINCT id, id) FROM test_bucket10_table GROUP BY id """

    sql "set experimental_enable_pipeline_engine=true"
    qt_sql_distinct_same_col2 """SELECT COUNT(DISTINCT id, id) FROM test_bucket10_table GROUP BY id """
}
