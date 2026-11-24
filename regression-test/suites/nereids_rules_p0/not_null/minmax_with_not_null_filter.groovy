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

suite("test_minmax_with_not_null_filter") {
    def originalThreshold = sql("show variables like 'keep_is_not_null_num_nulls_threshold'")[0][1]
    def originalDisabledRules = sql("show variables like 'disable_nereids_rules'")[0][1]
    def originalPushDown = sql("show variables like 'enable_push_down_no_group_agg'")[0][1]
    try {
        sql "drop table if exists test_minmax_with_not_null_filter"
        sql """
            create table test_minmax_with_not_null_filter (
                id int not null,
                val int null
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        sql "insert into test_minmax_with_not_null_filter values (1, 3), (2, null), (3, 9), (4, null)"
        sql "analyze table test_minmax_with_not_null_filter with sync"
        sql "set disable_nereids_rules='REWRITE_SIMPLE_AGG_TO_CONSTANT'"
        sql "set keep_is_not_null_num_nulls_threshold=1"

        def inferred = "select min(val), max(val) from test_minmax_with_not_null_filter"
        def explicit = inferred + " where val is not null"
        def projected = """
            select min(aliased), max(aliased)
            from (select val as aliased from test_minmax_with_not_null_filter) t
            where aliased is not null
        """
        [inferred, explicit, projected].each { query ->
            explain {
                sql query
                contains "pushAggOp=MINMAX"
                notContains "IS NOT NULL"
            }
        }

        def differentColumn = "select min(val), max(id) from test_minmax_with_not_null_filter where val is not null"
        def mixedCount = "select min(val), count(*) from test_minmax_with_not_null_filter where val is not null"
        def extraPredicate = explicit + " and id > 1"
        def grouped = "select id, min(val) from test_minmax_with_not_null_filter where val is not null group by id"
        [differentColumn, mixedCount, extraPredicate, grouped].each { query ->
            explain {
                sql query
                contains "pushAggOp=NONE"
                contains "IS NOT NULL"
            }
        }

        order_qt_inferred inferred
        order_qt_explicit explicit
        order_qt_projected projected
        order_qt_different_column differentColumn
        order_qt_mixed_count mixedCount
        order_qt_extra_predicate extraPredicate
        order_qt_grouped grouped

        sql "set enable_push_down_no_group_agg=false"
        explain {
            sql inferred
            contains "pushAggOp=NONE"
            contains "IS NOT NULL"
        }
        order_qt_scan_fallback inferred
    } finally {
        sql "set enable_push_down_no_group_agg=${originalPushDown}"
        sql "set keep_is_not_null_num_nulls_threshold=${originalThreshold}"
        sql "set disable_nereids_rules='${originalDisabledRules}'"
    }
}
