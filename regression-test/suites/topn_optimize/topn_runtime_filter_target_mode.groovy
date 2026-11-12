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

suite("topn_runtime_filter_target_mode") {
    sql "DROP TABLE IF EXISTS topn_target_input"
    sql """CREATE TABLE topn_target_input (k INT NULL, v BIGINT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_target_input VALUES (1,10),(1,20),(2,40),(2,50),(NULL,7),(NULL,8)"

    sql "DROP TABLE IF EXISTS topn_target_aggregate"
    sql """CREATE TABLE topn_target_aggregate (k INT NULL, v BIGINT SUM NOT NULL)
        AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'disable_auto_compaction'='true')"""
    // Keep separate rowsets so the scan must combine stored contributions.
    sql "INSERT INTO topn_target_aggregate VALUES (1,10),(2,40),(NULL,7)"
    sql "INSERT INTO topn_target_aggregate VALUES (1,20),(2,50),(NULL,8)"

    sql "DROP TABLE IF EXISTS topn_target_control"
    sql """CREATE TABLE topn_target_control (k INT NULL, v BIGINT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_target_control VALUES (1,999),(2,999),(NULL,999)"

    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET force_sort_algorithm='heap'"
    sql "SET parallel_pipeline_task_num=1"
    sql "SET push_topn_to_agg=false"
    sql "SET disable_nereids_rules='PUSH_DOWN_TOP_N_THROUGH_UNION'"

    def checkQuery = { String name, String query, String expectedValue, boolean sqlAggregate ->
        // A second ordering key would hide the single-key strict-bound behavior.
        // Check the complete payload while accepting either legal boundary tie.
        String checked = "SELECT k, (v <=> (${expectedValue}) OR v <=> 999) AS valid_row " +
                "FROM (${query}) selected"
        sql "SET topn_filter_ratio=0"
        "order_qt_${name}_disabled"(checked)
        sql "SET topn_filter_ratio=1000000"
        explain {
            sql checked
            contains "UNION"
            contains "TOP-N"
            if (sqlAggregate) {
                contains "AGGREGATE"
            }
            // Both targets remain enabled, and an aggregate target keeps the entire source inclusive.
            multiContains "TOPN OPT:", 2
            contains "TOPN filter boundary: ${query.contains('DESC') ? '>=' : '<='}"
        }
        "order_qt_${name}_enabled"(checked)
    }

    String groupValue = "CASE WHEN k IS NULL THEN 15 WHEN k=1 THEN 30 ELSE 90 END"
    for (String order : ["ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST"]) {
        String suffix = order.toLowerCase().replace(" ", "_")
        checkQuery("grouped_${suffix}", """SELECT k,v FROM (
            SELECT k,SUM(v) AS v FROM topn_target_input GROUP BY k
            UNION ALL SELECT k,v FROM topn_target_control
        ) u ORDER BY k ${order} LIMIT 1""", groupValue, true)
        checkQuery("storage_${suffix}", """SELECT k,v FROM (
            SELECT k,v FROM topn_target_aggregate
            UNION ALL SELECT k,v FROM topn_target_control
        ) u ORDER BY k ${order} LIMIT 1""", groupValue, false)
    }

    checkQuery("group_expression", """SELECT k,v FROM (
        SELECT k,SUM(v) AS v FROM topn_target_input GROUP BY k
        UNION ALL SELECT k,v FROM topn_target_control
    ) u ORDER BY ABS(k) NULLS LAST LIMIT 1""", groupValue, true)
    checkQuery("storage_expression", """SELECT k,v FROM (
        SELECT k,v FROM topn_target_aggregate
        UNION ALL SELECT k,v FROM topn_target_control
    ) u ORDER BY ABS(k) NULLS LAST LIMIT 1""", groupValue, false)
    checkQuery("scalar_constant", """SELECT k,v FROM (
        SELECT CAST(1 AS INT) AS k,SUM(v) AS v FROM topn_target_input
        UNION ALL SELECT k,v FROM topn_target_control
    ) u ORDER BY k NULLS LAST LIMIT 1""", "135", true)

    for (String direction : ["ASC", "DESC"]) {
        explain {
            sql "SELECT k AS alias_key,v FROM topn_target_control WHERE v=999 " +
                    "ORDER BY alias_key ${direction} NULLS LAST LIMIT 1 OFFSET 1"
            contains "TOPN OPT:"
            contains "TOPN filter boundary: ${direction == 'ASC' ? '<' : '>'}\n"
        }
        for (String orderKey : ["k ${direction},v", "ABS(k) ${direction}"]) {
            explain {
                sql "SELECT k,v FROM topn_target_control ORDER BY ${orderKey} LIMIT 1"
                contains "TOPN OPT:"
                contains "TOPN filter boundary: ${direction == 'ASC' ? '<=' : '>='}"
            }
        }
    }

    // An aggregate result is not a grouping key. It cannot become a scan predicate.
    explain {
        sql "SELECT k,SUM(v) AS s FROM topn_target_input GROUP BY k ORDER BY s LIMIT 1"
        notContains "TOPN OPT:"
    }
}
