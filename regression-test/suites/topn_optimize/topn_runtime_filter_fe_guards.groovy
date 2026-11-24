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

suite("topn_runtime_filter_fe_guards") {
    sql "DROP TABLE IF EXISTS topn_runtime_filter_fe_guards"
    sql """CREATE TABLE topn_runtime_filter_fe_guards (
        id INT NOT NULL, k INT NOT NULL, v BIGINT NOT NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_runtime_filter_fe_guards VALUES (1,1,1), (2,1,1), (9,1,999)"

    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET disable_join_reorder=true"
    sql "SET parallel_pipeline_task_num=1"
    sql "SET force_sort_algorithm='heap'"
    sql "SET push_topn_to_agg=false"
    sql "SET disable_nereids_rules='PUSH_DOWN_TOP_N_THROUGH_UNION,PUSH_DOWN_TOP_N_THROUGH_JOIN'"

    // Check complete-row validity without adding a second ordering key, which would
    // disable strict bounds. Either original boundary tie is a valid Top1 result.
    def validateRow = { String query, String predicate ->
        "SELECT k, (${predicate}) AS valid_row FROM (${query}) selected"
    }
    def checkQuery = { String name, String query, String operator, int targets ->
        sql "SET topn_filter_ratio=0"
        "order_qt_${name}_disabled"(query)

        sql "SET topn_filter_ratio=1000000"
        explain {
            sql query
            contains "UNION"
            contains "TOP-N"
            contains operator
            // Verify the eligible paths, including inclusive aggregate targets.
            multiContains "TOPN OPT:", targets
        }
        "order_qt_${name}_enabled"(query)
    }

    def scalarQuery = """SELECT k,s FROM (
        SELECT CAST(1 AS INT) AS k, SUM(v + 1) AS s
        FROM topn_runtime_filter_fe_guards WHERE id=1
        UNION ALL
        SELECT k,v AS s FROM topn_runtime_filter_fe_guards WHERE id=9
    ) u ORDER BY k NULLS LAST LIMIT 1"""
    // Emptying SUM's input can synthesize (1,NULL), absent from the original result.
    checkQuery("scalar_sum", validateRow(scalarQuery, "s <=> 2 OR s <=> 999"), "AGGREGATE", 2)

    def groupedQuery = """SELECT k,s FROM (
        SELECT k,SUM(v + 1) AS s FROM (
            SELECT k,v FROM topn_runtime_filter_fe_guards WHERE id=1
            UNION ALL
            SELECT k,v FROM topn_runtime_filter_fe_guards WHERE id=2
        ) d GROUP BY k
        UNION ALL
        SELECT k,v AS s FROM topn_runtime_filter_fe_guards WHERE id=9
    ) u ORDER BY ABS(k) NULLS LAST LIMIT 1"""
    // Separate inputs expose a group that can consume a prefix before a bound arrives.
    checkQuery("grouped_sum", validateRow(groupedQuery, "s <=> 4 OR s <=> 999"), "AGGREGATE", 3)

    def antiQuery = """SELECT k,p FROM (
        SELECT b.k,b.v AS p
        FROM (SELECT k FROM topn_runtime_filter_fe_guards WHERE id=1) a
        RIGHT ANTI JOIN (SELECT k,v FROM topn_runtime_filter_fe_guards WHERE id=2) b
        ON a.k=b.k
        UNION ALL
        SELECT k,v AS p FROM topn_runtime_filter_fe_guards WHERE id=9
    ) u ORDER BY k NULLS LAST LIMIT 1"""
    // The anti branch is empty. Removing its left match would invent (1,1).
    checkQuery("right_anti", antiQuery, "RIGHT ANTI JOIN", 1)
    def constantAntiQuery = antiQuery.replace("SELECT b.k,b.v AS p", "SELECT CAST(1 AS INT) AS k,b.v AS p")
            .replace("ON a.k=b.k", "ON a.k<b.k+1")
    checkQuery("right_anti_constant", constantAntiQuery, "RIGHT ANTI JOIN", 1)

    def outerQuery = """SELECT k,p FROM (
        SELECT b.k,a.v AS p
        FROM (SELECT k,v FROM topn_runtime_filter_fe_guards WHERE id=1) a
        RIGHT OUTER JOIN (SELECT k FROM topn_runtime_filter_fe_guards WHERE id=2) b
        ON a.k=b.k
        UNION ALL
        SELECT k,v AS p FROM topn_runtime_filter_fe_guards WHERE id=9
    ) u ORDER BY k NULLS LAST LIMIT 1"""
    // Filtering a matching left row preserves b.k but changes its left output to NULL.
    for (String joinType : ["RIGHT OUTER", "FULL OUTER"]) {
        String query = outerQuery.replace("RIGHT OUTER", joinType)
        String name = joinType.toLowerCase().replace(" ", "_")
        checkQuery(name, validateRow(query, "p <=> 1 OR p <=> 999"), "${joinType} JOIN", 1)

        // An empty input-column set cannot prove that an ordering key comes from the left.
        String constantQuery = query.replace("SELECT b.k,a.v AS p", "SELECT CAST(1 AS INT) AS k,a.v AS p")
                .replace("ON a.k=b.k", "ON a.k<b.k+1")
        checkQuery("${name}_constant", validateRow(constantQuery, "p <=> 1 OR p <=> 999"),
                "${joinType} JOIN", 1)
    }

    def volatileQuery = """SELECT k,id FROM (
        SELECT IF(RAND(3)<0.5,CAST(0 AS INT),CAST(1 AS INT)) AS k,id
        FROM topn_runtime_filter_fe_guards WHERE id IN (1,2)
        UNION ALL
        SELECT k,id FROM topn_runtime_filter_fe_guards WHERE id=9
    ) u ORDER BY k NULLS LAST LIMIT 1"""
    // Retain id in the output expression so the scan must return actual rows.
    checkQuery("volatile", validateRow(volatileQuery, "id IN (1,2)"), "UNION", 1)
    String slotVolatileQuery = volatileQuery.replace("CAST(1 AS INT)) AS k,id", "CAST(1 AS INT))+k-1 AS k,id")
    checkQuery("volatile_with_slot", validateRow(slotVolatileQuery, "id IN (1,2)"), "UNION", 1)

    // Positive control: a safe join keeps its scan target as well as the UNION sibling.
    String innerQuery = outerQuery.replace("RIGHT OUTER JOIN", "INNER JOIN")
            .replace("SELECT b.k,a.v AS p", "SELECT a.k,a.v AS p")
    checkQuery("inner_control", validateRow(innerQuery, "p <=> 1 OR p <=> 999"), "INNER JOIN", 2)

    // EXISTS can use either mark-join orientation. The FE unit test constructs the
    // RIGHT SEMI alternative explicitly; this result check does not fix a CBO choice.
    def markQuery = """SELECT k,m FROM (
        SELECT CAST(1 AS INT) AS k,
            EXISTS (SELECT 1 FROM topn_runtime_filter_fe_guards a
                    WHERE a.id=1 AND a.k=b.k) AS m
        FROM topn_runtime_filter_fe_guards b WHERE b.id=2
        UNION ALL
        SELECT k,v>0 AS m FROM topn_runtime_filter_fe_guards WHERE id=9
    ) u ORDER BY k NULLS LAST LIMIT 1"""
    sql "SET disable_join_reorder=false"
    sql "SET topn_filter_ratio=0"
    order_qt_mark_disabled markQuery
    sql "SET topn_filter_ratio=1000000"
    explain {
        sql markQuery
        verbose true
        contains "UNION"
        contains "isMarkJoin: true"
        contains "TOPN OPT:"
    }
    order_qt_mark_enabled markQuery
}
