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

suite("shared_cte_reset_test", "rec_cte") {
    qt_shared_cte_reset """
        WITH RECURSIVE
        org_mp_bridge AS (
            SELECT '1' AS organization_id, 'a' AS marketplace_account_id
            UNION ALL SELECT '2', 'a'
            UNION ALL SELECT '3', 'b'
        ),
        edges AS (
            SELECT CONCAT('ORG:', organization_id) AS src,
                   CONCAT('MP:', marketplace_account_id) AS dst
            FROM org_mp_bridge
            UNION
            SELECT CONCAT('MP:', marketplace_account_id),
                   CONCAT('ORG:', organization_id)
            FROM org_mp_bridge
        ),
        nodes AS (
            SELECT src AS node FROM edges
            UNION
            SELECT dst FROM edges
        ),
        reach (start_node, node) AS (
            SELECT node, node FROM nodes
            UNION
            SELECT r.start_node, e.dst
            FROM reach r
            JOIN edges e ON r.node = e.src
        ),
        node_group AS (
            SELECT node, MIN(start_node) AS reconciliation_group_id
            FROM reach
            GROUP BY node
        ),
        org_group AS (
            SELECT DISTINCT b.organization_id, g.reconciliation_group_id
            FROM org_mp_bridge b
            JOIN node_group g ON g.node = CONCAT('ORG:', b.organization_id)
        ),
        mp_group AS (
            SELECT DISTINCT b.marketplace_account_id, g.reconciliation_group_id
            FROM org_mp_bridge b
            JOIN node_group g ON g.node = CONCAT('MP:', b.marketplace_account_id)
        )
        SELECT organization_id, reconciliation_group_id FROM org_group
        UNION ALL
        SELECT marketplace_account_id, reconciliation_group_id FROM mp_group
        ORDER BY organization_id, reconciliation_group_id
    """

    // Each recursive controller must own its transitive ordinary CTE dependencies.
    // Keep materialization enabled so this also checks that the planner enforces inlining.
    sql "set enable_cte_materialize = true"
    sql "set inline_cte_referenced_threshold = 1"
    for (def input : ["base", "middle"]) {
        def query = """
            WITH RECURSIVE
            base AS (
                SELECT 1 AS src, 2 AS dst
                UNION ALL SELECT 2, 3
                UNION ALL SELECT 3, 4
            ),
            middle AS (
                SELECT * FROM base
                UNION ALL SELECT * FROM base
            ),
            edges AS (
                SELECT * FROM ${input}
                UNION ALL SELECT * FROM ${input}
            ),
            r1(n) AS (
                SELECT 1
                UNION ALL
                SELECT e.dst FROM r1 r JOIN edges e ON r.n = e.src
            ),
            r2(n) AS (
                SELECT 2
                UNION ALL
                SELECT e.dst FROM r2 r JOIN edges e ON r.n = e.src
            )
            SELECT r1.n, r2.n FROM r1 JOIN r2 ON r1.n = r2.n
            ORDER BY r1.n, r2.n
        """
        explain {
            sql query
            notContains "MultiCastDataSinks"
        }
        // Exercise multiple rounds and final close, which previously raised NOT_FOUND.
        sql query
    }
}
