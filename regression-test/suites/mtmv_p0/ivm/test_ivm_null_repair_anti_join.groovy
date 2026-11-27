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

suite("test_ivm_null_repair_anti_join", "nonConcurrent") {
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_repair_anti_mv"
    sql "DROP TABLE IF EXISTS ivm_repair_anti_retained"
    sql "DROP TABLE IF EXISTS ivm_repair_anti_snapshot"

    sql """
        CREATE TABLE ivm_repair_anti_retained (id INT NOT NULL, k INT NOT NULL)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "changelog.enable"="true", "changelog.need_historical_value"="true")
    """
    sql """
        CREATE TABLE ivm_repair_anti_snapshot (id INT NOT NULL, k INT NOT NULL)
        UNIQUE KEY(id)
        PARTITION BY RANGE(id) (
            PARTITION p_changed VALUES LESS THAN (100),
            PARTITION p_stable VALUES LESS THAN (MAXVALUE)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "changelog.enable"="true", "changelog.need_historical_value"="true")
    """
    sql "INSERT INTO ivm_repair_anti_retained VALUES (1, 10), (2, 20), (3, 30), (4, 40)"
    sql "INSERT INTO ivm_repair_anti_snapshot VALUES (1, 10), (2, 10), (3, 20)"
    // Simulate an old NDV on one column after the table row count shrinks. Each column statistic is
    // valid on its own, and the final scan validation detects the mismatch against the new table count.
    sql """
        ALTER TABLE ivm_repair_anti_retained MODIFY COLUMN id SET STATS
        ('row_count'='1000', 'ndv'='1000', 'min_value'='1', 'max_value'='1000')
    """
    sql """
        ALTER TABLE ivm_repair_anti_retained MODIFY COLUMN k SET STATS
        ('row_count'='4', 'ndv'='4', 'min_value'='10', 'max_value'='40')
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_repair_anti_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num"="1")
        AS SELECT a.id AS retained_id, b.id AS matching_id
        FROM ivm_repair_anti_retained a LEFT OUTER JOIN ivm_repair_anti_snapshot b ON a.k = b.k
    """

    def refresh = { String mode ->
        sql "REFRESH MATERIALIZED VIEW ivm_repair_anti_mv ${mode}"
        waitingMTMVTaskFinishedByMvName("ivm_repair_anti_mv")
    }
    def explainAntiJoin = { String type, boolean broadcast ->
        explain {
            sql "SHAPE PLAN REFRESH MATERIALIZED VIEW ivm_repair_anti_mv INCREMENTAL"
            check { plan ->
                def antiJoins = plan.readLines().findAll { it.contains("ANTI_JOIN") }
                !antiJoins.isEmpty() && antiJoins.every {
                    it.contains("hashJoin[${type} ") && (!broadcast || it.contains(" broadcast]"))
                }
            }
        }
    }

    // REFRESH uses an admin planning context. A small global row cap exercises both sides of the
    // broadcast threshold with a modest fixture.
    def cap = sql_return_maparray("SHOW GLOBAL VARIABLES LIKE 'broadcast_row_count_limit'")[0].Value
    def reorderDisabled = sql_return_maparray("SHOW GLOBAL VARIABLES LIKE 'disable_join_reorder'")[0].Value
    try {
        sql "SET GLOBAL broadcast_row_count_limit = 128"
        sql "SET GLOBAL disable_join_reorder = false"
        refresh("COMPLETE")
        sql "INSERT INTO ivm_repair_anti_snapshot VALUES (4, 30)"
        explainAntiJoin("LEFT_ANTI_JOIN", true)
        refresh("INCREMENTAL")
        order_qt_small "SELECT retained_id, matching_id FROM ivm_repair_anti_mv"

        sql """
            INSERT INTO ivm_repair_anti_snapshot
            SELECT number + 100, number + 100 FROM numbers("number"="4096")
        """
        refresh("COMPLETE")
        sql "INSERT INTO ivm_repair_anti_snapshot VALUES (5, 40)"
        sql "DELETE FROM ivm_repair_anti_snapshot WHERE id IN (1, 3)"
        explainAntiJoin("RIGHT_ANTI_JOIN", false)

        sql "SET GLOBAL disable_join_reorder = true"
        explainAntiJoin("LEFT_ANTI_JOIN", false)
        sql "SET GLOBAL disable_join_reorder = false"
        refresh("INCREMENTAL")
        order_qt_large "SELECT retained_id, matching_id FROM ivm_repair_anti_mv"
    } finally {
        sql "SET GLOBAL broadcast_row_count_limit = ${cap}"
        sql "SET GLOBAL disable_join_reorder = ${reorderDisabled}"
    }
}
