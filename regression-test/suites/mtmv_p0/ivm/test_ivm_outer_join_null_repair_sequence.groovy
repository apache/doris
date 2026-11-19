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

suite("test_ivm_outer_join_null_repair_sequence") {
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    def inner = "SELECT a.id, a.ak FROM ivm_null_repair_a a JOIN ivm_null_repair_b b ON a.bk = b.id"
    def joins = [
        left_events: "ivm_null_repair_l l LEFT JOIN (${inner}) r ON l.k1 = r.ak",
        left_repair: "ivm_null_repair_l l LEFT JOIN (${inner}) r ON l.k1 = r.ak AND l.v < r.id",
        right_events: "(${inner}) r RIGHT JOIN ivm_null_repair_l l ON l.k1 = r.ak",
        full_repair: "ivm_null_repair_l l FULL JOIN (${inner}) r ON l.k1 = r.ak"
    ]

    joins.each { kind, join ->
        sql "DROP MATERIALIZED VIEW IF EXISTS ivm_null_repair_${kind}"
    }
    sql "DROP TABLE IF EXISTS ivm_null_repair_l"
    sql "DROP TABLE IF EXISTS ivm_null_repair_a"
    sql "DROP TABLE IF EXISTS ivm_null_repair_b"

    sql """
        CREATE TABLE ivm_null_repair_l (k1 INT, v INT) UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "changelog.enable"="true", "changelog.need_historical_value"="true")
    """
    sql """
        CREATE TABLE ivm_null_repair_a (id INT, bk INT, ak INT) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "changelog.enable"="true", "changelog.need_historical_value"="true")
    """
    sql """
        CREATE TABLE ivm_null_repair_b (id INT) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "changelog.enable"="true", "changelog.need_historical_value"="true")
    """
    sql "INSERT INTO ivm_null_repair_l VALUES (1,10),(2,20)"
    sql "INSERT INTO ivm_null_repair_a VALUES (100,1,1)"
    sql "INSERT INTO ivm_null_repair_b VALUES (1)"

    joins.each { kind, join ->
        sql """
            CREATE MATERIALIZED VIEW ivm_null_repair_${kind}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES("replication_num"="1")
            AS SELECT l.k1, l.v, r.id AS a_id FROM ${join}
        """
        sql "REFRESH MATERIALIZED VIEW ivm_null_repair_${kind} COMPLETE"
        waitingMTMVTaskFinishedByMvName("ivm_null_repair_${kind}")
        quickRunTest("order_${kind}_before", "SELECT * FROM ivm_null_repair_${kind}", true)
    }

    // The subtree emits +(100,ak=2) and -(100,ak=2), although neither snapshot contains ak=2.
    // Both NULL-row repairs target the same row-id for l.k1=2 and must preserve that NULL row.
    sql "UPDATE ivm_null_repair_a SET ak=2 WHERE id=100"
    sql "DELETE FROM ivm_null_repair_b WHERE id=1"

    joins.each { kind, join ->
        quickRunTest("order_${kind}_delta",
                "REFRESH MATERIALIZED VIEW ivm_null_repair_${kind} INCREMENTAL WITH DRY RUN", true,
                { row, meta ->
                    def normalized = new ArrayList(row)
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        if (meta.getColumnLabel(i).equalsIgnoreCase("__DORIS_SEQUENCE_COL__")) {
                            // Keep the operation bit so the test detects equal repair sequences in every mode.
                            normalized[i - 1] = new BigInteger(row[i - 1].toString()).and(BigInteger.ONE).toString()
                        }
                    }
                    normalized
                })
        sql "REFRESH MATERIALIZED VIEW ivm_null_repair_${kind} INCREMENTAL"
        waitingMTMVTaskFinishedByMvName("ivm_null_repair_${kind}")
        def task = sql_return_maparray("""
            SELECT IvmRefreshMethod FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_null_repair_${kind}'
            ORDER BY CreateTime DESC LIMIT 1
        """)
        assertEquals("INCREMENTAL", task[0].IvmRefreshMethod)
        quickRunTest("order_${kind}_incremental", "SELECT * FROM ivm_null_repair_${kind}", true)
        quickRunTest("order_${kind}_source", "SELECT l.k1,l.v,r.id AS a_id FROM ${join}", true)
        sql "REFRESH MATERIALIZED VIEW ivm_null_repair_${kind} COMPLETE"
        waitingMTMVTaskFinishedByMvName("ivm_null_repair_${kind}")
        quickRunTest("order_${kind}_complete", "SELECT * FROM ivm_null_repair_${kind}", true)
    }
}
