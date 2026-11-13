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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_unique_key_null_repair") {
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    def downstream = ["ivm_unique_repair_left", "ivm_unique_repair_right",
                      "ivm_unique_repair_partial_key", "ivm_unique_repair_residual"]
    downstream.each { sql "DROP MATERIALIZED VIEW IF EXISTS ${it}" }
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_unique_repair_groups"
    sql "DROP TABLE IF EXISTS ivm_unique_repair_contacts"
    sql "DROP TABLE IF EXISTS ivm_unique_repair_details"

    sql """
        CREATE TABLE ivm_unique_repair_contacts (
            id INT NOT NULL, organisationid INT, contactid INT,
            label VARCHAR(32), threshold_value INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true", "changelog.need_historical_value" = "true"
        )
    """
    sql """
        CREATE TABLE ivm_unique_repair_details (
            id INT NOT NULL, organisationid INT, contactid INT, amount INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true", "changelog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_unique_repair_contacts VALUES
        (1, 1, 10, 'first', 15), (2, 1, 20, 'second', 15),
        (3, 2, 10, 'other_org', 15), (4, 1, NULL, 'null_key', 15),
        (5, 1, 10, 'duplicate_join_key', 25), (6, 1, 30, 'unmatched', 15)
    """
    sql """INSERT INTO ivm_unique_repair_details VALUES
        (1, 1, 10, 10), (2, 1, 10, 10), (3, 2, 10, 30), (4, 1, NULL, 40)
    """

    // GROUP BY proves the complete business key unique; the L2 stream can still contain
    // several versions of that key. A SUM payload update must retain -old/+new detail events.
    sql """
        CREATE MATERIALIZED VIEW ivm_unique_repair_groups
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1", "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
        AS SELECT organisationid, contactid, SUM(amount) AS total, COUNT(*) AS cnt
        FROM ivm_unique_repair_details GROUP BY organisationid, contactid
    """

    // CreateTime has second granularity and TaskId is not monotonically increasing.
    // Capture existing ids, then wait for the exact task added by this refresh.
    def taskIdsOf = { String mv ->
        sql_return_maparray("""
            SELECT TaskId FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mv}'
        """).collect { it.TaskId.toString() } as Set
    }
    def refresh = { String mv, String mode ->
        def previousIds = taskIdsOf(mv)
        sql "REFRESH MATERIALIZED VIEW ${mv} ${mode}"
        String taskId = null
        Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until {
            taskId = taskIdsOf(mv).find { !previousIds.contains(it) }
            return taskId != null
        }
        def latest
        Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until {
            latest = sql_return_maparray("""
                SELECT Status, RefreshMode, ErrorMsg FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mv}'
                  AND TaskId = '${taskId}'
            """)
            return !latest.isEmpty() && !['PENDING', 'RUNNING'].contains(latest[0].Status.toString())
        }
        assertEquals('SUCCESS', latest[0].Status.toString())
        if (mode == 'INCREMENTAL') {
            assertTrue(latest[0].RefreshMode.toString() != 'COMPLETE',
                    "Unexpected complete fallback for ${mv}: ${latest}")
        }
    }
    refresh('ivm_unique_repair_groups', 'COMPLETE')

    def fullKey = 'a.organisationid = b.organisationid AND a.contactid = b.contactid'
    def conditions = [
        (downstream[0]): fullKey,
        (downstream[1]): fullKey,
        // organisationid alone is not unique: a net positive change can be 1->2, not 0->1.
        (downstream[2]): 'a.organisationid = b.organisationid',
        // A value update may change matching despite an unchanged number of L1 rows.
        (downstream[3]): "${fullKey} AND b.total > a.threshold_value"
    ]
    downstream.each { mv ->
        def from = mv == downstream[1]
                ? 'ivm_unique_repair_groups b RIGHT JOIN ivm_unique_repair_contacts a'
                : 'ivm_unique_repair_contacts a LEFT JOIN ivm_unique_repair_groups b'
        sql """
            CREATE MATERIALIZED VIEW ${mv}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS SELECT a.id, a.organisationid, a.contactid, a.label, b.total, b.cnt
            FROM ${from} ON ${conditions[mv]}
        """
        refresh(mv, 'COMPLETE')
    }

    def columns = 'id, organisationid, contactid, label, total, cnt'
    def fullGroups = """(SELECT organisationid, contactid, SUM(amount) AS total, COUNT(*) AS cnt
        FROM ivm_unique_repair_details GROUP BY organisationid, contactid)"""
    def check = { String stage ->
        delegate.quickRunTest("order_${stage}_values",
                "SELECT ${columns} FROM ivm_unique_repair_left", true)
        downstream.eachWithIndex { mv, index ->
            // Compare signed bag multiplicities, including NULLs and duplicate join matches,
            // against a fresh query over the original tables instead of the upstream MV.
            delegate.quickRunTest("order_${stage}_difference_${index}", """
                SELECT ${columns}, SUM(weight) AS multiplicity_difference
                FROM (
                    SELECT ${columns}, 1 AS weight FROM ${mv}
                    UNION ALL
                    SELECT a.id, a.organisationid, a.contactid, a.label, b.total, b.cnt, -1 AS weight
                    FROM ivm_unique_repair_contacts a LEFT JOIN ${fullGroups} b
                      ON ${conditions[mv]}
                ) differences
                GROUP BY ${columns}
                HAVING SUM(weight) <> 0
            """, true)
        }
    }
    def refreshAndCheck = { String stage ->
        refresh('ivm_unique_repair_groups', 'INCREMENTAL')
        downstream.each { refresh(it, 'INCREMENTAL') }
        check(stage)
    }
    check('baseline')

    // First match for key (1,20): retract its NULL row; a second organisation-1 group
    // simultaneously exercises the nonunique-key fallback.
    sql "INSERT INTO ivm_unique_repair_details VALUES (5, 1, 20, 5)"
    refreshAndCheck('first_match')

    // Multiple same-key payload versions have net row count zero. The residual predicate
    // changes from true to false, then back above the two contacts' different thresholds.
    sql "UPDATE ivm_unique_repair_details SET amount = 1 WHERE id = 1"
    sql "UPDATE ivm_unique_repair_details SET amount = 35 WHERE id = 1"
    refreshAndCheck('same_key_updates')
    sql "UPDATE ivm_unique_repair_details SET amount = 0 WHERE id = 1"
    refreshAndCheck('residual_loses_match')

    // Last match disappears, while another group's payload changes and NULL keys change.
    sql "DELETE FROM ivm_unique_repair_details WHERE id = 5"
    sql "INSERT INTO ivm_unique_repair_details VALUES (6, 2, 10, 5), (7, 1, NULL, 7)"
    refreshAndCheck('last_match')

    // A real matching row with NULL payload must remain distinguishable from a NULL extension.
    sql "INSERT INTO ivm_unique_repair_details VALUES (9, 1, 20, NULL)"
    refreshAndCheck('null_payload_match')

    // Move a whole group between business keys and change the retained side in the same batch.
    sql "UPDATE ivm_unique_repair_details SET contactid = 30 WHERE organisationid = 1 AND contactid = 10"
    sql "UPDATE ivm_unique_repair_contacts SET label = 'changed', threshold_value = 5 WHERE id = 6"
    sql "INSERT INTO ivm_unique_repair_contacts VALUES (7, 1, 30, 'new_contact', 5)"
    refreshAndCheck('key_migration_and_both_sides')

    // Delete/reinsert an existing group and insert/delete a new group before refresh.
    sql "DELETE FROM ivm_unique_repair_details WHERE id = 3"
    sql "INSERT INTO ivm_unique_repair_details VALUES (3, 2, 10, 32), (8, 1, 40, 8)"
    sql "DELETE FROM ivm_unique_repair_details WHERE id = 8"
    refreshAndCheck('batch_cancellation')
}
