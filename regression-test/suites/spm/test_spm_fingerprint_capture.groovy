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

suite("test_spm_fingerprint_capture", "spm") {
    sql "set enable_spm_rewrite = true"
    sql "set enable_spm_fallback = false"
    sql "set enable_sql_cache = false"

    sql("SHOW BASELINE PLANS").findAll { it[1].toString().contains("spm_fingerprint_capture") }
            .each { row -> sql "DROP BASELINE PLAN ${row[0]}" }
    sql "DROP TABLE IF EXISTS spm_fingerprint_capture"
    sql """
        CREATE TABLE spm_fingerprint_capture (k INT, v INT)
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p1 VALUES LESS THAN ("10"),
            PARTITION p2 VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1", "light_schema_change" = "true")
    """
    sql "INSERT INTO spm_fingerprint_capture VALUES (1, 10), (11, 110)"

    def createBaseline = { String query ->
        (sql("CREATE GLOBAL BASELINE PLAN '${query}' WITH '${query}'")[0][0] as Long)
    }
    def explanation = { String query -> sql("EXPLAIN ${query}").toString() }
    def metadata = { long id ->
        sql("SELECT plan_frozen, schema_fingerprint FROM __internal_schema.spm_baselines WHERE id = ${id}")[0]
    }
    String query = "SELECT k, v FROM spm_fingerprint_capture WHERE k >= 1 ORDER BY k"
    String variant = "SELECT k, v FROM spm_fingerprint_capture WHERE k >= 11 ORDER BY k"
    long id = createBaseline(query)
    def original = metadata(id)
    assertTrue(original[0].toString() in ["1", "true"], "baseline must freeze: " + original)
    assertTrue(original[1].toString().contains("|olap-schema:"), "native OLAP schema version: " + original)
    assertTrue(explanation(query).contains("SPM baseline hit: id=${id}"))
    assertTrue(explanation(variant).contains("SPM baseline hit: id=${id}"))
    qt_initial """SELECT k, v FROM spm_fingerprint_capture WHERE k >= 1 ORDER BY k"""
    qt_variant """SELECT k, v FROM spm_fingerprint_capture WHERE k >= 11 ORDER BY k"""

    // A different session-visible schema must not reuse a frozen output list.
    sql "set show_hidden_columns = true"
    assertFalse(explanation(query).contains("SPM baseline hit: id=${id}"))
    sql "set show_hidden_columns = false"
    assertTrue(explanation(query).contains("SPM baseline hit: id=${id}"))

    // Data and partition changes must not invalidate a plan that doesn't pin partitions.
    sql "INSERT INTO spm_fingerprint_capture VALUES (2, 20)"
    sql """ALTER TABLE spm_fingerprint_capture ADD PARTITION p3 VALUES LESS THAN ("30")"""
    sql "INSERT INTO spm_fingerprint_capture VALUES (21, 210)"
    sql "ALTER TABLE spm_fingerprint_capture RENAME PARTITION p1 p01"
    assertTrue(explanation(query).contains("SPM baseline hit: id=${id}"))
    assertTrue(explanation(variant).contains("SPM baseline hit: id=${id}"))
    qt_after_partition_change """SELECT k, v FROM spm_fingerprint_capture WHERE k >= 1 ORDER BY k"""

    String pinned = "SELECT k, v FROM spm_fingerprint_capture PARTITION(p01, p2) WHERE k >= 1 ORDER BY k"
    long pinId = createBaseline(pinned)
    assertTrue(metadata(pinId)[0].toString() in ["1", "true"])
    assertTrue(explanation(pinned).contains("SPM baseline hit: id=${pinId}"))
    qt_pinned """SELECT k, v FROM spm_fingerprint_capture PARTITION(p01, p2) WHERE k >= 1 ORDER BY k"""

    // Even restoring the old column shape must not revive the old schema version.
    sql "ALTER TABLE spm_fingerprint_capture ADD COLUMN extra INT DEFAULT '0'"
    assertFalse(explanation(query).contains("SPM baseline hit: id=${id}"))
    assertFalse(explanation(pinned).contains("SPM baseline hit: id=${pinId}"))
    sql "ALTER TABLE spm_fingerprint_capture DROP COLUMN extra"
    assertFalse(explanation(query).contains("SPM baseline hit: id=${id}"))
    long replacement = createBaseline(query)
    assertNotEquals(id, replacement)
    assertNotEquals(original[1], metadata(replacement)[1])
    assertTrue(explanation(query).contains("SPM baseline hit: id=${replacement}"))
    qt_new_schema_version """SELECT k, v FROM spm_fingerprint_capture WHERE k >= 1 ORDER BY k"""
}
