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

// Time travel correctness tests for structural table operations:
// DROP PARTITION, TRUNCATE TABLE, and SCHEMA CHANGE (ADD/DROP COLUMN).
suite("test_cloud_time_travel_structural_changes", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip")
        return
    }

    // ── DROP PARTITION: historical snapshot must include dropped partition rows ─

    def tblPart = "test_cloud_tt_drop_partition"
    sql "DROP TABLE IF EXISTS ${tblPart} FORCE"
    sql """
        CREATE TABLE ${tblPart} (
            dt   DATE,
            id   INT,
            val  VARCHAR(50)
        ) DUPLICATE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p_jan VALUES LESS THAN ('2024-02-01'),
            PARTITION p_feb VALUES LESS THAN ('2024-03-01'),
            PARTITION p_mar VALUES LESS THAN ('2024-04-01')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num"            = "1",
            "enable_time_travel"         = "true",
            "time_travel_retention_days" = "7"
        )
    """

    sql "INSERT INTO ${tblPart} VALUES ('2024-01-10', 1, 'jan_a'), ('2024-02-10', 2, 'feb_a'), ('2024-03-10', 3, 'mar_a')"
    def t1 = sql "SELECT NOW()"
    t1 = t1[0][0].toString().substring(0, 19)
    sleep(2000)

    sql "ALTER TABLE ${tblPart} DROP PARTITION p_feb"

    sleep(2000)
    def t2 = sql "SELECT NOW()"
    t2 = t2[0][0].toString().substring(0, 19)

    // After drop: only jan + mar visible
    def current = sql "SELECT id FROM ${tblPart} ORDER BY id"
    assertEquals(2, current.size())
    assertEquals(1, current[0][0] as int)
    assertEquals(3, current[1][0] as int)

    // At T1 (before drop): all 3 partitions should be visible
    def snap = sql "SELECT id FROM ${tblPart} FOR SYSTEM_TIME AS OF '${t1}' ORDER BY id"
    assertEquals(3, snap.size(), "T1 snapshot must include the dropped partition (feb)")
    assertEquals(1, snap[0][0] as int)
    assertEquals(2, snap[1][0] as int)
    assertEquals(3, snap[2][0] as int)

    // At T2 (after drop): feb is gone
    def snap2 = sql "SELECT id FROM ${tblPart} FOR SYSTEM_TIME AS OF '${t2}' ORDER BY id"
    assertEquals(2, snap2.size(), "T2 snapshot must not include the dropped partition")

    sql "DROP TABLE IF EXISTS ${tblPart} FORCE"

    // ── TRUNCATE TABLE: historical snapshot must show pre-truncation rows ───────

    def tblTrunc = "test_cloud_tt_truncate"
    sql "DROP TABLE IF EXISTS ${tblTrunc} FORCE"
    sql """
        CREATE TABLE ${tblTrunc} (
            id  INT,
            val VARCHAR(50)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num"            = "1",
            "enable_time_travel"         = "true",
            "time_travel_retention_days" = "7"
        )
    """

    sql "INSERT INTO ${tblTrunc} VALUES (1, 'before_trunc'), (2, 'before_trunc_2')"
    def tt1 = sql "SELECT NOW()"
    tt1 = tt1[0][0].toString().substring(0, 19)
    sleep(2000)

    sql "TRUNCATE TABLE ${tblTrunc}"

    sleep(2000)
    def tt2 = sql "SELECT NOW()"
    tt2 = tt2[0][0].toString().substring(0, 19)

    sql "INSERT INTO ${tblTrunc} VALUES (10, 'after_trunc')"

    // Current table: only post-truncation row
    def curTrunc = sql "SELECT id FROM ${tblTrunc} ORDER BY id"
    assertEquals(1, curTrunc.size())
    assertEquals(10, curTrunc[0][0] as int)

    // At TT1 (before truncate): 2 original rows
    def snapTrunc = sql "SELECT id FROM ${tblTrunc} FOR SYSTEM_TIME AS OF '${tt1}' ORDER BY id"
    assertEquals(2, snapTrunc.size(), "Pre-truncation TT snapshot must return original rows")
    assertEquals(1, snapTrunc[0][0] as int)
    assertEquals(2, snapTrunc[1][0] as int)

    // At TT2 (after truncate, before new insert): empty
    def snapTrunc2 = sql "SELECT id FROM ${tblTrunc} FOR SYSTEM_TIME AS OF '${tt2}' ORDER BY id"
    assertEquals(0, snapTrunc2.size(), "Post-truncation TT snapshot must be empty")

    sql "DROP TABLE IF EXISTS ${tblTrunc} FORCE"

    // ── SCHEMA CHANGE (ADD COLUMN): TT query before change sees old schema ──────

    def tblSchema = "test_cloud_tt_schema_change"
    sql "DROP TABLE IF EXISTS ${tblSchema} FORCE"
    sql """
        CREATE TABLE ${tblSchema} (
            id  INT,
            val VARCHAR(50)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num"            = "1",
            "enable_time_travel"         = "true",
            "time_travel_retention_days" = "7"
        )
    """

    sql "INSERT INTO ${tblSchema} VALUES (1, 'v1'), (2, 'v2')"
    def ts1 = sql "SELECT NOW()"
    ts1 = ts1[0][0].toString().substring(0, 19)
    sleep(2000)

    // Add a new column after T1
    sql "ALTER TABLE ${tblSchema} ADD COLUMN extra VARCHAR(50) DEFAULT 'default_val'"
    // Wait for schema change to complete
    def maxWait = 60
    def waited = 0
    while (waited < maxWait) {
        def jobs = sql "SHOW ALTER TABLE COLUMN WHERE TableName = '${tblSchema}' ORDER BY CreateTime DESC LIMIT 1"
        if (jobs && jobs[0][9] == 'FINISHED') break
        sleep(3000)
        waited += 3
    }

    sql "INSERT INTO ${tblSchema} VALUES (3, 'v3', 'extra_v3')"
    def ts2 = sql "SELECT NOW()"
    ts2 = ts2[0][0].toString().substring(0, 19)

    // Current query: 3 columns, 3 rows
    def curSchema = sql "SELECT id, val, extra FROM ${tblSchema} ORDER BY id"
    assertEquals(3, curSchema.size())
    assertEquals('default_val', curSchema[0][2])  // backfilled for old rows

    // At TS1 (before schema change): 2 rows, old schema (no extra column)
    // The FE should resolve historical schema and allow query with 2-column projection
    def snapSchema = sql "SELECT id, val FROM ${tblSchema} FOR SYSTEM_TIME AS OF '${ts1}' ORDER BY id"
    assertEquals(2, snapSchema.size(), "Pre-schema-change TT snapshot must return 2 rows")
    assertEquals(1, snapSchema[0][0] as int)
    assertEquals(2, snapSchema[1][0] as int)

    sql "DROP TABLE IF EXISTS ${tblSchema} FORCE"
}
