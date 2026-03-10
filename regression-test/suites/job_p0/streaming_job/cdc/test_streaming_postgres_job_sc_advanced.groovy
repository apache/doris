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

/**
 * Advanced schema-change regression for the PostgreSQL CDC streaming job.
 *
 * Key differences from the basic schema-change test:
 *   - Uses offset=latest (incremental-only, no snapshot) to cover the code path where
 *     tableSchemas are discovered from PG JDBC rather than derived from snapshot splits.
 *     This exercises the feHadNoSchema=true branch in PipelineCoordinator.
 *
 * Covers uncommon scenarios:
 *   1. Simultaneous double ADD – two columns added in PG before any DML triggers detection;
 *      both ALTER TABLEs are generated and executed in a single detection event.
 *   2. DROP + ADD simultaneously (rename guard) – dropping one column while adding another
 *      is treated as a potential rename; no DDL is emitted but the cached schema is updated.
 *   3. UPDATE on existing rows after rename guard – verifies that a row whose old column (c1)
 *      was dropped in PG gets c1=NULL in Doris after the next UPDATE (stream load replaces the
 *      whole row without c1 since PG no longer has it).
 *   4. ADD COLUMN with DEFAULT value – verifies that the DEFAULT clause is passed through to
 *      Doris and that pre-existing rows automatically receive the default value after the DDL.
 *   5. ADD COLUMN NOT NULL with DEFAULT – verifies the NOT NULL path in SchemaChangeHelper
 *      (col.isOptional()=false → appends NOT NULL) and that Doris accepts the DDL when a
 *      DEFAULT is present (satisfying the NOT NULL constraint for existing rows).
 */
suite("test_streaming_postgres_job_sc_advanced",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {

    def jobName   = "test_streaming_pg_sc_advanced"
    def currentDb = (sql "select database()")[0][0]
    def table1    = "user_info_pg_normal1_sc_adv"
    def pgDB      = "postgres"
    def pgSchema  = "cdc_test"
    def pgUser    = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port       = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint   = getS3Endpoint()
        String bucket        = getS3BucketName()
        String driver_url    = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // ── helpers ───────────────────────────────────────────────────────────

        def waitForRow = { String rowName ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${rowName}'"
                )[0][0] as int > 0
            })
        }

        def waitForRowGone = { String rowName ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${rowName}'"
                )[0][0] as int == 0
            })
        }

        def waitForColumn = { String colName, boolean shouldExist ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def desc = sql "DESC ${table1}"
                desc.any { it[0] == colName } == shouldExist
            })
        }

        // Comparison is done as strings to avoid JDBC numeric type mismatches.
        def waitForValue = { String rowName, String colName, Object expected ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${colName} FROM ${table1} WHERE name='${rowName}'"
                rows.size() == 1 && String.valueOf(rows[0][0]) == String.valueOf(expected)
            })
        }

        def dumpJobState = {
            log.info("jobs  : " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks : " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
        }

        // ── 0. Pre-create PG table with existing rows ─────────────────────────
        // A1, B1 are inserted BEFORE the job starts with offset=latest.
        // They will NOT appear in Doris (no snapshot taken).
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgSchema}.${table1} (
                       name VARCHAR(200) PRIMARY KEY,
                       age  INT4
                   )"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES ('A1', 10)"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES ('B1', 20)"""
        }

        // ── 1. Start streaming job with offset=latest ─────────────────────────
        // The Doris table is auto-created from the PG schema at job creation time.
        // Streaming begins from the current WAL LSN — A1 and B1 are not captured.
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url"       = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url"     = "${driver_url}",
                    "driver_class"   = "org.postgresql.Driver",
                    "user"           = "${pgUser}",
                    "password"       = "${pgPassword}",
                    "database"       = "${pgDB}",
                    "schema"         = "${pgSchema}",
                    "include_tables" = "${table1}",
                    "offset"         = "latest"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        assert (sql "SHOW TABLES FROM ${currentDb} LIKE '${table1}'").size() == 1

        // Wait for job to enter RUNNING state (streaming split established).
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until({
                def rows = sql """select Status from jobs("type"="insert")
                                   where Name='${jobName}' and ExecuteType='STREAMING'"""
                rows.size() == 1 && rows[0][0] == "RUNNING"
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // Baseline: insert C1 to verify streaming is active.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgSchema}.${table1} VALUES ('C1', 30)"""
        }

        try {
            waitForRow('C1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // A1, B1 must NOT be present (offset=latest, no snapshot).
        assert (sql "SELECT COUNT(*) FROM ${table1} WHERE name='A1'")[0][0] as int == 0 \
            : "A1 must not be present (offset=latest)"
        assert (sql "SELECT COUNT(*) FROM ${table1} WHERE name='B1'")[0][0] as int == 0 \
            : "B1 must not be present (offset=latest)"

        // Only C1(30) should be in Doris.
        qt_baseline """ SELECT name, age FROM ${table1} ORDER BY name """

        // ── Phase 1: Simultaneous double ADD (c1 TEXT, c2 INT4) ──────────────
        // Both ALTER TABLEs happen in PG before any DML triggers CDC detection.
        // The single INSERT D1 triggers the detection, which fetches the fresh PG schema
        // (already containing both c1 and c2), and generates two ADD COLUMN DDLs in one shot.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN c1 TEXT"""
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN c2 INT4"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age, c1, c2) VALUES ('D1', 40, 'hello', 42)"""
        }

        try {
            waitForColumn('c1', true)
            waitForColumn('c2', true)
            waitForRow('D1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // DESC columns: Field(0), Type(1), Null(2), Key(3), Default(4), Extra(5)
        def descAfterDoubleAdd = sql "DESC ${table1}"
        assert descAfterDoubleAdd.find { it[0] == 'c1' }[1] == 'text' : "c1 must be added as text"
        assert descAfterDoubleAdd.find { it[0] == 'c2' }[1] == 'int'  : "c2 must be added as int"

        // Pre-double-ADD row C1 must have NULL for both new columns.
        assert (sql "SELECT c1 FROM ${table1} WHERE name='C1'")[0][0] == null : "C1.c1 must be NULL"
        assert (sql "SELECT c1 FROM ${table1} WHERE name='C1'")[0][0] == null : "C1.c2 must be NULL"

        // C1(30,null,null), D1(40,'hello',42)
        qt_double_add """ SELECT name, age, c1, c2 FROM ${table1} ORDER BY name """

        // ── Phase 2: DROP c1 + ADD c3 simultaneously (rename guard) ──────────
        // Dropping c1 and adding c3 in the same batch looks like a rename to the CDC detector:
        // simultaneous ADD+DROP triggers the guard → no DDL emitted, cached schema updated to
        // reflect the fresh PG state (c1 gone, c3 present).
        // Doris table is left with c1 still present; c3 is never added.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} DROP COLUMN c1"""
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN c3 INT4"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age, c2, c3) VALUES ('E1', 50, 10, 99)"""
        }

        try {
            waitForRow('E1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        def descAfterRenameGuard = sql "DESC ${table1}"
        assert  descAfterRenameGuard.any { it[0] == 'c1' } : "c1 must remain (rename guard prevented DROP)"
        assert !descAfterRenameGuard.any { it[0] == 'c3' } : "c3 must NOT be added (rename guard prevented ADD)"

        // E1.c1=NULL: PG has c3 (not c1), Doris ignores c3 and writes NULL for c1.
        assert (sql "SELECT c1 FROM ${table1} WHERE name='E1'")[0][0] == null : "E1.c1 must be NULL"

        // C1(30,null,null), D1(40,'hello',42), E1(50,null,10)
        qt_rename_guard """ SELECT name, age, c1, c2 FROM ${table1} ORDER BY name """

        // ── Phase 3: UPDATE existing row after rename guard ───────────────────
        // D1 had c1='hello' at insert time. After the rename guard fires, the cached schema
        // reflects PG reality (c1 gone, c3 present). When D1 is updated in PG (only c3 exists
        // for non-key columns), the DML record carries no c1 field. Stream load replaces the
        // entire row → D1.c1 becomes NULL in Doris.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // PG now has columns: name, age, c2, c3 (c1 was dropped)
            sql """UPDATE ${pgSchema}.${table1} SET age=99, c3=88 WHERE name='D1'"""
        }

        try {
            waitForValue('D1', 'age', 99)
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // D1.c1 was 'hello' but after UPDATE the stream load has no c1 field →
        // Doris replaces the row without c1 → c1=NULL.
        assert (sql "SELECT c1 FROM ${table1} WHERE name='D1'")[0][0] == null \
            : "D1.c1 must be NULL after UPDATE (c1 dropped from PG, not in stream load record)"

        // C1(30,null,null), D1(99,null,null), E1(50,null,10)
        qt_rename_guard_update """ SELECT name, age, c1, c2 FROM ${table1} ORDER BY name """

        // ── Phase 4: ADD COLUMN with DEFAULT value ────────────────────────────
        // PG adds a nullable TEXT column with a DEFAULT value.
        // buildAddColumnSql picks up col.defaultValueExpression() and appends DEFAULT 'default_val'
        // to the Doris ALTER TABLE.  After the DDL, Doris fills the default for all pre-existing
        // rows (metadata operation), so C1/D1/E1 all get c4='default_val' without any DML replay.
        // F1 is inserted without an explicit c4 value → PG fills in the default → WAL record
        // already carries c4='default_val', so Doris writes 'default_val' for F1 as well.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // PG current cols: name, age, c2, c3
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN c4 TEXT DEFAULT 'default_val'"""
            // Trigger schema-change detection; omit c4 → PG fills default in WAL record.
            sql """INSERT INTO ${pgSchema}.${table1} (name, age, c2, c3) VALUES ('F1', 60, 20, 77)"""
        }

        try {
            waitForColumn('c4', true)
            waitForRow('F1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // DESC columns: Field(0), Type(1), Null(2), Key(3), Default(4), Extra(5)
        def descAfterDefaultAdd = sql "DESC ${table1}"
        def c4Row = descAfterDefaultAdd.find { it[0] == 'c4' }
        assert c4Row != null : "c4 must be added"
        assert c4Row[4] == 'default_val' : "c4 must carry DEFAULT 'default_val', got: ${c4Row[4]}"

        // Pre-existing rows receive the default value from Doris's ALTER TABLE (not from DML replay).
        try {
            waitForValue('C1', 'c4', 'default_val')
            waitForValue('D1', 'c4', 'default_val')
            waitForValue('E1', 'c4', 'default_val')
            waitForValue('F1', 'c4', 'default_val')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // C1(30,_,default_val), D1(99,_,default_val), E1(50,_,default_val), F1(60,_,default_val)
        qt_default_col """ SELECT name, age, c4 FROM ${table1} ORDER BY name """

        // ── Phase 5: ADD COLUMN NOT NULL with DEFAULT ─────────────────────────
        // In PG, adding a NOT NULL column to a non-empty table requires a DEFAULT so existing rows
        // satisfy the constraint.  Debezium captures col.isOptional()=false, so SchemaChangeHelper
        // appends NOT NULL to the Doris column type, and the DEFAULT clause is also passed through.
        // With both NOT NULL and DEFAULT, Doris can apply the DDL: existing rows get the default
        // value (satisfying NOT NULL), and new rows must supply a value or receive the default.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // PG current cols: name, age, c2, c3, c4
            sql """ALTER TABLE ${pgSchema}.${table1}
                       ADD COLUMN c5 TEXT NOT NULL DEFAULT 'required'"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age, c2, c3, c4, c5)
                       VALUES ('G1', 70, 30, 66, 'g1c4', 'explicit')"""
        }

        try {
            waitForColumn('c5', true)
            waitForRow('G1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // DESC columns: Field(0), Type(1), Null(2), Key(3), Default(4), Extra(5)
        def descAfterNotNullAdd = sql "DESC ${table1}"
        def c5Row = descAfterNotNullAdd.find { it[0] == 'c5' }
        assert c5Row != null : "c5 must be added"
        assert c5Row[4] == 'required' : "c5 must carry DEFAULT 'required', got: ${c5Row[4]}"

        // Pre-existing rows must have the default value (Doris ALTER TABLE fills it).
        // G1 was inserted with an explicit 'explicit' value.
        try {
            waitForValue('C1', 'c5', 'required')
            waitForValue('D1', 'c5', 'required')
            waitForValue('G1', 'c5', 'explicit')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // C1(_,default_val,required), D1(_,default_val,required), ...G1(_,g1c4,explicit)
        qt_not_null_col """ SELECT name, age, c4, c5 FROM ${table1} ORDER BY name """

        assert (sql """select * from jobs("type"="insert") where Name='${jobName}'""")[0][5] == "RUNNING"

        // ── Cleanup ───────────────────────────────────────────────────────────
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        assert (sql """select count(1) from jobs("type"="insert") where Name='${jobName}'""")[0][0] == 0
    }
}
