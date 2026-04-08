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
 * Schema-change regression for the PostgreSQL CDC streaming job.
 *
 * Covers four scenarios in sequence on a single table:
 *   1. ADD COLUMN    – column added in PG → DDL executed in Doris, new data lands correctly.
 *                      Also verifies: pre-ADD rows get NULL for the new column (existing-data
 *                      correctness), and UPDATE/DELETE right after ADD COLUMN are propagated.
 *   2. DROP COLUMN   – column dropped in PG → DDL executed in Doris, subsequent data lands correctly.
 *   3. RENAME COLUMN – rename detected as simultaneous ADD+DROP (rename guard) →
 *                      no DDL in Doris, 'age' column remains, new rows get age=NULL.
 *   4. MODIFY COLUMN – type-only change is invisible to the name-based diff →
 *                      no DDL in Doris, data continues to flow.
 */
suite("test_streaming_postgres_job_sc", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName     = "test_streaming_postgres_job_name_sc"
    def currentDb   = (sql "select database()")[0][0]
    def table1      = "user_info_pg_normal1_sc"
    def pgDB        = "postgres"
    def pgSchema    = "cdc_test"
    def pgUser      = "postgres"
    def pgPassword  = "123456"

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

        // Wait until a specific row appears in the Doris target table.
        def waitForRow = { String rowName ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${rowName}'"
                )[0][0] as int > 0
            })
        }

        // Wait until a column either exists or no longer exists in the Doris table.
        def waitForColumn = { String colName, boolean shouldExist ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def desc = sql "DESC ${table1}"
                desc.any { it[0] == colName } == shouldExist
            })
        }

        // Wait until a specific row disappears from the Doris target table.
        def waitForRowGone = { String rowName ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${rowName}'"
                )[0][0] as int == 0
            })
        }

        // Wait until a specific column value matches the expected value for a row.
        // Comparison is done as strings to avoid JDBC numeric type mismatches (e.g. Short vs Integer).
        def waitForValue = { String rowName, String colName, Object expected ->
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${colName} FROM ${table1} WHERE name='${rowName}'"
                rows.size() == 1 && String.valueOf(rows[0][0]) == String.valueOf(expected)
            })
        }

        // Dump job/task state on assertion failures for easier debugging.
        def dumpJobState = {
            log.info("jobs  : " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks : " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
        }

        // ── 0. Create PG table and insert snapshot rows ───────────────────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgSchema}.${table1} (
                       name VARCHAR(200) PRIMARY KEY,
                       age  INT2
                   )"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES ('A1', 1)"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES ('B1', 2)"""
        }

        // ── 1. Start streaming job ────────────────────────────────────────────
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
                    "offset"         = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        // Verify the table was auto-created with the expected initial schema.
        assert (sql "SHOW TABLES FROM ${currentDb} LIKE '${table1}'").size() == 1
        // DESC columns: Field(0), Type(1), Null(2), Key(3), Default(4), Extra(5)
        def initDesc = sql "DESC ${currentDb}.${table1}"
        assert initDesc.find { it[0] == 'name' }[1] == 'varchar(65533)' : "name must be varchar(65533)"
        assert initDesc.find { it[0] == 'age'  }[1] == 'smallint'       : "age must be smallint"
        assert initDesc.find { it[0] == 'name' }[3] == 'true'           : "name must be primary key"

        // Wait for snapshot to finish (job completes ≥ 2 tasks).
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert")
                                  where Name='${jobName}' and ExecuteType='STREAMING'"""
                cnt.size() == 1 && cnt[0][0] as int >= 2
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // Snapshot data: A1(1), B1(2)
        qt_snapshot """ SELECT name, age FROM ${table1} ORDER BY name """

        // ── Phase 1: ADD COLUMN c1 ────────────────────────────────────────────
        // PG adds VARCHAR column c1; CDC detects ADD via name diff and executes
        // ALTER TABLE … ADD COLUMN c1 on Doris.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN c1 VARCHAR(50)"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age, c1) VALUES ('C1', 10, 'hello')"""
        }

        try {
            waitForColumn('c1', true)
            waitForRow('C1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // Verify c1 was added to Doris and the new row is present.
        assert (sql "DESC ${table1}").any { it[0] == 'c1' } : "c1 column must exist in Doris after ADD COLUMN"

        // Pre-ADD rows must have NULL for the new column (existing-data correctness).
        assert (sql "SELECT c1 FROM ${table1} WHERE name='A1'")[0][0] == null : "A1.c1 must be NULL (pre-ADD row)"
        assert (sql "SELECT c1 FROM ${table1} WHERE name='B1'")[0][0] == null : "B1.c1 must be NULL (pre-ADD row)"

        // A1(1,null), B1(2,null), C1(10,'hello')
        qt_add_column """ SELECT name, age, c1 FROM ${table1} ORDER BY name """

        // ── Phase 1b: UPDATE / DELETE immediately after ADD COLUMN ───────────
        // Verifies that UPDATE (touching the new column) and DELETE on pre-existing rows
        // are correctly propagated to Doris after the schema change.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // Update the new column on the just-inserted row.
            sql """UPDATE ${pgSchema}.${table1} SET c1='world' WHERE name='C1'"""
            // Update both an old column and the new column on a pre-existing row.
            sql """UPDATE ${pgSchema}.${table1} SET age=99, c1='updated' WHERE name='B1'"""
            // Delete a pre-existing row.
            sql """DELETE FROM ${pgSchema}.${table1} WHERE name='A1'"""
        }

        try {
            waitForRowGone('A1')
            waitForValue('B1', 'age', 99)
            waitForValue('C1', 'c1', 'world')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // A1 deleted; B1(99,'updated'); C1(10,'world')
        qt_add_column_dml """ SELECT name, age, c1 FROM ${table1} ORDER BY name """

        // ── Phase 2: DROP COLUMN c1 ───────────────────────────────────────────
        // PG drops c1; CDC detects DROP and executes ALTER TABLE … DROP COLUMN c1 on Doris.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} DROP COLUMN c1"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age) VALUES ('D1', 20)"""
        }

        try {
            waitForColumn('c1', false)
            waitForRow('D1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // Verify c1 was removed from Doris and data flows without it.
        assert !(sql "DESC ${table1}").any { it[0] == 'c1' } : "c1 column must be gone from Doris after DROP COLUMN"
        // B1(99), C1(10), D1(20)  [A1 was deleted in Phase 1b]
        qt_drop_column """ SELECT name, age FROM ${table1} ORDER BY name """

        // ── Phase 3: RENAME COLUMN age → age2 (rename guard) ─────────────────
        // PG rename looks like a simultaneous ADD(age2) + DROP(age) to the name diff.
        // The rename guard detects this and emits a WARN with no DDL, so Doris schema
        // is unchanged.  New PG rows carry 'age2' which has no matching column in Doris,
        // so 'age' is NULL for those rows.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} RENAME COLUMN age TO age2"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age2) VALUES ('E1', 30)"""
        }

        try {
            waitForRow('E1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // 'age' must still exist; 'age2' must NOT have been added.
        def descAfterRename = sql "DESC ${table1}"
        assert  descAfterRename.any { it[0] == 'age'  } : "'age' column must remain after rename guard"
        assert !descAfterRename.any { it[0] == 'age2' } : "'age2' must NOT be added (rename guard, no DDL)"
        // B1(99), C1(10), D1(20), E1(null) — age=NULL because PG sends age2 which Doris ignores
        qt_rename """ SELECT name, age FROM ${table1} ORDER BY name """

        // ── Phase 4: MODIFY COLUMN type (name-only diff, no DDL) ─────────────
        // Type-only change is invisible to the name-based diff, so no DDL is emitted.
        // Data continues to flow; age2 values still have no mapping in Doris → age=NULL.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} ALTER COLUMN age2 TYPE INT4"""
            sql """INSERT INTO ${pgSchema}.${table1} (name, age2) VALUES ('F1', 50)"""
        }

        try {
            waitForRow('F1')
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        // Doris 'age' column type must remain smallint (mapped from PG int2).
        assert (sql "DESC ${table1}").find { it[0] == 'age' }[1] == 'smallint' \
            : "Doris 'age' type must remain smallint after type-only change in PG"
        // B1(99), C1(10), D1(20), E1(null), F1(null)
        qt_modify """ SELECT name, age FROM ${table1} ORDER BY name """

        assert (sql """select * from jobs("type"="insert") where Name='${jobName}'""")[0][5] == "RUNNING"

        // ── Cleanup ───────────────────────────────────────────────────────────
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        assert (sql """select count(1) from jobs("type"="insert") where Name='${jobName}'""")[0][0] == 0
    }
}
