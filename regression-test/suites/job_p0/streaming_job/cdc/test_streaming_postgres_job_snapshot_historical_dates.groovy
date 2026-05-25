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
 * Verify snapshot and binlog paths produce identical values for historical
 * dates that previously drifted in the snapshot path (PG JDBC's
 * GregorianCalendar + JVM-zone LMT).
 *
 * Phases:
 *   1. snapshot batch (ids 1..N) inserted in postgres before the job starts;
 *      after sync, assert values in doris match the original input.
 *   2. binlog INSERT batch (ids 11..10+N) with the same boundary values;
 *      assert each binlog row equals its snapshot counterpart cell-for-cell.
 *   3. binlog UPDATE: rewrite id=1's columns to a different boundary value
 *      and assert the streamed change lands.
 *   4. binlog DELETE: remove id=2 and assert it disappears in doris.
 */
suite("test_streaming_postgres_job_snapshot_historical_dates",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_snapshot_historical_dates_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_pg_historical_dates"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String pg_port = context.config.otherConfigs.get("pg_14_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

    // Boundary rows. Picked to exercise:
    //   - Julian/Gregorian cutover (0001-01-01 ⇒ 2-day drift, 1582-10-04/15 boundary)
    //   - LMT offset for pre-1901 values in zones like Asia/Shanghai (1900-12-31 vs 1901-01-02)
    //   - sub-millisecond precision on a pre-1970 value (negative micros, bug B)
    //   - NULL across all three columns
    def boundaryRows = [
            [ts: "0001-01-01 00:00:00.000123", tstz: "0001-01-01 00:00:00.000123+00", date: "0001-01-01"],
            [ts: "1582-10-04 12:34:56.000000", tstz: "1582-10-04 12:34:56+00",        date: "1582-10-04"],
            [ts: "1582-10-15 00:00:00.000000", tstz: "1582-10-15 00:00:00+00",        date: "1582-10-15"],
            [ts: "1900-12-31 23:59:59.999000", tstz: "1900-12-31 23:59:59.999+00",    date: "1900-12-31"],
            [ts: "1901-01-02 00:00:00.000000", tstz: "1901-01-02 00:00:00+09",        date: "1901-01-02"],
            [ts: "1969-12-31 23:59:59.999123", tstz: "1969-12-31 23:59:59.999123+00", date: "1969-12-31"],
            [ts: null,                         tstz: null,                            date: null],
    ]
    def rowsPerBatch = boundaryRows.size()
    def snapshotIdBase = 1
    def binlogIdBase = 11

    def buildInsertValues = { int idBase ->
        boundaryRows.withIndex().collect { row, i ->
            def id = idBase + i
            def tsLit   = row.ts   == null ? "NULL" : "TIMESTAMP '${row.ts}'"
            def tstzLit = row.tstz == null ? "NULL" : "TIMESTAMPTZ '${row.tstz}'"
            def dateLit = row.date == null ? "NULL" : "DATE '${row.date}'"
            "(${id}, ${tsLit}, ${tstzLit}, ${dateLit})"
        }.join(",\n        ")
    }

    def dumpJobOnFailure = {
        log.info("show job: " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
        log.info("show task: " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
    }

    // ── postgres setup + snapshot batch ───────────────────────────────────────
    connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
        sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
        sql """
        CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
            id            bigint PRIMARY KEY,
            ts_col        timestamp(6),
            tstz_col      timestamp(6) with time zone,
            date_col      date
        )
        """
        sql """
        INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES
        ${buildInsertValues(snapshotIdBase)}
        """
    }

    // ── start streaming job (offset=initial ⇒ snapshot + binlog) ──────────────
    sql """CREATE JOB ${jobName}
            ON STREAMING
            FROM POSTGRES (
                "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}?timezone=UTC",
                "driver_url" = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user" = "${pgUser}",
                "password" = "${pgPassword}",
                "database" = "${pgDB}",
                "schema" = "${pgSchema}",
                "include_tables" = "${table1}",
                "offset" = "initial"
            )
            TO DATABASE ${currentDb} (
              "table.create.properties.replication_num" = "1"
            )
        """

    // ── phase 1: snapshot ─────────────────────────────────────────────────────
    try {
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until {
            def cnt = sql """SELECT count(1) FROM ${currentDb}.${table1}"""
            log.info("snapshot row count: ${cnt}")
            cnt.size() == 1 && (cnt.get(0).get(0) as long) == (long) rowsPerBatch
        }
    } catch (Exception ex) {
        dumpJobOnFailure()
        throw ex
    }

    qt_snapshot """SELECT id, ts_col, tstz_col, date_col FROM ${currentDb}.${table1} ORDER BY id"""

    // ── phase 2: binlog INSERT ────────────────────────────────────────────────
    connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
        sql """
        INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES
        ${buildInsertValues(binlogIdBase)}
        """
    }
    try {
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until {
            def cnt = sql """SELECT count(1) FROM ${currentDb}.${table1}"""
            cnt.size() == 1 && (cnt.get(0).get(0) as long) == (long) (rowsPerBatch * 2)
        }
    } catch (Exception ex) {
        dumpJobOnFailure()
        throw ex
    }

    qt_binlog_insert """SELECT id, ts_col, tstz_col, date_col FROM ${currentDb}.${table1}
                        WHERE id >= ${binlogIdBase} ORDER BY id"""

    // Parity: snapshot row i must equal binlog row i+10 cell-for-cell.
    def snapshotRows = sql """SELECT ts_col, tstz_col, date_col FROM ${currentDb}.${table1}
                              WHERE id <  ${binlogIdBase} ORDER BY id"""
    def binlogRows   = sql """SELECT ts_col, tstz_col, date_col FROM ${currentDb}.${table1}
                              WHERE id >= ${binlogIdBase} ORDER BY id"""
    assert snapshotRows.size() == rowsPerBatch
    assert binlogRows.size()   == rowsPerBatch
    for (int i = 0; i < rowsPerBatch; i++) {
        def s = snapshotRows.get(i)
        def b = binlogRows.get(i)
        assert s.get(0)?.toString() == b.get(0)?.toString() :
                "ts_col mismatch at row ${i}: snapshot=${s.get(0)} binlog=${b.get(0)}"
        assert s.get(1)?.toString() == b.get(1)?.toString() :
                "tstz_col mismatch at row ${i}: snapshot=${s.get(1)} binlog=${b.get(1)}"
        assert s.get(2)?.toString() == b.get(2)?.toString() :
                "date_col mismatch at row ${i}: snapshot=${s.get(2)} binlog=${b.get(2)}"
    }

    // ── phase 3: binlog UPDATE ────────────────────────────────────────────────
    // Rewrite id=1 (originally 0001-01-01) to a different boundary value via UPDATE.
    def updatedTs   = "1582-10-15 12:00:00.000123"
    def updatedTstz = "1582-10-15 12:00:00.000123+00"
    def updatedDate = "1900-12-31"
    connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
        sql """
        UPDATE ${pgDB}.${pgSchema}.${table1}
        SET ts_col   = TIMESTAMP '${updatedTs}',
            tstz_col = TIMESTAMPTZ '${updatedTstz}',
            date_col = DATE '${updatedDate}'
        WHERE id = 1
        """
    }
    try {
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until {
            def row = sql """SELECT cast(date_col as string) FROM ${currentDb}.${table1} WHERE id = 1"""
            row.size() == 1 && row.get(0).get(0) == updatedDate
        }
    } catch (Exception ex) {
        dumpJobOnFailure()
        throw ex
    }
    qt_binlog_update """SELECT id, ts_col, tstz_col, date_col FROM ${currentDb}.${table1}
                        WHERE id = 1"""

    // ── phase 4: binlog DELETE ────────────────────────────────────────────────
    connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
        sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE id = 2"""
    }
    try {
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until {
            def cnt = sql """SELECT count(1) FROM ${currentDb}.${table1} WHERE id = 2"""
            cnt.size() == 1 && (cnt.get(0).get(0) as long) == 0L
        }
    } catch (Exception ex) {
        dumpJobOnFailure()
        throw ex
    }
    qt_binlog_after_delete """SELECT id, ts_col, tstz_col, date_col FROM ${currentDb}.${table1}
                              ORDER BY id"""

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
}
