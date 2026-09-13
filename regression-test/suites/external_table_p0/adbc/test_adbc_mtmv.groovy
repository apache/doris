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

// ############################################################################
// A materialized view built on an ADBC external table.
//
// This is the first thing in this directory that puts the connector under a
// component with a LIFECYCLE. Everything else runs a statement and looks at
// the rows; an MTMV registers the external table as a base table, plans and
// runs the scan from a background job rather than from a user session, and
// then has to decide -- later, from a different thread -- whether what it
// built is still current.
//
// So there are two questions here and they fail differently:
//   1. can an MTMV be defined and refreshed over this catalog at all? If
//      CREATE MATERIALIZED VIEW is refused, that is the answer, and the
//      failure belongs in a bug report rather than in a change to this file.
//      It is the first thing the suite does, for exactly that reason.
//   2. does a refresh pick up what changed on the source? An ADBC source has
//      no snapshot or version for Doris to compare, so the only honest test is
//      to change the source and refresh again.
//
// Transparent query rewriting is exercised but NOT asserted. Whether the
// optimizer considers an MTMV whose base table cannot report a version is a
// property of the rewrite rules, not of this connector; the suite runs the
// base query with rewriting enabled, asserts the ANSWER is right either way,
// and logs which plan was chosen.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_mtmv", "p0,external") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String driverPath = context.config.otherConfigs.get("adbcDriverPath")
    if (driverPath == null || driverPath.isEmpty()) {
        driverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }

    if (!new File(driverPath).canRead()) {
        logger.info("SKIPPED test_adbc_mtmv: no readable ADBC Flight SQL driver at ${driverPath}. "
                + "Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', or set "
                + "adbcDriverPath in regression-conf.groovy. "
                + "MATERIALIZED VIEWS OVER ADBC TABLES ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_mtmv_catalog"
    String dbName = "test_adbc_mtmv_db"
    String mvName = "test_adbc_mtmv_mv"
    String joinMvName = "test_adbc_mtmv_join_mv"
    String aggMvName = "test_adbc_mtmv_agg_mv"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${joinMvName}"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${aggMvName}"""
    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.facts (
          `id` int NOT NULL,
          `grp` varchar(16) NULL,
          `amount` decimalv3(10, 2) NULL,
          `d` date NULL
        ) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    // UNIQUE, not the default DUPLICATE, for one reason: the refresh test below changes a value in
    // place with UPDATE, and Doris refuses UPDATE on any other table model ("Only unique table could
    // be updated"). Changing a value rather than only appending one is the point -- a COMPLETE refresh
    // that re-read the table but kept an old value would still hold the right row COUNT.
    sql """
        INSERT INTO internal.${dbName}.facts VALUES
          (1, 'a', 10.00, '2024-01-01'),
          (2, 'a', 20.00, '2024-01-02'),
          (3, 'b', 30.00, '2024-02-01'),
          (4, 'b', 40.00, '2024-02-02'),
          (5, 'c', 50.00, '2024-03-01')
    """

    // Lives in the internal catalog and never goes through ADBC: the join MTMV below needs one base table
    // of each kind, which is the realistic shape and a different code path from an all-external one.
    sql """
        CREATE TABLE internal.${dbName}.dims (
          `grp` varchar(16) NOT NULL,
          `label` varchar(32) NULL
        ) DISTRIBUTED BY HASH(`grp`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO internal.${dbName}.dims VALUES ('a', 'alpha'), ('b', 'beta'), ('c', 'gamma')"""

    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "required"
        )
    """

    try {
        String table = "${catalogName}.${dbName}.facts"

        // waitingMTMVTaskFinishedByMvName asserts SUCCESS but never says why a refresh failed: the
        // framework helper does not select ErrorMsg, so a broken refresh reports exactly
        // "expected:<SUCCESS> but was:<FAILED>" and the reason has to be dug out of the FE log
        // afterwards -- by which point this suite has already dropped the catalog it was about. Read
        // the task's own error and put it in the log before the assertion failure propagates.
        def awaitRefresh = { String mv ->
            try {
                waitingMTMVTaskFinishedByMvName(mv)
            } catch (Throwable failure) {
                logger.info("the refresh of ${mv} did not succeed; its task row is "
                        + sql("""
                            SELECT TaskId, Status, ErrorMsg FROM tasks('type'='mv')
                            WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mv}'
                            ORDER BY CreateTime DESC LIMIT 1
                        """).toString())
                throw failure
            }
        }

        // The catalog itself works, so any MTMV failure below is about the MTMV.
        assertEquals(5L, sql("SELECT count(*) FROM ${table}")[0][0],
                "the ADBC catalog cannot read its base table, so nothing about MTMV is testable here")

        // ---- can an MTMV be defined over this catalog at all ----
        //
        // BUILD DEFERRED plus an explicit REFRESH, so that building and refreshing fail separately: a
        // BUILD IMMEDIATE that failed would not say which of the two went wrong.

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT id, grp, amount, d FROM ${table}
        """
        sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
        awaitRefresh(mvName)

        // The MV holds what the source holds. Compared against the source table rather than against a
        // baseline, because a baseline generated from a refresh that read the wrong rows would record the
        // wrong rows.
        assertEquals(
                sql("SELECT id, grp, amount, d FROM internal.${dbName}.facts ORDER BY id").toString(),
                sql("SELECT id, grp, amount, d FROM ${mvName} ORDER BY id").toString(),
                "the materialized view does not hold what its ADBC base table holds")
        qt_mv_content """SELECT id, grp, amount, d FROM ${mvName} ORDER BY id"""

        // ---- a refresh picks up what changed on the source ----
        //
        // An ADBC source has no version Doris can compare, so a COMPLETE refresh re-reading everything is
        // the only mechanism available. What must not happen is a refresh that reports success and leaves
        // the old rows.

        sql """INSERT INTO internal.${dbName}.facts VALUES (6, 'c', 60.00, '2024-03-02')"""
        sql """UPDATE internal.${dbName}.facts SET amount = 11.00 WHERE id = 1"""

        sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
        awaitRefresh(mvName)

        assertEquals(6L, sql("SELECT count(*) FROM ${mvName}")[0][0],
                "the refreshed view did not pick up the row added to the source")
        assertEquals(
                sql("SELECT id, grp, amount, d FROM internal.${dbName}.facts ORDER BY id").toString(),
                sql("SELECT id, grp, amount, d FROM ${mvName} ORDER BY id").toString(),
                "a refresh reported success but the view still holds the old rows")
        qt_mv_after_refresh """SELECT id, grp, amount, d FROM ${mvName} ORDER BY id"""

        // ---- an aggregating MTMV ----
        //
        // The shape an MTMV is actually for. It also puts an aggregate between the ADBC scan and the sink,
        // which the plain projection above does not.

        sql """
            CREATE MATERIALIZED VIEW ${aggMvName}
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT grp, count(*) AS n, sum(amount) AS total, max(d) AS latest
                FROM ${table} GROUP BY grp
        """
        awaitRefresh(aggMvName)

        assertEquals(
                sql("""SELECT grp, count(*), sum(amount), max(d) FROM internal.${dbName}.facts
                       GROUP BY grp ORDER BY grp""").toString(),
                sql("SELECT grp, n, total, latest FROM ${aggMvName} ORDER BY grp").toString(),
                "the aggregating materialized view disagrees with the same aggregate over the source")
        qt_agg_mv """SELECT grp, n, total, latest FROM ${aggMvName} ORDER BY grp"""

        // ---- an MTMV over an ADBC table joined to an internal one ----

        sql """
            CREATE MATERIALIZED VIEW ${joinMvName}
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT f.id, f.grp, d.label, f.amount
                FROM ${table} f JOIN internal.${dbName}.dims d ON d.grp = f.grp
        """
        awaitRefresh(joinMvName)

        assertEquals(
                sql("""SELECT f.id, f.grp, d.label, f.amount
                       FROM internal.${dbName}.facts f
                       JOIN internal.${dbName}.dims d ON d.grp = f.grp
                       ORDER BY f.id""").toString(),
                sql("SELECT id, grp, label, amount FROM ${joinMvName} ORDER BY id").toString(),
                "the materialized view over a mixed ADBC/internal join does not match the same join run "
                        + "directly")
        qt_join_mv """SELECT id, grp, label, amount FROM ${joinMvName} ORDER BY id"""

        // A change on the ADBC side must flow through the join view too.
        sql """INSERT INTO internal.${dbName}.facts VALUES (7, 'a', 70.00, '2024-04-01')"""
        sql """REFRESH MATERIALIZED VIEW ${joinMvName} COMPLETE"""
        awaitRefresh(joinMvName)
        assertEquals(7L, sql("SELECT count(*) FROM ${joinMvName}")[0][0],
                "the join view did not pick up the row added to its ADBC base table")

        // ---- what Doris says about these views ----

        def mvRows = sql("SHOW CREATE MATERIALIZED VIEW ${mvName}")
        assertFalse(mvRows.isEmpty(), "SHOW CREATE MATERIALIZED VIEW returned nothing for ${mvName}")
        assertTrue(mvRows[0].toString().contains(catalogName),
                "the view definition does not name the ADBC catalog it reads: ${mvRows[0]}")

        def jobName = getJobName(context.dbName, mvName)
        logger.info("the refresh job for ${mvName} is ${jobName}")
        assertNotNull(jobName, "the materialized view has no refresh job")

        // ---- transparent rewriting: run it, do not require it ----
        //
        // Whether the optimizer will substitute an MTMV whose base table cannot report a version is a
        // property of the rewrite rules. What must hold regardless is that the answer is right.

        sql """SET enable_materialized_view_rewrite = true"""
        try {
            def direct = sql("""
                SELECT grp, count(*), sum(amount) FROM ${table} GROUP BY grp ORDER BY grp
            """)
            def expected = sql("""
                SELECT grp, count(*), sum(amount) FROM internal.${dbName}.facts GROUP BY grp ORDER BY grp
            """)
            assertEquals(expected.toString(), direct.toString(),
                    "with materialized view rewriting enabled, the aggregate over the ADBC table returned "
                            + "the wrong answer -- a rewrite substituted a view that does not match it")

            String[] plan = new String[1]
            explain {
                sql("SELECT grp, count(*), sum(amount) FROM ${table} GROUP BY grp")
                check { String text ->
                    plan[0] = text
                    return true
                }
            }
            logger.info("with rewriting enabled, the aggregate over the ADBC table was planned as "
                    + (plan[0].contains(aggMvName) ? "a READ OF ${aggMvName}" : "a direct ADBC scan"))
        } finally {
            sql """SET enable_materialized_view_rewrite = false"""
        }

        // ---- dropping ----
        //
        // The views hold a reference to an external table; dropping them must not need the catalog to be
        // consulted, and must actually remove them.

        sql """DROP MATERIALIZED VIEW ${joinMvName}"""
        sql """DROP MATERIALIZED VIEW ${aggMvName}"""
        String droppedMessage = null
        try {
            sql "SELECT count(*) FROM ${joinMvName}"
        } catch (Exception e) {
            droppedMessage = e.getMessage()
        }
        assertNotNull(droppedMessage, "a dropped materialized view still answers queries")

        // And the last one is dropped AFTER its catalog, which is the order an operator gets wrong: the
        // view outlives the source it was built from, and removing it must still work.
        sql """DROP CATALOG ${catalogName}"""
        assertEquals(6L, sql("SELECT count(*) FROM ${mvName}")[0][0],
                "a materialized view stopped answering once its ADBC catalog was dropped, even though it "
                        + "holds its own copy of the data")
        sql """DROP MATERIALIZED VIEW ${mvName}"""
    } finally {
        sql """SET enable_materialized_view_rewrite = false"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${joinMvName}"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${aggMvName}"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
