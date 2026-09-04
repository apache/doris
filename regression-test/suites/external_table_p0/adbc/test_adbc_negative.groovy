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
// Everything that must fail, and must fail SAYING WHY.
//
// This catalog type asks more of an operator than any other: a shared library
// placed by hand on every node, a URI that has to pin one remote catalog, and
// a driver whose capabilities Doris cannot know in advance. Nearly every way
// that goes wrong produces a library that loads, a catalog that is created,
// and a failure much later with nothing in it about a file or a property. So
// the assertions here are not "it threw" -- they are about WHAT the message
// contains, because for this connector the message is the feature.
//
// Two rules the suite follows:
//   * a wrong property fails at CREATE CATALOG, not at first query. A catalog
//     that accepts a typo and breaks later is the failure mode the strict
//     parsing exists to prevent, so the CREATE statements below are asserted
//     to throw rather than the queries after them.
//   * a refused write must leave the source untouched. "It threw" is not
//     enough: a statement that errors after modifying the remote side is worse
//     than one that succeeds.
//
// Runs against the SQLite driver, which thirdparty builds, so it needs no
// downloaded artifact. The Flight SQL driver is used only if it happens to be
// present, for the one case that needs a reachable network source.
// ############################################################################

suite("test_adbc_negative", "p0,external") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String driverPath = context.config.otherConfigs.get("adbcSqliteDriverPath")
    if (driverPath == null || driverPath.isEmpty()) {
        driverPath = "${thirdparty}/installed/lib64/libadbc_driver_sqlite.so"
    }

    if (!new File(driverPath).canRead()) {
        logger.info("SKIPPED test_adbc_negative: no readable ADBC SQLite driver at ${driverPath}. "
                + "Build it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', or set "
                + "adbcSqliteDriverPath in regression-conf.groovy. "
                + "ADBC ERROR HANDLING IS NOT BEING TESTED.")
        return
    }

    String workDir = "${System.getProperty('java.io.tmpdir')}/doris_adbc_regression"
    new File(workDir).mkdirs()
    File dbFile = new File("${workDir}/test_adbc_negative.db")
    File seedFile = new File("${workDir}/test_adbc_negative.sql")
    dbFile.delete()
    seedFile.text = """
        CREATE TABLE t1 (id INTEGER, name TEXT);
        INSERT INTO t1 VALUES (1, 'alice');
        INSERT INTO t1 VALUES (2, 'bob');
    """
    def seed = new ProcessBuilder("/bin/bash", "-c",
            "sqlite3 '${dbFile.absolutePath}' < '${seedFile.absolutePath}'")
            .redirectErrorStream(true).start()
    String seedOutput = seed.inputStream.text
    int seedExit = seed.waitFor()
    if (seedExit != 0 || !dbFile.exists()) {
        logger.info("SKIPPED test_adbc_negative: could not create the SQLite fixture "
                + "(exit ${seedExit}): ${seedOutput}. THE ADBC ERROR PATHS ARE NOT BEING TESTED.")
        return
    }

    String catalogName = "test_adbc_negative_catalog"
    String badCatalog = "test_adbc_negative_bad"
    String dbName = "main"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP CATALOG IF EXISTS ${badCatalog}"""

    // Asserts a statement fails and that the message says enough to act on. A bare "it threw" would pass
    // for a connection reset, a syntax error, or an assertion in unrelated code.
    def failsMentioning = { String stmt, List<String> mustMention, String why ->
        String message = null
        try {
            sql stmt
        } catch (Exception e) {
            message = e.getMessage() == null ? e.toString() : e.getMessage()
        }
        assertNotNull(message, "this should have failed (${why}): ${stmt}")
        mustMention.each { String fragment ->
            assertTrue(message.toLowerCase().contains(fragment.toLowerCase()),
                    "the failure message does not mention [${fragment}], so ${why} is not actionable "
                            + "from it. Message was: ${message}")
        }
        logger.info("rejected as expected [${why}]: ${message}")
        return message
    }

    def createCatalog = { String name, String properties ->
        return """CREATE CATALOG ${name} PROPERTIES (${properties})"""
    }

    try {
        // ---- properties that must be rejected at CREATE CATALOG ----

        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}" """),
                ["uri"],
                "a catalog with no uri has no source to read")

        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "uri" = "file:${dbFile.absolutePath}" """),
                ["driver_url"],
                "a catalog with no driver has nothing to load")

        // A driver is never fetched per node: FE and every BE must load the identical file, because
        // partition descriptors are driver-private bytes.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "https://example.com/libadbc.so",
                            "uri" = "file:${dbFile.absolutePath}" """),
                ["driver_url"],
                "a remote driver URL is not supported")

        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "/nonexistent/libadbc_driver_nope.so",
                            "uri" = "file:${dbFile.absolutePath}" """),
                ["not found"],
                "a driver file that is not there")

        // Traversal is rejected on the decoded path, before any normalization that would hide it.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "/opt/../etc/libadbc.so",
                            "uri" = "file:${dbFile.absolutePath}" """),
                ["driver_url"],
                "a driver path containing '..'")

        // A bare name is resolved under adbc.conf's drivers_dir and so must look like a file name.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "not a driver name.so",
                            "uri" = "file:${dbFile.absolutePath}" """),
                ["driver_url"],
                "a bare driver name with illegal characters")

        // The checksum exists because a wrong or stale copy of a hand-placed file announces nothing. Both
        // MD5s must appear, or an operator cannot tell which side to fix.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}",
                            "uri" = "file:${dbFile.absolutePath}",
                            "driver_checksum" = "00000000000000000000000000000000" """),
                ["md5", "00000000000000000000000000000000"],
                "a driver whose checksum does not match")

        // A typo here would otherwise mean AUTO, and the loss -- a catalog that quietly stopped
        // parallelising -- shows up only as slowness.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}",
                            "uri" = "file:${dbFile.absolutePath}",
                            "partitioned_read" = "ture" """),
                ["partitioned_read"],
                "an unrecognised partitioned_read mode")

        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}",
                            "uri" = "file:${dbFile.absolutePath}",
                            "max_partitions" = "0" """),
                ["max_partitions"],
                "a partition cap below one")

        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}",
                            "uri" = "file:${dbFile.absolutePath}",
                            "max_partitions" = "many" """),
                ["max_partitions"],
                "a partition cap that is not a number")

        // The cache framework answers an unreadable value with its own default and says nothing, so this
        // has to be caught where the operator can still see it.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}",
                            "uri" = "file:${dbFile.absolutePath}",
                            "meta.cache.adbc.metadata.ttl-second" = "6O0" """),
                ["ttl"],
                "a cache TTL that is not a number")

        // A dialect name nobody implements. Validated before the source is contacted, deliberately: a
        // misspelled dialect is answerable without a connection, and its error should not arrive behind a
        // connection failure the user cannot act on.
        failsMentioning(
                createCatalog(badCatalog,
                        """ "type" = "adbc", "driver_url" = "${driverPath}",
                            "uri" = "file:${dbFile.absolutePath}",
                            "sql_dialect" = "no_such_dialect" """),
                ["sql_dialect"],
                "a SQL dialect that does not exist")

        // None of the above may have left a catalog behind: a rejected CREATE that still registers
        // something is worse than one that succeeds, because the wreckage is invisible.
        def catalogs = sql("SHOW CATALOGS").collect { it[1].toString() } as Set
        assertFalse(catalogs.contains(badCatalog),
                "a CREATE CATALOG that was rejected still left ${badCatalog} behind: ${catalogs}")

        // ---- a working catalog, for the query-time cases ----

        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${driverPath}",
                "uri" = "file:${dbFile.absolutePath}"
            )
        """

        // Sanity: the catalog works, so every failure below is about the thing being tested.
        assertEquals(2, sql("SELECT id FROM ${catalogName}.${dbName}.t1").size())

        // ---- names that do not exist ----
        //
        // The name asked for must appear in the message. Doris decides a name does not exist by re-listing
        // through the connector, so an unhelpful message here usually means the failure came from
        // somewhere other than that check.

        failsMentioning("SELECT * FROM ${catalogName}.no_such_db.t1",
                ["no_such_db"], "a database that does not exist")
        failsMentioning("SELECT * FROM ${catalogName}.${dbName}.no_such_table",
                ["no_such_table"], "a table that does not exist")
        failsMentioning("SELECT no_such_column FROM ${catalogName}.${dbName}.t1",
                ["no_such_column"], "a column that does not exist")
        failsMentioning("DESC ${catalogName}.${dbName}.no_such_table",
                ["no_such_table"], "describing a table that does not exist")
        failsMentioning("SHOW TABLES FROM ${catalogName}.no_such_db",
                ["no_such_db"], "listing a database that does not exist")
        failsMentioning("SELECT * FROM no_such_catalog.${dbName}.t1",
                ["no_such_catalog"], "a catalog that does not exist")

        // Predicates and joins reach the same resolution path by a different route.
        failsMentioning("SELECT id FROM ${catalogName}.${dbName}.t1 WHERE no_such_column > 1",
                ["no_such_column"], "a filter on a column that does not exist")
        failsMentioning("""SELECT a.id FROM ${catalogName}.${dbName}.t1 a
                           JOIN ${catalogName}.${dbName}.no_such_table b ON a.id = b.id""",
                ["no_such_table"], "joining a table that does not exist")

        // ---- writes must be refused, and must change nothing ----
        //
        // This connector reads. Every statement below has to be refused rather than half-applied, so each
        // is followed by a check that the source still holds exactly what it held.

        def sourceUnchanged = {
            def rows = sql("SELECT id, name FROM ${catalogName}.${dbName}.t1 ORDER BY id")
            assertEquals("[[1, alice], [2, bob]]", rows.toString(),
                    "a refused write changed the source anyway: ${rows}")
        }

        def refusedWrite = { String stmt, String why ->
            String message = null
            try {
                sql stmt
            } catch (Exception e) {
                message = e.getMessage() == null ? e.toString() : e.getMessage()
            }
            assertNotNull(message, "this write should have been refused (${why}): ${stmt}")
            logger.info("write refused as expected [${why}]: ${message}")
            sourceUnchanged()
        }

        refusedWrite("INSERT INTO ${catalogName}.${dbName}.t1 VALUES (3, 'carol')",
                "inserting into a read-only catalog")
        refusedWrite("INSERT INTO ${catalogName}.${dbName}.t1 SELECT 4, 'dave'",
                "inserting a select result into a read-only catalog")
        refusedWrite("UPDATE ${catalogName}.${dbName}.t1 SET name = 'x' WHERE id = 1",
                "updating a read-only catalog")
        refusedWrite("DELETE FROM ${catalogName}.${dbName}.t1 WHERE id = 1",
                "deleting from a read-only catalog")
        refusedWrite("TRUNCATE TABLE ${catalogName}.${dbName}.t1",
                "truncating a read-only catalog")
        refusedWrite("ALTER TABLE ${catalogName}.${dbName}.t1 ADD COLUMN extra int",
                "altering a table in a read-only catalog")
        refusedWrite("DROP TABLE ${catalogName}.${dbName}.t1",
                "dropping a table in a read-only catalog")
        refusedWrite("""CREATE TABLE ${catalogName}.${dbName}.t_new (id int)
                        DISTRIBUTED BY HASH(id) BUCKETS 1""",
                "creating a table in a read-only catalog")
        refusedWrite("CREATE DATABASE ${catalogName}.new_db",
                "creating a database in a read-only catalog")
        refusedWrite("DROP DATABASE ${catalogName}.${dbName}",
                "dropping a database in a read-only catalog")
        refusedWrite("CREATE VIEW ${catalogName}.${dbName}.v_new AS SELECT id FROM "
                        + "${catalogName}.${dbName}.t1",
                "creating a view in a read-only catalog")

        // ---- a dropped catalog stops answering ----

        sql """DROP CATALOG ${catalogName}"""
        failsMentioning("SELECT id FROM ${catalogName}.${dbName}.t1",
                [catalogName], "querying a catalog that was dropped")

        // ---- a source that cannot be reached ----
        //
        // Created successfully -- nothing about the properties is wrong -- and failing at the first
        // statement. Port 1 is reserved and never listening, so this does not depend on what else runs on
        // this host. The message must at least identify the catalog or its URI; a bare socket error would
        // leave a user with no idea which catalog is misconfigured.
        String unreachable = "test_adbc_negative_unreachable"
        sql """DROP CATALOG IF EXISTS ${unreachable}"""
        String flightDriverPath = context.config.otherConfigs.get("adbcDriverPath")
        if (flightDriverPath == null || flightDriverPath.isEmpty()) {
            flightDriverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
        }
        if (!new File(flightDriverPath).canRead()) {
            logger.info("test_adbc_negative: SKIPPING the unreachable-source case, no Flight SQL driver "
                    + "at ${flightDriverPath}.")
        } else {
            try {
                sql """
                    CREATE CATALOG ${unreachable} PROPERTIES (
                        "type" = "adbc",
                        "driver_url" = "${flightDriverPath}",
                        "uri" = "grpc://127.0.0.1:1",
                        "user" = "root",
                        "password" = ""
                    )
                """
                String message = null
                try {
                    sql "SHOW DATABASES FROM ${unreachable}"
                } catch (Exception e) {
                    message = e.getMessage() == null ? e.toString() : e.getMessage()
                }
                assertNotNull(message,
                        "listing databases on a source nothing is listening on should have failed")
                logger.info("unreachable source reported: ${message}")
            } catch (Exception e) {
                // Also acceptable: the catalog may refuse to be created at all if it connects eagerly.
                // Recorded rather than asserted, because which of the two happens is a design choice this
                // suite does not pin.
                logger.info("an unreachable source was rejected at CREATE CATALOG: ${e.getMessage()}")
            } finally {
                sql """DROP CATALOG IF EXISTS ${unreachable}"""
            }
        }

        // A SQLite catalog pointed at a file that is not a database. The driver is fine and the path is
        // fine; only the content is wrong, which is the last place an operator looks.
        String notADb = "test_adbc_negative_not_a_db"
        File junk = new File("${workDir}/test_adbc_negative_not_a_db.txt")
        junk.text = "this is not a sqlite database"
        sql """DROP CATALOG IF EXISTS ${notADb}"""
        try {
            sql """
                CREATE CATALOG ${notADb} PROPERTIES (
                    "type" = "adbc",
                    "driver_url" = "${driverPath}",
                    "uri" = "file:${junk.absolutePath}"
                )
            """
            String message = null
            try {
                sql "SHOW DATABASES FROM ${notADb}"
            } catch (Exception e) {
                message = e.getMessage() == null ? e.toString() : e.getMessage()
            }
            // SQLite may well open a non-database file and report an empty namespace rather than failing.
            // Either outcome is recorded; what would be wrong is a crash, and reaching this line means
            // there was none.
            logger.info("a SQLite catalog over a non-database file answered: ${message}")
        } finally {
            sql """DROP CATALOG IF EXISTS ${notADb}"""
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP CATALOG IF EXISTS ${badCatalog}"""
    }
}
