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
// Reading a Doris that is itself reading something else -- an ADBC catalog
// whose source is a Doris whose tables are, in turn, an ADBC catalog over
// SQLite.
//
// One cluster stands in for two here. Doris's Flight SQL endpoint reports
// EVERY catalog it has, not just internal (FlightSqlSchemaHelper.getCatalogs
// adds every entry of the catalog manager, and getSchemas carries a catalog
// name per database), so a Doris pointed at itself sees its own external
// catalogs exactly the way it would see another cluster's. That makes this
// chain reachable without a second deployment.
//
// WHY THIS IS WORTH TESTING, and what it is likely to find: an ADBC catalog
// flattens the source's three-level catalog/db_schema/table namespace onto
// Doris's two levels, and the design makes that safe by REQUIRING the uri to
// pin exactly one remote catalog. A Doris source breaks that premise -- it
// reports many catalogs, and checkRemoteCatalogIsPinned only verifies that
// getCurrentCatalog() answered something non-empty ("internal"), which it
// does. So the catalog is created, databases from every remote catalog show up
// in SHOW DATABASES, and the two-part name the query builder emits
// (`schema`.`table`) is resolved by the source against ITS current catalog.
//
// The assertions are written accordingly. Whether the chained read works is
// NOT asserted -- that is the open question this suite exists to answer. What
// IS asserted is the invariant that must hold either way: it must never
// quietly return SOMEBODY ELSE'S ROWS. The last section builds the collision
// on purpose, with two SQLite catalogs that both call their namespace "main".
//
// Requires both drivers -- see the headers of test_adbc_sqlite_catalog_scan
// and test_adbc_catalog_scan.
// ############################################################################

suite("test_adbc_nested_catalog", "p0,external") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String sqliteDriverPath = context.config.otherConfigs.get("adbcSqliteDriverPath")
    if (sqliteDriverPath == null || sqliteDriverPath.isEmpty()) {
        sqliteDriverPath = "${thirdparty}/installed/lib64/libadbc_driver_sqlite.so"
    }
    String flightDriverPath = context.config.otherConfigs.get("adbcDriverPath")
    if (flightDriverPath == null || flightDriverPath.isEmpty()) {
        flightDriverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }

    if (!new File(sqliteDriverPath).canRead() || !new File(flightDriverPath).canRead()) {
        logger.info("SKIPPED test_adbc_nested_catalog: this suite needs BOTH ADBC drivers -- SQLite at "
                + "${sqliteDriverPath} and Flight SQL at ${flightDriverPath}. Install them with "
                + "'cd thirdparty && ./build-thirdparty.sh arrow_adbc'. "
                + "READING A DORIS THROUGH ITS OWN EXTERNAL CATALOGS IS NOT BEING TESTED.")
        return
    }

    // ---- two SQLite files, so a name collision can be built on purpose ----

    String workDir = "${System.getProperty('java.io.tmpdir')}/doris_adbc_regression"
    new File(workDir).mkdirs()
    File dbFileA = new File("${workDir}/test_adbc_nested_a.db")
    File dbFileB = new File("${workDir}/test_adbc_nested_b.db")
    File seedFile = new File("${workDir}/test_adbc_nested.sql")
    dbFileA.delete()
    dbFileB.delete()

    def seedSqlite = { File target, String statements ->
        seedFile.text = statements
        def run = new ProcessBuilder("/bin/bash", "-c",
                "sqlite3 '${target.absolutePath}' < '${seedFile.absolutePath}'")
                .redirectErrorStream(true).start()
        String out = run.inputStream.text
        int code = run.waitFor()
        return code == 0 && target.exists() ? null : "exit ${code}: ${out}"
    }

    // shared_t exists in BOTH files with DIFFERENT contents. That is the collision: each SQLite catalog
    // calls its namespace "main", so a Doris reading both of them through one ADBC catalog sees two
    // databases with one name.
    String failureA = seedSqlite(dbFileA, """
        CREATE TABLE chain_a (id INTEGER, note TEXT);
        INSERT INTO chain_a VALUES (1, 'only-in-a');
        INSERT INTO chain_a VALUES (2, 'also-only-in-a');
        CREATE TABLE shared_t (id INTEGER, origin TEXT);
        INSERT INTO shared_t VALUES (1, 'from-a');
    """)
    String failureB = seedSqlite(dbFileB, """
        CREATE TABLE chain_b (id INTEGER, note TEXT);
        INSERT INTO chain_b VALUES (9, 'only-in-b');
        CREATE TABLE shared_t (id INTEGER, origin TEXT);
        INSERT INTO shared_t VALUES (7, 'from-b');
        INSERT INTO shared_t VALUES (8, 'from-b');
    """)
    if (failureA != null || failureB != null) {
        logger.info("SKIPPED test_adbc_nested_catalog: could not build the SQLite fixtures "
                + "(${failureA}, ${failureB}). The sqlite3 CLI is needed. "
                + "READING A DORIS THROUGH ITS OWN EXTERNAL CATALOGS IS NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String innerA = "test_adbc_nested_inner_a"
    String innerB = "test_adbc_nested_inner_b"
    String outerCatalog = "test_adbc_nested_outer"
    String dbName = "test_adbc_nested_db"
    String sqliteDb = "main"

    sql """DROP CATALOG IF EXISTS ${outerCatalog}"""
    sql """DROP CATALOG IF EXISTS ${innerA}"""
    sql """DROP CATALOG IF EXISTS ${innerB}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.plain (
          `id` int NOT NULL,
          `name` varchar(64) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO internal.${dbName}.plain VALUES (1, 'internal-row'), (2, 'another')"""

    // The inner catalogs are created BEFORE the outer one, so the outer catalog's first listing already
    // sees them -- its namespace list is cached, and a catalog that appeared afterwards would be a
    // different test (that one is covered in test_adbc_metadata_ops).
    sql """
        CREATE CATALOG ${innerA} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${sqliteDriverPath}",
            "uri" = "file:${dbFileA.absolutePath}"
        )
    """

    try {
        // The inner catalog works on its own. Everything below is about reaching it through another hop,
        // so if this fails nothing further would mean anything.
        def innerRows = sql("SELECT id, note FROM ${innerA}.${sqliteDb}.chain_a ORDER BY id")
        assertEquals("[[1, only-in-a], [2, also-only-in-a]]", innerRows.toString(),
                "the inner SQLite catalog does not work, so the chained reads below are untestable")

        sql """
            CREATE CATALOG ${outerCatalog} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${flightDriverPath}",
                "uri" = "grpc://127.0.0.1:${arrowPort}",
                "user" = "root",
                "password" = "",
                "partitioned_read" = "required"
            )
        """

        // ---- the baseline hop: outer -> this Doris -> an ordinary internal table ----
        //
        // One level of indirection, no chaining. It must work, and it is what separates "the chain is
        // broken" from "the outer catalog is broken".

        assertEquals(
                sql("SELECT id, name FROM internal.${dbName}.plain ORDER BY id").toString(),
                sql("SELECT id, name FROM ${outerCatalog}.${dbName}.plain ORDER BY id").toString(),
                "the outer ADBC catalog cannot even read an ordinary internal table")
        qt_outer_reads_internal """
            SELECT id, name FROM ${outerCatalog}.${dbName}.plain ORDER BY id
        """

        // ---- what the outer catalog sees of the inner one ----

        def outerDatabases = sql("SHOW DATABASES FROM ${outerCatalog}").collect { it[0].toString() }
        logger.info("the outer ADBC catalog lists these databases: ${outerDatabases}")
        assertTrue(outerDatabases.contains(dbName),
                "the outer catalog cannot see the internal database it is supposed to read: "
                        + "${outerDatabases}")
        // Recorded rather than required: whether a source's OTHER catalogs surface as databases here is
        // precisely the question. Both answers are informative and neither is asserted.
        boolean sqliteNamespaceVisible = outerDatabases.contains(sqliteDb)
        logger.info("the inner SQLite catalog's namespace '${sqliteDb}' is "
                + (sqliteNamespaceVisible ? "VISIBLE" : "NOT visible")
                + " through the outer ADBC catalog")

        // ---- the chained read ----
        //
        // THE INVARIANT, and the only thing asserted: if rows come back they must be the inner catalog's
        // rows. Failing is acceptable -- the two-level flattening cannot address a table in a non-current
        // remote catalog, and saying so is a legitimate outcome. Returning something else is not.

        def chainedRead = { String table, String expected ->
            def rows = null
            String message = null
            try {
                rows = sql("SELECT id, note FROM ${outerCatalog}.${sqliteDb}.${table} ORDER BY id")
            } catch (Exception e) {
                message = e.getMessage() == null ? e.toString() : e.getMessage()
            }
            if (rows == null) {
                logger.info("a chained read of ${table} failed, which is an acceptable outcome for a "
                        + "two-level namespace addressing a non-current remote catalog: ${message}")
                assertTrue(message.toLowerCase().contains(table)
                                || message.toLowerCase().contains(sqliteDb)
                                || message.toLowerCase().contains(outerCatalog.toLowerCase()),
                        "the chained read failed without naming the table, the database or the catalog, "
                                + "so a user has nothing to act on: ${message}")
                return false
            }
            logger.info("a chained read of ${table} SUCCEEDED and returned: ${rows}")
            assertEquals(expected, rows.toString(),
                    "a chained read returned rows that are NOT the inner catalog's -- this is the failure "
                            + "that must never happen, because it is indistinguishable from a correct "
                            + "answer at the SQL layer")
            return true
        }

        boolean chainWorks = chainedRead("chain_a", "[[1, only-in-a], [2, also-only-in-a]]")

        if (chainWorks) {
            // Only meaningful if the chain resolves at all: the same table read directly and through the
            // chain must agree, and a join across the two hops must not double or drop anything.
            assertEquals(
                    sql("SELECT id, note FROM ${innerA}.${sqliteDb}.chain_a ORDER BY id").toString(),
                    sql("SELECT id, note FROM ${outerCatalog}.${sqliteDb}.chain_a ORDER BY id").toString(),
                    "the same SQLite table disagreed when read directly and when read through a Doris")
            qt_chained_read """
                SELECT id, note FROM ${outerCatalog}.${sqliteDb}.chain_a ORDER BY id
            """
            qt_chained_join """
                SELECT a.id, a.note, b.note
                FROM ${innerA}.${sqliteDb}.chain_a a
                JOIN ${outerCatalog}.${sqliteDb}.chain_a b ON a.id = b.id
                ORDER BY a.id
            """
        }

        // ---- the collision ----
        //
        // A second SQLite catalog, whose namespace is ALSO called "main". The outer catalog now sees two
        // remote namespaces that flatten to one Doris database name. Whatever it does with that, it must
        // not blend them: the rows it returns for shared_t have to be exactly one file's rows.

        sql """
            CREATE CATALOG ${innerB} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${sqliteDriverPath}",
                "uri" = "file:${dbFileB.absolutePath}"
            )
        """
        try {
            String fromA = sql("SELECT id, origin FROM ${innerA}.${sqliteDb}.shared_t ORDER BY id")
                    .toString()
            String fromB = sql("SELECT id, origin FROM ${innerB}.${sqliteDb}.shared_t ORDER BY id")
                    .toString()
            assertEquals("[[1, from-a]]", fromA, "fixture A is not what this suite seeded")
            assertEquals("[[7, from-b], [8, from-b]]", fromB, "fixture B is not what this suite seeded")

            // The outer catalog is refreshed so it re-lists and can see the catalog that appeared after it
            // was created; without this the collision would not even be reachable.
            sql """REFRESH CATALOG ${outerCatalog}"""

            def collided = null
            String collidedMessage = null
            try {
                collided = sql("SELECT id, origin FROM ${outerCatalog}.${sqliteDb}.shared_t ORDER BY id")
                        .toString()
            } catch (Exception e) {
                collidedMessage = e.getMessage() == null ? e.toString() : e.getMessage()
            }

            if (collided == null) {
                logger.info("with two remote catalogs sharing the namespace name '${sqliteDb}', the "
                        + "chained read failed: ${collidedMessage}. That is an acceptable outcome; "
                        + "silently picking one of them without saying so would not be.")
            } else {
                logger.info("with two remote catalogs sharing the namespace name '${sqliteDb}', the "
                        + "chained read returned: ${collided}")
                assertTrue(collided == fromA || collided == fromB,
                        "a query against a database name that two remote catalogs both claim returned "
                                + "neither catalog's rows -- ${collided} is a BLEND of ${fromA} and "
                                + "${fromB}, which no user could detect from the SQL layer")
            }

            // The same question from the other side: a table that exists ONLY in the second file. If the
            // outer catalog resolved the shared namespace to the first file, this must not be found;
            // if it resolved to the second, it must return exactly that file's row. What it must never
            // do is return chain_a's rows under chain_b's name.
            def onlyInB = null
            try {
                onlyInB = sql("SELECT id, note FROM ${outerCatalog}.${sqliteDb}.chain_b ORDER BY id")
                        .toString()
            } catch (Exception e) {
                logger.info("chain_b, which exists only in the second SQLite file, was not reachable "
                        + "through the chained namespace: ${e.getMessage()}")
            }
            if (onlyInB != null) {
                assertEquals("[[9, only-in-b]]", onlyInB,
                        "a table that exists only in the second remote catalog came back with different "
                                + "rows, so the namespace collision resolved to the wrong source")
            }
        } finally {
            sql """DROP CATALOG IF EXISTS ${innerB}"""
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${outerCatalog}"""
        sql """DROP CATALOG IF EXISTS ${innerA}"""
        sql """DROP CATALOG IF EXISTS ${innerB}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
