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
// End-to-end ADBC scan against the SQLite driver.
//
// SQLite is not the source this connector targets, but it travels the
// identical path -- FE (Java -> JNI bridge -> C driver manager -> driver .so)
// and BE (C driver manager -> the SAME driver .so) -- and thirdparty already
// builds it, so this runs without downloading anything. The Flight SQL suite
// beside this one covers the target source and needs a driver Doris does not
// ship; run this one first, because a failure here is in the connector rather
// than in the environment.
//
// THIS SUITE HAS NEVER BEEN RUN. Every expectation is a claim to check, not a
// passing baseline.
//
// Requirements:
//   1. thirdparty built with arrow-adbc:
//      cd thirdparty && ./build-thirdparty.sh arrow_adbc
//   2. FE built and deployed FROM THIS BRANCH, so it carries the adbc
//      connector plugin, lib/libadbc_driver_jni.so, and fe.conf's
//      -Darrow.adbc.driver.jni.library.path=${DORIS_HOME}/lib
//   3. BE built from this branch, on the SAME machine as FE -- FE resolves the
//      driver to an absolute path and BE dlopens that exact path, and the
//      SQLite database is a local file both must be able to read
//   4. the sqlite3 CLI, used only to create the fixture
//
// The driver location can be overridden with adbcSqliteDriverPath in
// regression-conf.groovy; by default it is taken from thirdparty.
//
// There is deliberately NO .out baseline: one written without running would be
// a guess presented as a verified result. Generate it with -genOut on the
// first real run and read every line before committing it. The properties that
// actually matter are asserted directly rather than through qt_, because
// -genOut would happily record a wrong result for those.
// ############################################################################

suite("test_adbc_sqlite_catalog_scan", "p0,external") {

    // ---- locate the driver ----

    // suitePath is <repo>/regression-test/suites, so two levels up is the repo root.
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
        // Not a pass. Nothing about ADBC has been exercised by this run.
        logger.info("SKIPPED test_adbc_sqlite_catalog_scan: no readable ADBC SQLite driver at "
                + "${driverPath}. Build it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcSqliteDriverPath in regression-conf.groovy. "
                + "THE ADBC SCAN PATH IS NOT BEING TESTED.")
        return
    }

    // ---- build the fixture ----

    // A local file, because FE and BE both open it directly: this suite already requires them to
    // share a machine (BE dlopens the absolute driver path FE resolved), so a local path is no extra
    // constraint. Kept out of the data directory, which holds the .out baselines.
    String workDir = "${System.getProperty('java.io.tmpdir')}/doris_adbc_regression"
    new File(workDir).mkdirs()
    File dbFile = new File("${workDir}/test_adbc_sqlite_catalog_scan.db")
    File seedFile = new File("${workDir}/test_adbc_sqlite_catalog_scan.sql")
    // A database left by an earlier run would fail CREATE TABLE and, worse, could be seeded
    // differently from what is asserted below.
    dbFile.delete()

    // Every column gets at least one non-null value on purpose. The SQLite ADBC driver derives the
    // Arrow schema from the values present rather than from the declared column types, so a column
    // that is null in every row is reported as int64 -- and DESC would then be recording a type
    // mapping the driver never actually produced from this table.
    // Row 4 holds a quote on purpose: it is what gives the escaping assertion below any teeth. With
    // no such row, "name = 'O''Brien'" answers zero whether the literal was escaped correctly, was
    // never pushed at all, or matched nothing for some third reason.
    seedFile.text = """
        CREATE TABLE t1 (id INTEGER, name TEXT, score REAL);
        INSERT INTO t1 VALUES (1, 'alice', 1.5);
        INSERT INTO t1 VALUES (2, 'bob', 2.5);
        INSERT INTO t1 VALUES (3, NULL, 3.5);
        INSERT INTO t1 VALUES (4, 'O''Brien', 4.5);
        CREATE TABLE t2 (a INTEGER);
        INSERT INTO t2 VALUES (10);
        CREATE VIEW v1 AS SELECT * FROM t1;
    """

    def seed = new ProcessBuilder("/bin/bash", "-c",
            "sqlite3 '${dbFile.absolutePath}' < '${seedFile.absolutePath}'")
            .redirectErrorStream(true).start()
    String seedOutput = seed.inputStream.text
    int seedExit = seed.waitFor()
    if (seedExit != 0 || !dbFile.exists()) {
        logger.info("SKIPPED test_adbc_sqlite_catalog_scan: could not create the SQLite fixture "
                + "(exit ${seedExit}): ${seedOutput}. The sqlite3 CLI is needed to build it. "
                + "THE ADBC SCAN PATH IS NOT BEING TESTED.")
        return
    }
    logger.info("adbc sqlite fixture at ${dbFile.absolutePath}, driver ${driverPath}")

    String catalogName = "test_adbc_sqlite_catalog_scan_catalog"
    // SQLite reports catalog "main" and an empty db_schema, so the Doris database name is "main".
    String dbName = "main"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "file:${dbFile.absolutePath}"
        )
    """

    try {
        // ---- metadata ----

        def databases = sql """SHOW DATABASES FROM ${catalogName}"""
        assertTrue(databases.any { it[0] == dbName },
                "SHOW DATABASES did not report the SQLite namespace as 'main': ${databases}")

        // Views are excluded: the connector asks the source for base tables only, so v1 must not be
        // here even though it exists in the fixture.
        def tables = sql """SHOW TABLES FROM ${catalogName}.${dbName}"""
        def tableNames = tables.collect { it[0] } as Set
        assertTrue(tableNames.contains("t1"), "t1 missing from ${tableNames}")
        assertTrue(tableNames.contains("t2"), "t2 missing from ${tableNames}")
        assertFalse(tableNames.contains("v1"), "the view v1 was surfaced as a table: ${tableNames}")

        qt_desc_t1 """DESC ${catalogName}.${dbName}.t1"""

        // ---- reading ----

        def allRows = sql """SELECT id, name, score FROM ${catalogName}.${dbName}.t1 ORDER BY id"""
        assertEquals(4, allRows.size())
        assertEquals(3, allRows[0].size())
        assertEquals("alice", allRows[0][1])
        assertEquals(null, allRows[2][1])
        assertEquals("O'Brien", allRows[3][1])

        // A projection. BE rejects any column the statement did not ask for, so this fails outright
        // rather than over-reading if FE ever widens the select list.
        def projected = sql """SELECT id FROM ${catalogName}.${dbName}.t1 ORDER BY id"""
        assertEquals(4, projected.size())
        assertEquals(1, projected[0].size())

        // ---- predicates in the pushable set ----
        // The answer must be the same whether or not they were pushed, since BE re-applies them.

        assertEquals(3, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id > 1""").size())

        // Load-bearing beyond the row count, and the query that found a real defect on the first
        // run. The projection is id, but BE re-evaluates the predicate, so name is a query slot too
        // and FE selects it -- and the one surviving row has name NULL, so the source returns a
        // column with no non-null value in it. A source that infers Arrow types from values then
        // reports it as int64 rather than as text (measured: the SQLite driver does exactly this),
        // and BE used to fail with "Unsupported arrow type for string column: 9". FE cannot avoid
        // it: it cannot know which rows a filter will leave.
        assertEquals(1, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name IS NULL""").size())
        assertEquals(3, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name IS NOT NULL""").size())
        assertEquals(2, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id IN (1, 3)""").size())
        assertEquals(1, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name = 'alice'""").size())
        assertEquals(1, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE score < 2.0""").size())
        assertEquals(2, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id > 1 AND name IS NOT NULL""").size())

        // A quote inside a string literal has to survive to the source rather than end the literal.
        // Row 4 is the one that makes this mean something: a wrong escaping either fails the remote
        // statement outright or matches nothing, and both differ from the one row expected here.
        assertEquals(1, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name = 'O''Brien'""").size())

        // ---- predicates outside it ----

        // LIKE is not pushed; it stays in Doris and the rows must still be right.
        assertEquals(1, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name LIKE 'a%'""").size())

        // THE row-count case. If the limit were pushed alongside a predicate that stayed in Doris,
        // the source would truncate first and Doris would filter what little came back, answering
        // with too few rows. One row matches, and one row must come back.
        assertEquals(1,
                sql("""SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name LIKE 'b%' LIMIT 5""").size())

        // ---- limit and count ----

        assertEquals(2, sql("""SELECT id FROM ${catalogName}.${dbName}.t1 ORDER BY id LIMIT 2""").size())

        // COUNT(*) projects no columns at all; BE counts rows without materializing any.
        def counted = sql """SELECT count(*) FROM ${catalogName}.${dbName}.t1"""
        assertEquals(1, counted.size())
        assertEquals(4L, counted[0][0])

        assertEquals(1L, sql("""SELECT count(*) FROM ${catalogName}.${dbName}.t2""")[0][0])

        // ---- what was actually sent to the source ----
        //
        // Asserted on the QUERY line alone rather than on the whole plan: the plan text carries
        // Doris's own limit/predicate lines, so a substring match over all of it would answer a
        // question about the remote statement with something Doris printed about itself.
        // ExplainAction runs EITHER check{} OR contains/notContains, never both -- adding a
        // contains() beside a check() silently disables it.

        def remoteQuery = { String plan ->
            String line = plan.readLines().find { it.trim().startsWith("QUERY: ") }
            assertNotNull(line, "no QUERY line in the plan:\n${plan}")
            return line.trim().substring("QUERY: ".length())
        }

        // The projection is explicit, never a star: BE rejects a column the query did not request,
        // so this is the difference between a scan that works and one that fails.
        explain {
            sql("SELECT id FROM ${catalogName}.${dbName}.t1")
            check { String plan ->
                String query = remoteQuery.call(plan)
                assertFalse(query.contains("*"), "the projection was widened to a star: ${query}")
                assertTrue(query.contains("\"id\""), query)
                return true
            }
        }

        // A pushable predicate reaches the source. If this fails while the row counts above passed,
        // look first at whether the planner wrapped the comparison in a CAST: this connector
        // declines CAST predicates, so such a conjunct never reaches the query builder.
        explain {
            sql("SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id > 1")
            check { String plan ->
                String query = remoteQuery.call(plan)
                assertTrue(query.contains("\"id\" > 1"), "predicate not pushed: ${query}")
                return true
            }
        }

        // LIKE is outside the pushable set, so it must not reach the source.
        explain {
            sql("SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name LIKE 'a%'")
            check { String plan ->
                String query = remoteQuery.call(plan)
                assertFalse(query.contains("LIKE"), "LIKE was pushed: ${query}")
                return true
            }
        }

        // NOTHING here asserts the row limit, deliberately. EXPLAIN regenerates the statement with no
        // limit at all -- it has none to show, and the JDBC connector does the same -- so a
        // "LIMIT is absent" assertion would hold for a reason unrelated to what it claims to test,
        // and a "LIMIT is present" one could never hold. The limit rule that matters (a limit is
        // pushed only when every predicate was, or the source truncates before Doris filters) is
        // covered above by the row count of the LIKE-plus-LIMIT query.
        // ---- partitioned_read=required must fail on a driver that has no partitions ----
        //
        // SQLite genuinely answers NOT_IMPLEMENTED to executePartitioned, which makes it the only
        // driver here that can prove the mode does what it says. The sibling Flight SQL suite pins
        // the other half: it sets 'required' on a driver that DOES partition and expects success, so
        // a downgrade there fails the run instead of quietly testing the fallback.
        String strictCatalog = "${catalogName}_strict"
        sql """DROP CATALOG IF EXISTS ${strictCatalog}"""
        sql """
            CREATE CATALOG ${strictCatalog} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${driverPath}",
                "uri" = "file:${dbFile.absolutePath}",
                "partitioned_read" = "required"
            )
        """
        try {
            test {
                sql "SELECT id FROM ${strictCatalog}.${dbName}.t1"
                // The message has to name the property and carry the driver's own answer, or a user
                // cannot tell a driver that lacks the feature from one that failed for another reason.
                exception "partitioned_read"
            }
        } finally {
            sql """DROP CATALOG IF EXISTS ${strictCatalog}"""
        }

        // ---- what the catalog remembers, and what forgets it ----
        //
        // A table created behind Doris's back has to be reachable without a REFRESH: the engine
        // decides a name does not exist by re-listing through the connector, and a listing served
        // from the connector's cache would turn that last check into a formality. This is the
        // end-to-end half of that rule; which layer holds which listing is covered by the unit tests.
        def sqliteExec = { String statements ->
            File script = new File("${workDir}/test_adbc_sqlite_refresh.sql")
            script.text = statements
            def run = new ProcessBuilder("/bin/bash", "-c",
                    "sqlite3 '${dbFile.absolutePath}' < '${script.absolutePath}'")
                    .redirectErrorStream(true).start()
            String out = run.inputStream.text
            assertEquals(0, run.waitFor(), "sqlite3 failed: ${out}")
        }

        sql """SHOW TABLES FROM ${catalogName}.${dbName}"""
        sqliteExec("CREATE TABLE t_created_later (id INTEGER); INSERT INTO t_created_later VALUES (7);")

        // Queried, not listed. SHOW TABLES is served from the engine's own name cache and is expected
        // to lag; resolving the name is what must not, and it is the engine's re-list -- through the
        // connector, which therefore may not answer it from memory -- that makes this pass.
        assertEquals("[[7]]",
                sql("""SELECT id FROM ${catalogName}.${dbName}.t_created_later""").toString(),
                "a table created after the catalog was first listed could not be queried")

        // REFRESH CATALOG is the only thing that drops what the connector remembers -- it does NOT
        // rebuild the connector -- so without the hook it reaches nothing and a schema read once
        // stays read forever. Asserted through a schema change, because that is what the engine
        // cannot re-derive on its own: it caches the schema too, and clears its copy either way.
        sqliteExec("ALTER TABLE t_created_later ADD COLUMN extra TEXT;"
                + " UPDATE t_created_later SET extra = 'x';")
        sql """REFRESH CATALOG ${catalogName}"""
        def refreshedColumns = sql("""DESC ${catalogName}.${dbName}.t_created_later""")
                .collect { it[0] } as Set
        assertTrue(refreshedColumns.contains("extra"),
                "REFRESH CATALOG did not reach the connector's schema cache: ${refreshedColumns}")

        // The escape hatch has to actually be spelled that way, and an unreadable value has to fail
        // where the operator can still see it -- the cache framework otherwise answers a typo with
        // its own default and says nothing. A plugin that predates these properties ignores both,
        // which fails the second half here rather than passing quietly.
        String noCacheCatalog = "${catalogName}_nocache"
        sql """DROP CATALOG IF EXISTS ${noCacheCatalog}"""
        sql """
            CREATE CATALOG ${noCacheCatalog} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${driverPath}",
                "uri" = "file:${dbFile.absolutePath}",
                "meta.cache.adbc.metadata.enable" = "false"
            )
        """
        try {
            assertEquals("[[4]]",
                    sql("""SELECT count(*) FROM ${noCacheCatalog}.${dbName}.t1""").toString(),
                    "a catalog with the metadata cache turned off must still work")
        } finally {
            sql """DROP CATALOG IF EXISTS ${noCacheCatalog}"""
        }

        test {
            sql """
                CREATE CATALOG ${noCacheCatalog} PROPERTIES (
                    "type" = "adbc",
                    "driver_url" = "${driverPath}",
                    "uri" = "file:${dbFile.absolutePath}",
                    "meta.cache.adbc.metadata.ttl-second" = "6O0"
                )
            """
            exception "meta.cache.adbc.metadata.ttl-second"
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
