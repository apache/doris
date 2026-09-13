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
// Every way of asking Doris what is in an ADBC catalog, and every way of
// telling it to forget.
//
// SHOW TABLES is not the only route to a table list, and it is not the one
// that has caused trouble: information_schema.tables and SHOW TABLE STATUS
// reach the connector through FrontendServiceImpl, which is where a view once
// leaked out as a table. A leaked view is the failure that hides -- it reads
// fine through ADBC, so a catalog offering one looks entirely healthy -- and
// only a source that ignores the base-table filter can expose it. A Doris
// source is exactly that source: its Flight SQL endpoint recognises the
// literal "VIEW" and answers anything else with every object it has, so the
// filtering happens on the read side and needs a live test.
//
// The invalidation half needs the opposite: a source that can be changed
// behind Doris's back. SQLite gets that for free through the sqlite3 CLI, so
// the REFRESH assertions run there. Which layer holds which listing is a unit
// test's job; what only an end-to-end run can show is that the name fe-core
// hands the connector is the name the remote side knows -- "the remote name
// equals the Doris name" is an ASSUMPTION everywhere else.
//
// SQLite is required; the Flight SQL section is skipped on its own with a log.
// ############################################################################

suite("test_adbc_metadata_ops", "p0,external") {
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

    if (!new File(sqliteDriverPath).canRead()) {
        logger.info("SKIPPED test_adbc_metadata_ops: no readable ADBC SQLite driver at "
                + "${sqliteDriverPath}. Build it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc'. "
                + "ADBC METADATA AND REFRESH ARE NOT BEING TESTED.")
        return
    }
    boolean hasFlight = new File(flightDriverPath).canRead()

    String workDir = "${System.getProperty('java.io.tmpdir')}/doris_adbc_regression"
    new File(workDir).mkdirs()
    File dbFile = new File("${workDir}/test_adbc_metadata_ops.db")
    File seedFile = new File("${workDir}/test_adbc_metadata_ops.sql")
    dbFile.delete()
    // Every column gets a non-null value: the SQLite driver derives Arrow types from the values present,
    // so an all-null column would be reported as int64 and DESC would record a mapping this table never
    // actually produced.
    seedFile.text = """
        CREATE TABLE meta_a (id INTEGER, name TEXT, score REAL);
        INSERT INTO meta_a VALUES (1, 'alice', 1.5);
        INSERT INTO meta_a VALUES (2, 'bob', 2.5);
        CREATE TABLE meta_b (k TEXT, v INTEGER);
        INSERT INTO meta_b VALUES ('x', 10);
        CREATE VIEW meta_view AS SELECT id, name FROM meta_a;
    """
    def seed = new ProcessBuilder("/bin/bash", "-c",
            "sqlite3 '${dbFile.absolutePath}' < '${seedFile.absolutePath}'")
            .redirectErrorStream(true).start()
    String seedOutput = seed.inputStream.text
    if (seed.waitFor() != 0 || !dbFile.exists()) {
        logger.info("SKIPPED test_adbc_metadata_ops: could not create the SQLite fixture: ${seedOutput}. "
                + "ADBC METADATA AND REFRESH ARE NOT BEING TESTED.")
        return
    }

    def sqliteExec = { String statements ->
        File script = new File("${workDir}/test_adbc_metadata_ops_exec.sql")
        script.text = statements
        def run = new ProcessBuilder("/bin/bash", "-c",
                "sqlite3 '${dbFile.absolutePath}' < '${script.absolutePath}'")
                .redirectErrorStream(true).start()
        String out = run.inputStream.text
        assertEquals(0, run.waitFor(), "sqlite3 failed: ${out}")
    }

    String catalogName = "test_adbc_metadata_ops_catalog"
    String renamedCatalog = "test_adbc_metadata_ops_renamed"
    String flightCatalog = "test_adbc_metadata_ops_flight"
    String flightDb = "test_adbc_metadata_ops_db"
    String sqliteDb = "main"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP CATALOG IF EXISTS ${renamedCatalog}"""
    sql """DROP CATALOG IF EXISTS ${flightCatalog}"""

    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${sqliteDriverPath}",
            "uri" = "file:${dbFile.absolutePath}"
        )
    """

    try {
        // ---- listing, through each route ----

        def catalogNames = sql("SHOW CATALOGS").collect { it[1].toString() } as Set
        assertTrue(catalogNames.contains(catalogName),
                "the catalog is missing from SHOW CATALOGS: ${catalogNames}")

        qt_show_create_catalog """SHOW CREATE CATALOG ${catalogName}"""

        qt_show_databases """SHOW DATABASES FROM ${catalogName}"""
        qt_show_tables """SHOW TABLES FROM ${catalogName}.${sqliteDb}"""
        qt_show_tables_like """SHOW TABLES FROM ${catalogName}.${sqliteDb} LIKE 'meta_%'"""
        qt_desc_meta_a """DESC ${catalogName}.${sqliteDb}.meta_a"""

        // SHOW TABLE STATUS goes through FrontendServiceImpl rather than through the same path as
        // SHOW TABLES, which is the whole reason it is here.
        def statusRows = sql("SHOW TABLE STATUS FROM ${catalogName}.${sqliteDb}")
        def statusNames = statusRows.collect { it[0].toString() } as Set
        assertTrue(statusNames.contains("meta_a"), "meta_a missing from SHOW TABLE STATUS: ${statusNames}")
        assertFalse(statusNames.contains("meta_view"),
                "the view leaked out through SHOW TABLE STATUS: ${statusNames}")
        logger.info("SHOW TABLE STATUS reported: ${statusNames}")

        // A third route. information_schema is per-catalog, so it is addressed through the catalog name.
        def infoTables = sql("""
            SELECT TABLE_NAME, TABLE_TYPE FROM ${catalogName}.information_schema.tables
            WHERE TABLE_SCHEMA = '${sqliteDb}' ORDER BY TABLE_NAME
        """)
        def infoNames = infoTables.collect { it[0].toString() } as Set
        assertTrue(infoNames.contains("meta_a"),
                "meta_a missing from information_schema.tables: ${infoTables}")
        assertFalse(infoNames.contains("meta_view"),
                "the view leaked out through information_schema.tables: ${infoTables}")
        qt_info_tables """
            SELECT TABLE_NAME, TABLE_TYPE FROM ${catalogName}.information_schema.tables
            WHERE TABLE_SCHEMA = '${sqliteDb}' ORDER BY TABLE_NAME
        """
        qt_info_columns """
            SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE
            FROM ${catalogName}.information_schema.columns
            WHERE TABLE_SCHEMA = '${sqliteDb}' AND TABLE_NAME = 'meta_a'
            ORDER BY ORDINAL_POSITION
        """

        // ---- switching into the catalog ----
        //
        // An unqualified query resolves through the session's current catalog and database, which is a
        // different name-resolution path from the fully qualified one used everywhere else.

        sql """SWITCH ${catalogName}"""
        try {
            sql """USE ${sqliteDb}"""
            qt_unqualified """SELECT id, name FROM meta_a ORDER BY id"""
            assertEquals(2, sql("SELECT id FROM meta_a").size())
            qt_current """SELECT database()"""
        } finally {
            sql """SWITCH internal"""
        }

        // ---- REFRESH, at each level ----
        //
        // The connector remembers a table's schema per catalog. REFRESH is the only thing that drops it --
        // it does NOT rebuild the connector -- so each level below is asserted through a SCHEMA change,
        // which is what Doris cannot re-derive on its own.

        // REFRESH TABLE. fe-core hands the connector a REMOTE name here; this is the only place the
        // assumption that it equals the Doris name is actually exercised.
        sql """DESC ${catalogName}.${sqliteDb}.meta_a"""
        sqliteExec("ALTER TABLE meta_a ADD COLUMN added_by_refresh_table TEXT;"
                + " UPDATE meta_a SET added_by_refresh_table = 'x';")
        sql """REFRESH TABLE ${catalogName}.${sqliteDb}.meta_a"""
        def afterTableRefresh = sql("DESC ${catalogName}.${sqliteDb}.meta_a").collect { it[0] } as Set
        assertTrue(afterTableRefresh.contains("added_by_refresh_table"),
                "REFRESH TABLE did not reach the connector's schema cache: ${afterTableRefresh}")
        // And the new column is readable, not merely described.
        qt_after_refresh_table """
            SELECT id, added_by_refresh_table FROM ${catalogName}.${sqliteDb}.meta_a ORDER BY id
        """

        // REFRESH DATABASE, which must reach every table under it.
        sql """DESC ${catalogName}.${sqliteDb}.meta_b"""
        sqliteExec("ALTER TABLE meta_b ADD COLUMN added_by_refresh_db TEXT;"
                + " UPDATE meta_b SET added_by_refresh_db = 'y';")
        sql """REFRESH DATABASE ${catalogName}.${sqliteDb}"""
        def afterDbRefresh = sql("DESC ${catalogName}.${sqliteDb}.meta_b").collect { it[0] } as Set
        assertTrue(afterDbRefresh.contains("added_by_refresh_db"),
                "REFRESH DATABASE did not reach the connector's schema cache: ${afterDbRefresh}")

        // REFRESH CATALOG, the coarsest level.
        sqliteExec("ALTER TABLE meta_b ADD COLUMN added_by_refresh_catalog TEXT;"
                + " UPDATE meta_b SET added_by_refresh_catalog = 'z';")
        sql """REFRESH CATALOG ${catalogName}"""
        def afterCatalogRefresh = sql("DESC ${catalogName}.${sqliteDb}.meta_b").collect { it[0] } as Set
        assertTrue(afterCatalogRefresh.contains("added_by_refresh_catalog"),
                "REFRESH CATALOG did not reach the connector's schema cache: ${afterCatalogRefresh}")

        // A table created behind Doris's back must be queryable without any REFRESH at all: the engine
        // decides a name does not exist by re-listing through the connector, and a listing served from the
        // connector's own cache would turn that last check into a formality.
        sqliteExec("CREATE TABLE meta_created_later (id INTEGER); INSERT INTO meta_created_later VALUES (7);")
        assertEquals("[[7]]",
                sql("SELECT id FROM ${catalogName}.${sqliteDb}.meta_created_later").toString(),
                "a table created after the catalog was first listed could not be queried")

        // A table dropped behind Doris's back must stop resolving rather than answer from memory.
        sqliteExec("DROP TABLE meta_created_later;")
        sql """REFRESH CATALOG ${catalogName}"""
        String droppedMessage = null
        try {
            sql "SELECT id FROM ${catalogName}.${sqliteDb}.meta_created_later"
        } catch (Exception e) {
            droppedMessage = e.getMessage()
        }
        assertNotNull(droppedMessage,
                "a table dropped on the source and then refreshed still answered queries")
        logger.info("a dropped table reported: ${droppedMessage}")

        // ---- SHOW CREATE TABLE ----
        //
        // Late in the suite on purpose: it is the statement least likely to be wired up for a plugin
        // catalog, and a gap there must not stop everything above from being checked.
        qt_show_create_table """SHOW CREATE TABLE ${catalogName}.${sqliteDb}.meta_a"""

        // ---- renaming the catalog ----
        //
        // The connector is keyed by catalog, so a rename that left a stale key behind would keep answering
        // under the old name or stop answering under the new one.
        sql """ALTER CATALOG ${catalogName} RENAME ${renamedCatalog}"""
        try {
            assertEquals(2, sql("SELECT id FROM ${renamedCatalog}.${sqliteDb}.meta_a").size(),
                    "the catalog stopped working after being renamed")
            String oldNameMessage = null
            try {
                sql "SELECT id FROM ${catalogName}.${sqliteDb}.meta_a"
            } catch (Exception e) {
                oldNameMessage = e.getMessage()
            }
            assertNotNull(oldNameMessage, "the catalog still answers under its old name after a rename")
        } finally {
            sql """ALTER CATALOG ${renamedCatalog} RENAME ${catalogName}"""
        }

        // ---- the same routes against a source that does NOT filter views itself ----

        if (!hasFlight) {
            logger.info("test_adbc_metadata_ops: SKIPPING the Flight SQL section, no driver at "
                    + "${flightDriverPath}. VIEW EXCLUSION AGAINST A SOURCE THAT IGNORES THE BASE-TABLE "
                    + "FILTER IS NOT BEING TESTED -- and that is the source the filter exists for.")
        } else {
            def frontends = sql "show frontends"
            String arrowPort = frontends[0][6]

            sql """DROP DATABASE IF EXISTS internal.${flightDb} FORCE"""
            sql """CREATE DATABASE internal.${flightDb}"""
            sql """
                CREATE TABLE internal.${flightDb}.base_table (
                  `id` int NOT NULL,
                  `name` varchar(64) NULL
                ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            sql """INSERT INTO internal.${flightDb}.base_table VALUES (1, 'a'), (2, 'b')"""
            // Exists only to be absent below.
            sql """CREATE VIEW internal.${flightDb}.leaky_view AS SELECT id FROM internal.${flightDb}.base_table"""

            sql """
                CREATE CATALOG ${flightCatalog} PROPERTIES (
                    "type" = "adbc",
                    "driver_url" = "${flightDriverPath}",
                    "uri" = "grpc://127.0.0.1:${arrowPort}",
                    "user" = "root",
                    "password" = "",
                    "partitioned_read" = "required"
                )
            """

            def flightTables = sql("SHOW TABLES FROM ${flightCatalog}.${flightDb}")
                    .collect { it[0].toString() } as Set
            assertTrue(flightTables.contains("base_table"), "base_table missing: ${flightTables}")
            assertFalse(flightTables.contains("leaky_view"),
                    "the view was surfaced as a table by SHOW TABLES: ${flightTables}")

            def flightStatus = sql("SHOW TABLE STATUS FROM ${flightCatalog}.${flightDb}")
                    .collect { it[0].toString() } as Set
            assertTrue(flightStatus.contains("base_table"),
                    "base_table missing from SHOW TABLE STATUS: ${flightStatus}")
            assertFalse(flightStatus.contains("leaky_view"),
                    "the view leaked out through SHOW TABLE STATUS: ${flightStatus}")

            def flightInfo = sql("""
                SELECT TABLE_NAME, TABLE_TYPE FROM ${flightCatalog}.information_schema.tables
                WHERE TABLE_SCHEMA = '${flightDb}' ORDER BY TABLE_NAME
            """)
            def flightInfoNames = flightInfo.collect { it[0].toString() } as Set
            assertTrue(flightInfoNames.contains("base_table"),
                    "base_table missing from information_schema.tables: ${flightInfo}")
            assertFalse(flightInfoNames.contains("leaky_view"),
                    "the view leaked out through information_schema.tables: ${flightInfo}")

            // A view that is not listed must also not be readable by name: a catalog that hid it from the
            // listing but still resolved it would be inconsistent in the most confusing direction.
            String viewMessage = null
            try {
                sql "SELECT id FROM ${flightCatalog}.${flightDb}.leaky_view"
            } catch (Exception e) {
                viewMessage = e.getMessage()
            }
            assertNotNull(viewMessage,
                    "a view that SHOW TABLES excludes was still readable through the catalog")
            logger.info("reading an excluded view reported: ${viewMessage}")

            qt_flight_info_tables """
                SELECT TABLE_NAME, TABLE_TYPE FROM ${flightCatalog}.information_schema.tables
                WHERE TABLE_SCHEMA = '${flightDb}' ORDER BY TABLE_NAME
            """
        }
    } finally {
        sql """SWITCH internal"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP CATALOG IF EXISTS ${renamedCatalog}"""
        sql """DROP CATALOG IF EXISTS ${flightCatalog}"""
        sql """DROP DATABASE IF EXISTS internal.${flightDb} FORCE"""
    }
}
