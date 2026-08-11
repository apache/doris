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
// One query, several sources: a SQLite file, a remote Doris over Arrow Flight
// SQL, and Doris's own tables -- joined together.
//
// This is where the connector stops being tested in isolation. Two ADBC
// catalogs in one query means two DIFFERENT drivers loaded in the same FE and
// the same BE at the same time, each with its own dialect (SQLite gets ANSI
// double quotes, a Doris source gets backticks) and its own answer to whether
// it can partition a scan. A defect that shows up only when a second driver is
// present -- a shared registry, a dialect resolved once per process instead of
// once per catalog -- cannot be reached by any single-source suite.
//
// The oracle is a mirror: internal Doris tables holding exactly what the
// SQLite fixture holds. Every cross-source query is run once over the external
// catalogs and once over the mirrors, and the two must agree. Without that,
// a join across three sources has no independently known answer.
//
// SQLite is required. The Flight SQL half is skipped on its own with a log
// when that driver is absent, because it needs a driver no Doris release
// ships.
//
// Requirements are the union of the two existing suites -- see the headers of
// test_adbc_sqlite_catalog_scan and test_adbc_catalog_scan.
// ############################################################################

suite("test_adbc_cross_source", "p0,external") {
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
        logger.info("SKIPPED test_adbc_cross_source: no readable ADBC SQLite driver at "
                + "${sqliteDriverPath}. Build it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc'. "
                + "CROSS-SOURCE QUERIES ARE NOT BEING TESTED.")
        return
    }
    boolean hasFlight = new File(flightDriverPath).canRead()
    if (!hasFlight) {
        logger.info("test_adbc_cross_source: no readable ADBC Flight SQL driver at ${flightDriverPath}, "
                + "so the SQLite-plus-Flight-SQL half of this suite will be skipped. The "
                + "SQLite-plus-internal half still runs.")
    }

    // ---- the SQLite fixture ----

    String workDir = "${System.getProperty('java.io.tmpdir')}/doris_adbc_regression"
    new File(workDir).mkdirs()
    File dbFile = new File("${workDir}/test_adbc_cross_source.db")
    File seedFile = new File("${workDir}/test_adbc_cross_source.sql")
    // A database left by an earlier run would fail CREATE TABLE and could be seeded differently from what
    // the mirrors below assert.
    dbFile.delete()

    // Every column has at least one non-null value: the SQLite ADBC driver derives Arrow types from the
    // values present rather than from the declared column types, so an all-null column would arrive as
    // int64 and the join keys would stop matching for a reason unrelated to anything under test.
    seedFile.text = """
        CREATE TABLE products (p_id INTEGER, p_name TEXT, p_price REAL);
        INSERT INTO products VALUES (1, 'widget', 9.5);
        INSERT INTO products VALUES (2, 'gadget', 19.5);
        INSERT INTO products VALUES (3, 'doohickey', 29.5);
        INSERT INTO products VALUES (4, 'orphan', 39.5);
        CREATE TABLE regions (r_code TEXT, r_name TEXT);
        INSERT INTO regions VALUES ('E', 'east');
        INSERT INTO regions VALUES ('W', 'west');
        INSERT INTO regions VALUES ('N', 'north');
    """

    def seed = new ProcessBuilder("/bin/bash", "-c",
            "sqlite3 '${dbFile.absolutePath}' < '${seedFile.absolutePath}'")
            .redirectErrorStream(true).start()
    String seedOutput = seed.inputStream.text
    int seedExit = seed.waitFor()
    if (seedExit != 0 || !dbFile.exists()) {
        logger.info("SKIPPED test_adbc_cross_source: could not create the SQLite fixture "
                + "(exit ${seedExit}): ${seedOutput}. The sqlite3 CLI is needed to build it. "
                + "CROSS-SOURCE QUERIES ARE NOT BEING TESTED.")
        return
    }

    String sqliteCatalog = "test_adbc_cross_source_sqlite"
    String flightCatalog = "test_adbc_cross_source_flight"
    String dbName = "test_adbc_cross_source_db"
    String sqliteDb = "main"

    sql """DROP CATALOG IF EXISTS ${sqliteCatalog}"""
    sql """DROP CATALOG IF EXISTS ${flightCatalog}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // ---- Doris's own tables ----

    sql """
        CREATE TABLE internal.${dbName}.sales (
          `s_id` int NOT NULL,
          `s_pid` int NULL,
          `s_qty` int NULL,
          `s_region` varchar(8) NULL
        ) DISTRIBUTED BY HASH(`s_id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    // Sale 5 refers to a product that is not in the SQLite fixture, and product 4 has no sale: the outer
    // joins below are only meaningful with an unmatched row on each side.
    sql """
        INSERT INTO internal.${dbName}.sales VALUES
          (1, 1, 10, 'E'),
          (2, 1,  5, 'W'),
          (3, 2,  7, 'E'),
          (4, 3,  1, 'N'),
          (5, 99, 3, 'W'),
          (6, 2,  2, NULL)
    """

    // The oracle. Types match what SQLite reports through ADBC -- INTEGER becomes int64 becomes BIGINT,
    // TEXT becomes STRING, REAL becomes DOUBLE -- so a comparison between the two runs is a comparison of
    // values, not of type coercions.
    sql """
        CREATE TABLE internal.${dbName}.products_mirror (
          `p_id` bigint NOT NULL,
          `p_name` string NULL,
          `p_price` double NULL
        ) DISTRIBUTED BY HASH(`p_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.products_mirror VALUES
          (1, 'widget', 9.5), (2, 'gadget', 19.5), (3, 'doohickey', 29.5), (4, 'orphan', 39.5)
    """
    sql """
        CREATE TABLE internal.${dbName}.regions_mirror (
          `r_code` varchar(8) NOT NULL,
          `r_name` string NULL
        ) DISTRIBUTED BY HASH(`r_code`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.regions_mirror VALUES ('E', 'east'), ('W', 'west'), ('N', 'north')
    """

    sql """
        CREATE CATALOG ${sqliteCatalog} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${sqliteDriverPath}",
            "uri" = "file:${dbFile.absolutePath}"
        )
    """
    if (hasFlight) {
        def frontends = sql "show frontends"
        String arrowPort = frontends[0][6]
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
    }

    try {
        // @PRODUCTS@ / @REGIONS@ come from SQLite through ADBC, or from the mirrors; @SALES@ is either the
        // Doris table itself or the same table read back through a second ADBC catalog. The two runs must
        // agree whichever combination is in play.
        def sameAsMirror = { String query, String productsRef, String regionsRef, String salesRef ->
            def viaExternal = sql(query.replace('@PRODUCTS@', productsRef)
                    .replace('@REGIONS@', regionsRef)
                    .replace('@SALES@', salesRef))
            def viaMirror = sql(query.replace('@PRODUCTS@', "internal.${dbName}.products_mirror")
                    .replace('@REGIONS@', "internal.${dbName}.regions_mirror")
                    .replace('@SALES@', "internal.${dbName}.sales"))
            assertEquals(viaMirror.toString(), viaExternal.toString(),
                    "a cross-source query answered differently than the same query over local mirrors:\n"
                            + query)
            return viaExternal
        }

        String sqliteProducts = "${sqliteCatalog}.${sqliteDb}.products"
        String sqliteRegions = "${sqliteCatalog}.${sqliteDb}.regions"
        String internalSales = "internal.${dbName}.sales"

        // ---- SQLite joined to a Doris internal table ----

        def crossQueries = [
            // The plain inner join, and the shape everything else builds on.
            """SELECT p.p_id, p.p_name, s.s_id, s.s_qty
               FROM @PRODUCTS@ p JOIN @SALES@ s ON p.p_id = s.s_pid
               ORDER BY p.p_id, s.s_id""",
            // Product 4 has no sale, so a left join that dropped it would still look reasonable.
            """SELECT p.p_id, p.p_name, count(s.s_id) AS n
               FROM @PRODUCTS@ p LEFT JOIN @SALES@ s ON p.p_id = s.s_pid
               GROUP BY p.p_id, p.p_name ORDER BY p.p_id""",
            // Sale 5 refers to a missing product.
            """SELECT s.s_id, p.p_name
               FROM @PRODUCTS@ p RIGHT JOIN @SALES@ s ON p.p_id = s.s_pid
               ORDER BY s.s_id""",
            // An aggregate whose inputs come from both sources, including arithmetic across them.
            """SELECT p.p_name, sum(s.s_qty) AS qty, sum(s.s_qty * p.p_price) AS revenue
               FROM @PRODUCTS@ p JOIN @SALES@ s ON p.p_id = s.s_pid
               GROUP BY p.p_name ORDER BY p.p_name""",
            // A predicate on each side: both are pushed into their own source, in two different dialects.
            """SELECT p.p_id, s.s_id FROM @PRODUCTS@ p JOIN @SALES@ s ON p.p_id = s.s_pid
               WHERE p.p_price > 9.5 AND s.s_qty >= 2
               ORDER BY p.p_id, s.s_id""",
            // A three-way join across both SQLite tables and the Doris one.
            """SELECT r.r_name, p.p_name, sum(s.s_qty) AS qty
               FROM @SALES@ s
               JOIN @PRODUCTS@ p ON p.p_id = s.s_pid
               JOIN @REGIONS@ r ON r.r_code = s.s_region
               GROUP BY r.r_name, p.p_name ORDER BY r.r_name, p.p_name""",
            // A remote table on the inner side of a semi join.
            """SELECT s_id, s_pid FROM @SALES@
               WHERE s_pid IN (SELECT p_id FROM @PRODUCTS@ WHERE p_price < 25.0)
               ORDER BY s_id""",
            // ... and of an anti join.
            """SELECT s_id, s_pid FROM @SALES@ s
               WHERE NOT EXISTS (SELECT 1 FROM @PRODUCTS@ p WHERE p.p_id = s.s_pid)
               ORDER BY s_id""",
            // A set operation across sources: the column types come from different mappings and still have
            // to unify.
            """SELECT cast(p_id AS bigint) AS k FROM @PRODUCTS@
               UNION
               SELECT cast(s_pid AS bigint) AS k FROM @SALES@
               ORDER BY k""",
            // A CTE over one source consumed alongside another.
            """WITH cheap AS (SELECT p_id, p_name FROM @PRODUCTS@ WHERE p_price < 25.0)
               SELECT c.p_name, count(*) AS n
               FROM cheap c JOIN @SALES@ s ON s.s_pid = c.p_id
               GROUP BY c.p_name ORDER BY c.p_name""",
            // A window function whose partition comes from one source and ordering from the other.
            """SELECT p.p_name, s.s_id,
                      row_number() OVER (PARTITION BY p.p_name ORDER BY s.s_id) AS rn
               FROM @PRODUCTS@ p JOIN @SALES@ s ON p.p_id = s.s_pid
               ORDER BY p.p_name, s.s_id"""
        ]

        crossQueries.each { String query ->
            sameAsMirror(query, sqliteProducts, sqliteRegions, internalSales)
        }

        qt_sqlite_join_internal """
            SELECT p.p_id, p.p_name, s.s_id, s.s_qty
            FROM ${sqliteProducts} p JOIN ${internalSales} s ON p.p_id = s.s_pid
            ORDER BY p.p_id, s.s_id
        """
        qt_sqlite_agg_internal """
            SELECT p.p_name, sum(s.s_qty) AS qty, sum(s.s_qty * p.p_price) AS revenue
            FROM ${sqliteProducts} p JOIN ${internalSales} s ON p.p_id = s.s_pid
            GROUP BY p.p_name ORDER BY p.p_name
        """
        qt_sqlite_threeway """
            SELECT r.r_name, p.p_name, sum(s.s_qty) AS qty
            FROM ${internalSales} s
            JOIN ${sqliteProducts} p ON p.p_id = s.s_pid
            JOIN ${sqliteRegions} r ON r.r_code = s.s_region
            GROUP BY r.r_name, p.p_name ORDER BY r.r_name, p.p_name
        """

        // The result of a cross-source join, loaded back into Doris.
        sql """DROP TABLE IF EXISTS internal.${dbName}.cross_loaded"""
        sql """
            CREATE TABLE internal.${dbName}.cross_loaded (
              `p_id` bigint NOT NULL,
              `p_name` string NULL,
              `qty` bigint NULL
            ) DISTRIBUTED BY HASH(`p_id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO internal.${dbName}.cross_loaded
            SELECT p.p_id, p.p_name, sum(s.s_qty)
            FROM ${sqliteProducts} p JOIN ${internalSales} s ON p.p_id = s.s_pid
            GROUP BY p.p_id, p.p_name
        """
        qt_cross_loaded """
            SELECT p_id, p_name, qty FROM internal.${dbName}.cross_loaded ORDER BY p_id
        """

        // ---- SQLite joined to a Doris reached over Flight SQL ----
        //
        // Two ADBC catalogs, two drivers, two dialects, in one query. The Doris side of this join is the
        // same table as above, only reached through a second connector instead of natively -- so the
        // mirror comparison still holds, and any difference is the second catalog's doing.

        if (!hasFlight) {
            logger.info("test_adbc_cross_source: SKIPPING the SQLite-plus-Flight-SQL section, no Flight "
                    + "SQL driver at ${flightDriverPath}. TWO ADBC CATALOGS IN ONE QUERY ARE NOT BEING "
                    + "TESTED.")
        } else {
            String flightSales = "${flightCatalog}.${dbName}.sales"

            // The Flight SQL catalog alone must agree with the table it is reading, or nothing below means
            // anything.
            assertEquals(
                    sql("SELECT s_id, s_pid, s_qty, s_region FROM ${internalSales} ORDER BY s_id")
                            .toString(),
                    sql("SELECT s_id, s_pid, s_qty, s_region FROM ${flightSales} ORDER BY s_id")
                            .toString(),
                    "the Flight SQL catalog does not agree with the table it reads, so the cross-source "
                            + "comparisons below would be meaningless")

            // Every cross-source query again, now with BOTH sides external and on different drivers.
            crossQueries.each { String query ->
                sameAsMirror(query, sqliteProducts, sqliteRegions, flightSales)
            }

            qt_sqlite_join_flight """
                SELECT p.p_id, p.p_name, s.s_id, s.s_qty
                FROM ${sqliteProducts} p JOIN ${flightSales} s ON p.p_id = s.s_pid
                ORDER BY p.p_id, s.s_id
            """
            qt_sqlite_flight_agg """
                SELECT p.p_name, sum(s.s_qty) AS qty, sum(s.s_qty * p.p_price) AS revenue
                FROM ${sqliteProducts} p JOIN ${flightSales} s ON p.p_id = s.s_pid
                GROUP BY p.p_name ORDER BY p.p_name
            """

            // All three at once: SQLite, Flight SQL, and a Doris internal table in a single statement.
            qt_three_sources """
                SELECT r.r_name, p.p_name, sum(s.s_qty) AS qty, count(DISTINCT m.p_id) AS mirrored
                FROM ${flightSales} s
                JOIN ${sqliteProducts} p ON p.p_id = s.s_pid
                JOIN ${sqliteRegions} r ON r.r_code = s.s_region
                JOIN internal.${dbName}.products_mirror m ON m.p_id = p.p_id
                GROUP BY r.r_name, p.p_name ORDER BY r.r_name, p.p_name
            """

            def threeSources = sql """
                SELECT count(*) FROM ${flightSales} s
                JOIN ${sqliteProducts} p ON p.p_id = s.s_pid
                JOIN internal.${dbName}.regions_mirror r ON r.r_code = s.s_region
            """
            def threeMirrors = sql """
                SELECT count(*) FROM ${internalSales} s
                JOIN internal.${dbName}.products_mirror p ON p.p_id = s.s_pid
                JOIN internal.${dbName}.regions_mirror r ON r.r_code = s.s_region
            """
            assertEquals(threeMirrors.toString(), threeSources.toString(),
                    "a join over two ADBC catalogs and one internal table disagreed with the same join "
                            + "over local mirrors")

            // A load whose input spans two drivers.
            sql """DROP TABLE IF EXISTS internal.${dbName}.two_driver_loaded"""
            sql """
                CREATE TABLE internal.${dbName}.two_driver_loaded
                PROPERTIES ("replication_num" = "1")
                AS SELECT p.p_id, p.p_name, sum(s.s_qty) AS qty
                   FROM ${sqliteProducts} p JOIN ${flightSales} s ON p.p_id = s.s_pid
                   GROUP BY p.p_id, p.p_name
            """
            qt_two_driver_loaded """
                SELECT p_id, p_name, qty FROM internal.${dbName}.two_driver_loaded ORDER BY p_id
            """
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${sqliteCatalog}"""
        sql """DROP CATALOG IF EXISTS ${flightCatalog}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
