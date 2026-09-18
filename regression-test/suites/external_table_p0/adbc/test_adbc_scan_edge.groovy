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
// The shapes a scan can take that are not "some rows with some values".
//
// Each section here exists because of something specific:
//
//   a column that is null in EVERY returned row
//       The first end-to-end run of this connector found a real defect here.
//       A source that infers Arrow types from values has nothing to infer from
//       and reports int64, so a text column arrives as an integer and BE used
//       to fail with "Unsupported arrow type for string column: 9". FE cannot
//       prevent it -- it cannot know which rows a filter will leave -- and the
//       fix lives in BE. Nothing has pinned it since.
//
//   an empty table, and a filter that matches nothing
//       Partition counts describe the source's parallelism, not its
//       cardinality, so an empty table should still yield a partition that
//       returns nothing. But the Flight protocol allows zero endpoints for no
//       data, and the connector treats zero partitions as an error rather than
//       as an empty result -- it cannot tell that apart from a lost result
//       set. Which of the two a Doris source actually does has never been
//       observed. The single-statement path is asserted first, so the answer
//       is known even if the partitioned path turns out to fail.
//
//   identifiers that need quoting
//       Every identifier is quoted by the dialect, and a keyword column is the
//       only thing that shows the quoting is load-bearing rather than
//       decorative: unquoted, the remote statement does not parse.
//
//   many columns, and concurrent readers
//       Both are ordinary in production and absent from every other suite.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_scan_edge", "p0,external") {
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
        logger.info("SKIPPED test_adbc_scan_edge: no readable ADBC Flight SQL driver at ${driverPath}. "
                + "Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', or set "
                + "adbcDriverPath in regression-conf.groovy. "
                + "ADBC SCAN EDGE CASES ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_scan_edge_catalog"
    String singleRangeCatalog = "test_adbc_scan_edge_single_range"
    String dbName = "test_adbc_scan_edge_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // ---- fixtures ----

    // Every value of all_null is null, and half of sometimes_null are.
    sql """
        CREATE TABLE internal.${dbName}.nullish (
          `id` int NOT NULL,
          `all_null` varchar(64) NULL,
          `all_null_int` int NULL,
          `all_null_date` date NULL,
          `sometimes_null` varchar(64) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.nullish VALUES
          (1, NULL, NULL, NULL, 'present'),
          (2, NULL, NULL, NULL, NULL),
          (3, NULL, NULL, NULL, 'also present'),
          (4, NULL, NULL, NULL, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.empty_table (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `amount` decimalv3(10, 2) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE internal.${dbName}.one_row (
          `id` int NOT NULL,
          `name` varchar(64) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO internal.${dbName}.one_row VALUES (1, 'only')"""

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
    sql """
        CREATE CATALOG ${singleRangeCatalog} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "disabled"
        )
    """

    try {
        // ---- a column with no non-null value in it ----

        qt_desc_nullish """DESC ${catalogName}.${dbName}.nullish"""

        // Projected directly: the source has no values to infer a type from, and Doris still has to
        // materialise four nulls into a text column.
        qt_all_null_projected """
            SELECT id, all_null, all_null_int, all_null_date
            FROM ${catalogName}.${dbName}.nullish ORDER BY id
        """
        def allNullRows = sql """
            SELECT id, all_null FROM ${catalogName}.${dbName}.nullish ORDER BY id
        """
        assertEquals(4, allNullRows.size())
        allNullRows.each { assertEquals(null, it[1], "an all-null column returned a value: ${it}") }

        // THE original defect's shape. The projection is id alone, but BE re-evaluates the predicate, so
        // sometimes_null is a query slot too and the source returns it -- with no non-null value among the
        // surviving rows. This is the query that used to fail outright.
        assertEquals(2, sql("""
            SELECT id FROM ${catalogName}.${dbName}.nullish WHERE sometimes_null IS NULL
        """).size(), "a filter that leaves a projected column entirely null did not return its rows")
        qt_null_filtered """
            SELECT id FROM ${catalogName}.${dbName}.nullish WHERE sometimes_null IS NULL ORDER BY id
        """

        // The same shape on the other column, where every row is null before filtering too.
        assertEquals(4, sql("""
            SELECT id FROM ${catalogName}.${dbName}.nullish WHERE all_null IS NULL
        """).size())
        assertEquals(0, sql("""
            SELECT id FROM ${catalogName}.${dbName}.nullish WHERE all_null IS NOT NULL
        """).size())

        // Aggregates over a column that is entirely null: count is zero, the rest are null.
        qt_all_null_aggregates """
            SELECT count(*), count(all_null), count(all_null_int),
                   min(all_null), max(all_null_int), sum(all_null_int)
            FROM ${catalogName}.${dbName}.nullish
        """

        // ---- an empty table ----
        //
        // The single-statement path first, because it does not involve partition counts at all and so
        // settles what an empty table SHOULD look like before the partitioned path is asked.

        assertEquals(0, sql("SELECT id, name FROM ${singleRangeCatalog}.${dbName}.empty_table").size(),
                "an empty table did not read as zero rows through the single-statement path")
        assertEquals(0L, sql("SELECT count(*) FROM ${singleRangeCatalog}.${dbName}.empty_table")[0][0])
        qt_desc_empty """DESC ${singleRangeCatalog}.${dbName}.empty_table"""

        // Now the partitioned path. A source that reports zero partitions for an empty table makes this
        // fail by design -- the connector cannot tell zero partitions apart from a lost result set. If it
        // does fail here, that is the open question in the design being answered, and the answer is that
        // empty tables need a fix; it is not a test that guessed wrong.
        assertEquals(0, sql("SELECT id, name FROM ${catalogName}.${dbName}.empty_table").size(),
                "an empty table did not read as zero rows through the partitioned path. If the message "
                        + "mentions 'no partitions', the source reported zero endpoints for an empty "
                        + "result and the connector refused it -- see the decision about zero partitions "
                        + "in AdbcScanPlanProvider.")
        assertEquals(0L, sql("SELECT count(*) FROM ${catalogName}.${dbName}.empty_table")[0][0])
        qt_empty_aggregates """
            SELECT count(*), count(name), sum(amount), min(id), max(id)
            FROM ${catalogName}.${dbName}.empty_table
        """

        // An empty table on the outer side of a join still has to produce the outer rows.
        qt_empty_join """
            SELECT o.id, e.name
            FROM ${catalogName}.${dbName}.one_row o
            LEFT JOIN ${catalogName}.${dbName}.empty_table e ON o.id = e.id
            ORDER BY o.id
        """

        // ---- a filter that matches nothing ----
        //
        // Different from an empty table: the source runs a real query and returns a batch with no rows in
        // it, rather than having nothing to return.

        assertEquals(0, sql("SELECT id, name FROM ${catalogName}.${dbName}.one_row WHERE id = 999").size())
        assertEquals(0L, sql("SELECT count(*) FROM ${catalogName}.${dbName}.one_row WHERE id = 999")[0][0])
        qt_no_match """
            SELECT id, name FROM ${catalogName}.${dbName}.one_row WHERE name = 'nothing matches this'
        """

        // ---- one row ----

        qt_one_row """SELECT id, name FROM ${catalogName}.${dbName}.one_row"""
        assertEquals(1, sql("SELECT id FROM ${catalogName}.${dbName}.one_row").size())
        assertEquals(0, sql("SELECT id FROM ${catalogName}.${dbName}.one_row LIMIT 0").size())
        assertEquals(1, sql("SELECT id FROM ${catalogName}.${dbName}.one_row LIMIT 100").size())
        assertEquals(0, sql("SELECT id FROM ${catalogName}.${dbName}.one_row ORDER BY id LIMIT 5 OFFSET 5").size())

        // ---- identifiers that have to be quoted ----
        //
        // `order` is a reserved word: unquoted, the statement the connector builds does not parse at the
        // source at all. A Doris source needs backticks and rejects the ANSI double quotes the default
        // dialect would use, which is why the dialect is chosen from the source's vendor rather than
        // assumed.

        sql """
            CREATE TABLE internal.${dbName}.quoted_names (
              `id` int NOT NULL,
              `order` int NULL,
              `select` varchar(32) NULL,
              `MixedCase` int NULL,
              `中文列` varchar(32) NULL,
              `col_123` int NULL
            ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO internal.${dbName}.quoted_names VALUES
              (1, 10, 'a', 100, '中文值', 1000),
              (2, 20, 'b', 200, '另一个', 2000)
        """

        qt_desc_quoted """DESC ${catalogName}.${dbName}.quoted_names"""
        qt_select_quoted """
            SELECT id, `order`, `select`, `MixedCase`, `中文列`, `col_123`
            FROM ${catalogName}.${dbName}.quoted_names ORDER BY id
        """
        // A keyword column in a predicate as well as in the projection, so the quoting is exercised on
        // both sides of the generated statement.
        qt_quoted_predicate """
            SELECT id, `order` FROM ${catalogName}.${dbName}.quoted_names WHERE `order` > 10 ORDER BY id
        """
        assertEquals(
                sql("""SELECT id, `order`, `MixedCase`, `中文列` FROM internal.${dbName}.quoted_names
                       ORDER BY id""").toString(),
                sql("""SELECT id, `order`, `MixedCase`, `中文列` FROM ${catalogName}.${dbName}.quoted_names
                       ORDER BY id""").toString(),
                "a table with quoted identifiers read differently through ADBC than natively")

        // ---- many columns ----
        //
        // Sixty columns in one row: the projection list, the Arrow schema and the slot matching all scale
        // with column count, and every other fixture in this directory has fewer than a dozen.

        StringBuilder wideDdl = new StringBuilder(
                "CREATE TABLE internal.${dbName}.very_wide (\n  `id` int NOT NULL")
        StringBuilder wideValues = new StringBuilder("(1")
        StringBuilder wideColumns = new StringBuilder("id")
        for (int i = 1; i <= 60; i++) {
            wideDdl.append(",\n  `c${i}` ").append(i % 3 == 0 ? "varchar(32)" : (i % 3 == 1 ? "int" : "double"))
                    .append(" NULL")
            wideValues.append(", ").append(i % 3 == 0 ? "'v${i}'" : "${i}")
            wideColumns.append(", c${i}")
        }
        wideDdl.append("\n) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")")
        wideValues.append(")")
        sql wideDdl.toString()
        sql "INSERT INTO internal.${dbName}.very_wide VALUES ${wideValues}"

        assertEquals(61, sql("DESC ${catalogName}.${dbName}.very_wide").size(),
                "a 61-column table did not describe all of its columns")
        assertEquals(
                sql("SELECT ${wideColumns} FROM internal.${dbName}.very_wide").toString(),
                sql("SELECT ${wideColumns} FROM ${catalogName}.${dbName}.very_wide").toString(),
                "a 61-column table read differently through ADBC than natively")
        qt_wide_narrow_projection """
            SELECT c1, c30, c60 FROM ${catalogName}.${dbName}.very_wide
        """

        // ---- concurrent readers on one catalog ----
        //
        // The connector holds per-catalog state -- a metadata cache with manual miss loading, a probed
        // schema strategy, a resolved dialect -- all of which are first written by whichever query gets
        // there first. Serial tests never have two queries racing for that.

        def failures = java.util.Collections.synchronizedList(new ArrayList<String>())
        String expected = sql("SELECT id, name FROM ${catalogName}.${dbName}.one_row").toString()
        def threads = (1..6).collect { int worker ->
            Thread.start {
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                        5.times {
                            def rows = sql("SELECT id, name FROM ${catalogName}.${dbName}.one_row")
                            if (rows.toString() != expected) {
                                failures.add("worker ${worker} read ${rows} instead of ${expected}")
                            }
                            // Metadata and data at the same time, on the same catalog.
                            def described = sql("DESC ${catalogName}.${dbName}.nullish")
                            if (described.size() != 5) {
                                failures.add("worker ${worker} described ${described.size()} columns "
                                        + "instead of 5")
                            }
                        }
                    }
                } catch (Throwable t) {
                    failures.add("worker ${worker} threw: ${t}")
                }
            }
        }
        threads.each { it.join(300000) }
        threads.each { assertFalse(it.isAlive(), "a concurrent reader did not finish in five minutes") }
        assertTrue(failures.isEmpty(), "concurrent readers on one ADBC catalog disagreed: ${failures}")
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
