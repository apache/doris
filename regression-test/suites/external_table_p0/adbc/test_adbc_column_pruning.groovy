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
//
// ############################################################################
// Exactly which columns the source is asked for.
//
// The existing suites check that the projection is not a star. That is the
// weakest possible form of this assertion: it passes for a statement that asks
// for nine columns out of ten. Here the select list is parsed out of the
// statement and compared as a SET, so asking for one column too many fails
// just as loudly as asking for one too few.
//
// Over-projection is not cosmetic on this path. Every extra column is a column
// the source materialises, serialises to Arrow and ships across the wire to be
// discarded, and on the partitioned path it is paid once per partition.
//
// Under-projection is worse and fails differently: BE matches returned columns
// against the query's slots by name and rejects anything it did not ask for,
// so a column the statement forgot is a failed scan rather than a wrong
// answer. The other half of this suite -- reading the rows and comparing them
// against the source -- is what covers that direction.
//
// Note what a "requested column" is: it is a query SLOT, not an output column.
// A column named only in a WHERE clause is still selected, because BE
// re-evaluates the predicate on what comes back and needs the values to do it.
// Several assertions below exist purely to pin that.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_column_pruning", "p0,external") {
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
        logger.info("SKIPPED test_adbc_column_pruning: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "ADBC COLUMN PRUNING IS NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_column_pruning_catalog"
    String dbName = "test_adbc_column_pruning_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // Eight columns, so "one too many" and "all of them" are far apart: with three columns an
    // over-projecting scan is easy to mistake for a correct one when reading a statement by eye.
    sql """
        CREATE TABLE internal.${dbName}.t_wide (
          `id` int NOT NULL,
          `c_a` int NULL,
          `c_b` varchar(64) NULL,
          `c_c` double NULL,
          `c_d` decimalv3(10, 2) NULL,
          `c_e` date NULL,
          `c_f` bigint NULL,
          `c_g` varchar(64) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_wide VALUES
          (1, 10, 'a', 1.5, 11.11, '2024-01-01', 100, 'x'),
          (2, 20, 'b', 2.5, 22.22, '2024-02-01', 200, 'y'),
          (3, 30, NULL, 3.5, NULL, '2024-03-01', 300, NULL),
          (4, 40, 'd', NULL, 44.44, NULL, 400, 'z')
    """

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
        String table = "${catalogName}.${dbName}.t_wide"

        def remoteStatements = { String stmt ->
            List<String> found = new ArrayList<String>()
            explain {
                sql(stmt)
                check { String plan ->
                    plan.readLines().each { String line ->
                        String trimmed = line.trim()
                        if (trimmed.startsWith("QUERY: ")) {
                            found.add(trimmed.substring("QUERY: ".length()))
                        }
                    }
                    return true
                }
            }
            assertFalse(found.isEmpty(), "the plan for [${stmt}] has no remote statement in it")
            return found
        }

        // The select list, as a set of bare column names. Splitting on commas is safe here because every
        // column in the fixture has an ordinary name -- the point of the parse is to compare sets, not to
        // be a SQL parser.
        def projectionOf = { String query ->
            assertTrue(query.startsWith("SELECT "), "unexpected statement shape: ${query}")
            int from = query.indexOf(" FROM ")
            assertTrue(from > 0, "no FROM clause in: ${query}")
            String list = query.substring("SELECT ".length(), from)
            return list.split(",").collect { it.trim().replace("`", "") } as Set
        }

        // For the statements where WHICH column the source is asked for is the planner's choice rather
        // than the query's: only how MANY is the connector's business.
        def countProjects = { String stmt, int expected ->
            def queries = remoteStatements(stmt)
            assertEquals(1, queries.size(),
                    "expected exactly one remote scan for [${stmt}], got ${queries.size()}: ${queries}")
            String query = queries[0]
            assertFalse(query.contains("*"),
                    "the projection was widened to a star, which BE rejects outright: ${query}")
            assertEquals(expected, projectionOf(query).size(),
                    "[${stmt}] should ask the source for ${expected} column(s), but asked for "
                            + "${projectionOf(query)}: ${query}")
            return query
        }

        def projects = { String stmt, Set expected ->
            def queries = remoteStatements(stmt)
            assertEquals(1, queries.size(),
                    "expected exactly one remote scan for [${stmt}], got ${queries.size()}: ${queries}")
            String query = queries[0]
            assertFalse(query.contains("*"),
                    "the projection was widened to a star, which BE rejects outright: ${query}")
            assertEquals(expected, projectionOf(query),
                    "wrong columns requested from the source for [${stmt}]: ${query}")
            return query
        }

        // ---- one column means one column ----

        projects("SELECT c_a FROM ${table}", ["c_a"] as Set)
        projects("SELECT c_b FROM ${table}", ["c_b"] as Set)
        projects("SELECT id FROM ${table}", ["id"] as Set)

        // Two of eight, named out of table order: the statement must carry those two and nothing else.
        projects("SELECT c_g, c_a FROM ${table}", ["c_a", "c_g"] as Set)

        // The same column twice is still one column.
        projects("SELECT c_a, c_a FROM ${table}", ["c_a"] as Set)

        // An expression over one column requests that column, not the whole row.
        projects("SELECT c_a + 1 FROM ${table}", ["c_a"] as Set)
        projects("SELECT upper(c_b) FROM ${table}", ["c_b"] as Set)

        // ---- a filtered column is a requested column ----
        //
        // Not an oversight: BE re-evaluates every conjunct on what comes back, so it needs the values even
        // when the user never asked to see them. A scan that pruned c_c here would fail outright.

        projects("SELECT c_a FROM ${table} WHERE c_c > 1.0", ["c_a", "c_c"] as Set)
        projects("SELECT c_a FROM ${table} WHERE c_b LIKE 'a%'", ["c_a", "c_b"] as Set)
        projects("SELECT c_a FROM ${table} WHERE c_c > 1.0 AND c_f < 500", ["c_a", "c_c", "c_f"] as Set)

        // Ordering and grouping columns are slots too.
        projects("SELECT c_a FROM ${table} ORDER BY c_f", ["c_a", "c_f"] as Set)
        projects("SELECT c_b, sum(c_a) FROM ${table} GROUP BY c_b", ["c_a", "c_b"] as Set)
        projects("SELECT c_b FROM ${table} GROUP BY c_b HAVING count(*) > 0", ["c_b"] as Set)

        // ---- aggregates ----

        // COUNT(*) needs no VALUES, and the point of this assertion is that the source is not asked for
        // the table's width to count its rows. It is not asserted as the literal 1, though the query
        // builder does render that for an empty column list: Doris never hands the scan an empty one.
        // When projection pruning empties a scan tuple the planner puts the narrowest column back
        // (updateScanSlotsMaterialization -> getSmallestSlot), so what arrives here is one narrow
        // column. Which one is a planner decision, so the assertion is the count, not the name.
        String countQuery = countProjects("SELECT count(*) FROM ${table}", 1)
        logger.info("count(*) reached the source as: ${countQuery}")

        // COUNT of a column does need that column, and only that one.
        projects("SELECT count(c_a) FROM ${table}", ["c_a"] as Set)
        projects("SELECT min(c_e), max(c_e) FROM ${table}", ["c_e"] as Set)

        // A constant projection needs no column either -- same one-narrow-column floor as count(*).
        countProjects("SELECT 1 FROM ${table}", 1)

        // ---- through a subquery ----
        //
        // The inner select lists two columns and the outer uses one. Pruning that stops at the subquery
        // boundary would ask the source for both.

        projects("SELECT c_a FROM (SELECT c_a, c_b FROM ${table}) sub", ["c_a"] as Set)
        projects("SELECT x FROM (SELECT c_a AS x, c_b AS y FROM ${table}) sub WHERE x > 10",
                ["c_a"] as Set)

        // ---- a star still becomes an explicit list ----
        //
        // SELECT * is the case that must NOT survive as a star: BE rejects a column the statement did not
        // request, and a star means the statement requested nothing by name.

        String starQuery = projects("SELECT * FROM ${table}",
                ["id", "c_a", "c_b", "c_c", "c_d", "c_e", "c_f", "c_g"] as Set)
        assertFalse(starQuery.contains("*"), "SELECT * reached the source as a star: ${starQuery}")

        // ---- two scans of the same table ----
        //
        // A self join plans two scans, and each is pruned on its own. If pruning were computed per TABLE
        // rather than per scan, both sides would ask for the union of the two column sets.

        def joinQueries = remoteStatements("""
            SELECT a.c_a, b.c_g FROM ${table} a JOIN ${table} b ON a.id = b.id
        """)
        assertEquals(2, joinQueries.size(),
                "a self join should plan two remote scans: ${joinQueries}")
        def joinProjections = joinQueries.collect { projectionOf(it) }
        assertTrue(joinProjections.contains(["id", "c_a"] as Set),
                "no scan asked for just (id, c_a): ${joinQueries}")
        assertTrue(joinProjections.contains(["id", "c_g"] as Set),
                "no scan asked for just (id, c_g): ${joinQueries}")

        // ---- and the rows are still right ----
        //
        // The other direction. Pruning one column too FEW is not a wasted byte, it is a failed or wrong
        // scan, and only reading the data can show that. Compared against a native read of the same source
        // table, so the answer does not depend on a baseline.

        def sameAsSource = { String select, String tail ->
            def viaAdbc = sql("SELECT ${select} FROM ${table} ${tail}")
            def viaSource = sql("SELECT ${select} FROM internal.${dbName}.t_wide ${tail}")
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "projecting [${select}] through ADBC returned different rows than the source")
        }

        sameAsSource("c_a", "ORDER BY c_a")
        sameAsSource("c_g, c_a", "ORDER BY c_a")
        sameAsSource("c_a", "WHERE c_c > 1.0 ORDER BY c_a")
        sameAsSource("c_a", "WHERE c_b LIKE 'a%' ORDER BY c_a")
        sameAsSource("c_a", "ORDER BY c_f DESC")
        sameAsSource("c_b, sum(c_a)", "GROUP BY c_b ORDER BY c_b")
        sameAsSource("count(*)", "")
        sameAsSource("count(c_a), count(c_b), count(c_d)", "")
        sameAsSource("*", "ORDER BY id")

        // ---- baselines ----

        qt_prune_one """SELECT c_a FROM ${table} ORDER BY c_a"""
        qt_prune_two """SELECT c_g, c_a FROM ${table} ORDER BY c_a"""
        qt_prune_filtered """SELECT c_a FROM ${table} WHERE c_c > 1.0 ORDER BY c_a"""
        qt_prune_star """SELECT * FROM ${table} ORDER BY id"""
        qt_prune_count """SELECT count(*), count(c_b), count(c_d) FROM ${table}"""
        qt_prune_group """SELECT c_b, sum(c_a) FROM ${table} GROUP BY c_b ORDER BY c_b"""
        qt_prune_subquery """
            SELECT x FROM (SELECT c_a AS x, c_b AS y FROM ${table}) sub WHERE x > 10 ORDER BY x
        """
        qt_prune_selfjoin """
            SELECT a.c_a, b.c_g FROM ${table} a JOIN ${table} b ON a.id = b.id ORDER BY a.c_a
        """
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
