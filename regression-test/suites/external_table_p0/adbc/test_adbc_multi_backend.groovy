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
// THE CROSS-BACKEND CASE -- the reason partitioned reads exist, and the one
// thing no single-backend run can say anything about.
//
// SKIPPED, LOUDLY, ON A SINGLE-BACKEND CLUSTER. A pure scan of a
// single-backend source is one partition no matter what, so every assertion
// here would pass on one backend while proving nothing about several. Rather
// than assert something weaker, the suite says out loud that it did not run.
//
// What a second backend adds that nothing else can:
//   * the source splits a scan per backend, so the partition COUNT stops being
//     pinned at one and the floor below becomes a real assertion;
//   * Doris hands those partitions to different backends, so the driver is
//     dlopened on more than one node -- a driver present on only one BE, or a
//     different build on one BE, fails here and nowhere else;
//   * partitions are read concurrently by separate processes, so a partition
//     read twice or not at all becomes possible for the first time.
//
// The partition count is READ OUT of the connector, never assumed: a catalog
// capped at one partition either answers the query (so there was one) or
// refuses and names the number it found. Nothing here encodes a theory about
// how the source plans.
//
// Setup is the same as test_adbc_catalog_scan -- see its header. In addition,
// EVERY backend must be able to read the driver at the same absolute path.
// ############################################################################

suite("test_adbc_multi_backend", "p0,external") {
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
        logger.info("SKIPPED test_adbc_multi_backend: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "CROSS-BACKEND ADBC READS ARE NOT BEING TESTED.")
        return
    }

    def backends = sql_return_maparray "show backends"
    int aliveBackends = backends.count { it.Alive.toString().equalsIgnoreCase("true") }

    if (aliveBackends <= 1) {
        // Not a pass, and not a failure either: the cluster cannot express what this suite tests. Said in
        // full because a green run with this line in the log means something completely different from a
        // green run without it.
        logger.info("SKIPPED test_adbc_multi_backend: this cluster has ${aliveBackends} alive backend(s), "
                + "and a pure scan of a single-backend source always reports exactly one partition. "
                + "Every assertion in this suite would hold on one backend while proving nothing about "
                + "several, so it is not run. CROSS-BACKEND ADBC READS ARE NOT BEING TESTED. "
                + "test_adbc_partitioned_read still covers the one-partition case.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]
    logger.info("test_adbc_multi_backend: ${aliveBackends} alive backends, driver ${driverPath}, "
            + "arrow flight sql port ${arrowPort}")

    String catalogName = "test_adbc_multi_backend_catalog"
    String probeCatalog = "test_adbc_multi_backend_probe"
    String singleRangeCatalog = "test_adbc_multi_backend_single_range"
    String dbName = "test_adbc_multi_backend_db"

    // Enough buckets that every backend holds part of the table. A backend with no tablet contributes no
    // partition, and the count would then fall short for a reason that has nothing to do with the
    // connector.
    int buckets = 8 * aliveBackends
    int rows = 200000

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP CATALOG IF EXISTS ${probeCatalog}"""
    sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.spread (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `grp` int NULL,
          `amount` decimalv3(12, 2) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS ${buckets}
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.spread
        SELECT number, concat('n', number), number % 7, number / 100
        FROM numbers("number" = "${rows}")
    """

    def adbcCatalog = { String name, String extra ->
        sql """
            CREATE CATALOG ${name} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${driverPath}",
                "uri" = "grpc://127.0.0.1:${arrowPort}",
                "user" = "root",
                "password" = "",
                "partitioned_read" = "required"
                ${extra}
            )
        """
    }

    try {
        // 'required' throughout, never 'auto': under 'auto' a driver that stopped partitioning would be
        // downgraded to a single statement and every assertion below would still hold, on a run that
        // tested nothing this suite is named after.
        adbcCatalog(catalogName, "")

        String table = "${catalogName}.${dbName}.spread"

        // ---- how many partitions, and is it at least one per backend ----

        adbcCatalog(probeCatalog, """, "max_partitions" = "1" """)
        int partitions
        try {
            def atOne = sql """SELECT min(id) FROM ${probeCatalog}.${dbName}.spread"""
            assertEquals(0, atOne[0][0])
            partitions = 1
        } catch (Exception e) {
            def matcher = (e.getMessage() =~ /into (\d+) partitions/)
            assertTrue(matcher.find(), "a scan capped at one partition failed for an unexpected reason, "
                    + "so the partition count could not be read: ${e.getMessage()}")
            partitions = matcher.group(1) as int
        }
        logger.info("the source split this scan into ${partitions} partition(s) across "
                + "${aliveBackends} alive backend(s)")

        // THE assertion of this suite. With ${buckets} buckets and one replica every backend holds part of
        // the table, so a source that splits a scan per backend cannot report fewer partitions than it has
        // backends. Fewer means the scan was not split across them -- whatever else still passes.
        assertTrue(partitions >= aliveBackends,
                "a ${aliveBackends}-backend source split this scan into only ${partitions} partition(s), "
                        + "so it was NOT read across backends. If some backend legitimately holds no "
                        + "tablet of ${dbName}.spread, that premise -- not the connector -- is what "
                        + "broke.")

        // ---- reading every partition loses nothing and repeats nothing ----
        //
        // Checked against the source table rather than against a second ADBC read: the right answer is
        // known here, so a fault shared by both read paths cannot hide inside an agreement. count(*) alone
        // would miss a partition read twice that also lost one; sum and count(DISTINCT) together do not.

        def expected = sql """
            SELECT count(*), count(DISTINCT id), sum(id), min(id), max(id), sum(amount)
            FROM internal.${dbName}.spread
        """
        def actual = sql """
            SELECT count(*), count(DISTINCT id), sum(id), min(id), max(id), sum(amount)
            FROM ${table}
        """
        assertEquals(expected.toString(), actual.toString(),
                "a ${partitions}-partition read across ${aliveBackends} backends returned different data "
                        + "than the source table holds")

        // A string column too: the numeric aggregates above would survive a partition whose string buffer
        // was mis-sliced.
        assertEquals(
                sql("SELECT count(DISTINCT name), min(name), max(name) FROM internal.${dbName}.spread")
                        .toString(),
                sql("SELECT count(DISTINCT name), min(name), max(name) FROM ${table}").toString(),
                "the string column did not survive a partitioned read intact")

        // ---- the projection and the predicate travel with every partition ----

        // BE rejects a column the statement did not request, so a partition that carried a different
        // projection would fail rather than return extra data.
        def projected = sql """SELECT count(*), count(DISTINCT name) FROM ${table}"""
        assertEquals(rows, projected[0][0])
        assertEquals(rows, projected[0][1])

        // A pushed predicate is part of the statement the source partitions, so every partition must come
        // back already filtered and the total must still be exact.
        def filtered = sql """
            SELECT count(*), min(id), max(id) FROM ${table} WHERE id >= ${rows - 10}
        """
        assertEquals(10, filtered[0][0])
        assertEquals(rows - 10, filtered[0][1])
        assertEquals(rows - 1, filtered[0][2])

        // A predicate that matches nothing: every partition returns an empty batch, which is a different
        // code path from every partition returning rows.
        assertEquals(0L, sql("SELECT count(*) FROM ${table} WHERE id < 0")[0][0])

        // An aggregation over all partitions, grouped: each group's rows are spread over every partition,
        // so a lost or duplicated partition shows up in exactly one group rather than in the total.
        assertEquals(
                sql("SELECT grp, count(*), sum(id) FROM internal.${dbName}.spread GROUP BY grp ORDER BY grp")
                        .toString(),
                sql("SELECT grp, count(*), sum(id) FROM ${table} GROUP BY grp ORDER BY grp").toString(),
                "a grouped aggregation over a partitioned read disagreed with the source")

        // ---- the partitioned and single-statement paths agree ----
        //
        // The single-statement path reads the whole table through one backend. On a multi-backend cluster
        // this is the direct comparison between "read by many" and "read by one".

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
        ["SELECT count(*), sum(id), min(id), max(id) FROM @CATALOG@.${dbName}.spread",
         "SELECT grp, count(*) FROM @CATALOG@.${dbName}.spread GROUP BY grp ORDER BY grp",
         "SELECT count(*) FROM @CATALOG@.${dbName}.spread WHERE id % 1000 = 0",
         "SELECT id, name FROM @CATALOG@.${dbName}.spread WHERE id >= ${rows - 3} ORDER BY id"
        ].each { String query ->
            def viaPartitions = sql(query.replace('@CATALOG@', catalogName))
            def viaOneRange = sql(query.replace('@CATALOG@', singleRangeCatalog))
            assertEquals(viaOneRange.toString(), viaPartitions.toString(),
                    "reading across ${partitions} partitions disagreed with reading through one "
                            + "statement: ${query}")
        }

        // ---- the partition cap is enforced at run time ----
        //
        // The CREATE-time validation of this property is covered in test_adbc_negative; this is the other
        // half, where the cap is legal but the source exceeds it. Deliberately not a fallback to a single
        // range: by this point the source has already executed the statement, and re-planning it would
        // execute it a second time while the first result sits unread.

        String cappedCatalog = "test_adbc_multi_backend_capped"
        sql """DROP CATALOG IF EXISTS ${cappedCatalog}"""
        adbcCatalog(cappedCatalog, """, "max_partitions" = "${partitions - 1}" """)
        try {
            String message = null
            try {
                sql "SELECT count(*) FROM ${cappedCatalog}.${dbName}.spread"
            } catch (Exception e) {
                message = e.getMessage()
            }
            assertNotNull(message,
                    "a scan capped at ${partitions - 1} partitions should have failed, because the source "
                            + "splits it into ${partitions}")
            assertTrue(message.contains("${partitions} partitions"),
                    "the failure does not say how many partitions there actually were, which is the only "
                            + "number that tells an operator what to raise the cap to: ${message}")
            assertTrue(message.contains("max_partitions"),
                    "the failure does not name the property to change: ${message}")
        } finally {
            sql """DROP CATALOG IF EXISTS ${cappedCatalog}"""
        }

        // ---- reading the same table repeatedly ----
        //
        // Each statement re-plans and re-partitions on the source. A descriptor kept from a previous query,
        // or a driver registry that is not safe to reuse across backends, shows up as a second read that
        // disagrees with the first.
        def firstRead = sql("SELECT count(*), sum(id) FROM ${table}")
        3.times {
            assertEquals(firstRead.toString(), sql("SELECT count(*), sum(id) FROM ${table}").toString(),
                    "repeated partitioned reads of the same table disagreed")
        }

        // ---- concurrent partitioned reads ----
        //
        // Several statements in flight at once, each partitioning the same table across the same backends.
        // Serial correctness above does not cover a shared driver handle or a registry entry being reused
        // while another query holds it.
        def failures = java.util.Collections.synchronizedList(new ArrayList<String>())
        String expectedConcurrent = firstRead.toString()
        def threads = (1..4).collect { int worker ->
            Thread.start {
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                        3.times {
                            def got = sql("SELECT count(*), sum(id) FROM ${table}")
                            if (got.toString() != expectedConcurrent) {
                                failures.add("worker ${worker} read ${got} instead of "
                                        + expectedConcurrent)
                            }
                        }
                    }
                } catch (Throwable t) {
                    failures.add("worker ${worker} threw: ${t}")
                }
            }
        }
        threads.each { it.join(600000) }
        threads.each {
            assertFalse(it.isAlive(), "a concurrent partitioned read did not finish in ten minutes")
        }
        assertTrue(failures.isEmpty(), "concurrent partitioned reads disagreed: ${failures}")
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP CATALOG IF EXISTS ${probeCatalog}"""
        sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
