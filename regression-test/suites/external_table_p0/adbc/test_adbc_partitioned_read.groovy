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
// What partitioned reads are FOR: the source splits one query into as many
// result partitions as it has backends holding the data, and Doris reads each
// partition on a backend of its own. test_adbc_catalog_scan covers the rows a
// partitioned scan returns; this suite covers HOW MANY partitions there were
// and that reading all of them loses and duplicates nothing.
//
// The count is read back out of the connector rather than assumed, so nothing
// here hardcodes a number or a theory of how the source plans. The source is
// this cluster itself: on one backend the count is pinned at 1, and on several
// it must be at least one per backend -- the cross-backend case, and the whole
// point of the feature. A single-backend run says out loud that it did not
// cover that, because everything else in the suite passes either way.
//
// Setup is the same as test_adbc_catalog_scan -- see its header. In short:
// thirdparty must hold libadbc_driver_flightsql.so, and FE and every BE must
// be able to read the SAME file at the same absolute path.
// ############################################################################

suite("test_adbc_partitioned_read", "p0,external") {
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
        // Not a pass. Nothing about ADBC has been exercised by this run.
        logger.info("SKIPPED test_adbc_partitioned_read: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "THE ADBC PARTITIONED READ PATH IS NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    def backends = sql_return_maparray "show backends"
    int aliveBackends = backends.count { it.Alive.toString().equalsIgnoreCase("true") }
    assertTrue(aliveBackends >= 1, "no alive backend to read from: ${backends}")
    logger.info("adbc partitioned read suite targets arrow flight sql port ${arrowPort} "
            + "with ${aliveBackends} alive backend(s), driver ${driverPath}")

    String catalogName = "test_adbc_partitioned_read_catalog"
    String limitedCatalog = "test_adbc_partitioned_read_count_probe"
    String dbName = "test_adbc_partitioned_read_db"

    // Enough buckets that every backend ends up holding some of the table. A backend with no tablet of
    // this table contributes no partition, and the count below would then be under the backend count for
    // a reason that has nothing to do with the connector.
    int buckets = 8 * aliveBackends
    int rows = 10000

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP CATALOG IF EXISTS ${limitedCatalog}"""
    sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
    sql """CREATE DATABASE ${dbName}"""

    sql """
        CREATE TABLE ${dbName}.wide (
          `id` int NOT NULL,
          `name` varchar(64) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS ${buckets}
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO ${dbName}.wide
        SELECT number, concat('n', number) FROM numbers("number" = "${rows}")
    """

    def adbcCatalog = { String name, String extraProperties ->
        sql """
            CREATE CATALOG ${name} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${driverPath}",
                "uri" = "grpc://127.0.0.1:${arrowPort}",
                "user" = "root",
                "password" = "",
                "partitioned_read" = "required"
                ${extraProperties}
            )
        """
    }

    // 'required' throughout, never 'auto': under 'auto' a driver that stopped partitioning would be
    // silently downgraded to the single-statement path and every assertion below would still hold, on a
    // run that tested nothing this suite is named after.
    adbcCatalog(catalogName, "")

    // ---- how many partitions the source made ----
    //
    // Read back rather than assumed. A catalog capped at one partition has exactly two outcomes, and both
    // are informative: it answers the query, which it can only do if there really was one partition, or it
    // refuses and names the number it found. So the count below is observed, never a guess about how the
    // source plans -- and the suite asserts a floor on it instead of an exact value, because how far a
    // source splits a scan beyond one partition per backend is the source's business, not the connector's.
    adbcCatalog(limitedCatalog, """, "max_partitions" = "1" """)
    int partitions
    try {
        def atOnePartition = sql """SELECT min(id) FROM ${limitedCatalog}.${dbName}.wide"""
        assertEquals(0, atOnePartition[0][0])
        partitions = 1
    } catch (Exception e) {
        def matcher = (e.getMessage() =~ /into (\d+) partitions/)
        assertTrue(matcher.find(), "a scan capped at one partition failed for an unexpected reason, so "
                + "the partition count could not be read: ${e.getMessage()}")
        partitions = matcher.group(1) as int
    }
    logger.info("the source split the scan into ${partitions} partition(s) "
            + "across ${aliveBackends} alive backend(s)")

    if (aliveBackends > 1) {
        // The point of the feature. With 8 buckets per backend and one replica, every backend holds part
        // of this table, so a source that split the scan per backend cannot report fewer partitions than
        // it has backends. Fewer means the scan was NOT split across them -- which is the failure this
        // suite exists to catch, whatever else still passes.
        assertTrue(partitions >= aliveBackends,
                "a ${aliveBackends}-backend source split this scan into only ${partitions} partition(s), "
                        + "so it was not read across backends. If some backend legitimately holds no "
                        + "tablet of ${dbName}.wide, that premise -- not the connector -- is what broke.")
    } else {
        // Said out loud because everything below still passes on one backend: it is verified at ONE
        // partition, which is correct here and is no evidence at all about the case this suite is named
        // after. A green run on a single-backend cluster does not mean cross-backend reads work.
        assertEquals(1, partitions)
        logger.info("test_adbc_partitioned_read ran against a SINGLE-BACKEND source, so the scan was "
                + "read as ONE partition. Everything below is verified, but reading N partitions across "
                + "N backends is NOT covered by this run.")
    }

    // ---- reading every partition loses nothing and repeats nothing ----
    //
    // Checked against the source table itself rather than against a second read of the ADBC catalog: the
    // right answer is known here, so a fault shared by both read paths cannot hide in an agreement.
    // count(*) alone would miss a partition read twice that also lost one; distinct and sum would miss
    // a duplicate of an entire partition. Together they do not.
    def expected = sql """
        SELECT count(*), count(DISTINCT id), sum(id), min(id), max(id) FROM ${dbName}.wide
    """
    def actual = sql """
        SELECT count(*), count(DISTINCT id), sum(id), min(id), max(id)
        FROM ${catalogName}.${dbName}.wide
    """
    assertEquals(expected.toString(), actual.toString(),
            "a partitioned read returned different data than the source table holds")

    // A column the projection did not ask for is rejected by BE, so this also covers the partition path
    // carrying the projection through: every range must read the same two columns.
    def projected = sql """
        SELECT count(*), count(DISTINCT name) FROM ${catalogName}.${dbName}.wide
    """
    assertEquals(rows, projected[0][0])
    assertEquals(rows, projected[0][1])

    // A predicate is pushed into the statement the source partitions, so each partition must come back
    // already filtered -- and the total must still be exact.
    def filtered = sql """
        SELECT count(*), min(id), max(id) FROM ${catalogName}.${dbName}.wide WHERE id >= ${rows - 10}
    """
    assertEquals(10, filtered[0][0])
    assertEquals(rows - 10, filtered[0][1])
    assertEquals(rows - 1, filtered[0][2])

    sql """DROP CATALOG ${limitedCatalog}"""
    sql """DROP CATALOG ${catalogName}"""
    sql """DROP DATABASE ${dbName} FORCE"""
}
