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
// The source is another Doris, reached over Arrow Flight SQL -- the source
// phase one exists to replace. The first real run of this suite found two
// defects that no unit test could have: ANSI identifier quoting, which a Doris
// source cannot parse at all, and a driver stream whose release callback left
// Arrow C++ to abort the whole BE.
//
// This driver supports partitioned execution, so the queries below take the
// partitioned path: the query runs on the source at planning time and each
// result partition is read back as its own scan range. A single-backend source
// reports one partition, so what is proven here is that the partition path
// returns the right rows -- NOT that it parallelizes.
//
// The catalog sets partitioned_read=required ON PURPOSE. Under the default
// 'auto' a driver that stopped partitioning would be silently downgraded to the
// single-statement path, every assertion below would still pass, and the run
// would look exactly like a successful test of the partitioned path. 'required'
// turns that into a failure, which is the only way these assertions can mean
// what they say.
//
// To run it you need, in this order:
//   1. BE built with arrow-adbc (cd thirdparty && ./build-thirdparty.sh arrow_adbc),
//      which also installs the prebuilt libadbc_driver_flightsql.so into
//      thirdparty. It is a test artifact: no release of Doris ships it.
//   2. FE and every BE able to read that file at the same absolute path. They
//      must load the SAME file -- partition descriptors are driver-private
//      bytes with no interoperability across builds.
//   3. adbcDriverPath in regression-conf.groovy only to point somewhere else.
//
// The .out baseline was generated with -genOut from a run that passed end to
// end, and every line was checked against the fixture before committing. Note
// that -genOut records instead of comparing, so a run under it proves only
// that nothing threw: the run that proves the values is the one without it.
// ############################################################################

suite("test_adbc_catalog_scan", "p0,external") {
    // suitePath is <repo>/regression-test/suites, so two levels up is the repo root.
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
        logger.info("SKIPPED test_adbc_catalog_scan: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "THE ADBC SCAN PATH IS NOT BEING TESTED.")
        return
    }

    // The source is this cluster's own Arrow Flight SQL endpoint, the same one the remote_doris
    // suites use. It is the phase-one target source, and it needs no extra container.
    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]
    logger.info("adbc suite targets arrow flight sql port ${arrowPort}, driver ${driverPath}")

    String catalogName = "test_adbc_catalog_scan_catalog"
    String dbName = "test_adbc_catalog_scan_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
    sql """CREATE DATABASE ${dbName}"""

    sql """
        CREATE TABLE ${dbName}.t1 (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `score` double NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${dbName}.t1 VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, 3.5)"""

    // Exists only to be absent below. A Doris source is the one that does NOT honour the base-table
    // filter the connector sends -- its Flight SQL endpoint recognises the literal "VIEW" and answers
    // anything else with every object it has -- so this is the source that has to prove views stay out.
    sql """CREATE VIEW ${dbName}.v1 AS SELECT id, name FROM ${dbName}.t1"""

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

    // ---- metadata ----

    qt_show_databases """SHOW DATABASES FROM ${catalogName} LIKE '${dbName}'"""
    qt_show_tables """SHOW TABLES FROM ${catalogName}.${dbName}"""
    qt_desc """DESC ${catalogName}.${dbName}.t1"""

    // Spelled out as well as baselined: the baseline above would also stay green if SHOW TABLES broke
    // the other way and returned nothing, and a leaked view is the failure that hides -- it reads fine
    // through ADBC, so a catalog offering one looks entirely healthy.
    def tableNames = sql("""SHOW TABLES FROM ${catalogName}.${dbName}""").collect { it[0] } as Set
    assertTrue(tableNames.contains("t1"), "t1 missing from ${tableNames}")
    assertFalse(tableNames.contains("v1"), "the view v1 was surfaced as a table: ${tableNames}")

    // ---- scan ----

    qt_select_all """SELECT id, name, score FROM ${catalogName}.${dbName}.t1 ORDER BY id"""

    // A projection, because BE rejects any column the statement did not ask for: this fails outright
    // rather than over-reading if FE ever widens the select list.
    qt_select_projection """SELECT id FROM ${catalogName}.${dbName}.t1 ORDER BY id"""

    // Predicates from the pushable set. The answer must be the same whether or not they were pushed,
    // since BE re-applies them either way.
    qt_select_where """SELECT id, name FROM ${catalogName}.${dbName}.t1 WHERE id > 1 ORDER BY id"""
    qt_select_where_null """SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name IS NULL"""
    qt_select_where_in """SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id IN (1, 3) ORDER BY id"""
    qt_select_where_string """SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name = 'alice'"""

    // A predicate outside the pushable set. It stays in Doris, and the rows must still be right --
    // this is the case that silently returns extra rows if the limit interacts with it wrongly.
    qt_select_unpushable """SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name LIKE 'a%'"""
    qt_select_unpushable_limit """SELECT id FROM ${catalogName}.${dbName}.t1 WHERE name LIKE '%b%' LIMIT 5"""

    qt_select_limit """SELECT id FROM ${catalogName}.${dbName}.t1 ORDER BY id LIMIT 2"""

    // COUNT(*) projects no columns at all; BE counts rows without materializing any.
    qt_select_count """SELECT count(*) FROM ${catalogName}.${dbName}.t1"""

    // ---- what EXPLAIN shows ----

    // The statement sent to the source, which must be the one planScan would build. A pushed
    // predicate appears in it; the LIKE above must not.
    //
    // EXPLAIN plans the scan for real -- that is where inputSplitNum comes from -- so this also
    // covers the rule that describing a query must not partition it: asking this source for
    // partitions executes the query, and an EXPLAIN that did so would run the very query it was
    // asked only to describe. Nothing here can see that from the outside, so the guarantee lives in
    // AdbcScanPlanProviderTest; what this proves is that EXPLAIN still works and still shows the
    // statement.
    explain {
        sql("SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id > 1")
        contains "QUERY: "
    }

    // ---- the two planning paths must agree ----
    //
    // Everything above ran through the partitioned path: this driver splits a query into the
    // source's own result partitions and Doris reads each one on its own backend. A second catalog
    // with that turned off reads the same tables by sending the statement to one backend, the way
    // the JDBC connector always has. The rows must be identical -- that, not the range count, is
    // what a user notices if the partition path drops or duplicates anything.
    //
    // LIMITED BY THIS SETUP: the source is a single-backend cluster, so it reports ONE partition per
    // query and the partitioned path here is exercised at N=1. Reading N partitions across N
    // backends needs a multi-backend source, and nothing below proves it works.
    String singleRangeCatalog = "test_adbc_catalog_scan_single_range"
    sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
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

    // Compared as values rather than against a baseline: a baseline would pass if BOTH paths broke
    // the same way, and the point here is that the two agree. The partitioned side is pinned to
    // 'required', so "they agree" cannot be satisfied by both sides quietly being the same path.
    def sameOnBothPaths = { String query ->
        def partitioned = sql(query.replace('@CATALOG@', catalogName))
        def singleRange = sql(query.replace('@CATALOG@', singleRangeCatalog))
        assertEquals(singleRange.toString(), partitioned.toString(),
                "partitioned and single-range planning disagreed on: ${query}")
        return partitioned
    }

    def allRows = sameOnBothPaths(
            "SELECT id, name, score FROM @CATALOG@.${dbName}.t1 ORDER BY id")
    assertEquals(3, allRows.size())

    sameOnBothPaths("SELECT id FROM @CATALOG@.${dbName}.t1 ORDER BY id")
    sameOnBothPaths("SELECT id, name FROM @CATALOG@.${dbName}.t1 WHERE id > 1 ORDER BY id")
    sameOnBothPaths("SELECT id FROM @CATALOG@.${dbName}.t1 WHERE name IS NULL")
    sameOnBothPaths("SELECT id FROM @CATALOG@.${dbName}.t1 WHERE name LIKE 'a%'")
    sameOnBothPaths("SELECT id FROM @CATALOG@.${dbName}.t1 ORDER BY id LIMIT 2")
    // Projects no columns at all, so BE counts rows without materializing any -- on both paths.
    sameOnBothPaths("SELECT count(*) FROM @CATALOG@.${dbName}.t1")

    sql """DROP CATALOG ${singleRangeCatalog}"""
    sql """DROP CATALOG ${catalogName}"""
    sql """DROP DATABASE ${dbName} FORCE"""
}
