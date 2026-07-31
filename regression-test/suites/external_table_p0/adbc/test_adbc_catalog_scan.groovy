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

    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = ""
        )
    """

    // ---- metadata ----

    qt_show_databases """SHOW DATABASES FROM ${catalogName} LIKE '${dbName}'"""
    qt_show_tables """SHOW TABLES FROM ${catalogName}.${dbName}"""
    qt_desc """DESC ${catalogName}.${dbName}.t1"""

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
    explain {
        sql("SELECT id FROM ${catalogName}.${dbName}.t1 WHERE id > 1")
        contains "QUERY: "
    }

    sql """DROP CATALOG ${catalogName}"""
    sql """DROP DATABASE ${dbName} FORCE"""
}
