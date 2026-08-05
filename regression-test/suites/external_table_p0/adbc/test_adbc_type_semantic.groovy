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
// The types that survive the trip as DATA but not as a TYPE.
//
// Arrow is the whole contract between the two ends, and it has no LARGEINT, no
// JSON and no IP address. A Doris source therefore serialises each of them as
// something else (arrow_row_batch.cpp), and the connector maps back only what
// Arrow said:
//
//   LARGEINT -> utf8   -> STRING     value kept, arithmetic lost
//   JSON     -> utf8   -> STRING     text kept, json functions lost
//   VARIANT  -> utf8   -> STRING     text kept, sub-column access lost
//   IPV6     -> utf8   -> STRING     text kept
//   IPV4     -> int32  -> INT        *** the value itself changes form ***
//
// IPV4 is the one that is not merely a weaker type: a native read prints
// 192.168.1.1 and an ADBC read prints 3232235777. Nothing fails, so the only
// way anyone learns this is a test that says it out loud. Doris does attach
// doris_type=IPV4 metadata to the Arrow field (arrow_row_batch.cpp:178) -- the
// connector does not read it today, and the assertion below is what will fail,
// loudly and in the right place, on the day it does.
//
// Each type gets a table of its own so that a defect in one cannot stop the
// others from being described, and they are ordered by how likely the FIXTURE
// is to be the thing that breaks.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_type_semantic", "p0,external") {
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
        logger.info("SKIPPED test_adbc_type_semantic: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "THE LOSSY ADBC TYPE MAPPINGS ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_type_semantic_catalog"
    String dbName = "test_adbc_type_semantic_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.t_largeint (
          `id` int NOT NULL,
          `c_large` largeint NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // The extremes are the point: a LARGEINT that came back through a 64-bit type would still read
    // correctly for small values and be wrong only here. The negative bound is one short of the true
    // minimum on purpose -- writing -2^127 requires the parser to read 2^127 first, which is already out of
    // LARGEINT range, and a fixture that fails to build says nothing about ADBC.
    sql """
        INSERT INTO internal.${dbName}.t_largeint VALUES
          (1, 170141183460469231731687303715884105727),
          (2, -170141183460469231731687303715884105727),
          (3, 0),
          (4, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_json (
          `id` int NOT NULL,
          `c_json` json NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_json VALUES
          (1, '{"a": 1, "b": "two"}'),
          (2, '[1, 2, 3]'),
          (3, '{"nested": {"deep": [1, {"x": null}]}}'),
          (4, 'null'),
          (5, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_ipv6 (
          `id` int NOT NULL,
          `c_ip6` ipv6 NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_ipv6 VALUES
          (1, '::1'),
          (2, '2001:db8::ff00:42:8329'),
          (3, '::'),
          (4, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_ipv4 (
          `id` int NOT NULL,
          `c_ip4` ipv4 NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // 192.168.1.1 is 3232235777 and 255.255.255.255 is 4294967295 -- the latter does not fit in a SIGNED
    // int32, which is what a Doris source sends it as, so it is the row that shows whether the sign
    // survived the trip.
    sql """
        INSERT INTO internal.${dbName}.t_ipv4 VALUES
          (1, '192.168.1.1'),
          (2, '0.0.0.0'),
          (3, '255.255.255.255'),
          (4, NULL)
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
        def externalTypeOf = { String table, String column ->
            def described = sql("""DESC ${catalogName}.${dbName}.${table}""")
            def row = described.find { it[0] == column }
            assertNotNull(row, "column ${column} is missing from ${table}: ${described}")
            return row[1].toString().toLowerCase()
        }

        // The text form of the source value is what a lossless string mapping must produce. Compared
        // against the source rather than a baseline, because a baseline recorded from a run that mangled a
        // digit would record the mangled digit.
        def sameAsSourceText = { String table, String column ->
            def viaAdbc = sql("SELECT id, ${column} FROM ${catalogName}.${dbName}.${table} ORDER BY id")
            def viaSource = sql(
                    "SELECT id, CAST(${column} AS STRING) FROM internal.${dbName}.${table} ORDER BY id")
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "${table}.${column} read through ADBC does not match the source value as text")
        }

        // ---- LARGEINT: a 128-bit integer arrives as text ----

        qt_desc_largeint """DESC ${catalogName}.${dbName}.t_largeint"""
        qt_select_largeint """
            SELECT id, c_large FROM ${catalogName}.${dbName}.t_largeint ORDER BY id
        """
        assertEquals("text", externalTypeOf("t_largeint", "c_large"),
                "LARGEINT is serialised as Arrow utf8 by the source, so the external column is a string. "
                        + "If this now reports largeint, the round trip changed and the baseline above "
                        + "describes the old one.")
        sameAsSourceText("t_largeint", "c_large")

        // ---- JSON: the document arrives as its text ----

        qt_desc_json """DESC ${catalogName}.${dbName}.t_json"""
        qt_select_json """SELECT id, c_json FROM ${catalogName}.${dbName}.t_json ORDER BY id"""
        assertEquals("text", externalTypeOf("t_json", "c_json"),
                "JSON is serialised as Arrow utf8, so the external column is a string")
        sameAsSourceText("t_json", "c_json")

        // The functional consequence, stated as a query rather than as a comment: the text is still
        // parseable, so a user has a way back to the document even though the column is not JSON.
        qt_select_json_extract """
            SELECT id, json_extract(c_json, '\$.a') FROM ${catalogName}.${dbName}.t_json ORDER BY id
        """

        // ---- IPV6: the address arrives as its text ----

        qt_desc_ipv6 """DESC ${catalogName}.${dbName}.t_ipv6"""
        qt_select_ipv6 """SELECT id, c_ip6 FROM ${catalogName}.${dbName}.t_ipv6 ORDER BY id"""
        assertEquals("text", externalTypeOf("t_ipv6", "c_ip6"),
                "IPV6 is serialised as Arrow utf8, so the external column is a string")

        // ---- IPV4: the address arrives as a NUMBER ----
        //
        // The one mapping in this suite that changes the value's form rather than just its type. Both
        // assertions below are documenting a known loss, not endorsing it: when the connector learns to
        // read the doris_type=IPV4 field metadata, they are what will point at this file.

        qt_desc_ipv4 """DESC ${catalogName}.${dbName}.t_ipv4"""
        qt_select_ipv4 """SELECT id, c_ip4 FROM ${catalogName}.${dbName}.t_ipv4 ORDER BY id"""

        String ipv4External = externalTypeOf("t_ipv4", "c_ip4")
        assertEquals("int", ipv4External,
                "A Doris source sends IPV4 as Arrow int32 (arrow_row_batch.cpp:80) and the connector maps "
                        + "int32 to INT, so the external column is a plain integer. Getting 'ipv4' here "
                        + "means the connector started honouring the doris_type field metadata -- a "
                        + "welcome change, but then the baselines above and the value check below all "
                        + "describe the old behaviour and must be regenerated.")

        // Spelled out because it is the surprise: the address arrives as the address's 32 bits read
        // as a SIGNED integer, so every address above 127.255.255.255 is negative. Doris itself
        // chose that encoding on both sides of the wire -- FlightSqlSchemaHelper describes IPV4 as
        // Int(32, signed) and arrow_row_batch.cpp writes arrow::int32() with a comment saying why --
        // so the connector has nothing better to map: the address's unsignedness is not in the Arrow
        // type it is handed. 255.255.255.255 arriving as -1 is the same statement said loudly.
        def ipv4Rows = sql("""
            SELECT id, c_ip4 FROM ${catalogName}.${dbName}.t_ipv4 WHERE id IN (1, 3) ORDER BY id
        """)
        assertEquals(2, ipv4Rows.size())
        logger.info("IPV4 read through ADBC is numeric: ${ipv4Rows}")
        assertEquals("-1062731519", ipv4Rows[0][1].toString(),
                "192.168.1.1 is 3232235777 unsigned, which is -1062731519 as a signed int32 -- the "
                        + "same 32 bits. Getting 3232235777 here means the value stopped being "
                        + "truncated (a wider Doris type, or the connector honouring the doris_type "
                        + "field metadata); that is the fix, and this assertion is then what points "
                        + "at the baselines above that describe the lossy behaviour.")
        assertEquals("-1", ipv4Rows[1][1].toString(),
                "255.255.255.255 is 4294967295 unsigned, all 32 bits set, which reads as -1 signed")

        // ---- VARIANT ----
        //
        // Last: the fixture itself is the least portable thing in this suite.
        sql """
            CREATE TABLE internal.${dbName}.t_variant (
              `id` int NOT NULL,
              `c_var` variant NULL
            ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO internal.${dbName}.t_variant VALUES
              (1, '{"a": 1, "b": "two"}'),
              (2, '{"nested": {"deep": 3}}'),
              (3, NULL)
        """

        qt_desc_variant """DESC ${catalogName}.${dbName}.t_variant"""
        qt_select_variant """SELECT id, c_var FROM ${catalogName}.${dbName}.t_variant ORDER BY id"""
        assertEquals("text", externalTypeOf("t_variant", "c_var"),
                "VARIANT is serialised as Arrow utf8, so the external column is a string and its "
                        + "sub-columns are no longer addressable through the catalog")
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
