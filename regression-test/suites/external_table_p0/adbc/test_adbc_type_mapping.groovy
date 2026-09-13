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
// The scalar type surface, read through ADBC from a source that is another
// Doris over Arrow Flight SQL.
//
// A wrong type mapping does not fail: it returns wrong data. That is why every
// table here is checked TWICE and in two different ways.
//
//   qt_desc_*    what Doris says the external column is. This is the mapping
//                itself, and the .out baseline is the only place it is written
//                down. Read every line of it before committing.
//   qt_select_*  the values, as a baseline.
//   sameAsSource the same values compared against a NATIVE read of the very
//                same Doris table. This is the assertion that cannot be
//                fooled: a .out generated from a run that already corrupted a
//                value would record the corruption and pass forever after,
//                whereas the native read is an independent path to a known
//                answer. If the two disagree, ADBC changed the data.
//
// The whole round trip is derivable from two files, so what each column
// SHOULD come back as is not a guess:
//   be/src/format/arrow/arrow_row_batch.cpp  Doris type -> Arrow type
//   AdbcTypeMapper.java                      Arrow type -> external column type
// The types with a lossy or surprising round trip (LARGEINT, IPV4, JSON,
// VARIANT, BITMAP ...) are deliberately NOT here: they live in
// test_adbc_type_semantic and test_adbc_type_binary_state so that a defect in
// one of them cannot stop this suite from running.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_type_mapping", "p0,external") {
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
        logger.info("SKIPPED test_adbc_type_mapping: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "THE ADBC TYPE MAPPING IS NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_type_mapping_catalog"
    String dbName = "test_adbc_type_mapping_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // ---- the fixture, in the source ----

    // Integers, at the ends of every range. The boundary values are the point: an integer mapped one
    // width too narrow reads back correctly for small numbers and wraps silently at the top of the range,
    // so a fixture of small values would pass on a broken mapping.
    sql """
        CREATE TABLE internal.${dbName}.t_int (
          `id` int NOT NULL,
          `c_bool` boolean NULL,
          `c_tinyint` tinyint NULL,
          `c_smallint` smallint NULL,
          `c_int` int NULL,
          `c_bigint` bigint NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_int VALUES
          (1, false, -128, -32768, -2147483648, -9223372036854775808),
          (2, true, 127, 32767, 2147483647, 9223372036854775807),
          (3, false, 0, 0, 0, 0),
          (4, NULL, NULL, NULL, NULL, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_float (
          `id` int NOT NULL,
          `c_float` float NULL,
          `c_double` double NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // No NaN or infinity on purpose: neither has a portable spelling, the connector's literal renderer
    // refuses them by design (AnsiDialect.renderLiteral), and their text form differs between Doris
    // versions -- a difference this suite would report as an ADBC fault.
    sql """
        INSERT INTO internal.${dbName}.t_float VALUES
          (1, -1.5, -2.25),
          (2, 3.4028235E38, 1.7976931348623157E308),
          (3, 0.0, 0.0),
          (4, 1.4E-45, 4.9E-324),
          (5, NULL, NULL)
    """

    // One table per decimal width, because each takes a different branch: DECIMAL32/64/128 all become an
    // Arrow Decimal128 and only DECIMAL256 becomes a Decimal256 (arrow_row_batch.cpp:111).
    sql """
        CREATE TABLE internal.${dbName}.t_decimal (
          `id` int NOT NULL,
          `c_d32` decimalv3(9, 2) NULL,
          `c_d64` decimalv3(18, 6) NULL,
          `c_d128` decimalv3(38, 18) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_decimal VALUES
          (1, 1234567.89, 123456789012.345678, 12345678901234567890.123456789012345678),
          (2, -1234567.89, -123456789012.345678, -12345678901234567890.123456789012345678),
          (3, 0.00, 0.000000, 0.000000000000000000),
          (4, 0.01, 0.000001, 0.000000000000000001),
          (5, NULL, NULL, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_string (
          `id` int NOT NULL,
          `c_char` char(16) NULL,
          `c_varchar` varchar(64) NULL,
          `c_string` string NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Row 2 carries the quote that makes the escaping assertions elsewhere mean something, row 3 is
    // multibyte, row 4 is empty rather than null -- a distinction a source that maps "" to NULL loses --
    // and row 5 is long enough to cross an inline-string boundary.
    sql """
        INSERT INTO internal.${dbName}.t_string VALUES
          (1, 'plain', 'plain varchar', 'plain string'),
          (2, "O'Brien", "quote ' inside", "back\\\\slash and quote '"),
          (3, '中文', '中文 emoji 🍺', '多字节 utf8 中文'),
          (4, '', '', ''),
          (5, 'padded', 'x', repeat('ab', 2000)),
          (6, NULL, NULL, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_datetime (
          `id` int NOT NULL,
          `c_date` date NULL,
          `c_dt0` datetime(0) NULL,
          `c_dt3` datetime(3) NULL,
          `c_dt6` datetime(6) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // The three datetime scales take three different Arrow branches -- SECOND, MILLI and MICRO
    // (arrow_row_batch.cpp:103) -- so a fixture with only whole seconds would not tell them apart. The
    // fractional digits are what proves the scale survived.
    sql """
        INSERT INTO internal.${dbName}.t_datetime VALUES
          (1, '1900-01-01', '1900-01-01 00:00:00', '1900-01-01 00:00:00.001', '1900-01-01 00:00:00.000001'),
          (2, '2024-02-29', '2024-02-29 23:59:59', '2024-02-29 23:59:59.999', '2024-02-29 23:59:59.999999'),
          (3, '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59.123', '9999-12-31 23:59:59.123456'),
          (4, NULL, NULL, NULL, NULL)
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
        // Reads the same rows twice -- once through ADBC, once natively from the table the source is
        // serving -- and demands they be identical. Independent of the .out baselines, and the only check
        // here that a wrong baseline cannot satisfy.
        def sameAsSource = { String table, String columns ->
            def viaAdbc = sql("SELECT ${columns} FROM ${catalogName}.${dbName}.${table} ORDER BY id")
            def viaSource = sql("SELECT ${columns} FROM internal.${dbName}.${table} ORDER BY id")
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "reading ${table}(${columns}) through ADBC returned different values than a native "
                            + "read of the same source table")
        }

        // ---- integers ----

        qt_desc_int """DESC ${catalogName}.${dbName}.t_int"""
        qt_select_int """
            SELECT id, c_bool, c_tinyint, c_smallint, c_int, c_bigint
            FROM ${catalogName}.${dbName}.t_int ORDER BY id
        """
        sameAsSource("t_int", "id, c_bool, c_tinyint, c_smallint, c_int, c_bigint")

        // Aggregated rather than listed, because a width that silently wrapped would still produce four
        // rows: only the extremes reveal it.
        qt_select_int_range """
            SELECT min(c_tinyint), max(c_tinyint), min(c_smallint), max(c_smallint),
                   min(c_int), max(c_int), min(c_bigint), max(c_bigint)
            FROM ${catalogName}.${dbName}.t_int
        """

        // ---- floating point ----

        qt_desc_float """DESC ${catalogName}.${dbName}.t_float"""
        qt_select_float """SELECT id, c_float, c_double FROM ${catalogName}.${dbName}.t_float ORDER BY id"""

        // Compared INSIDE Doris rather than through sameAsSource, and not because ADBC needs the
        // help: the fixture's DBL_MAX cannot make the trip to the test client at all. Doris renders a
        // double with 16 significant digits, so 1.7976931348623157E308 comes back as the text
        // 1.797693134862316e+308 -- a value ABOVE DBL_MAX, which parses to infinity and makes the
        // JDBC driver throw "Value '∞' is outside of valid range" before any comparison happens. The
        // native read of the source table prints exactly the same text, so this is Doris's own
        // double-to-text rounding, not an ADBC fault, and swapping the fixture for a rounder number
        // would drop the one row that proves a double is not narrowed to a float somewhere in the
        // Arrow round trip. <=> is null-safe, so row 5's nulls have to match as nulls, and the join
        // makes every value a bit-for-bit comparison the client never sees.
        def floatRowsMatched = sql("""
            SELECT count(*) FROM ${catalogName}.${dbName}.t_float a
            JOIN internal.${dbName}.t_float s ON a.id = s.id
            WHERE (a.c_float <=> s.c_float) AND (a.c_double <=> s.c_double)
        """)[0][0]
        assertEquals(5L, floatRowsMatched as long,
                "reading t_float(c_float, c_double) through ADBC returned different values than a "
                        + "native read of the same source table")

        // ---- decimals ----

        qt_desc_decimal """DESC ${catalogName}.${dbName}.t_decimal"""
        qt_select_decimal """
            SELECT id, c_d32, c_d64, c_d128 FROM ${catalogName}.${dbName}.t_decimal ORDER BY id
        """
        sameAsSource("t_decimal", "id, c_d32, c_d64, c_d128")

        // Scale is the part that fails quietly: a decimal read one digit off still looks like a number.
        // Summing forces every digit of every row through the comparison.
        qt_select_decimal_sum """
            SELECT sum(c_d32), sum(c_d64), sum(c_d128) FROM ${catalogName}.${dbName}.t_decimal
        """

        // ---- strings ----

        qt_desc_string """DESC ${catalogName}.${dbName}.t_string"""
        qt_select_string """
            SELECT id, c_char, c_varchar, c_string FROM ${catalogName}.${dbName}.t_string ORDER BY id
        """
        sameAsSource("t_string", "id, c_char, c_varchar, c_string")

        // Lengths, because the values above are also compared as text: this separates "the string is
        // wrong" from "the string was truncated", and pins the empty-string row as empty rather than null.
        qt_select_string_length """
            SELECT id, length(c_char), length(c_varchar), length(c_string),
                   c_varchar IS NULL, c_string IS NULL
            FROM ${catalogName}.${dbName}.t_string ORDER BY id
        """

        // ---- dates and timestamps ----

        qt_desc_datetime """DESC ${catalogName}.${dbName}.t_datetime"""
        qt_select_datetime """
            SELECT id, c_date, c_dt0, c_dt3, c_dt6
            FROM ${catalogName}.${dbName}.t_datetime ORDER BY id
        """
        // Cast back to the source's own type before comparing, and only for the datetime columns: a
        // Doris source stamps the session's zone onto every DATETIMEV2 it writes to Arrow (the Arrow
        // timestamp type carries a zone), so this connector maps them to TIMESTAMPTZ and the values
        // come back rendered with a +hh:mm offset that the native read does not print. The cast is a
        // no-op on the source side, so what remains under comparison is the instant -- which is the
        // thing that must survive. The rendered form is pinned by qt_select_datetime above. c_date is
        // left alone: DATEV2 has no zone to stamp and comes back a plain date.
        sameAsSource("t_datetime", "id, c_date, CAST(c_dt0 AS DATETIME(0)), "
                + "CAST(c_dt3 AS DATETIME(3)), CAST(c_dt6 AS DATETIME(6))")

        // The sub-second digits on their own. A scale collapsed to seconds still prints a plausible
        // timestamp, and the microsecond column is where a nanosecond source would be truncated.
        qt_select_datetime_micros """
            SELECT id, microsecond(c_dt3), microsecond(c_dt6), year(c_date), day(c_date)
            FROM ${catalogName}.${dbName}.t_datetime ORDER BY id
        """

        // ---- DECIMAL256, the only branch that is not a Decimal128 ----
        //
        // Last on purpose. It needs a session variable, so if the feature is off in this build the failure
        // lands here rather than taking every assertion above with it.
        sql """SET enable_decimal256 = true"""
        sql """
            CREATE TABLE internal.${dbName}.t_decimal256 (
              `id` int NOT NULL,
              `c_d256` decimalv3(76, 20) NULL
            ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO internal.${dbName}.t_decimal256 VALUES
              (1, 123456789012345678901234567890123456789012345678901234.12345678901234567890),
              (2, -123456789012345678901234567890123456789012345678901234.12345678901234567890),
              (3, 0.00000000000000000001),
              (4, NULL)
        """
        // A new table in a database the catalog has already listed, so this also travels the path where
        // the connector must not answer a table lookup from its own cache.
        qt_desc_decimal256 """DESC ${catalogName}.${dbName}.t_decimal256"""
        qt_select_decimal256 """
            SELECT id, c_d256 FROM ${catalogName}.${dbName}.t_decimal256 ORDER BY id
        """
        sameAsSource("t_decimal256", "id, c_d256")
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
