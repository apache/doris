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
// The aggregate-state types: BITMAP, HLL, QUANTILE_STATE -- and AGG_STATE,
// which the source cannot serialise at all.
//
// Held apart from every other type suite because this is the family most
// likely to fail, and a failure here must not stop the ordinary types from
// being checked. A Doris source turns the first three into Arrow BINARY
// (arrow_row_batch.cpp:157) and the connector maps binary to STRING, so what
// arrives is the SERIALISED STATE as opaque bytes -- not a bitmap, and not
// anything bitmap_count can be pointed at.
//
// The question this suite answers first is the one that actually matters to a
// user: a table that merely CONTAINS such a column must still be usable for
// its other columns. That is asserted for all three types before anything
// touches the exotic column itself, so a defect in reading the bytes cannot
// hide the more important answer.
//
// The probes that follow are exactly that -- probes. They record what comes
// back. A red line there is a real finding about the ADBC read path, not a
// test that guessed wrong, and it should be triaged as such.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_type_binary_state", "p0,external") {
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
        logger.info("SKIPPED test_adbc_type_binary_state: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "THE AGGREGATE-STATE TYPES ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_type_binary_state_catalog"
    String dbName = "test_adbc_type_binary_state_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // Each state type needs an aggregate table, and each carries a plain column beside it -- that plain
    // column is what the first round of assertions reads.
    sql """
        CREATE TABLE internal.${dbName}.t_bitmap (
          `id` int NOT NULL,
          `tag` varchar(32) NOT NULL,
          `c_bitmap` bitmap BITMAP_UNION
        ) AGGREGATE KEY(`id`, `tag`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_bitmap VALUES
          (1, 'a', to_bitmap(100)),
          (2, 'b', to_bitmap(200)),
          (3, 'c', bitmap_empty())
    """

    sql """
        CREATE TABLE internal.${dbName}.t_hll (
          `id` int NOT NULL,
          `tag` varchar(32) NOT NULL,
          `c_hll` hll HLL_UNION
        ) AGGREGATE KEY(`id`, `tag`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_hll VALUES
          (1, 'a', hll_hash('one')),
          (2, 'b', hll_hash('two')),
          (3, 'c', hll_empty())
    """

    sql """
        CREATE TABLE internal.${dbName}.t_quantile (
          `id` int NOT NULL,
          `tag` varchar(32) NOT NULL,
          `c_quantile` quantile_state QUANTILE_UNION
        ) AGGREGATE KEY(`id`, `tag`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_quantile VALUES
          (1, 'a', to_quantile_state(1, 2048)),
          (2, 'b', to_quantile_state(2, 2048)),
          (3, 'c', to_quantile_state(3, 2048))
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
        // ---- first: a table with such a column is still usable ----
        //
        // Reading the ordinary columns never touches the state column -- the connector projects exactly
        // the requested columns -- so this must hold even if none of the probes below do. It is also the
        // realistic case: nobody selects a raw bitmap, they select the dimensions next to it.

        assertEquals(3, sql("""SELECT id, tag FROM ${catalogName}.${dbName}.t_bitmap""").size(),
                "a table with a BITMAP column could not be read for its ordinary columns")
        assertEquals(3, sql("""SELECT id, tag FROM ${catalogName}.${dbName}.t_hll""").size(),
                "a table with an HLL column could not be read for its ordinary columns")
        assertEquals(3, sql("""SELECT id, tag FROM ${catalogName}.${dbName}.t_quantile""").size(),
                "a table with a QUANTILE_STATE column could not be read for its ordinary columns")

        // count(*) projects no columns at all, so it is the cheapest proof the tables are scannable.
        qt_count_bitmap """SELECT count(*) FROM ${catalogName}.${dbName}.t_bitmap"""
        qt_count_hll """SELECT count(*) FROM ${catalogName}.${dbName}.t_hll"""
        qt_count_quantile """SELECT count(*) FROM ${catalogName}.${dbName}.t_quantile"""

        // A predicate on the ordinary column, which is what makes the state column's presence irrelevant
        // to the query rather than merely unselected.
        qt_filter_bitmap """
            SELECT id, tag FROM ${catalogName}.${dbName}.t_bitmap WHERE id > 1 ORDER BY id
        """

        // ---- then: how the state column itself maps ----

        qt_desc_bitmap """DESC ${catalogName}.${dbName}.t_bitmap"""
        qt_desc_hll """DESC ${catalogName}.${dbName}.t_hll"""
        qt_desc_quantile """DESC ${catalogName}.${dbName}.t_quantile"""

        def externalTypeOf = { String table, String column ->
            def described = sql("""DESC ${catalogName}.${dbName}.${table}""")
            def row = described.find { it[0] == column }
            assertNotNull(row, "column ${column} is missing from ${table}: ${described}")
            return row[1].toString().toLowerCase()
        }

        // Derivable rather than guessed: the source serialises all three as Arrow binary, and
        // AdbcTypeMapper has no binary column type to offer, so all three become strings.
        assertEquals("text", externalTypeOf("t_bitmap", "c_bitmap"),
                "BITMAP reaches the connector as Arrow binary, which maps to a string column")
        assertEquals("text", externalTypeOf("t_hll", "c_hll"),
                "HLL reaches the connector as Arrow binary, which maps to a string column")
        assertEquals("text", externalTypeOf("t_quantile", "c_quantile"),
                "QUANTILE_STATE reaches the connector as Arrow binary, which maps to a string column")

        // ---- last: reading the bytes ----
        //
        // PROBE. The value is a serialised aggregate state, so nothing about its content is asserted --
        // only that projecting the column returns a row per source row instead of failing the scan. If
        // this throws, the finding is in the BE read path for Arrow binary, and it belongs in a bug
        // report rather than in a change to this file.

        def bitmapRows = sql """SELECT id, length(c_bitmap) FROM ${catalogName}.${dbName}.t_bitmap ORDER BY id"""
        assertEquals(3, bitmapRows.size(), "projecting the BITMAP column changed the row count")
        logger.info("BITMAP state read through ADBC, byte lengths: ${bitmapRows}")

        def hllRows = sql """SELECT id, length(c_hll) FROM ${catalogName}.${dbName}.t_hll ORDER BY id"""
        assertEquals(3, hllRows.size(), "projecting the HLL column changed the row count")
        logger.info("HLL state read through ADBC, byte lengths: ${hllRows}")

        def quantileRows = sql """
            SELECT id, length(c_quantile) FROM ${catalogName}.${dbName}.t_quantile ORDER BY id
        """
        assertEquals(3, quantileRows.size(), "projecting the QUANTILE_STATE column changed the row count")
        logger.info("QUANTILE_STATE read through ADBC, byte lengths: ${quantileRows}")

        // ---- AGG_STATE: the source itself has no Arrow form for it ----
        //
        // convert_to_arrow_type has no case for AGG_STATE, so the SOURCE refuses before the connector ever
        // sees a type. The requirement is not that it work -- it cannot -- but that the refusal name the
        // table, because the connector reaches this while DESCRIBING and an error without the table in it
        // would leave a user with no idea which table is unreadable.
        //
        // The fixture is built defensively: AGG_STATE's DDL spelling has moved between versions, and this
        // suite must not turn "the CREATE TABLE syntax changed" into a red line about ADBC.
        boolean hasAggState = true
        try {
            sql """
                CREATE TABLE internal.${dbName}.t_aggstate (
                  `id` int NOT NULL,
                  `c_agg` agg_state<sum(int)> GENERIC
                ) AGGREGATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            sql """INSERT INTO internal.${dbName}.t_aggstate VALUES (1, sum_state(1))"""
        } catch (Exception e) {
            hasAggState = false
            logger.info("test_adbc_type_binary_state: SKIPPING the AGG_STATE case, this Doris could not "
                    + "build the fixture: ${e.getMessage()}")
        }

        if (hasAggState) {
            try {
                def described = sql("""DESC ${catalogName}.${dbName}.t_aggstate""")
                // Not asserted as a failure: if the source grows an Arrow form for AGG_STATE this starts
                // working, and that is not a regression. What is recorded is that it did.
                logger.info("a table with an AGG_STATE column could be described through ADBC: "
                        + "${described}")
            } catch (Exception e) {
                String message = e.getMessage() == null ? "" : e.getMessage()
                logger.info("a table with an AGG_STATE column failed to describe, as expected: ${message}")
                assertTrue(message.contains("t_aggstate"),
                        "the failure does not name the table, so a user cannot tell which table is "
                                + "unreadable: ${message}")
            }
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
