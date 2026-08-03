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

// Reading the lake side of a fluss table on its own, through `tbl$lake`.
//
// This is the first place the delegation runs for real. The fluss plugin bundles
// no paimon at all: it asks the plugin manager for a paimon connector, hands it
// synthesized catalog properties, and routes the scan to the handle that
// connector returns. In unit tests the sibling is a stand-in on the same class
// loader, so what only a deployed run can show is that a real plugin is found,
// that a handle crossing the plugin boundary is routed rather than cast, and
// that paimon's own ServiceLoader lookups resolve under the class loader the
// call is pinned to.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and
// are frozen: the tiering service is stopped before the log tail is written, so
// exactly the rows below are in paimon and no others ever will be.
suite("test_fluss_lake_only", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_lake_only"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}"
        );
    """
    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    // The connector is wired into the v2 file scanner only, and fuzzy sessions
    // randomize this variable.
    sql """set enable_file_scanner_v2 = true"""

    def scalarOf = { String query -> sql(query)[0][0].toString() }

    // --- the lake holds what was tiered, and nothing that came after ---------
    // lake_log got four rows before the tiering service was stopped and two
    // after. Reading the lake alone must return the first four: a $lake that
    // quietly fell back to the fluss read would return all six.
    def lakeRows = sql """select id, name, price from lake_log\$lake order by id"""
    assertEquals(4, lakeRows.size())
    assertEquals(["1", "lake1", "1.10"], lakeRows[0].collect { it.toString() })
    assertEquals(["2", "lake2", "2.20"], lakeRows[1].collect { it.toString() })
    assertEquals(["3", "lake3", "3.30"], lakeRows[2].collect { it.toString() })
    assertEquals(["4", "lake4", "4.40"], lakeRows[3].collect { it.toString() })

    // --- the three columns fluss adds to every lake table --------------------
    // They belong to the lake table and not to the fluss one, which is the whole
    // reason the two are exposed as separate tables rather than one.
    def systemColumns = sql """
        select count(*) from lake_log\$lake
            where __bucket >= 0 and __bucket < 3
              and __offset >= 0
              and __timestamp is not null
    """
    assertEquals("4", systemColumns[0][0].toString())

    def lakeSchema = sql """desc lake_log\$lake"""
    def lakeColumnNames = lakeSchema.collect { it[0].toString() }
    assertEquals(["id", "name", "price", "__bucket", "__offset", "__timestamp"], lakeColumnNames)

    // The fluss table itself has none of them.
    def flussColumnNames = sql("""desc lake_log""").collect { it[0].toString() }
    assertEquals(["id", "name", "price"], flussColumnNames)

    // --- type parity between the two doors ----------------------------------
    // The connector's fluss->Doris mapping has to equal fluss->paimon->Doris, or
    // `tbl` and `tbl$lake` present two different schemas for one table. Until now
    // that was checked by reading both mappings side by side; here the second one
    // is the paimon connector actually running.
    def typesOf = { String table ->
        def result = [:]
        sql("""desc ${table}""").each { row -> result.put(row[0].toString(), row[1].toString()) }
        return result
    }
    def flussTypes = typesOf("lake_types")
    def lakeTypes = typesOf("lake_types\$lake")
    flussTypes.each { column, type ->
        assertEquals(type, lakeTypes.get(column),
                "column ${column} is ${type} on the fluss table but ${lakeTypes.get(column)} on its lake")
    }
    assertEquals(flussTypes.size() + 3, lakeTypes.size())

    // Parity of the values, not just of the declared types: row 1 of lake_types
    // is compared against itself read the other way round, through fluss rather
    // than paimon. Two decoders, one row, one set of literals.
    def lakeTypeRow = sql """
        select count(*) from lake_types\$lake where id = 1
            and f_boolean = true
            and f_tinyint = 1 and f_smallint = 2 and f_int = 3 and f_bigint = 4
            and f_float = cast(1.5 as float) and f_double = 2.5
            and f_decimal = 123.4567
            and f_char = 'char1' and f_string = 'string1'
            and hex(f_binary) = '010203' and hex(f_bytes) = '0A0B'
            and f_date = '2026-01-01'
            -- Compared as text, not as a timestamp: an equality on a microsecond TIMESTAMP is pushed
            -- into paimon and matches nothing there, while a range predicate on the same column and
            -- the value itself are both right. That is the paimon connector's own behaviour (a plain
            -- paimon catalog over this warehouse does the same), so pinning it here would assert
            -- someone else's bug. The value is what this suite is about, and casting keeps the
            -- comparison in Doris.
            and cast(f_timestamp as string) = '2026-01-01 01:02:03.456789'
            and f_timestamp_ltz is not null
            and array_size(f_array) = 3 and f_array[1] = 1 and f_array[3] = 3
            and f_map['k1'] = 1 and f_map['k2'] = 2
            and struct_element(f_row, 'r_int') = 1
            and struct_element(f_row, 'r_string') = 'nested1'
    """
    assertEquals("1", lakeTypeRow[0][0].toString())

    // The all-NULL row was written after tiering stopped, so it is not here.
    assertEquals("1", scalarOf("""select count(*) from lake_types\$lake"""))

    // --- a table the lake holds in full --------------------------------------
    def coldRows = sql """select id, name from lake_cold\$lake order by id"""
    assertEquals([["1", "cold1"], ["2", "cold2"], ["3", "cold3"]],
            coldRows.collect { row -> row.collect { it.toString() } })

    // --- partitioning survives the delegation --------------------------------
    // The lake table is partitioned by the same column, so the partition value
    // has to come back with its own row and not with a neighbour's.
    def partRows = sql """select id, name, dt from lake_part\$lake order by id"""
    assertEquals(3, partRows.size())
    assertEquals(["1", "lp1a", "20260101"], partRows[0].collect { it.toString() })
    assertEquals(["2", "lp1b", "20260101"], partRows[1].collect { it.toString() })
    assertEquals(["3", "lp2a", "20260102"], partRows[2].collect { it.toString() })

    // Pruning is the sibling's, not fluss's: the predicate is pushed to the
    // paimon connector, which owns the plan for this table.
    def prunedPart = sql """select id from lake_part\$lake where dt = '20260101' order by id"""
    assertEquals(["1", "2"], prunedPart.collect { it[0].toString() })

    // --- a primary-key table's lake is its merged state at the tiering point --
    // Row 2 was updated before tiering, so the lake holds the update, not both
    // versions. Row 3's later update and row 1's delete came after and are absent,
    // which is exactly how this differs from the fluss-only read of the same table.
    def pkLakeRows = sql """select id, name from lake_pk\$lake order by id"""
    assertEquals([["1", "lp1"], ["2", "lp2-lake"], ["3", "lp3"]],
            pkLakeRows.collect { row -> row.collect { it.toString() } })

    // --- projection and aggregation through the sibling ----------------------
    assertEquals("4", scalarOf("""select count(*) from lake_log\$lake"""))
    assertEquals("11.00", scalarOf("""select sum(price) from lake_log\$lake"""))
    def namesOnly = sql """select name from lake_log\$lake where id > 2 order by name"""
    assertEquals(["lake3", "lake4"], namesOnly.collect { it[0].toString() })

    // --- tables with no lake -------------------------------------------------
    // A table with no lake never offers the sub-table, so the name does not resolve
    // and the engine answers before the connector is asked anything. That is the
    // deliberate choice: advertising $lake on every fluss table would offer a
    // sub-table whose only possible outcome is an error. The connector still
    // re-checks when it IS asked -- discovery and resolution are two round trips,
    // and the lake can be switched off in between -- but that guard is unreachable
    // from here, which is why it is pinned in the unit tests instead.
    test {
        sql """select * from log_basic\$lake"""
        exception "Unknown sys table"
    }

    // $lake is a way to read a table, not a table of its own: it must not appear
    // in the catalog listing, or every tool that walks the schema would show each
    // lake table twice.
    def tableNames = sql("""show tables""").collect { it[0].toString() }
    assertTrue(tableNames.contains("lake_log"), "lake_log missing from ${tableNames}")
    assertTrue(tableNames.every { !it.contains("\$") },
            "system tables leaked into show tables: ${tableNames}")

    sql """drop catalog if exists ${catalogName}"""
}
