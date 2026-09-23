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
    // The lake sits in an object store. Fluss removes every lake option whose name holds
    // key, secret or password before it hands a table's properties to a client, and Doris
    // configures storage once per catalog rather than per table, so the whole of how to
    // reach that store is stated on the catalog instead of learned from the fluss cluster.
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_lake_only"
    String mappedCatalog = "test_fluss_lake_only_mapped"
    String maskingCatalog = "test_fluss_lake_only_masking"

    // SHOW CREATE CATALOG is available without opening a connection. Exercise both
    // credential namespaces with unique markers so this fails if any raw value leaks.
    sql """drop catalog if exists ${maskingCatalog}"""
    sql """
        create catalog ${maskingCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.client.security.sasl.username" = "FLUSS_USER_SECRET_MARKER",
            "fluss.client.security.sasl.password" = "FLUSS_PASSWORD_SECRET_MARKER",
            "fluss.client.security.sasl.jaas.config" = "FLUSS_JAAS_SECRET_MARKER",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "FLUSS_LAKE_KEY_SECRET_MARKER",
            "fluss.lake.paimon.s3.secret-key" = "FLUSS_LAKE_SECRET_MARKER"
        );
    """
    def maskingDdl = sql("""show create catalog ${maskingCatalog}""")[0][1].toString()
    ["FLUSS_USER_SECRET_MARKER", "FLUSS_PASSWORD_SECRET_MARKER", "FLUSS_JAAS_SECRET_MARKER",
            "FLUSS_LAKE_KEY_SECRET_MARKER", "FLUSS_LAKE_SECRET_MARKER"].each { marker ->
        assertFalse(maskingDdl.contains(marker), "SHOW CREATE CATALOG leaked ${marker}: ${maskingDdl}")
    }
    ["fluss.client.security.sasl.username", "fluss.client.security.sasl.password",
            "fluss.client.security.sasl.jaas.config", "fluss.lake.paimon.s3.access-key",
            "fluss.lake.paimon.s3.secret-key"].each { key ->
        assertTrue(maskingDdl.contains("\"${key}\" = \"*XXX\""),
                "SHOW CREATE CATALOG did not mask ${key}: ${maskingDdl}")
    }
    assertTrue(maskingDdl.contains("http://${externalEnvIp}:${minioPort}"),
            "non-sensitive lake endpoint should remain visible: ${maskingDdl}")
    sql """drop catalog ${maskingCatalog}"""

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin"
        );
    """
    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    // The connector is wired into the v2 file scanner only, and fuzzy sessions
    // randomize this variable.
    sql """set enable_file_scanner_v2 = true"""
    // TIMESTAMP_LTZ renders through the session time zone, and the baselines below
    // record what it rendered as.
    sql """set time_zone = 'Asia/Shanghai'"""

    // --- the lake holds what was tiered, and nothing that came after ---------
    // lake_log got four rows before the tiering service was stopped and two
    // after. Reading the lake alone must return the first four: a $lake that
    // quietly fell back to the fluss read would return all six.
    order_qt_lake_rows """select id, name, price from lake_log\$lake"""

    // --- the lake table is this table's columns and no others ----------------
    // Fluss 1.0.0 tiers into a paimon table that carries no fluss system columns:
    // where earlier releases appended __bucket / __offset / __timestamp to every lake
    // table, 1.0.0 only refuses a fluss column of those names and marks the table with
    // paimon 2.0's `lakestream.enabled` option instead. The paimon connector's own
    // metadata columns (__paimon_file_path, __paimon_row_index) are invisible, and
    // DESC hides them here the way it hides them on any paimon table. Both DESCs are
    // recorded so that a column appearing on one side only is a visible change.
    qt_desc_lake_log_lake """desc lake_log\$lake"""
    qt_desc_lake_log """desc lake_log"""

    // --- type parity between the two doors ----------------------------------
    // The connector's fluss->Doris mapping has to equal fluss->paimon->Doris, or
    // `tbl` and `tbl$lake` present two different schemas for one table. Both
    // schemas are recorded, and the equality is ALSO asserted here: a reader
    // comparing two recorded blocks by eye is not what should be guarding an
    // invariant this quiet.
    qt_desc_lake_types """desc lake_types"""
    qt_desc_lake_types_lake """desc lake_types\$lake"""
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
    // The count is the other half of the parity: a column present on the lake side
    // only would satisfy the loop.
    assertEquals(flussTypes.size(), lakeTypes.size(),
            "the lake table should list exactly the fluss table's columns, but has ${lakeTypes.keySet()}")

    // Mapping switches are catalog-wide. They must reach both the Fluss mapping and the
    // embedded Paimon sibling, or one table exposes two schemas depending on its suffix.
    sql """drop catalog if exists ${mappedCatalog}"""
    sql """
        create catalog ${mappedCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "enable.mapping.varbinary" = "true",
            "enable.mapping.timestamp_tz" = "true"
        );
    """
    sql """switch ${mappedCatalog}"""
    sql """use fluss_test"""
    def mappedFlussTypes = typesOf("lake_types")
    def mappedLakeTypes = typesOf("lake_types\$lake")
    assertEquals(mappedFlussTypes, mappedLakeTypes,
            "mapping switches must produce the same schema through tbl and tbl\$lake")
    assertTrue(mappedFlussTypes.get("f_binary").toLowerCase().startsWith("varbinary"),
            "enable.mapping.varbinary did not reach both readers: ${mappedFlussTypes}")
    assertTrue(mappedFlussTypes.get("f_timestamp_ltz").toLowerCase().startsWith("timestamptz"),
            "enable.mapping.timestamp_tz did not reach both readers: ${mappedFlussTypes}")
    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    // Parity of the values, not just of the declared types: the row is recorded
    // here read through paimon, and the same row read through fluss is recorded in
    // test_fluss_log_table -- two decoders, one row, and the two baselines have to
    // agree column for column.
    //
    // BINARY and BYTES go through hex() for the same reason as everywhere else. Note
    // that an equality predicate on the microsecond TIMESTAMP would match nothing:
    // it is pushed into paimon, which does not find the row, while the value itself
    // and range predicates on it are right. That is the paimon connector's own
    // behaviour (a plain paimon catalog over this warehouse does the same), so it is
    // not pinned here -- recording the value sidesteps it entirely.
    //
    // The MAP goes in as its sorted keys and sorted values rather than whole: the order
    // a map renders its entries in is neither the order they were written in nor stable
    // across runs, so a recorded whole-map cell pins something no reader promises.
    order_qt_lake_types_row """
        select id, f_boolean, f_tinyint, f_smallint, f_int, f_bigint, f_float, f_double,
               f_decimal, f_char, f_string, hex(f_binary) as f_binary_hex,
               hex(f_bytes) as f_bytes_hex, f_date, f_timestamp, f_timestamp_ltz,
               f_array, array_sort(map_keys(f_map)) as f_map_keys,
               array_sort(map_values(f_map)) as f_map_values, f_row
        from lake_types\$lake
    """

    // The all-NULL row was written after tiering stopped, so it is not here.
    order_qt_lake_types_count """select count(*) from lake_types\$lake"""

    // --- a table the lake holds in full --------------------------------------
    order_qt_cold_rows """select id, name from lake_cold\$lake"""

    // --- partitioning survives the delegation --------------------------------
    // The lake table is partitioned by the same column, so the partition value
    // has to come back with its own row and not with a neighbour's.
    order_qt_part_rows """select id, name, dt from lake_part\$lake"""

    // Pruning is the sibling's, not fluss's: the predicate is pushed to the
    // paimon connector, which owns the plan for this table.
    order_qt_part_pruned """select id from lake_part\$lake where dt = '20260101'"""

    // --- a primary-key table's lake is its merged state at the tiering point --
    // Row 2 was updated before tiering, so the lake holds the update, not both
    // versions. Row 3's later update and row 1's delete came after and are absent,
    // which is exactly how this differs from the fluss-only read of the same table
    // (recorded in test_fluss_lake_pk, against this same fixture).
    order_qt_pk_lake_rows """select id, name from lake_pk\$lake"""

    // --- projection and aggregation through the sibling ----------------------
    order_qt_lake_count """select count(*) from lake_log\$lake"""
    order_qt_lake_sum """select sum(price) from lake_log\$lake"""
    order_qt_lake_names """select name from lake_log\$lake where id > 2"""

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
    // lake table twice. The recorded listing is what says so.
    order_qt_tables """show tables"""

    sql """drop catalog if exists ${mappedCatalog}"""
    sql """drop catalog if exists ${catalogName}"""
}
