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

// Complex types nested inside complex types, down every path a value can take.
//
// The other type suites cover one column per fluss type, never more than one level
// deep. That is a different question: an element decoder is chosen per level, so a
// reader can be right about MAP<STRING,INT> and wrong about the MAP<STRING,ROW<..>>
// beside it, and being right about ARRAY<INT> says nothing about ARRAY<ARRAY<INT>>.
// Fluss stores these in three unrelated row formats -- arrow for a log table,
// compacted for a primary-key one, parquet through paimon for the lake half -- and
// each has its own nested decoder, so all three are read here.
//
// Fixtures: docker/thirdparties/docker-compose/fluss/sql/init.sql (log_nested,
// pk_nested, lake_nested) and init-lake-tail.sql.
suite("test_fluss_nested_types", "p0,external") {
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
    String catalogName = "test_fluss_nested_types"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.union_read.mode" = "required"
        );
    """
    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    // The connector is wired into the v2 file scanner only, and fuzzy sessions
    // randomize the variable that chooses between them.
    sql """set enable_file_scanner_v2 = true"""

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }

    // --- the mapping, before any row is read ---------------------------------
    // Recorded unsorted: which level of nesting each constructor sits at is the whole
    // subject, and a type printed one level flat is the bug this suite is about.
    qt_desc_log_nested """desc log_nested"""
    qt_desc_pk_nested """desc pk_nested"""

    // --- a log table's arrow rows --------------------------------------------
    // All three rows whole: the populated one, the one whose collections are present
    // but hold NULL elements, and the one whose columns are NULL outright. The middle
    // row is the one worth having -- a decoder that confuses "no element" with "a null
    // element" gets the other two right and this one wrong.
    order_qt_log_rows """
        select id, f_arr_arr, f_arr_map, f_arr_row, f_map_arr, f_map_row, f_row_deep,
               f_arr_arr_arr
        from log_nested
    """

    // Element by element, so a value that rendered plausibly but decoded into the wrong
    // slot has somewhere to fail. Reaching two levels in at once is the point: the
    // outer accessor has to hand the inner one a value it can still take apart.
    order_qt_log_elements """
        select id,
               array_size(f_arr_arr), f_arr_arr[1], array_size(f_arr_arr[1]), f_arr_arr[1][2],
               f_arr_map[1]['a'],
               struct_element(f_arr_row[1], 'a'), struct_element(f_arr_row[1], 'b'),
               f_map_arr['k1'], f_map_arr['k1'][2],
               struct_element(f_map_row['k1'], 'a'), struct_element(f_map_row['k1'], 'b')
        from log_nested
    """

    // Three levels of the same constructor, and a struct holding one of each. Nothing
    // else here asks a decoder to recurse into itself twice.
    order_qt_log_deep """
        select id,
               f_arr_arr_arr[1][2][1],
               struct_element(f_row_deep, 'r_int'),
               struct_element(f_row_deep, 'r_arr')[2],
               struct_element(f_row_deep, 'r_map')['m'],
               struct_element(struct_element(f_row_deep, 'r_row'), 'x'),
               struct_element(struct_element(f_row_deep, 'r_row'), 'y')
        from log_nested
    """

    // A predicate on a nested element, which is evaluated after the value has been
    // decoded rather than in the scanner: the row it selects is the check.
    order_qt_log_nested_predicate """
        select id from log_nested
        where f_arr_arr[1][1] = 1 and struct_element(f_map_row['k1'], 'a') = 9
    """

    // --- a primary-key table's compacted rows ---------------------------------
    // Key 1 was written twice, so what comes back must be the SECOND set of nested
    // values. A reader that returned the change log rather than the merged view would
    // answer with three rows here, two of them the same key.
    order_qt_pk_rows """
        select id, f_arr_arr, f_arr_row, f_map_arr, f_map_row, f_row_deep from pk_nested
    """
    order_qt_pk_elements """
        select id,
               array_size(f_arr_arr), f_arr_arr[1][1],
               struct_element(f_arr_row[1], 'b'),
               f_map_arr['k9'], f_map_arr['k9'][1],
               struct_element(f_map_row['k9'], 'a'),
               struct_element(f_row_deep, 'r_arr')[1],
               struct_element(struct_element(f_row_deep, 'r_row'), 'y')
        from pk_nested
    """

    // --- the same nesting, through paimon -------------------------------------
    // The connector's fluss->Doris mapping has to equal fluss->paimon->Doris, or one
    // table has two schemas depending on which door it is read through. For flat types
    // that is checked in test_fluss_lake_only; nesting is where the two mappings could
    // most easily part company, since each level is converted by its own rule on both
    // sides.
    qt_desc_lake_nested """desc lake_nested"""
    qt_desc_lake_nested_lake """desc lake_nested\$lake"""
    def typesOf = { String table ->
        def result = [:]
        sql("""desc ${table}""").each { row -> result.put(row[0].toString(), row[1].toString()) }
        return result
    }
    def flussTypes = typesOf("lake_nested")
    def lakeTypes = typesOf("lake_nested\$lake")
    flussTypes.each { column, type ->
        assertEquals(type, lakeTypes.get(column),
                "column ${column} is ${type} on the fluss table but ${lakeTypes.get(column)} on its lake")
    }
    // The lake table carries three system columns fluss adds to every one it creates
    // (__bucket, __offset, __timestamp) and nothing else, so the count is the other
    // half of the parity: a column present on one side only would satisfy the loop.
    assertEquals(flussTypes.size() + 3, lakeTypes.size(),
            "the lake table should differ by exactly the three system columns")

    // The tiered row read through paimon's own reader.
    order_qt_lake_side """
        select id, f_arr_arr, f_arr_row, f_map_arr, f_map_row, f_row_deep from lake_nested\$lake
    """

    // And the front door, where the tiered row arrives through paimon and the all-NULL
    // one through fluss: two nested decoders feeding one result set. A level dropped by
    // either shows up as a value in the wrong shape next to the same column from the
    // other half.
    def lakePlan = sql("""explain select * from lake_nested""").collect { it[0].toString() }.join("\n")
    assertTrue(lakePlan.contains("unionRead=yes"), "not a union read: ${lakePlan}")
    order_qt_lake_union """
        select id, f_arr_arr, f_arr_row, f_map_arr, f_map_row, f_row_deep from lake_nested
    """
    order_qt_lake_union_elements """
        select id, f_arr_arr[1][2], struct_element(f_arr_row[1], 'b'),
               f_map_arr['k1'][1], struct_element(f_map_row['k1'], 'b'),
               struct_element(struct_element(f_row_deep, 'r_row'), 'y')
        from lake_nested
    """

    // The union read and a reader that never touches the lake must agree row for row.
    // Two entirely different nested decoders over one table: the comparison says
    // something no pair of recorded blocks does.
    String flussOnlyCatalog = "test_fluss_nested_types_off"
    sql """drop catalog if exists ${flussOnlyCatalog}"""
    sql """
        create catalog ${flussOnlyCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.union_read.mode" = "disabled"
        );
    """
    def unionRows = rowsOf("""select id, f_arr_arr, f_arr_row, f_map_arr, f_map_row, f_row_deep
            from lake_nested order by id""")
    def flussOnlyRows = rowsOf("""select id, f_arr_arr, f_arr_row, f_map_arr, f_map_row, f_row_deep
            from ${flussOnlyCatalog}.fluss_test.lake_nested order by id""")
    assertEquals(flussOnlyRows, unionRows,
            "paimon and fluss decoded the same nested rows differently\nfluss-only=${flussOnlyRows}\nunion=${unionRows}")

    sql """switch internal"""
    sql """drop catalog if exists ${catalogName}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
