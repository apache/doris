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

// A query that reads one field of a nested column must read that field and no other.
//
// Fluss projects top-level fields only, so nothing narrower can be pushed to the
// cluster: the scanner is handed the pruned type and has to remap the sub-fields it
// wants onto the whole row while decoding. That remap is per row format, and this
// connector has three of them -- arrow for a log table, compacted kv for a
// primary-key one, and parquet through the paimon sibling for the lake half of a
// union read -- so each is read here. The failure this guards against is silent: a
// decoder that takes sub-field i of the source for sub-field i of the request
// returns a plausible value from the wrong field.
//
// Fixtures: docker/thirdparties/docker-compose/fluss/sql/init.sql (log_nested,
// pk_nested, lake_nested) and init-lake-tail.sql.
suite("test_fluss_nested_column_pruning", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_nested_column_pruning"

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

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { "${it}".toString() } } }

    // The assertion a recorded baseline cannot make: reading pruned and reading whole must
    // agree. A baseline only says "same as last time", which a decoder that reads the wrong
    // sub-field satisfies from the day it is recorded.
    def prunedMatchesUnpruned = { String query, String where ->
        sql "set enable_prune_nested_column = false"
        def whole = rowsOf(query)
        sql "set enable_prune_nested_column = true"
        def pruned = rowsOf(query)
        assertEquals(whole, pruned,
                "pruned and unpruned reads disagree (${where})\nunpruned=${whole}\npruned=${pruned}")
    }

    // The comparison above says the two reads agree; it cannot say either of them pruned.
    // "pruned type:" renders only when the slot type was really narrowed, and the type it
    // prints is the whole answer -- asserting it in full is what pins "only the touched
    // sub-field survives" rather than "something was dropped".
    def assertPrunedType = { String query, String prunedType, String label ->
        String explainText = sql("explain ${query}").collect { row -> row[0].toString() }.join("\n")
        assertTrue(explainText.contains("pruned type: ${prunedType}"),
                "expected pruned type ${prunedType} for ${label}\n${explainText}")
    }

    sql "set enable_prune_nested_column = true"
    assertPrunedType("select id, struct_element(f_row_deep, 'r_int') from log_nested order by id",
            "struct<r_int:int>", "log table")
    assertPrunedType("select id, struct_element(f_row_deep, 'r_int') from pk_nested order by id",
            "struct<r_int:int>", "primary-key table")
    assertPrunedType("select id, struct_element(f_row_deep, 'r_int') from lake_nested order by id",
            "struct<r_int:int>", "union read")

    // A log table's arrow rows: fluss hands back the whole struct, so this is the decoder remap.
    // r_row is the LAST of four fields and holds a struct, so a decoder that took sub-field i of
    // the source for sub-field i of the request would reach for an INT here, not get it right by
    // accident.
    prunedMatchesUnpruned("""
        select id, struct_element(f_row_deep, 'r_int'),
               struct_element(struct_element(f_row_deep, 'r_row'), 'y')
        from log_nested order by id""", "log table")
    // Two levels in, through an ARRAY and a MAP.
    prunedMatchesUnpruned("""
        select id, struct_element(f_arr_row[1], 'b'), struct_element(f_map_row['k1'], 'a')
        from log_nested order by id""", "log table, nested in array/map")
    // A primary-key table's compacted rows go through a different fluss reader.
    prunedMatchesUnpruned("""
        select id, struct_element(f_row_deep, 'r_int'), struct_element(f_arr_row[1], 'b')
        from pk_nested order by id""", "primary-key table")
    // The same pk rows through a MAP -- the key is 'k9' here: the second upsert replaced 'k1',
    // and a 'k1' probe would return NULL for every row and pass no matter what the decoder does.
    prunedMatchesUnpruned("""
        select id, struct_element(f_map_row['k9'], 'a') from pk_nested order by id""",
        "primary-key table, map value")
    // The front door: the lake half prunes for real through paimon, the fluss half remaps while
    // decoding, and the two must produce one consistent result set.
    prunedMatchesUnpruned("""
        select id, struct_element(f_row_deep, 'r_int'), struct_element(f_map_row['k1'], 'b')
        from lake_nested order by id""", "union read")
    // The lake half reached directly. It is a system table, which reads the whole column until
    // system tables opt in; the read must stay correct either way.
    prunedMatchesUnpruned("""
        select id, struct_element(f_row_deep, 'r_int') from lake_nested\$lake order by id""",
        "\$lake")

    sql "set enable_prune_nested_column = true"
    order_qt_log_pruned """
        select id, struct_element(f_row_deep, 'r_int'),
               struct_element(struct_element(f_row_deep, 'r_row'), 'y')
        from log_nested order by id"""
    order_qt_pk_pruned """
        select id, struct_element(f_row_deep, 'r_int'), struct_element(f_arr_row[1], 'b')
        from pk_nested order by id"""
    order_qt_union_pruned """
        select id, struct_element(f_row_deep, 'r_int'), struct_element(f_map_row['k1'], 'b')
        from lake_nested order by id"""

    sql """drop catalog if exists ${catalogName}"""
}
