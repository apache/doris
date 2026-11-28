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

suite("test_variant_desc_tablet_coverage", "p0") {
    sql "DROP TABLE IF EXISTS variant_desc_tablet_coverage"
    sql """
        CREATE TABLE variant_desc_tablet_coverage (
            id INT, payload VARIANT<PROPERTIES("variant_enable_doc_mode" = "false")>
        ) DUPLICATE KEY(id)
        PARTITION BY RANGE(id) (PARTITION p_wide VALUES LESS THAN (10))
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """
        ALTER TABLE variant_desc_tablet_coverage ADD PARTITION p_narrow VALUES LESS THAN (20)
        DISTRIBUTED BY HASH(id) BUCKETS 1
    """
    sql "ALTER TABLE variant_desc_tablet_coverage ADD PARTITION p_empty VALUES LESS THAN (30)"
    sql """
        INSERT INTO variant_desc_tablet_coverage VALUES
            (0, PARSE_TO_VARIANT('{"k0":1}')), (1, PARSE_TO_VARIANT('{"k1":1}')),
            (2, PARSE_TO_VARIANT('{"k2":1}')), (3, PARSE_TO_VARIANT('{"k3":1}')),
            (10, PARSE_TO_VARIANT('{"narrow":1}'))
    """

    // Give every wide tablet its own path so the result reveals the sample size.
    def wideTablets = sql_return_maparray("SHOW TABLETS FROM variant_desc_tablet_coverage PARTITIONS(p_wide)")
            .collect { it.TabletId }.unique()
    assertEquals(4, wideTablets.size())
    wideTablets.each { tabletId ->
        assertEquals(1, sql("SELECT id FROM variant_desc_tablet_coverage TABLET(${tabletId})").size())
    }

    def dbName = sql("SELECT DATABASE()")[0][0]
    def dbId = get_database_id("internal", dbName)
    def tableId = get_table_id("internal", dbName, "variant_desc_tablet_coverage")
    def indexPath = "/dbs/${dbId}/${tableId}/index_schema"
    def baseIndex = sql("SHOW PROC '${indexPath}'").find { it[1] == "variant_desc_tablet_coverage" }[0]
    def schemaPath = "${indexPath}/${baseIndex}"
    def variantPaths = { statement ->
        sql(statement).collect { it[0] }.findAll { it.startsWith("payload.") }.sort()
    }
    def allPaths = ["payload.k0", "payload.k1", "payload.k2", "payload.k3", "payload.narrow"]

    sql "SET describe_extend_variant_column = true"
    sql "SET max_fetch_remote_schema_tablet_count = 8"
    assertEquals(allPaths, variantPaths("DESC variant_desc_tablet_coverage"))
    assertEquals(allPaths, variantPaths("SHOW PROC '${schemaPath}'"))
    assertEquals(["payload.narrow"], variantPaths("DESC variant_desc_tablet_coverage PARTITION (p_narrow)"))
    assertEquals(["payload.narrow"], variantPaths("SHOW PROC '${schemaPath}/p_narrow'"))

    sql "SET max_fetch_remote_schema_tablet_count = 4"
    def assertTableSample = { statement ->
        def paths = variantPaths(statement)
        assertEquals(3, paths.size())
        assertTrue(paths.contains("payload.narrow"))
        assertTrue(allPaths.containsAll(paths))
    }
    assertTableSample("DESC variant_desc_tablet_coverage")
    assertTableSample("SHOW PROC '${schemaPath}'")

    def assertNamedSample = { statement ->
        def paths = variantPaths(statement)
        assertEquals(4, paths.size())
        assertTrue(allPaths.containsAll(paths))
    }
    assertNamedSample("DESC variant_desc_tablet_coverage PARTITION (p_wide,p_narrow)")
    assertNamedSample("SHOW PROC '${schemaPath}/p_wide,p_narrow'")

    sql "SET max_fetch_remote_schema_tablet_count = 1"
    assertEquals(["payload.narrow"], variantPaths("DESC variant_desc_tablet_coverage PARTITION (p_narrow,p_narrow)"))
    assertEquals(["payload.narrow"], variantPaths("SHOW PROC '${schemaPath}/p_narrow,p_narrow'"))

    sql "SET max_fetch_remote_schema_tablet_count = 0"
    assertTrue(variantPaths("DESC variant_desc_tablet_coverage").isEmpty())
    assertTrue(variantPaths("DESC variant_desc_tablet_coverage PARTITION (p_narrow)").isEmpty())
    assertTrue(variantPaths("DESC variant_desc_tablet_coverage PARTITION (p_empty)").isEmpty())

    sql """INSERT INTO variant_desc_tablet_coverage VALUES (11, PARSE_TO_VARIANT('{"later":1}'))"""
    sql "SET max_fetch_remote_schema_tablet_count = 8"
    def updatedPaths = allPaths + ["payload.later"]
    assertEquals(updatedPaths.sort(), variantPaths("DESC variant_desc_tablet_coverage"))
    assertEquals(["payload.later", "payload.narrow"],
            variantPaths("DESC variant_desc_tablet_coverage PARTITION (p_narrow)"))
}
