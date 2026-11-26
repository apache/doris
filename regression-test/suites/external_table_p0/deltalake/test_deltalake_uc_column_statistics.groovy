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

import groovy.json.JsonSlurper

suite("test_deltalake_uc_column_statistics", "p0,external,deltalake,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeUcTest"))) {
        return
    }
    // Prepare uc_managed with uc-managed.py and uc_external_statistics with --column-statistics-external SHARED_DIR.
    sql "set time_zone = 'Asia/Shanghai'"
    String uri = context.config.otherConfigs.get("deltalakeUcUri").replaceAll('/+$', '')
    String catalog = "test_deltalake_uc_column_statistics"
    sql "drop catalog if exists ${catalog}"
    sql """create catalog ${catalog} properties ('type'='deltalake','deltalake.catalog.type'='uc',
           'deltalake.uc.uri'='${uri}', 'deltalake.uc.catalog'='doris_delta', 'enable.auto.analyze'='true')"""

    def json = new JsonSlurper()
    String api = "${uri}/api/2.1/unity-catalog"
    assertEquals("EXTERNAL", json.parse(new URL("${api}/tables/doris_delta.m1.uc_external_statistics")).table_type)
    assertEquals("MANAGED", json.parse(new URL("${api}/tables/doris_delta.m1.uc_managed")).table_type)
    def checkStaged = {
        def state = json.parse(new URL("${api}/delta/v1/catalogs/doris_delta/schemas/m1/tables/uc_managed"))
        long version = state["latest-table-version"]
        def last = state.commits.find { it.version == version }
        assertNotNull(last)
        File log = new File(new File(new URI(state.metadata.location as String)), "_delta_log")
        assertTrue(new File(log, "_staged_commits/${last['file-name']}").isFile())
        assertFalse(new File(log, String.format("%020d.json", version)).exists())
    }
    checkStaged()

    def catalogId = get_catalog_id(catalog)
    def dbId = get_database_id(catalog, "m1")
    def ids = []
    for (String name : ["uc_external_statistics", "uc_managed"]) {
        String table = "${catalog}.m1.${name}"
        def id = get_table_id(catalog, "m1", name)
        ids << id
        // Exercise the automatic collector's actual FULL job before a manual collection populates the same columns.
        sql "drop stats ${table}"
        sql """analyze table ${table} properties ("use.auto.analyzer"="true")"""
        def system = sql("show column stats ${table}")
        assertEquals(["id", "label"] as Set, system*.getAt(0) as Set)
        assertTrue(system.every { it[9] == "FULL" && it[11] == "SYSTEM" })

        sql "analyze table ${table} with sync"
        String stored = """select col_id, count, ndv, null_count, min, max
                           from internal.__internal_schema.column_statistics
                           where catalog_id='${catalogId}' and db_id='${dbId}' and tbl_id='${id}'
                             and idx_id='-1' and part_id is null
                           order by col_id"""
        quickTest(name, stored)
        def persisted = sql(stored).collectEntries { [(it[0]): it[1..3].collect { n -> n as long }] }
        def shown = sql("show column stats ${table}")
        assertTrue(shown.every { it[9] == "FULL" && it[11] == "MANUAL" })
        assertEquals(persisted, shown.collectEntries { [(it[0]): it[2..4].collect { n -> n as double as long }] })
        def tableStats = sql("show table stats ${table}").first()
        assertEquals("MANUAL", tableStats[5])
        assertTrue(persisted.keySet().every { tableStats[4].contains("${name}:${it}") })
    }
    String history = catalog + '.m1.`uc_managed$history`'
    qt_history_versions "select version from ${history} order by version"
    test {
        sql "analyze table ${history} with sync"
        exception "Unknown table"
    }
    qt_stats_for_other_tables """select count(*) from internal.__internal_schema.column_statistics
                                where catalog_id='${catalogId}' and tbl_id not in ('${ids.join("','")}')"""
    checkStaged()
}
