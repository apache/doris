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

suite("st07_qcs_consistency") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        file_cache_background_ttl_gc_interval_ms : 1000,
        file_cache_background_ttl_info_update_interval_ms : 1000,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]

    setBeConfigTemporary(customBeConfig) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """use @${validCluster};"""

        String tableName = "st07_qcs_tpl"
        def ddl = new File("""${context.file.parent}/../ddl/st07_qcs_consistency.sql""").text
                .replace("\${TABLE_NAME}", tableName)
        sql ddl

        (0..<500).each { i ->
            sql """insert into ${tableName} values (${i}, 'qcs_tpl_${i}')"""
        }
        qt_q1 """select count(*) from ${tableName} where c1 like 'qcs_tpl_%'"""

        def tablets = sql """show tablets from ${tableName}"""
        assertTrue(tablets.size() > 0, "No tablets found for table ${tableName}")
        def tabletIds = tablets.collect { it[0] as Long }

        // ST-07 部分覆盖点模板：Query + Compaction + SchemaChange 混合后检查缓存类型一致性
        try {
            trigger_and_wait_compaction(tableName, "cumulative")
        } catch (Throwable t) {
            logger.warn("trigger_and_wait_compaction failed in template, continue. err=${t.message}")
        }

        sql """alter table ${tableName} add column c2 BIGINT default "0" """

        def waitSchemaChangeFinished = { String tbl, long timeoutMs = 300000L, long intervalMs = 5000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                def rows = sql """SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY CreateTime DESC LIMIT 1"""
                if (rows.isEmpty()) {
                    sleep(intervalMs)
                    continue
                }
                def state = rows[0][9].toString()
                if ("FINISHED".equalsIgnoreCase(state)) {
                    return
                }
                if ("CANCELLED".equalsIgnoreCase(state)) {
                    assertTrue(false, "schema change cancelled, table=${tbl}")
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting schema change finished, table=${tbl}")
        }
        waitSchemaChangeFinished.call(tableName)

        qt_q2 """select count(*) from ${tableName} where c2 = 0"""

        def waitNoMixedTypePerTablet = { List<Long> ids, long timeoutMs = 600000L, long intervalMs = 3000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                boolean allOk = true
                for (Long tabletId in ids) {
                    def rows = sql """select type from information_schema.file_cache_info where tablet_id=${tabletId}"""
                    if (rows.isEmpty()) {
                        allOk = false
                        break
                    }
                    def typeSet = rows.collect { it[0]?.toString()?.toLowerCase() }.toSet()
                    if (typeSet.size() > 1) {
                        allOk = false
                        break
                    }
                }
                if (allOk) {
                    return
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting no mixed cache types per tablet")
        }
        waitNoMixedTypePerTablet.call(tabletIds)

        sql """drop table if exists ${tableName}"""
    }
}
