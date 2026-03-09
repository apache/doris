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

import org.apache.doris.regression.suite.ClusterOptions

suite("ex06_ttl_restart_consistency", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.msNum = 1
    options.cloudMode = true
    options.beConfigs += [
        "enable_file_cache=true",
        "file_cache_enter_disk_resource_limit_mode_percent=99",
        "file_cache_background_ttl_gc_interval_ms=1000",
        "file_cache_background_ttl_info_update_interval_ms=1000",
        "file_cache_background_tablet_id_flush_interval_ms=1000"
    ]

    docker(options) {
        if (!isCloudMode()) {
            return
        }

        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """use @${validCluster};"""

        String tableName = "ex06_ttl_restart_consistency"
        def ddl = new File("""${context.file.parent}/../ddl/ex06_ttl_restart_consistency.sql""").text
                .replace("\${TABLE_NAME}", tableName)
        sql ddl

        try {
            def getTabletIds = {
                def tablets = sql """show tablets from ${tableName}"""
                assertTrue(!tablets.isEmpty(), "No tablets found for table ${tableName}")
                tablets.collect { it[0] as Long }
            }

            def waitForFileCacheType = { List<Long> tabletIds, String expectedType,
                                         long timeoutMs = 1800000L, long intervalMs = 2000L ->
                long start = System.currentTimeMillis()
                logger.info("tablets collection is ${tabletIds.toString()}")
                while (System.currentTimeMillis() - start < timeoutMs) {
                    boolean allMatch = true
                    for (Long tabletId in tabletIds) {
                        def rows = sql """select type from information_schema.file_cache_info where tablet_id = ${tabletId}"""
                        logger.info("Tablet ${tabletId} file cache info: ${rows.toString()}")
                        if (rows.isEmpty()) {
                            allMatch = false
                            break
                        }
                        def mismatch = rows.find { row -> !row[0]?.toString()?.equalsIgnoreCase(expectedType) }
                        if (mismatch) {
                            allMatch = false
                            break
                        }
                    }
                    if (allMatch) {
                        return
                    }
                    sleep(intervalMs)
                }
                assertTrue(false, "Timeout waiting type=${expectedType}, tablets=${tabletIds}")
            }

            def values = (0..<300).collect { i -> "(${i}, 'restart_${i}')" }.join(",")
            sql """insert into ${tableName} values ${values}"""
            qt_ex06_preheat """select count(*) from ${tableName} where c1 like 'restart_%'"""
            sleep(5000)

            def tabletIds = getTabletIds.call()
            waitForFileCacheType.call(tabletIds, "ttl")

            // EX-06: TTL 转换窗口内重启，重启后状态应一致收敛到 normal。
            sql """alter table ${tableName} set ("file_cache_ttl_seconds"="0")"""
            qt_ex06_create_table_1 """show create table ${tableName};"""
            cluster.restartBackends()
            qt_ex06_create_table_2 """show create table ${tableName};"""
            sleep(10000)

            qt_ex06_after_restart """select count(*) from ${tableName} where c1 like 'restart_%'"""
            waitForFileCacheType.call(tabletIds, "normal")
        } finally {
            sql """drop table if exists ${tableName}"""
        }
    }
}
