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

suite("test_hive_json_partition_only", "p0,external,hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String catalog = "test_hive_json_partition_only"
    def previousSettings = sql """SELECT @@enable_count_push_down_for_external_table,
                                       @@enable_sql_cache"""
    sql """DROP CATALOG IF EXISTS ${catalog}"""
    try {
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type" = "hms",
            "hive.metastore.uris" = "thrift://${externalEnvIp}:${hmsPort}"
        )"""
        sql """USE `${catalog}`.`default`"""
        sql "SET enable_sql_cache=false"

        // Reading a real JSON field provides the row-count oracle for the same partition files.
        def baseline = sql "SELECT id, dt FROM json_all_complex_types"
        assertTrue(!baseline.isEmpty())
        def partitions = baseline.groupBy { row -> row[1] }

        for (boolean pushdown : [false, true]) {
            sql "SET enable_count_push_down_for_external_table=${pushdown}"
            partitions.each { partition, rows ->
                assertNotNull(partition)
                String value = partition.toString().replace("'", "''")
                String predicate = "dt = '${value}'"
                String countQuery = "SELECT COUNT(*) FROM json_all_complex_types WHERE ${predicate}"
                explain {
                    sql countQuery
                    contains "pushdown agg=NONE"
                }
                def count = sql countQuery
                assertEquals(rows.size(), (count[0][0] as Number).intValue())

                // Partition values come from the split, so neither query needs a JSON file column.
                def projected = sql "SELECT dt FROM json_all_complex_types WHERE ${predicate}"
                assertEquals(rows.size(), projected.size())
                assertTrue(projected.every { row -> row[0] == partition })
                def partitionCount = sql "SELECT COUNT(dt) FROM json_all_complex_types WHERE ${predicate}"
                assertEquals(rows.size(), (partitionCount[0][0] as Number).intValue())

                def filtered = sql """SELECT COUNT(*) FROM json_all_complex_types
                                      WHERE ${predicate} AND id IS NOT NULL"""
                assertEquals(rows.count { row -> row[0] != null },
                             (filtered[0][0] as Number).intValue())
            }

            def grouped = sql "SELECT dt, COUNT(*) FROM json_all_complex_types GROUP BY dt"
            assertEquals(partitions.size(), grouped.size())
            grouped.each { row ->
                assertEquals(partitions[row[0]].size(), (row[1] as Number).intValue())
            }
            def empty = sql """SELECT COUNT(*) FROM json_all_complex_types
                               WHERE dt = '__missing_json_partition__'"""
            assertEquals(0, (empty[0][0] as Number).intValue())
        }
    } finally {
        sql "SET enable_count_push_down_for_external_table=${previousSettings[0][0]}"
        sql "SET enable_sql_cache=${previousSettings[0][1]}"
        sql """DROP CATALOG IF EXISTS ${catalog}"""
    }
}
