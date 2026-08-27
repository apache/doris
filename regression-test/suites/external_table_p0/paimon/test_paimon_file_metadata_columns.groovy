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

suite("test_paimon_file_metadata_columns", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Paimon test is disabled")
        return
    }

    String catalogName = "test_paimon_file_metadata_columns"
    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def verifyMetadata = { String tableName ->
        def rows = sql """
            select id, `__paimon_file_path`, `__paimon_row_index`
            from ${tableName}
            where id > 2
            order by id, `__paimon_file_path`, `__paimon_row_index`
        """
        assertTrue(rows.size() > 0)
        rows.each { row ->
            assertTrue(row[1] != null && !row[1].toString().isEmpty())
            assertTrue(row[2].toString().toLong() >= 0L)
        }

        def distinctLocations = sql """
            select count(*), count(`__paimon_file_path`), count(`__paimon_row_index`),
                   count(distinct concat(`__paimon_file_path`, '#', `__paimon_row_index`))
            from ${tableName}
            where id > 2
        """
        assertEquals(distinctLocations[0][0], distinctLocations[0][1])
        assertEquals(distinctLocations[0][0], distinctLocations[0][2])
        assertEquals(distinctLocations[0][0], distinctLocations[0][3])

        def groupedRows = sql """
            select `__paimon_file_path`, count(*), min(`__paimon_row_index`),
                   max(`__paimon_row_index`)
            from ${tableName}
            where `__paimon_row_index` >= 0
            group by `__paimon_file_path`
            order by `__paimon_file_path`
        """
        assertTrue(groupedRows.size() > 0)
        groupedRows.each { row ->
            assertTrue(row[0] != null && !row[0].toString().isEmpty())
            assertTrue(row[2].toString().toLong() >= 0L)
            assertTrue(row[3].toString().toLong() >= row[2].toString().toLong())
        }

        explain {
            sql("""select count(*) from ${tableName} where `__paimon_row_index` >= 0""")
            contains "pushdown agg=NONE"
        }
    }

    try {
        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog ${catalogName} properties (
            "type" = "paimon",
            "paimon.catalog.type" = "filesystem",
            "warehouse" = "hdfs://${externalEnvIp}:${hdfsPort}/user/doris/paimon1"
        )"""
        sql """switch ${catalogName}"""
        sql """use db1"""
        sql """set enable_file_scanner_v2=true"""
        sql """set force_jni_scanner=false"""
        // A one-byte split forces one native source file through multiple scanner ranges.
        sql """set file_split_size=1"""

        verifyMetadata("deletion_vector_orc")
        verifyMetadata("deletion_vector_parquet")

        sql """set force_jni_scanner=true"""
        test {
            sql """select `__paimon_file_path` from deletion_vector_parquet"""
            exception "Paimon metadata columns are only supported by FileScannerV2 native Parquet/ORC reader"
        }

        sql """set force_jni_scanner=false"""
        sql """set enable_file_scanner_v2=false"""
        test {
            sql """select `__paimon_row_index` from deletion_vector_orc"""
            exception "Paimon metadata columns require FileScannerV2 native Parquet/ORC reader"
        }
    } finally {
        sql """unset variable file_split_size"""
        sql """set force_jni_scanner=false"""
        sql """set enable_file_scanner_v2=true"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
