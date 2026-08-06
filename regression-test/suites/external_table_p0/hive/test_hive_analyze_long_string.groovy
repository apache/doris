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

import org.awaitility.Awaitility
import java.util.concurrent.TimeUnit

suite("test_hive_analyze_long_string", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    // Feature only triggers when values exceed 1024 bytes; this test assumes the FE default.
    def cfgRows = sql "admin show frontend config like 'statistics_max_string_column_length'"
    assertEquals(1, cfgRows.size())
    assertEquals("1024", cfgRows[0][1].toString())

    String longVal = "x" * 2048

    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalogName = hivePrefix + "_test_analyze_long_string"
        String dbName = "test_analyze_long_string_db"
        String tblName = "t1"

        // Seed the hive side via hive_docker so this suite is self-contained and
        // does not require any pre-install hql.
        hive_docker """drop table if exists ${dbName}.${tblName}"""
        hive_docker """drop database if exists ${dbName} cascade"""
        hive_docker """create database ${dbName}"""
        hive_docker """create table ${dbName}.${tblName} (id int, s string) stored as parquet"""
        hive_docker """insert into ${dbName}.${tblName} values (1, 'short'), (2, 'another'), (3, '${longVal}')"""

        sql "drop catalog if exists ${catalogName}"
        sql """
            create catalog if not exists ${catalogName} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                'fs.defaultFS' = 'hdfs://${extHiveHmsHost}:${hdfsPort}'
            )
        """
        sql "refresh catalog ${catalogName}"

        // Analyze column `s` which contains a >1024-byte row: task should be marked
        // FINISHED with a skip message, and no column stats should be persisted.
        def analyzeRes = sql "analyze table ${catalogName}.${dbName}.${tblName}(s)"
        assertEquals(1, analyzeRes.size())
        long jobId = Long.parseLong(analyzeRes[0][0].toString())
        logger.info("hive analyze long string jobId=${jobId}")

        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until {
                    def rows = sql "show analyze ${jobId}"
                    if (rows.isEmpty()) {
                        return false
                    }
                    String state = rows[0][9].toString()
                    return state == "FINISHED" || state == "FAILED"
                }

        def jobRows = sql "show analyze ${jobId}"
        assertEquals("FINISHED", jobRows[0][9].toString())
        String jobMessage = jobRows[0][7].toString()
        assertTrue(jobMessage.contains("statistics_max_string_column_length")
                   || jobMessage.contains("exceeds"),
                   "expected skip reason in job message but got: ${jobMessage}")

        def taskRows = sql "show analyze task status ${jobId}"
        def strTaskRow = taskRows.find { it[1].toString() == "s" }
        assertNotNull(strTaskRow, "missing analyze task row for column s")
        assertEquals("FINISHED", strTaskRow[6].toString())
        String taskMessage = strTaskRow[3].toString()
        assertTrue(taskMessage.contains("statistics_max_string_column_length")
                   || taskMessage.contains("exceeds"),
                   "expected skip reason in task message but got: ${taskMessage}")

        // The skipped column must NOT have any stored column statistics row.
        def colStats = sql "show column stats ${catalogName}.${dbName}.${tblName}(s)"
        assertTrue(colStats.isEmpty(), "expected no column stats for skipped column s")

        // A non-string column on the same table should still analyze successfully.
        def idAnalyze = sql "analyze table ${catalogName}.${dbName}.${tblName}(id)"
        long idJobId = Long.parseLong(idAnalyze[0][0].toString())
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until {
                    def rows = sql "show analyze ${idJobId}"
                    if (rows.isEmpty()) {
                        return false
                    }
                    String state = rows[0][9].toString()
                    return state == "FINISHED" || state == "FAILED"
                }
        def idJobRows = sql "show analyze ${idJobId}"
        assertEquals("FINISHED", idJobRows[0][9].toString())
        def idColStats = sql "show column stats ${catalogName}.${dbName}.${tblName}(id)"
        assertEquals(1, idColStats.size())

        sql "drop catalog if exists ${catalogName}"
        hive_docker """drop table if exists ${dbName}.${tblName}"""
        hive_docker """drop database if exists ${dbName} cascade"""
    }
}
