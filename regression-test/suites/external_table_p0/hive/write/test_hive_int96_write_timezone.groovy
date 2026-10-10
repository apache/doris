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

suite("test_hive_int96_write_timezone", "p0,external,hive,external_docker,external_docker_hive") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    def originalTimeZone = sql "SELECT @@time_zone"
    String dbName = "test_hive_int96_write_timezone"
    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalogName = "${hivePrefix}_int96_write_timezone"
        try {
            hive_docker "CREATE DATABASE IF NOT EXISTS ${dbName}"
            hive_docker "DROP TABLE IF EXISTS ${dbName}.events"
            hive_docker """CREATE TABLE ${dbName}.events (
                id INT, ts TIMESTAMP, items ARRAY<TIMESTAMP>, record STRUCT<ts:TIMESTAMP>
            ) STORED AS PARQUET"""
            // Unset and explicitly empty both mean wall-clock INT96. A named zone
            // must override the insert session for both scalar and nested values.
            for (String catalogZone : [null, "", "Asia/Shanghai", "America/Los_Angeles"]) {
                sql "SWITCH internal"
                sql "DROP CATALOG IF EXISTS ${catalogName}"
                String zoneProperty = catalogZone == null ? "" : ", 'hive.parquet.time-zone' = '${catalogZone}'"
                sql """CREATE CATALOG ${catalogName} PROPERTIES (
                    'type' = 'hms',
                    'hadoop.username' = 'hadoop',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
                    ${zoneProperty}
                )"""
                hive_docker "TRUNCATE TABLE ${dbName}.events"
                sql "SWITCH ${catalogName}"
                sql "USE ${dbName}"
                int id = 0
                for (String insertZone : ["Asia/Shanghai", "UTC", "America/Los_Angeles"]) {
                    sql "SET time_zone = '${insertZone}'"
                    sql """INSERT INTO events SELECT ${++id},
                        CAST('2023-04-20 00:00:00.123456' AS DATETIMEV2(6)),
                        ARRAY(CAST('2023-04-20 00:00:00.123456' AS DATETIMEV2(6))),
                        NAMED_STRUCT('ts', CAST('2023-04-20 00:00:00.123456' AS DATETIMEV2(6)))"""
                }
                for (String readZone : ["UTC", "Asia/Shanghai", "America/Los_Angeles"]) {
                    sql "SET time_zone = '${readZone}'"
                    def rows = sql """SELECT id, CAST(ts AS STRING), CAST(items[1] AS STRING),
                        CAST(STRUCT_ELEMENT(record, 'ts') AS STRING) FROM events ORDER BY id"""
                    assertEquals(3, rows.size())
                    rows.eachWithIndex { row, index ->
                        assertEquals(index + 1, row[0])
                        assertEquals(["2023-04-20 00:00:00.123456"] * 3, row[1..3])
                    }
                }
            }
        } finally {
            sql "SET time_zone = '${originalTimeZone[0][0]}'"
            sql "SWITCH internal"
            sql "DROP CATALOG IF EXISTS ${catalogName}"
            hive_docker "DROP TABLE IF EXISTS ${dbName}.events"
            hive_docker "DROP DATABASE IF EXISTS ${dbName}"
        }
    }
}
