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

suite("test_hive_cos_insert_overwrite", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String txYunAk = context.config.otherConfigs.get("txYunAk")
    String txYunSk = context.config.otherConfigs.get("txYunSk")
    if (externalEnvIp == null || txYunAk == null || txYunSk == null) {
        return
    }

    def getConfigOrDefault = { String key, String defaultValue ->
        String value = context.config.otherConfigs.get(key)
        return value == null ? defaultValue : value
    }

    String suffix = System.currentTimeMillis().toString()
    String catalogName = "test_hive_cos_insert_overwrite_catalog"
    String dbName = "test_hive_cos_insert_overwrite_${suffix}"
    String hmsPort = getConfigOrDefault("hive3HmsPort", "9383")
    String cosEndpoint = getConfigOrDefault("txYunEndpoint", "cos.ap-beijing.myqcloud.com")
    String cosParentPath = getConfigOrDefault("txYunBucket", "doris-build-1308700295")
    String dbLocation = "cosn://${cosParentPath}/hive/hms/overwrite/${suffix}"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "hms",
            "hive.metastore.uris" = "thrift://${externalEnvIp}:${hmsPort}",
            "cos.access_key" = "${txYunAk}",
            "cos.secret_key" = "${txYunSk}",
            "cos.endpoint" = "${cosEndpoint}"
        )
    """

    sql """SWITCH ${catalogName}"""
    sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
    sql """
        CREATE DATABASE ${dbName}
        PROPERTIES (
            "location" = "${dbLocation}"
        )
    """
    sql """USE ${dbName}"""

    sql """ drop table if EXISTS hive_cos_insert_overwrite_tbl """
    sql """
        CREATE TABLE hive_cos_insert_overwrite_tbl (
            id INT COMMENT 'id',
            name STRING COMMENT 'name',
            age INT COMMENT 'age'
        ) ENGINE=hive
        PROPERTIES (
            "file_format" = "parquet"
        );
    """

    sql """
        INSERT INTO hive_cos_insert_overwrite_tbl
        VALUES (1, 'alice', 20), (2, 'bob', 25);
    """
    def initialRows = sql """
        SELECT * FROM hive_cos_insert_overwrite_tbl ORDER BY id
    """
    assertEquals(2, initialRows.size())
    assertEquals(1, initialRows[0][0])
    assertEquals(2, initialRows[1][0])

    sql """
        INSERT OVERWRITE TABLE hive_cos_insert_overwrite_tbl
        VALUES (3, 'charlie', 30)
    """
    sql """REFRESH TABLE ${dbName}.hive_cos_insert_overwrite_tbl"""
    def firstOverwriteRows = sql """
        SELECT * FROM hive_cos_insert_overwrite_tbl ORDER BY id
    """
    assertEquals(1, firstOverwriteRows.size())
    assertEquals(3, firstOverwriteRows[0][0])

    sql """
        INSERT OVERWRITE TABLE hive_cos_insert_overwrite_tbl
        VALUES (4, 'david', 35), (5, 'eve', 28), (6, 'frank', 40)
    """
    sql """REFRESH TABLE ${dbName}.hive_cos_insert_overwrite_tbl"""
    def secondOverwriteRows = sql """
        SELECT * FROM hive_cos_insert_overwrite_tbl ORDER BY id
    """
    assertEquals(3, secondOverwriteRows.size())
    assertEquals(4, secondOverwriteRows[0][0])
    assertEquals(5, secondOverwriteRows[1][0])
    assertEquals(6, secondOverwriteRows[2][0])
}
