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

suite("test_iceberg_alter_properties", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is not enabled, skip this test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_iceberg_alter_properties"
    String dbName = "test_iceberg_alter_properties_db"
    String tableName = "iceberg_alter_properties"

    def refreshSparkTable = {
        spark_iceberg """REFRESH TABLE demo.${dbName}.${tableName}"""
    }
    def sparkProperty = { String key ->
        List<List<Object>> rows = spark_iceberg """
            SHOW TBLPROPERTIES demo.${dbName}.${tableName} ('${key}')
        """
        assertEquals(1, rows.size())
        return rows[0][1].toString()
    }

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1'
        )
    """
    sql """SWITCH ${catalogName}"""
    sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
    sql """CREATE DATABASE ${dbName}"""
    sql """USE ${dbName}"""

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """CREATE TABLE ${tableName} (id INT) PROPERTIES ('format-version' = '2')"""

    // Multiple connector properties are committed together.
    sql """
        ALTER TABLE ${tableName} SET (
            'write.target-file-size-bytes' = '134217728',
            'commit.manifest.min-count-to-merge' = '50'
        )
    """
    refreshSparkTable()
    assertEquals("134217728", sparkProperty("write.target-file-size-bytes"))
    assertEquals("50", sparkProperty("commit.manifest.min-count-to-merge"))

    // A later SET replaces the selected property and preserves unrelated ones.
    sql """
        ALTER TABLE ${tableName} SET (
            'write.target-file-size-bytes' = '268435456'
        )
    """
    refreshSparkTable()
    assertEquals("268435456", sparkProperty("write.target-file-size-bytes"))
    assertEquals("50", sparkProperty("commit.manifest.min-count-to-merge"))

    // Doris-owned properties stay on the Doris path and never reach Iceberg.
    sql """ALTER TABLE ${tableName} SET ('auto_analyze_policy' = 'disable')"""
    refreshSparkTable()
    assertEquals("268435456", sparkProperty("write.target-file-size-bytes"))
    test {
        sql """
            ALTER TABLE ${tableName} SET (
                'auto_analyze_policy' = 'enable',
                'write.target-file-size-bytes' = '536870912'
            )
        """
        exception "auto_analyze_policy cannot be set with external table properties"
    }
    refreshSparkTable()
    assertEquals("268435456", sparkProperty("write.target-file-size-bytes"))

    // Iceberg validates the whole UpdateProperties commit. A rejected format
    // downgrade must not commit the otherwise valid size change.
    test {
        sql """
            ALTER TABLE ${tableName} SET (
                'format-version' = '1',
                'write.target-file-size-bytes' = '536870912'
            )
        """
        exception "Cannot downgrade"
    }
    refreshSparkTable()
    assertEquals("2", sparkProperty("format-version"))
    assertEquals("268435456", sparkProperty("write.target-file-size-bytes"))
}
