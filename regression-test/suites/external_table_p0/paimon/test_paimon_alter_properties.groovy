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

suite("test_paimon_alter_properties", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_paimon_alter_properties"
    String dbName = "test_paimon_alter_properties_db"
    String tableName = "paimon_alter_properties"

    def schemaId = {
        return (sql """
            SELECT MAX(schema_id) FROM `${tableName}\$schemas`
        """)[0][0] as long
    }
    def optionRows = { String key ->
        return sql("""
            SELECT value
            FROM `${tableName}\$options`
            WHERE `key` = '${key}'
        """)
    }
    def optionValue = { String key ->
        List<List<Object>> rows = optionRows(key)
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """SWITCH `${catalogName}`"""
    sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
    sql """CREATE DATABASE `${dbName}`"""
    sql """USE `${dbName}`"""
    sql """DROP TABLE IF EXISTS `${tableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
            id INT NOT NULL,
            seq BIGINT NULL,
            payload STRING NULL
        ) ENGINE=paimon
        PROPERTIES (
            'primary-key' = 'id',
            'snapshot.num-retained.min' = '2',
            'snapshot.num-retained.max' = '5'
        )
    """

    // One Doris statement becomes one atomic Paimon schema change containing
    // every SetOption. The refreshed system tables are visible immediately.
    long beforeSchemaId = schemaId()
    sql """
        ALTER TABLE `${tableName}` SET (
            'snapshot.num-retained.min' = '3',
            'snapshot.num-retained.max' = '6'
        )
    """
    assertEquals(beforeSchemaId + 1, schemaId())
    assertEquals("3", optionValue("snapshot.num-retained.min"))
    assertEquals("6", optionValue("snapshot.num-retained.max"))

    // Updating one option replaces it without removing the other option.
    beforeSchemaId = schemaId()
    sql """
        ALTER TABLE `${tableName}` SET (
            'snapshot.num-retained.max' = '8'
        )
    """
    assertEquals(beforeSchemaId + 1, schemaId())
    assertEquals("3", optionValue("snapshot.num-retained.min"))
    assertEquals("8", optionValue("snapshot.num-retained.max"))

    // auto_analyze_policy is Doris metadata, so it does not create a Paimon
    // schema version. Mixing the two property domains is rejected up front.
    beforeSchemaId = schemaId()
    sql """ALTER TABLE `${tableName}` SET ('auto_analyze_policy' = 'disable')"""
    assertEquals(beforeSchemaId, schemaId())
    test {
        sql """
            ALTER TABLE `${tableName}` SET (
                'auto_analyze_policy' = 'enable',
                'snapshot.num-retained.max' = '9'
            )
        """
        exception "auto_analyze_policy cannot be set with external table properties"
    }
    assertEquals(beforeSchemaId, schemaId())
    assertEquals("8", optionValue("snapshot.num-retained.max"))

    // Paimon validates all SetOption changes before committing the next schema.
    // A bad field-scoped option must not leak the valid max-retention update.
    test {
        sql """
            ALTER TABLE `${tableName}` SET (
                'fields.missing.sequence-group' = 'seq',
                'snapshot.num-retained.max' = '10'
            )
        """
        exception "missing"
    }
    assertEquals(beforeSchemaId, schemaId())
    assertEquals("8", optionValue("snapshot.num-retained.max"))
    assertTrue(optionRows("fields.missing.sequence-group").isEmpty())
}
