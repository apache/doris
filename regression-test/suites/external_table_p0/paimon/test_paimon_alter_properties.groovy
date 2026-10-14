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
    sql """DROP TABLE IF EXISTS `paimon_alter_properties`"""
    sql """
        CREATE TABLE `paimon_alter_properties` (
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
    order_qt_created_schema """SELECT MAX(schema_id) FROM `paimon_alter_properties\$schemas`"""

    // One Doris statement becomes one atomic Paimon schema change containing
    // every SetOption, so the schema id moves by one. The refreshed system
    // tables are visible immediately.
    sql """
        ALTER TABLE `paimon_alter_properties` SET (
            'snapshot.num-retained.min' = '3',
            'snapshot.num-retained.max' = '6'
        )
    """
    order_qt_multi_set_schema """SELECT MAX(schema_id) FROM `paimon_alter_properties\$schemas`"""
    order_qt_multi_set_options """
        SELECT `key`, value FROM `paimon_alter_properties\$options`
        WHERE `key` IN ('snapshot.num-retained.min', 'snapshot.num-retained.max')
    """

    // Updating one option replaces it without removing the other option.
    sql """
        ALTER TABLE `paimon_alter_properties` SET (
            'snapshot.num-retained.max' = '8'
        )
    """
    order_qt_single_set_schema """SELECT MAX(schema_id) FROM `paimon_alter_properties\$schemas`"""
    order_qt_single_set_options """
        SELECT `key`, value FROM `paimon_alter_properties\$options`
        WHERE `key` IN ('snapshot.num-retained.min', 'snapshot.num-retained.max')
    """

    // auto_analyze_policy is Doris metadata, so it does not create a Paimon
    // schema version. Mixing the two property domains is rejected up front.
    sql """ALTER TABLE `paimon_alter_properties` SET ('auto_analyze_policy' = 'disable')"""
    test {
        sql """
            ALTER TABLE `paimon_alter_properties` SET (
                'auto_analyze_policy' = 'enable',
                'snapshot.num-retained.max' = '9'
            )
        """
        exception "auto_analyze_policy cannot be set with external table properties"
    }
    order_qt_doris_owned_schema """SELECT MAX(schema_id) FROM `paimon_alter_properties\$schemas`"""
    order_qt_doris_owned_options """
        SELECT `key`, value FROM `paimon_alter_properties\$options`
        WHERE `key` IN ('snapshot.num-retained.max', 'auto_analyze_policy')
    """

    // Paimon validates all SetOption changes before committing the next schema.
    // A bad field-scoped option must not leak the valid max-retention update.
    test {
        sql """
            ALTER TABLE `paimon_alter_properties` SET (
                'snapshot.num-retained.max' = '10',
                'fields.missing.sequence-group' = 'seq'
            )
        """
        exception "missing"
    }
    order_qt_rejected_schema """SELECT MAX(schema_id) FROM `paimon_alter_properties\$schemas`"""
    order_qt_rejected_options """
        SELECT `key`, value FROM `paimon_alter_properties\$options`
        WHERE `key` IN ('snapshot.num-retained.max', 'fields.missing.sequence-group')
    """
}
