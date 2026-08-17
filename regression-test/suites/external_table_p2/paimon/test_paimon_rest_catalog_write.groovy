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

suite("test_paimon_rest_catalog_write",
        "p2,external,paimon,external_remote,external_remote_paimon,new_catalog_property") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String catalogProperties = context.config.otherConfigs.get("paimonDlfRestCatalog")
    String catalogName = "test_paimon_rest_catalog_write"
    String dbName = "new_dlf_paimon_db"
    String appendTable = "test_paimon_rest_write_append"
    String primaryKeyTable = "test_paimon_rest_write_pk"

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            ${catalogProperties}
        )
    """
    sql """SWITCH `${catalogName}`"""
    sql """USE `${dbName}`"""

    try {
        sql """DROP TABLE IF EXISTS `${appendTable}`"""
        sql """DROP TABLE IF EXISTS `${primaryKeyTable}`"""

        sql """
            CREATE TABLE `${appendTable}` (
                id INT NULL,
                name STRING NULL,
                score DOUBLE NULL,
                region STRING NULL
            ) ENGINE=paimon
            PARTITION BY (region) ()
        """
        sql """
            INSERT INTO `${appendTable}` VALUES
                (1, 'alice', 95.5, 'east'),
                (2, 'bob', 87.0, 'west')
        """
        sql """
            INSERT INTO `${appendTable}` VALUES
                (3, 'charlie', 92.3, 'east'),
                (4, 'diana', 88.0, 'north')
        """
        sql """
            INSERT INTO `${appendTable}` (region, score, name, id)
            VALUES ('south', 86.5, 'erin', 5)
        """
        sql """
            INSERT INTO `${appendTable}` (region, id)
            VALUES ('east', 6)
        """
        order_qt_rest_append """
            SELECT id, name, score, region
            FROM `${appendTable}`
            ORDER BY id
        """

        sql """
            CREATE TABLE `${primaryKeyTable}` (
                user_id INT NOT NULL,
                event_time BIGINT NOT NULL,
                event_type STRING NULL,
                value DOUBLE NULL
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'user_id,event_time',
                'bucket' = '2',
                'bucket-key' = 'user_id'
            )
        """
        sql """
            INSERT INTO `${primaryKeyTable}` VALUES
                (1, 100, 'click', 1.0),
                (1, 200, 'view', 2.0),
                (2, 100, 'click', 3.0),
                (1, 100, 'click_updated', 99.0)
        """
        order_qt_rest_pk """
            SELECT user_id, event_time, event_type, value
            FROM `${primaryKeyTable}`
            ORDER BY user_id, event_time
        """
    } finally {
        sql """DROP TABLE IF EXISTS `${appendTable}`"""
        sql """DROP TABLE IF EXISTS `${primaryKeyTable}`"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
