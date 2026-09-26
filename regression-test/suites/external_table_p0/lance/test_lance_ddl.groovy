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

suite("test_lance_ddl", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance DDL test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_ddl"
    String databaseName = "lance_ddl_db"
    String tableName = "events"

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance_ddl",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        sql """DROP DATABASE IF EXISTS `${catalogName}`.`${databaseName}` FORCE"""
        sql """CREATE DATABASE `${catalogName}`.`${databaseName}`"""
        sql """
            CREATE TABLE `${catalogName}`.`${databaseName}`.`${tableName}` (
                id INT NOT NULL COMMENT 'identifier',
                name STRING NULL,
                tags ARRAY<STRING> NULL,
                amount DECIMAL(10, 2) NULL
            ) ENGINE=LANCE
            COMMENT 'Lance DDL regression table'
            PROPERTIES ("owner" = "doris")
        """

        def columns = sql """DESC `${catalogName}`.`${databaseName}`.`${tableName}`"""
        assertEquals(["id", "name", "tags", "amount"], columns.collect { it[0] })
        assertEquals("int", columns[0][1].toString().toLowerCase())
        assertEquals("array<text>", columns[2][1].toString().toLowerCase())

        sql """
            ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
            ADD COLUMN score INT NULL
        """
        sql """
            ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
            ADD COLUMN (
                active BOOLEAN NULL,
                note STRING NULL
            )
        """
        sql """
            ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
            MODIFY COLUMN score BIGINT NOT NULL
        """
        sql """
            ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
            RENAME COLUMN score TO ranking
        """
        sql """
            ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
            DROP COLUMN note
        """

        columns = sql """DESC `${catalogName}`.`${databaseName}`.`${tableName}`"""
        assertEquals(
                ["id", "name", "tags", "amount", "ranking", "active"],
                columns.collect { it[0] })
        assertEquals("bigint", columns[4][1].toString().toLowerCase())
        assertEquals("NO", columns[4][2])

        test {
            sql """
                ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
                ADD COLUMN positioned INT NULL FIRST
            """
            exception "does not support column positions"
        }
        test {
            sql """
                ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
                ADD COLUMN defaulted INT NULL DEFAULT '1'
            """
            exception "does not support default values"
        }
        test {
            sql """
                ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}`
                MODIFY COLUMN ranking BIGINT NOT NULL COMMENT 'rank'
            """
            exception "does not support column comments"
        }

        String showCreate = sql("""SHOW CREATE TABLE `${catalogName}`.`${databaseName}`.`${tableName}`""")[0][1]
        assertTrue(showCreate.contains("ENGINE=LANCE"))
        assertTrue(showCreate.contains("COMMENT 'Lance DDL regression table'"))
        assertTrue(showCreate.contains("\"owner\" = \"doris\""))
        assertFalse(showCreate.contains("password"))

        test {
            sql """ALTER TABLE `${catalogName}`.`${databaseName}`.`${tableName}` RENAME `renamed_events`"""
            exception "Lance table rename is not supported"
        }

        sql """DROP TABLE `${catalogName}`.`${databaseName}`.`${tableName}`"""
        sql """DROP DATABASE `${catalogName}`.`${databaseName}`"""
    } finally {
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
