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

// An iceberg table stored in HMS is reachable through two catalogs: a dedicated iceberg catalog
// ('type'='iceberg', 'iceberg.catalog.type'='hms') and a heterogeneous HMS gateway catalog ('type'='hms')
// that serves plain-hive and iceberg tables side by side. Everything the dedicated catalog can do to such a
// table, the gateway must be able to do as well; this suite pins the two places where it could not.
//
//   1. Nested (dotted-path) column DDL. The gateway forwards handle-carrying ALTER operations to its embedded
//      iceberg sibling, but the five path-addressed column ops were not forwarded, so nested ADD / RENAME /
//      MODIFY COMMENT / DROP COLUMN failed with "not supported" through the gateway while succeeding through
//      the dedicated catalog on the very same table.
//   2. The table comment. The comment accessor addresses a table by NAME, not by handle, so the gateway cannot
//      route it by handle type the way it routes the ALTER ops; it answered the empty default, and an
//      iceberg-on-HMS table rendered a blank COMMENT clause / TABLE_COMMENT / SHOW TABLE STATUS Comment.
suite("test_iceberg_on_hms_gateway_ddl_parity", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hive test.")
        return
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String icebergCatalog = "test_iceberg_on_hms_gateway_iceberg_${hivePrefix}"
        String gatewayCatalog = "test_iceberg_on_hms_gateway_hms_${hivePrefix}"
        String dbName = "test_iceberg_on_hms_gateway_db"
        String tableName = "gateway_nested_parity"
        String tableComment = "comment set through the iceberg catalog"

        try {
            sql """drop catalog if exists ${icebergCatalog}"""
            sql """create catalog ${icebergCatalog} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                'use_meta_cache' = 'true'
            );"""

            sql """drop catalog if exists ${gatewayCatalog}"""
            sql """create catalog ${gatewayCatalog} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                'use_meta_cache' = 'true'
            );"""

            sql """set enable_fallback_to_original_planner=false;"""

            // ---- create the table through the DEDICATED iceberg catalog ----
            sql """switch ${icebergCatalog}"""
            sql """create database if not exists ${dbName}"""
            sql """use ${dbName}"""
            sql """drop table if exists ${tableName}"""
            sql """
            create table ${tableName} (
                id INT NOT NULL,
                s STRUCT<a:INT, b:STRING>,
                arr ARRAY<STRUCT<x:INT>>,
                m MAP<STRING, STRUCT<v:INT>>
            ) COMMENT '${tableComment}'
            """
            sql """insert into ${tableName} values (1, STRUCT(10, 'old'), ARRAY(STRUCT(100)), MAP('k', STRUCT(1000)))"""

            // ---- 1. nested column DDL, driven entirely through the GATEWAY catalog ----
            sql """switch ${gatewayCatalog}"""
            sql """use ${dbName}"""

            sql """alter table ${tableName} add column s.c STRING NULL COMMENT 'added through the gateway'"""
            sql """alter table ${tableName} add column arr.element.y INT NULL"""
            sql """alter table ${tableName} add column m.value.y INT NULL"""
            sql """alter table ${tableName} add column s.drop_me STRING NULL"""
            sql """alter table ${tableName} modify column s.a BIGINT"""
            sql """alter table ${tableName} rename column s.c TO c2"""
            sql """alter table ${tableName} modify column s.`c2` COMMENT 'renamed through the gateway'"""
            sql """alter table ${tableName} drop column s.drop_me"""

            // The dedicated catalog must see every one of those changes: same table, same metadata.
            sql """switch ${icebergCatalog}"""
            sql """use ${dbName}"""
            def viaIceberg = sql """desc ${tableName}"""
            def structTypeViaIceberg = viaIceberg.find { it[0] == "s" }[1].toString().toLowerCase()
            assertTrue(structTypeViaIceberg.contains("c2"),
                    "nested ADD/RENAME COLUMN issued through the gateway is not visible on the table: ${structTypeViaIceberg}")
            assertTrue(structTypeViaIceberg.contains("bigint"),
                    "nested MODIFY COLUMN issued through the gateway is not visible on the table: ${structTypeViaIceberg}")
            assertFalse(structTypeViaIceberg.contains("drop_me"),
                    "nested DROP COLUMN issued through the gateway is not visible on the table: ${structTypeViaIceberg}")

            // The same statement must keep failing loud for a PLAIN hive table (nothing to delegate to).
            sql """switch ${gatewayCatalog}"""
            sql """use ${dbName}"""
            String plainHiveTable = "gateway_plain_hive"
            sql """drop table if exists ${plainHiveTable}"""
            sql """create table ${plainHiveTable} (id INT, s STRUCT<a:INT>) ENGINE=hive"""
            test {
                sql """alter table ${plainHiveTable} add column s.b STRING NULL"""
                exception "not supported"
            }

            // ---- 2. the table comment must be identical through both catalogs ----
            def commentViaGateway = sql """show create table ${tableName}"""
            assertTrue(commentViaGateway[0][1].contains(tableComment),
                    "SHOW CREATE TABLE through the HMS gateway lost the table comment: ${commentViaGateway[0][1]}")

            def statusViaGateway = sql """show table status like '${tableName}'"""
            assertEquals(tableComment, statusViaGateway[0][17],
                    "SHOW TABLE STATUS through the HMS gateway lost the table comment")

            def infoViaGateway = sql """select TABLE_COMMENT from information_schema.tables
                where TABLE_SCHEMA = '${dbName}' and TABLE_NAME = '${tableName}'
                and TABLE_CATALOG = '${gatewayCatalog}'"""
            def infoViaIceberg = sql """select TABLE_COMMENT from information_schema.tables
                where TABLE_SCHEMA = '${dbName}' and TABLE_NAME = '${tableName}'
                and TABLE_CATALOG = '${icebergCatalog}'"""
            assertEquals(infoViaIceberg[0][0], infoViaGateway[0][0],
                    "information_schema.tables.TABLE_COMMENT differs between the dedicated iceberg catalog and the "
                            + "HMS gateway for the same table")
            assertEquals(tableComment, infoViaGateway[0][0])

            // A plain-hive table keeps its historical empty comment (the gateway only answers for iceberg tables).
            def plainInfo = sql """select TABLE_COMMENT from information_schema.tables
                where TABLE_SCHEMA = '${dbName}' and TABLE_NAME = '${plainHiveTable}'
                and TABLE_CATALOG = '${gatewayCatalog}'"""
            assertTrue(plainInfo[0][0] == null || plainInfo[0][0].toString().isEmpty(),
                    "a plain hive table should keep its empty comment, got: ${plainInfo[0][0]}")
        } finally {
            sql """switch ${icebergCatalog}"""
            sql """drop table if exists ${dbName}.${tableName}"""
            sql """switch ${gatewayCatalog}"""
            sql """drop table if exists ${dbName}.gateway_plain_hive"""
            sql """switch internal"""
            sql """drop catalog if exists ${gatewayCatalog}"""
            sql """drop catalog if exists ${icebergCatalog}"""
        }
    }
}
