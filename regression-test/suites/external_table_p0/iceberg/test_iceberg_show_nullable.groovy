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

suite("test_iceberg_show_nullable", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_show_nullable"
    String dbName = "iceberg_show_nullable_db_" + UUID.randomUUID().toString().replace("-", "")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    String defaultFs = "hdfs://${externalEnvIp}:${hdfsPort}"
    String tableName = "${catalogName}.${dbName}.required_tbl"

    sql """drop catalog if exists ${catalogName}"""
    sql """create catalog ${catalogName} properties (
        'type' = 'iceberg',
        'iceberg.catalog.type' = 'hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
        'fs.defaultFS' = '${defaultFs}',
        'warehouse' = '${defaultFs}/warehouse'
    )"""

    try {
        sql """create database ${catalogName}.${dbName}"""
        sql """create table ${tableName} (
            id bigint not null,
            value string not null comment 'required value',
            event_time datetime
        ) properties ('format-version' = '2', 'write.format.default' = 'parquet')"""

        def checkSchema = { boolean valueIsRequired ->
            def nullable = sql("desc ${tableName}").collectEntries { row -> [(row[0]): row[2]] }
            assertEquals("No", nullable["id"])
            assertEquals(valueIsRequired ? "No" : "Yes", nullable["value"])
            assertEquals("Yes", nullable["event_time"])
            String ddl = sql("show create table ${tableName}")[0][1]
            assertTrue(ddl.contains("`id` bigint NOT NULL"))
            assertTrue(ddl.contains("`value` text " + (valueIsRequired ? "NOT NULL" : "NULL")))
            assertTrue(ddl.contains("`event_time` datetimev2(6) NULL"))
            assertTrue(ddl.contains("required value"))
        }

        // Empty tables have no snapshot yet, but still have a declared schema.
        checkSchema(true)
        sql """insert into ${tableName} values (1, 'ok', '2026-01-01 00:00:00'), (2, 'optional time', null)"""
        checkSchema(true)
        assertEquals(1L, sql("select count(*) from ${tableName} where event_time is null")[0][0])

        test {
            sql """insert into ${tableName} values (null, 'bad', '2026-01-02 00:00:00')"""
            exception "Column 'id' is declared non-nullable but contains nulls"
        }
        test {
            sql """insert into ${tableName} values (3, null, '2026-01-02 00:00:00')"""
            exception "Column 'value' is declared non-nullable but contains nulls"
        }
        assertEquals(2L, sql("select count(*) from ${tableName}")[0][0])

        // A schema-only change must be visible even though it creates no new data snapshot.
        def snapshotBefore = sql("select snapshot_id from ${tableName}\$snapshots order by snapshot_id")
        sql """alter table ${tableName} modify column value string null"""
        sql """refresh table ${tableName}"""
        assertEquals(snapshotBefore, sql("select snapshot_id from ${tableName}\$snapshots order by snapshot_id"))
        checkSchema(false)
        sql """insert into ${tableName} values (3, null, null)"""
        assertEquals(1L, sql("select count(*) from ${tableName} where value is null")[0][0])
        assertEquals(0L, sql("select count(*) from ${tableName} where id is null")[0][0])
        checkSchema(false)
    } finally {
        sql """drop database if exists ${catalogName}.${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
