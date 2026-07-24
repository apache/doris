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

suite("test_paimon_schema_metadata_atomicity_matrix", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_schema_metadata_atomicity_matrix"
    String dbName = "paimon_schema_metadata_atomicity_db"
    String tableName = "metadata_timeline"

    def createTag = { String tagName ->
        spark_paimon """
            call paimon.sys.create_tag(
                table => '${dbName}.${tableName}',
                tag => '${tagName}'
            )
        """
    }
    def snapshotCount = {
        return spark_paimon("""
            select count(*) from paimon.${dbName}.`${tableName}\$snapshots`
        """)[0][0].toString().toInteger()
    }
    def schemaCount = {
        return spark_paimon("""
            select count(*) from paimon.${dbName}.`${tableName}\$schemas`
        """)[0][0].toString().toInteger()
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        spark_paimon_multi """
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.${tableName};
            create table paimon.${dbName}.${tableName} (
                id int not null,
                required_name string not null,
                optional_value int,
                info struct<value:int, keep:int>
            ) using paimon
            tblproperties ('file.format'='parquet');
            insert into paimon.${dbName}.${tableName}
                values (1, 'name-1', 10, named_struct('value', 100, 'keep', 101));
        """
        createTag("metadata_before_change")
        int initialSnapshots = snapshotCount()

        // Scenario S19-comment: top-level and nested comments create schema versions, not data snapshots.
        spark_paimon """
            alter table paimon.${dbName}.${tableName}
                alter column required_name comment 'top-level-comment'
        """
        spark_paimon """
            alter table paimon.${dbName}.${tableName}
                alter column info.value comment 'nested-comment'
        """
        assertEquals(initialSnapshots, snapshotCount())

        // Scenario S19-nullability: required-to-optional keeps both current and tagged reads stable.
        spark_paimon """
            alter table paimon.${dbName}.${tableName}
                alter column required_name drop not null
        """
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh table ${tableName}"""
        assertEquals([[1, "name-1", 10, 100]], sql("""
            select id, required_name, optional_value, info.value
            from ${tableName}
            order by id
        """))
        assertEquals([[1, "name-1", 10, 100]], sql("""
            select id, required_name, optional_value, info.value
            from ${tableName}@tag(metadata_before_change)
            order by id
        """))

        // Scenario S20-default: defaults are schema changes and apply when a later insert omits the field.
        int schemasBeforeDefault = schemaCount()
        spark_paimon """
            alter table paimon.${dbName}.${tableName}
                alter column optional_value set default 7
        """
        assertTrue(schemaCount() > schemasBeforeDefault)
        spark_paimon """
            insert into paimon.${dbName}.${tableName} (id, required_name, info)
            values (2, 'name-2', named_struct('value', 200, 'keep', 201))
        """
        int snapshotsAfterDefault = snapshotCount()
        sql """refresh table ${tableName}"""
        assertEquals([[1, 10], [2, 7]], sql("""
            select id, optional_value from ${tableName} order by id
        """))

        // Scenario S20-nullability-strengthen: Paimon rejects optional-to-required atomically.
        int schemasBeforeNotNull = schemaCount()
        boolean notNullRejected = false
        try {
            spark_paimon """
                alter table paimon.${dbName}.${tableName}
                    alter column optional_value set not null
            """
        } catch (Exception e) {
            notNullRejected = true
            assertTrue(e.message.contains("Cannot change nullable column to non-nullable"))
        }
        assertTrue(notNullRejected, "Paimon must reject optional-to-required evolution")
        assertEquals(schemasBeforeNotNull, schemaCount())

        // Scenario S20-narrowing: incompatible narrowing is atomic.
        int schemasBeforeNarrowing = schemaCount()
        boolean narrowingRejected = false
        try {
            spark_paimon """
                alter table paimon.${dbName}.${tableName}
                    alter column optional_value type smallint
            """
        } catch (Exception e) {
            narrowingRejected = true
            assertTrue(e.message.toLowerCase().contains("type"))
        }
        assertTrue(narrowingRejected, "Paimon must reject incompatible narrowing")
        assertEquals(schemasBeforeNarrowing, schemaCount())

        sql """refresh table ${tableName}"""
        assertEquals([
                [1, "name-1", 10, 100],
                [2, "name-2", 7, 200]
        ], sql("""
            select id, required_name, optional_value, info.value
            from ${tableName}
            order by id
        """))
        assertEquals(snapshotsAfterDefault, snapshotCount())
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
