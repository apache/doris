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

suite("test_iceberg_schema_metadata_atomicity_matrix",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_schema_metadata_atomicity_matrix"
    String dbName = "iceberg_schema_metadata_atomicity_db"
    String tableName = "metadata_timeline"

    def snapshotCount = {
        return sql("""select count(*) from ${tableName}\$snapshots""")[0][0].toString().toInteger()
    }
    def schemaCount = {
        return sql("""select count(*) from ${tableName}\$metadata_log_entries""")[0][0]
                .toString().toInteger()
    }
    def allDescText = {
        return sql("""desc ${tableName}""").flatten().collect {
            it == null ? "" : it.toString()
        }.join(" ")
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1'
        )
    """
    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    try {
        sql """drop table if exists ${tableName}"""
        sql """
            create table ${tableName} (
                id int not null,
                required_name string not null,
                optional_value int,
                info struct<value:int, keep:int>,
                attrs map<string, int>
            ) engine=iceberg
            properties ('format-version'='2', 'write.format.default'='parquet')
        """
        sql """
            insert into ${tableName} values
            (1, 'name-1', 10, named_struct('value', 100, 'keep', 101), map('k', 1000))
        """
        sql """alter table ${tableName} create tag metadata_before_change"""
        int initialSnapshots = snapshotCount()

        // Scenario S19-comment: top-level and nested comments are metadata-only schema changes.
        sql """alter table ${tableName} modify column required_name comment 'top-level-comment'"""
        sql """alter table ${tableName} modify column info.value comment 'nested-comment'"""
        String descAfterComment = allDescText()
        List<List<Object>> sparkDescription = spark_iceberg("""
            describe table extended demo.${dbName}.${tableName}
        """)
        String sparkDescriptionText = sparkDescription.flatten().collect {
            it == null ? "" : it.toString()
        }.join(" ")
        assertTrue(sparkDescriptionText.contains("top-level-comment"))
        // Negative contract: Doris DESC currently omits Iceberg field comments.
        assertFalse(descAfterComment.contains("top-level-comment"))
        assertFalse(descAfterComment.contains("nested-comment"))
        assertEquals(initialSnapshots, snapshotCount())

        // Scenario S19-nullability: relaxing required to optional preserves data and historical refs.
        sql """alter table ${tableName} modify column required_name string null"""
        assertEquals([[1, "name-1", 100]], sql("""
            select id, required_name, info.value from ${tableName} order by id
        """))
        assertEquals([[1, "name-1", 100]], sql("""
            select id, required_name, info.value
            from ${tableName}@tag(metadata_before_change)
            order by id
        """))
        assertEquals(initialSnapshots, snapshotCount())

        // Scenario S20-required-add: Iceberg rejects non-null additions and commits no metadata.
        int metadataBeforeRequiredAdd = schemaCount()
        test {
            sql """alter table ${tableName} add column required_added int not null"""
            exception "default value"
        }
        assertEquals(metadataBeforeRequiredAdd, schemaCount())

        // Scenario S20-default: v2 non-null initial defaults are unsupported and atomic.
        int metadataBeforeDefault = schemaCount()
        test {
            sql """
                alter table ${tableName}
                add column default_added string default 'default-value'
            """
            exception "non-null default"
        }
        assertEquals(metadataBeforeDefault, schemaCount())

        // Scenario S20-nullability-strengthen: optional fields cannot become required with old NULLs.
        int metadataBeforeNotNull = schemaCount()
        test {
            sql """alter table ${tableName} modify column optional_value int not null"""
            exception "not null"
        }
        assertEquals(metadataBeforeNotNull, schemaCount())

        // Scenario S20-narrowing: illegal type narrowing is rejected without a schema commit.
        int metadataBeforeNarrowing = schemaCount()
        test {
            sql """alter table ${tableName} modify column optional_value smallint"""
            exception "Unsupported type for Iceberg"
        }
        assertEquals(metadataBeforeNarrowing, schemaCount())

        // Scenario S20-map-key: map keys cannot be evolved independently.
        int metadataBeforeMapKey = schemaCount()
        test {
            sql """alter table ${tableName} modify column attrs.`key` bigint"""
            exception "Cannot modify MAP key nested column"
        }
        assertEquals(metadataBeforeMapKey, schemaCount())

        sql """refresh table ${tableName}"""
        assertEquals([[1, "name-1", 10, 100, 1000]], sql("""
            select id, required_name, optional_value, info.value, attrs['k']
            from ${tableName}
            order by id
        """))
        assertEquals(initialSnapshots, snapshotCount())
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
