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

suite("test_paimon_partition_mutation_atomicity",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_partition_mutation_atomicity"
    String dbName = "paimon_partition_mutation_atomicity_db"
    String tableName = "partition_contract"

    def schemaCount = {
        return spark_paimon("""
            select count(*) from paimon.${dbName}.`${tableName}\$schemas`
        """)[0][0].toString().toInteger()
    }
    def assertSparkRejected = { String statement, String operation, String expectedMessage ->
        String error = null
        try {
            spark_paimon(statement)
        } catch (Exception e) {
            error = e.getMessage()
        }
        assertNotNull(error, "Paimon must reject partition-key ${operation}")
        assertTrue(error.contains(expectedMessage),
                "Partition-key ${operation} should report '${expectedMessage}': ${error}")
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
                id int,
                part_a string,
                payload string,
                part_b int
            ) using paimon
            partitioned by (part_a, part_b)
            tblproperties ('file.format'='parquet');
            insert into paimon.${dbName}.${tableName} values
                (1, 'A', 'base-a1', 1),
                (2, 'A', 'base-a2', 2),
                (3, 'B', 'base-b1', 1);
            call paimon.sys.create_tag(
                table => '${dbName}.${tableName}',
                tag => 'partition_contract_base'
            );
        """

        // Scenario PM-M01-positive: partition columns may move in schema order without changing
        // their partition identity, values, tag or filter semantics.
        spark_paimon_multi """
            alter table paimon.${dbName}.${tableName} alter column part_b first;
            alter table paimon.${dbName}.${tableName} alter column part_a after id;
            alter table paimon.${dbName}.${tableName} add column added_payload string after payload;
            insert into paimon.${dbName}.${tableName}
                (part_b, id, part_a, payload, added_payload)
                values (3, 4, 'A', 'after-reorder', 'added');
        """
        int schemasAfterSupportedChanges = schemaCount()

        // Scenario PM-M02-negative: rename, type change and drop of either partition key must be
        // rejected atomically. These are format restrictions, not missing Doris capabilities.
        assertSparkRejected("""
            alter table paimon.${dbName}.${tableName}
                rename column part_a to renamed_part_a
        """, "rename", "Cannot rename partition column")
        assertSparkRejected("""
            alter table paimon.${dbName}.${tableName}
                alter column part_b type bigint
        """, "type change", "Cannot update partition column")
        assertSparkRejected("""
            alter table paimon.${dbName}.${tableName} drop column part_a
        """, "drop", "Cannot drop partition key or primary key")
        assertSparkRejected("""
            alter table paimon.${dbName}.${tableName} drop column part_b
        """, "drop", "Cannot drop partition key or primary key")
        assertEquals(schemasAfterSupportedChanges, schemaCount(),
                "Rejected partition mutations must not create Paimon schemas")

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh table ${tableName}"""

        // Scenario PM-M03: current data and static partition filtering survive accepted reorder
        // and all rejected mutations.
        qt_current_partition_filter """
            select id from ${tableName}
            where part_a = 'A' and part_b in (1, 2, 3)
            order by id
        """
        qt_current_added_payload """
            select id, added_payload from ${tableName}
            where part_a = 'A' and part_b = 3
        """

        // Scenario PM-M04: the pre-reorder tag retains its original schema and partitions.
        qt_base_tag_partition_filter """
            select id, part_a, part_b, payload
            from ${tableName}@tag(partition_contract_base)
            where part_a in ('A', 'B')
            order by id
        """
        test {
            sql """
                select added_payload
                from ${tableName}@tag(partition_contract_base)
            """
            exception "Unknown column 'added_payload'"
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
