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

suite("test_hive_avro_read", "p0,external,hive,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        logger.info("disable Hive test")
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String versions = context.config.otherConfigs.get("hiveAvroTestVersions") ?: "hive2,hive3"
    for (String hivePrefix : versions.split(',')) {
        setHivePrefix(hivePrefix)
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String warehouse = context.config.otherConfigs.get("hiveAvroWarehouse")
        hive_docker("CREATE DATABASE IF NOT EXISTS doris_hive_avro_read" +
                (warehouse ? " LOCATION '${warehouse}'" : ""))
        if (warehouse) {
            hive_docker "ALTER DATABASE doris_hive_avro_read SET LOCATION '${warehouse}'"
        }
        // Hive 2 resolves partition ALTER calls in the session database even for qualified tables.
        hive_docker "USE doris_hive_avro_read"
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_evolved"
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_values"
        hive_docker """CREATE TABLE doris_hive_avro_read.avro_values (
            id INT, label STRING, amount DECIMAL(12,2), day DATE, clock TIMESTAMP,
            items ARRAY<INT>, labels MAP<STRING,STRING>, payload STRUCT<note:STRING,n:BIGINT>)
            STORED AS AVRO"""
        hive_docker """INSERT INTO doris_hive_avro_read.avro_values
            SELECT 1,'first',CAST(-123.45 AS DECIMAL(12,2)),CAST('1969-12-31' AS DATE),
                CAST('1969-12-31 23:59:59.123' AS TIMESTAMP),array(1,CAST(NULL AS INT)),
                map('a','one','b',CAST(NULL AS STRING)),named_struct('note','nested','n',CAST(7 AS BIGINT))"""
        // Hive 2/3 infer array() as ARRAY<STRING>; a typed NULL aggregate produces empty ARRAY<INT>.
        hive_docker """INSERT INTO doris_hive_avro_read.avro_values
            SELECT 2,CAST(NULL AS STRING),CAST(0 AS DECIMAL(12,2)),CAST('2024-01-02' AS DATE),
                CAST('2024-01-02 03:04:05.678' AS TIMESTAMP),empty_items,map(),
                CASE WHEN true THEN NULL ELSE named_struct('note','unused','n',CAST(0 AS BIGINT)) END
            FROM (SELECT collect_list(CAST(NULL AS INT)) AS empty_items) empty_array"""
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_partitions"
        hive_docker """CREATE TABLE doris_hive_avro_read.avro_partitions (id INT,label STRING)
            PARTITIONED BY (p STRING) STORED AS AVRO"""
        hive_docker """INSERT INTO doris_hive_avro_read.avro_partitions PARTITION(p='one')
            SELECT 1,'first'"""
        hive_docker """INSERT INTO doris_hive_avro_read.avro_partitions PARTITION(p='two')
            SELECT 2,CAST(NULL AS STRING)"""
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_empty"
        hive_docker "CREATE TABLE doris_hive_avro_read.avro_empty (id INT) STORED AS AVRO"
        def locations = hive_docker("DESCRIBE FORMATTED doris_hive_avro_read.avro_values").findAll {
            it[0]?.toString()?.trim()?.replace(':', '') == 'Location'
        }
        assertEquals(1, locations.size(), "Hive must report the physical Avro table location")
        String location = locations[0][1].toString().trim()
        // A second HMS schema reads the same old files with reordered and added columns.
        hive_docker """CREATE EXTERNAL TABLE doris_hive_avro_read.avro_evolved (
            label STRING,id INT,amount DECIMAL(12,2),day DATE,clock TIMESTAMP,
            items ARRAY<INT>,labels MAP<STRING,STRING>,payload STRUCT<note:STRING,n:BIGINT>,added STRING)
            STORED AS AVRO LOCATION '${location}'"""

        // The partition reader adds a default to old physical records; using the table schema
        // instead would silently produce NULL. Native reads must reject that differing override.
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_partition_override"
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_partition_source"
        String oldSchema = '{"type":"record","name":"partition_record","fields":[' +
                '{"name":"id","type":["null","int"],"default":null}]}'
        String partitionSchema = '{"type":"record","name":"partition_record","fields":[' +
                '{"name":"id","type":["null","int"],"default":null},' +
                '{"name":"added","type":"int","default":7}]}'
        hive_docker """CREATE TABLE doris_hive_avro_read.avro_partition_source (id INT)
            STORED AS AVRO TBLPROPERTIES ('avro.schema.literal'='${oldSchema}')"""
        hive_docker "INSERT INTO doris_hive_avro_read.avro_partition_source SELECT 1"
        def sourceLocations = hive_docker("DESCRIBE FORMATTED doris_hive_avro_read.avro_partition_source").findAll {
            it[0]?.toString()?.trim()?.replace(':', '') == 'Location'
        }
        assertEquals(1, sourceLocations.size(), "Hive must report the old Avro data location")
        String sourceLocation = sourceLocations[0][1].toString().trim()
        hive_docker """CREATE EXTERNAL TABLE doris_hive_avro_read.avro_partition_override (id INT,added INT)
            PARTITIONED BY (p STRING) STORED AS AVRO"""
        hive_docker """ALTER TABLE doris_hive_avro_read.avro_partition_override
            ADD PARTITION (p='old') LOCATION '${sourceLocation}'"""
        hive_docker """ALTER TABLE doris_hive_avro_read.avro_partition_override PARTITION (p='old')
            SET SERDEPROPERTIES ('avro.schema.literal'='${partitionSchema}')"""

        // A valid Parquet partition cannot share the table-level Avro physical reader.
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_mixed_format"
        hive_docker "DROP TABLE IF EXISTS doris_hive_avro_read.avro_parquet_source"
        hive_docker "CREATE TABLE doris_hive_avro_read.avro_parquet_source (id INT) STORED AS PARQUET"
        hive_docker "INSERT INTO doris_hive_avro_read.avro_parquet_source SELECT 2"
        def parquetLocations = hive_docker("DESCRIBE FORMATTED doris_hive_avro_read.avro_parquet_source").findAll {
            it[0]?.toString()?.trim()?.replace(':', '') == 'Location'
        }
        assertEquals(1, parquetLocations.size(), "Hive must report the physical Parquet data location")
        String parquetLocation = parquetLocations[0][1].toString().trim()
        hive_docker """CREATE EXTERNAL TABLE doris_hive_avro_read.avro_mixed_format (id INT)
            PARTITIONED BY (p STRING) STORED AS AVRO"""
        hive_docker """ALTER TABLE doris_hive_avro_read.avro_mixed_format
            ADD PARTITION (p='parquet') LOCATION '${parquetLocation}'"""
        hive_docker """ALTER TABLE doris_hive_avro_read.avro_mixed_format PARTITION (p='parquet')
            SET FILEFORMAT PARQUET"""
        hive_docker """ALTER TABLE doris_hive_avro_read.avro_mixed_format PARTITION (p='parquet')
            SET SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"""

        String storageProperties = ""
        if (warehouse?.startsWith("s3")) {
            String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
            storageProperties = """, 's3.endpoint'='http://${host}:${minioPort}',
                's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password',
                'use_path_style'='true'"""
        }
        sql "DROP CATALOG IF EXISTS hive_avro_read_catalog"
        sql """CREATE CATALOG hive_avro_read_catalog PROPERTIES (
            'type'='hms', 'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            'hadoop.username'='hadoop', 'fs.defaultFS'='hdfs://${host}:${hdfsPort}'${storageProperties})"""
        try {
            sql "SWITCH hive_avro_read_catalog"
            sql "USE doris_hive_avro_read"
            sql "SET enable_file_scanner_v2 = true"
            sql "SET time_zone = '+00:00'"
            // Avro field names are authoritative independently of the positional ORC/Parquet options.
            sql "SET hive_orc_use_column_names = false"
            sql "SET hive_parquet_use_column_names = false"
            "order_qt_${hivePrefix}_values" "SELECT * FROM avro_values ORDER BY id"
            "order_qt_${hivePrefix}_projection" "SELECT label,id,element_at(payload,'note') FROM avro_values WHERE id > 0 ORDER BY id"
            "qt_${hivePrefix}_count" "SELECT COUNT(*),COUNT(label),SUM(amount) FROM avro_values"
            "order_qt_${hivePrefix}_null" "SELECT id FROM avro_values WHERE label IS NULL ORDER BY id"
            "order_qt_${hivePrefix}_partitions" "SELECT p,label,id FROM avro_partitions ORDER BY id"
            "order_qt_${hivePrefix}_pruned" "SELECT id,label FROM avro_partitions WHERE p='one' ORDER BY id"
            "qt_${hivePrefix}_partition_count" "SELECT COUNT(*),COUNT(label) FROM avro_partitions"
            test {
                sql "SELECT id,added FROM avro_partition_override WHERE p='old'"
                exception "Native Hive Avro partition reader schema overrides are unsupported"
            }
            test {
                sql "SELECT id FROM avro_mixed_format WHERE p='parquet'"
                exception "Native Hive Avro reads do not support partition storage format"
            }
            "qt_${hivePrefix}_empty" "SELECT COUNT(*) FROM avro_empty"
            "order_qt_${hivePrefix}_evolved" "SELECT label,id,added,items FROM avro_evolved ORDER BY id"
        } finally {
            sql "SWITCH internal"
            sql "DROP CATALOG IF EXISTS hive_avro_read_catalog"
        }
    }
}
