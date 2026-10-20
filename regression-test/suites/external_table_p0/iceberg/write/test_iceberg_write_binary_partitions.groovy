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

suite("test_iceberg_write_binary_partitions", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = context.config.otherConfigs.get("iceberg_minio_endpoint")
            ?: "http://${externalEnvIp}:${minioPort}"
    String catalogName = "test_iceberg_write_binary_partitions"
    String dbName = "binary_partition_roundtrip"
    // Base64(0x9EE965) is "null": its partition must remain distinct from the NULL row.
    def hexValues = ["C3A900FF", "", "616263", "00FF80", "000102030405060708090A0B0C0D0E0FFF80", "9EE965"]
    try {
        sql "DROP CATALOG IF EXISTS ${catalogName}"
        sql """CREATE CATALOG ${catalogName} PROPERTIES (
            "type"="iceberg", "iceberg.catalog.type"="rest",
            "uri"="http://${externalEnvIp}:${restPort}",
            "s3.endpoint"="${endpoint}", "s3.access_key"="admin", "s3.secret_key"="password",
            "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SWITCH ${catalogName}"
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        sql "USE ${dbName}"
        for (String format : ["parquet", "orc"]) {
            for (String transform : ["identity", "bucket", "truncate"]) {
                String table = "binary_${format}_${transform}"
                String expression = transform == "identity" ? "binary_key"
                        : transform == "bucket" ? "bucket(16, binary_key)" : "truncate(1, binary_key)"
                sql "DROP TABLE IF EXISTS ${table}"
                spark_iceberg """CREATE TABLE demo.${dbName}.${table} (id INT, binary_key BINARY)
                    USING iceberg PARTITIONED BY (${expression})
                    TBLPROPERTIES ('format-version'='2', 'write.format.default'='${format}')"""
                // Include a prefix inside UTF-8, embedded NULs, invalid UTF-8, and arena-backed bytes.
                String rows = hexValues.withIndex().collect { value, index ->
                    "(${index + 1}, X'${value}')"
                }.join(", ") + ", (7, NULL)"
                for (String operation : ["INSERT INTO", "INSERT OVERWRITE TABLE"]) {
                    sql "${operation} ${table} VALUES ${rows}"
                    String tag = "${table}_${operation.replace(' ', '_')}"
                    // Transformed values have their own types; dropped bucket items also break COUNT limits.
                    spark_iceberg "REFRESH TABLE demo.${dbName}.${table}"
                    def partitionCount = spark_iceberg(
                            "SELECT count(*) FROM demo.${dbName}.${table}.partitions")[0][0]
                    for (String projection : ["id", "count(*)"]) {
                        explain {
                            sql "SELECT ${projection} FROM ${table}"
                            contains "partition=${partitionCount}/${partitionCount}"
                        }
                    }
                    // A source-column predicate must not prune raw transform keys as source values.
                    for (String projection : ["id", "count(*)"]) {
                        explain {
                            sql "SELECT ${projection} FROM ${table} WHERE binary_key IS NOT NULL"
                            contains "partition=${partitionCount}/${partitionCount}"
                        }
                    }
                    "order_qt_${tag}_non_null" "SELECT id FROM ${table} WHERE binary_key IS NOT NULL ORDER BY id"
                    "qt_${tag}_non_null_count" "SELECT count(*) FROM ${table} WHERE binary_key IS NOT NULL"
                    "order_qt_${tag}_bytes" "SELECT id, HEX(binary_key) FROM ${table} ORDER BY id"
                    "order_qt_${tag}_null" "SELECT id FROM ${table} WHERE binary_key IS NULL ORDER BY id"
                    def actual = sql("SELECT id, HEX(binary_key) FROM ${table} ORDER BY id")
                    spark_iceberg "REFRESH TABLE demo.${dbName}.${table}"
                    assertSparkDorisResultEquals(spark_iceberg("""
                        SELECT id, hex(binary_key) FROM demo.${dbName}.${table} ORDER BY id
                    """), actual)
                    // Spark's partition pruning verifies committed transforms independently of Doris scans.
                    hexValues.eachWithIndex { value, index ->
                        assertSparkDorisResultEquals(spark_iceberg("""
                            SELECT id FROM demo.${dbName}.${table} WHERE binary_key = X'${value}'
                        """), sql("SELECT id FROM ${table} WHERE HEX(binary_key) = '${value}' ORDER BY id"))
                    }
                    assertSparkDorisResultEquals(spark_iceberg("""
                        SELECT id FROM demo.${dbName}.${table} WHERE binary_key IS NULL
                    """), sql("SELECT id FROM ${table} WHERE binary_key IS NULL ORDER BY id"))
                }
            }
        }

        // The VALUES writes above have a single-instance source, so their sink has one writer and its exchange
        // does not hash. A multi-instance source reaches several writers through an exchange that routes each row
        // by its binary partition value, hashing the value's bytes, so one writer owns each partition and writes
        // its only data file: four files for identity, one for truncate(1), which folds the keys '10' to '13' into
        // the partition '1'. Without routing, each of the eight writers writes a file for every partition it
        // receives. Pin the writers, since a fuzzy session can pick one.
        sql "set parallel_pipeline_task_num = 8"
        for (String transform : ["identity", "truncate"]) {
            String table = "binary_multi_writer_${transform}"
            String expression = transform == "identity" ? "binary_key" : "truncate(1, binary_key)"
            sql "DROP TABLE IF EXISTS ${table}"
            spark_iceberg """CREATE TABLE demo.${dbName}.${table} (id BIGINT, binary_key BINARY)
                USING iceberg PARTITIONED BY (${expression})
                TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')"""
            for (String operation : ["INSERT INTO", "INSERT OVERWRITE TABLE"]) {
                sql """${operation} ${table}
                    SELECT number, CAST(CAST(number % 4 + 10 AS STRING) AS VARBINARY) FROM numbers("number" = "10000")"""
                "order_qt_${table}_${operation.replace(' ', '_')}" """
                    SELECT HEX(binary_key), count(*), min(id), max(id) FROM ${table} GROUP BY HEX(binary_key) ORDER BY 1"""
                "qt_${table}_${operation.replace(' ', '_')}_files" "SELECT count(*) FROM ${table}\$files"
            }
        }

        // MERGE routes the rows it writes by their binary partition value through the same exchange hash, so it
        // failed with "update_crc32c_batch is not supported for ColumnVarbinary" before VARBINARY could be hashed.
        // Rows 0 to 99 move from the keys '10'..'13' to '20'..'23', and rows 100 to 9999 are inserted.
        for (String transform : ["identity", "truncate"]) {
            String table = "binary_merge_${transform}"
            String expression = transform == "identity" ? "binary_key" : "truncate(1, binary_key)"
            sql "DROP TABLE IF EXISTS ${table}"
            spark_iceberg """CREATE TABLE demo.${dbName}.${table} (id BIGINT, binary_key BINARY, v STRING)
                USING iceberg PARTITIONED BY (${expression})
                TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
                    'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read',
                    'write.merge.mode'='merge-on-read')"""
            sql """INSERT INTO ${table}
                SELECT number, CAST(CAST(number % 4 + 10 AS STRING) AS VARBINARY), 'old' FROM numbers("number" = "100")"""
            sql """MERGE INTO ${table} t
                USING (SELECT number AS id, CAST(CAST(number % 4 + 10 AS STRING) AS VARBINARY) AS binary_key
                       FROM numbers("number" = "10000")) s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET binary_key = CAST(CAST(s.id % 4 + 20 AS STRING) AS VARBINARY), v = 'updated'
                WHEN NOT MATCHED THEN INSERT (id, binary_key, v) VALUES (s.id, s.binary_key, 'inserted')"""
            String summary = "SELECT HEX(binary_key), v, count(*), min(id), max(id) FROM ${table} GROUP BY HEX(binary_key), v"
            "order_qt_${table}" summary
            spark_iceberg "REFRESH TABLE demo.${dbName}.${table}"
            assertSparkDorisResultEquals(spark_iceberg("""
                    SELECT hex(binary_key), v, count(*), min(id), max(id) FROM demo.${dbName}.${table}
                    GROUP BY hex(binary_key), v ORDER BY 1, 2"""), sql("${summary} ORDER BY 1, 2"))
        }
    } finally {
        sql "SWITCH internal"
    }
}
