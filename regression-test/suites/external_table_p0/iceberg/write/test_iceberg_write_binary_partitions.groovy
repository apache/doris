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
    def hexValues = ["C3A900FF", "", "616263", "00FF80", "000102030405060708090A0B0C0D0E0FFF80"]
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
                }.join(", ") + ", (6, NULL)"
                for (String operation : ["INSERT INTO", "INSERT OVERWRITE TABLE"]) {
                    sql "${operation} ${table} VALUES ${rows}"
                    String tag = "${table}_${operation.replace(' ', '_')}"
                    if (transform == "identity") {
                        // Reads alone can pass if malformed binary metadata silently drops every partition.
                        explain {
                            sql "SELECT id FROM ${table}"
                            contains "partition=6/6"
                        }
                    }
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
    } finally {
        sql "SWITCH internal"
    }
}
