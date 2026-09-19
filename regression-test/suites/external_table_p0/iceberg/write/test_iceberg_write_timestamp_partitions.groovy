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

suite("test_iceberg_write_timestamp_partitions", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = context.config.otherConfigs.get("iceberg_minio_endpoint")
            ?: "http://${externalEnvIp}:${minioPort}"
    String catalogName = "test_iceberg_write_timestamp_partitions"
    String dbName = "timestamp_partition_roundtrip"
    def previousZone = sql("SELECT @@time_zone")[0][0]
    def expected = [[1, "2021-11-07 05:30:00.123456+00:00"],
                    [2, "2021-11-07 06:30:00.123456+00:00"],
                    [3, "1969-12-31 23:59:58.999999+00:00"], [4, null]]
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
            for (String transform : ["identity", "year", "month", "day", "hour", "bucket"]) {
                String table = "timestamp_${format}_${transform}"
                String expression = transform == "identity" ? "event_time"
                        : transform == "bucket" ? "bucket(16, event_time)" : "${transform}(event_time)"
                sql "DROP TABLE IF EXISTS ${table}"
                sql """CREATE TABLE ${table} (id INT, event_time TIMESTAMPTZ(6))
                    PARTITION BY LIST (${expression}) ()
                    PROPERTIES ("format-version"="2", "write.format.default"="${format}")"""
                if (format == "orc" && transform == "identity") {
                    // Reject the ORC-645 interval instead of silently aliasing a positive instant.
                    test {
                        sql "INSERT INTO ${table} VALUES (5, '1969-12-31 23:59:59.999999+00:00')"
                        exception "ORC cannot represent pre-epoch timestamp fractions"
                    }
                }
                for (String zone : ["Asia/Shanghai", "America/New_York"]) {
                    sql "SET time_zone = '${zone}'"
                    // Explicit offsets distinguish the two occurrences of 01:30 during the DST fold.
                    sql """INSERT OVERWRITE TABLE ${table} VALUES
                        (1, '2021-11-07 01:30:00.123456-04:00'),
                        (2, '2021-11-07 01:30:00.123456-05:00'),
                        (3, '1969-12-31 23:59:58.999999+00:00'), (4, NULL)"""
                    sql "SET time_zone = 'UTC'"
                    assertEquals(expected, sql("SELECT id, CAST(event_time AS STRING) FROM ${table} ORDER BY id"))
                    // Equality pruning must agree with the committed partition values, not merely a full scan.
                    assertEquals([[2]], sql("""SELECT id FROM ${table}
                        WHERE event_time = CAST('2021-11-07 06:30:00.123456+00:00' AS TIMESTAMPTZ(6))"""))
                    assertEquals([[3]], sql("""SELECT id FROM ${table}
                        WHERE event_time = CAST('1969-12-31 23:59:58.999999+00:00' AS TIMESTAMPTZ(6))"""))
                    assertEquals([[4]], sql("SELECT id FROM ${table} WHERE event_time IS NULL"))
                    if (transform == "identity") {
                        sql "SET time_zone = '${zone}'"
                        sql """INSERT OVERWRITE TABLE ${table}
                            PARTITION (event_time='2021-11-07 01:30:00.123456-05:00') SELECT 20"""
                        sql "SET time_zone = 'UTC'"
                        assertEquals([[1], [3], [4], [20]], sql("SELECT id FROM ${table} ORDER BY id"))
                        // Delete commits also carry partition metadata through the scan path.
                        sql "SET time_zone = 'America/New_York'"
                        sql "DELETE FROM ${table} WHERE id = 20"
                        assertEquals([[1], [3], [4]], sql("SELECT id FROM ${table} ORDER BY id"))
                    }
                }
                sql "DROP TABLE ${table}"
            }
        }
    } finally {
        sql "SET time_zone = '${previousZone}'"
        sql "SWITCH internal"
        sql "DROP CATALOG IF EXISTS ${catalogName}"
    }
}
