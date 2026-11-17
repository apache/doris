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

suite("test_iceberg_files_timestamp_partition", "p0,external,iceberg") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def previousZone = sql("SELECT @@time_zone")[0][0]
    // Parquet supports both pre-epoch fractions and NTZ. Use identity partitions so the
    // metadata rows retain the original microseconds, not a day/hour transform.
    spark_iceberg_multi """
        SET spark.sql.session.timeZone=UTC;
        CREATE DATABASE IF NOT EXISTS demo.files_timestamp_partition_db;
        DROP TABLE IF EXISTS demo.files_timestamp_partition_db.metadata_timestamp_precision;
        CREATE TABLE demo.files_timestamp_partition_db.metadata_timestamp_precision (
            id INT, ts TIMESTAMP_NTZ, tz TIMESTAMP
        ) USING iceberg PARTITIONED BY (ts, tz)
        TBLPROPERTIES ('write.format.default'='parquet');
        INSERT INTO demo.files_timestamp_partition_db.metadata_timestamp_precision
        SELECT id, CAST(value AS TIMESTAMP_NTZ), CAST(value AS TIMESTAMP)
        FROM VALUES
            (0, '1969-12-31 23:59:58.999999'),
            (1, '1969-12-31 23:59:59.999999'),
            (2, '1970-01-01 00:00:00.000001'),
            (3, '1970-01-01 00:00:00.000999'),
            (4, '1970-01-01 00:00:00.001000'),
            (5, '1970-01-01 00:00:01.234567'),
            (6, CAST(NULL AS STRING)) AS input(id, value);
    """
    try {
        for (boolean mapping : [false, true]) {
            sql "DROP CATALOG IF EXISTS test_iceberg_files_timestamp_partition"
            sql """CREATE CATALOG test_iceberg_files_timestamp_partition PROPERTIES (
                'type'='iceberg', 'iceberg.catalog.type'='rest',
                'uri'='http://${host}:${restPort}',
                's3.endpoint'='http://${host}:${minioPort}',
                's3.access_key'='admin', 's3.secret_key'='password', 's3.region'='us-east-1',
                'enable.mapping.timestamp_tz'='${mapping}')"""
            sql "SWITCH test_iceberg_files_timestamp_partition"
            sql "USE files_timestamp_partition_db"
            for (String zone : ["UTC", "Asia/Shanghai"]) {
                sql "SET time_zone = '${zone}'"
                String tag = "${mapping}_${zone.replace('/', '_')}"
                "order_qt_${tag}" """SELECT `partition`
                    FROM metadata_timestamp_precision\$files ORDER BY file_path"""
                // Exercise a pruned struct child as well as the entire partition above.
                "order_qt_${tag}_ntz" """SELECT `partition`.ts
                    FROM metadata_timestamp_precision\$files ORDER BY file_path"""
            }
        }
    } finally {
        sql "SET time_zone = '${previousZone}'"
        sql "SWITCH internal"
    }
}
