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

suite("test_iceberg_write_decimal_bounds", "p0,external,iceberg") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_write_decimal_bounds"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS ${catalogName}"
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "enable.mapping.varbinary" = "true"
        )
    """
    sql "SWITCH ${catalogName}"
    sql "CREATE DATABASE IF NOT EXISTS test_decimal_bounds"
    sql "USE test_decimal_bounds"
    sql "DROP TABLE IF EXISTS decimal_bounds"
    sql """
        CREATE TABLE decimal_bounds (
            case_id INT,
            d32 DECIMAL(9,2),
            d64 DECIMAL(18,2),
            d128 DECIMAL(38,2)
        )
        PARTITION BY LIST (case_id) ()
        PROPERTIES ("write-format" = "parquet")
    """
    // Separate partitions expose every sign boundary as a manifest bound, not just a data value.
    sql """
        INSERT INTO decimal_bounds VALUES
            (0, 0.00, 0.00, 0.00),
            (1, 0.01, 0.01, 0.01),
            (2, 1.27, 1.27, 1.27),
            (3, 1.28, 1.28, 1.28),
            (4, 2.55, 2.55, 2.55),
            (5, 2.56, 2.56, 2.56),
            (6, -0.01, -0.01, -0.01),
            (7, -1.28, -1.28, -1.28),
            (8, -1.29, -1.29, -1.29),
            (9, -2.56, -2.56, -2.56),
            (10, -327.68, -327.68, -327.68),
            (11, -327.69, -327.69, -327.69),
            (12, NULL, NULL, NULL),
            (13, 9999999.99, 9999999999999999.99, 999999999999999999999999999999999999.99),
            (14, -9999999.99, -9999999999999999.99, -999999999999999999999999999999999999.99),
            (15, -0.01, -0.01, -0.01),
            (15, 1.28, 1.28, 1.28)
    """
    // Reading the data alone would not detect redundant sign-extension bytes in metadata.
    qt_bounds """
        SELECT `partition`.case_id, record_count,
               from_hex(lower_bounds[2]), from_hex(upper_bounds[2]),
               from_hex(lower_bounds[3]), from_hex(upper_bounds[3]),
               from_hex(lower_bounds[4]), from_hex(upper_bounds[4])
        FROM decimal_bounds\$files
        ORDER BY `partition`.case_id
    """
    qt_values "SELECT * FROM decimal_bounds ORDER BY case_id, d32"
    qt_negative "SELECT * FROM decimal_bounds WHERE d128 < 0 ORDER BY case_id, d32"
    qt_positive "SELECT * FROM decimal_bounds WHERE d32 >= 1.28 ORDER BY case_id, d32"
}
