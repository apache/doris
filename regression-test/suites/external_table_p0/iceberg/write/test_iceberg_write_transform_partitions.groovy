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

suite("test_iceberg_write_transform_partitions", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_write_transform_partitions"
    String db_name = "transform_partition_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""

    // While we currently cannot reset the table data, we can only test whether the INSERT statements execute successfully
    def test_write_transform_partitions = { String table ->
        sql """ insert into ${table}_copy select * from ${table}; """
        sql """ insert into ${table}_copy select * from ${table}_copy limit 50; """
        sql """ insert into ${table}_copy select * from ${table}_copy limit 50; """
    }

    try {
        String validationTable = "create_partition_validation"
        sql """DROP TABLE IF EXISTS `${validationTable}`"""
        try {
            // Iceberg supports transforms, but explicit Doris partition bounds have no Iceberg equivalent.
            [
                ["PARTITION BY LIST(dt) (PARTITION p1 VALUES IN ('2026-01-01'))",
                        "Iceberg does not support explicit partition definitions"],
                ["PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2026-01-02'))",
                        "Iceberg does not support explicit partition definitions"],
                ["PARTITION BY (date_trunc(ts, 'day')) ()", "unsupported partition for date_trunc"]
            ].each { entry ->
                test {
                    sql """CREATE TABLE `${validationTable}` (id INT, ts DATETIME, dt DATE) ${entry[0]}"""
                    exception entry[1]
                }
                assertEquals([], sql("""SHOW TABLES LIKE '${validationTable}'"""))
            }

            sql """
                CREATE TABLE `${validationTable}` (id INT, ts DATETIME)
                PARTITION BY (bucket(4, id), day(ts)) ()
            """
            String ddl = sql("""SHOW CREATE TABLE `${validationTable}`""")[0][1].toString().toLowerCase()
            assertTrue(ddl.contains("bucket"))
            assertTrue(ddl.contains("day"))
            sql """INSERT INTO `${validationTable}` VALUES (1, '2026-01-01 12:00:00')"""
            assertEquals(1, sql("""SELECT COUNT(*) FROM `${validationTable}`""")[0][0].intValue())
        } finally {
            sql """DROP TABLE IF EXISTS `${validationTable}`"""
        }

        sql """ set time_zone = 'Asia/Shanghai'; """
        test_write_transform_partitions("bucket_int_4");
        test_write_transform_partitions("bucket_bigint_4");
        test_write_transform_partitions("bucket_string_4");
        test_write_transform_partitions("bucket_date_4");
        test_write_transform_partitions("bucket_timestamp_4");
        test_write_transform_partitions("bucket_timestamp_ntz_4");
        test_write_transform_partitions("bucket_decimal_4");
        test_write_transform_partitions("bucket_binary_4");
        test_write_transform_partitions("truncate_string_3");
        test_write_transform_partitions("truncate_binary_4");
        test_write_transform_partitions("truncate_int_10");
        test_write_transform_partitions("truncate_bigint_100");
        test_write_transform_partitions("truncate_decimal_10");
        
        // Time transform partitions (day/year/month/hour)
        test_write_transform_partitions("day_partitioned");
        test_write_transform_partitions("year_partitioned");
        test_write_transform_partitions("month_partitioned");
        test_write_transform_partitions("hour_partitioned");
    } finally {
        sql """ unset variable time_zone; """
    }
}
