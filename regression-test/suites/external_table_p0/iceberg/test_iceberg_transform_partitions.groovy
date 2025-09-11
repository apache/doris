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

suite("test_iceberg_transform_partitions", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_transform_partitions"
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

    def test_iceberg_transform_partitions = {
        // Bucket by INT (empty table)
        qt_bucket_int_empty_cnt1 """
            select count(*) from bucket_int_empty;
        """
        qt_bucket_int_empty_select1 """
            select * from bucket_int_empty order by id;
        """

        // Bucket by INT
        qt_bucket_int_4_select1 """
            select * from bucket_int_4 where partition_key = 1 order by id;
        """
        qt_bucket_int_4_select2 """
            select * from bucket_int_4 where partition_key in (2, 16) order by id;
        """
        qt_bucket_int_4_cnt1 """
            select count(*) from bucket_int_4 where partition_key = -100;
        """

        // Bucket by BIGINT
        qt_bucket_bigint_4_cnt1 """
            select count(*) from bucket_bigint_4 where partition_key = 1;
        """
        qt_bucket_bigint_4_cnt2 """
            select count(*) from bucket_bigint_4 where partition_key in (-1, 1234567890123);
        """
        qt_bucket_bigint_4_select1 """
            select * from bucket_bigint_4 where partition_key = 1 order by id;
        """

        // Bucket by STRING
        qt_bucket_string_4_cnt1 """
            select count(*) from bucket_string_4 where partition_key = 'abc';
        """
        qt_bucket_string_4_cnt2 """
            select count(*) from bucket_string_4 where partition_key in ('', '😊');
        """
        qt_bucket_string_4_select1 """
            select * from bucket_string_4 where partition_key = 'abc' order by id;
        """

        // Bucket by DATE
        qt_bucket_date_4_cnt1 """
            select count(*) from bucket_date_4 where partition_key = DATE '2024-02-29';
        """
        qt_bucket_date_4_cnt2 """
            select count(*) from bucket_date_4 where partition_key in (DATE '1970-01-01', DATE '1999-12-31');
        """
        qt_bucket_date_4_select1 """
            select * from bucket_date_4 where partition_key = DATE '2024-02-29' order by id;
        """

        // Bucket by TIMESTAMP
        qt_bucket_timestamp_4_cnt1 """
            select count(*) from bucket_timestamp_4 where partition_key = TIMESTAMP '2024-01-15 08:00:00';
        """
        qt_bucket_timestamp_4_cnt2 """
            select count(*) from bucket_timestamp_4 where partition_key in (TIMESTAMP '2024-06-30 23:59:59', TIMESTAMP '2030-12-31 23:59:59');
        """
        qt_bucket_timestamp_4_select1 """
            select * from bucket_timestamp_4 where partition_key = TIMESTAMP '2024-01-15 08:00:00' order by id;
        """

        // Bucket by TIMESTAMP_NTZ
        qt_bucket_timestamp_ntz_4_cnt1 """
            select count(*) from bucket_timestamp_ntz_4 where partition_key = '2024-01-15 08:00:00';
        """
        qt_bucket_timestamp_ntz_4_cnt2 """
            select count(*) from bucket_timestamp_ntz_4 where partition_key in ('2024-06-30 23:59:59', '2030-12-31 23:59:59');
        """
        qt_bucket_timestamp_ntz_4_select1 """
            select * from bucket_timestamp_ntz_4 where partition_key = '2024-01-15 08:00:00' order by id;
        """

        // Bucket by DECIMAL
        qt_bucket_decimal_4_cnt1 """
            select count(*) from bucket_decimal_4 where partition_key = 10.50;
        """
        qt_bucket_decimal_4_cnt2 """
            select count(*) from bucket_decimal_4 where partition_key in (-1.25, 9999999.99);
        """
        qt_bucket_decimal_4_select1 """
            select * from bucket_decimal_4 where partition_key = 10.50 order by id;
        """

        // Bucket by BINARY
        qt_bucket_binary_4_cnt1 """
            select count(*) from bucket_binary_4 where partition_key = 'abc';
        """
        qt_bucket_binary_4_cnt2 """
            select count(*) from bucket_binary_4 where partition_key in ('', '你好');
        """
        qt_bucket_binary_4_select1 """
            select * from bucket_binary_4 where partition_key = 'abc' order by id;
        """

        // Truncate STRING(3)
        qt_truncate_string_3_cnt1 """
            select count(*) from truncate_string_3 where partition_key = 'abcdef';
        """
        qt_truncate_string_3_cnt2 """
            select count(*) from truncate_string_3 where partition_key in ('abc', 'abcdef');
        """
        qt_truncate_string_3_select1 """
            select * from truncate_string_3 where partition_key = 'abcdef' order by id;
        """

        // Truncate BINARY(4)
        qt_truncate_binary_4_cnt1 """
            select count(*) from truncate_binary_4 where partition_key = 'abcdef';
        """
        qt_truncate_binary_4_cnt2 """
            select count(*) from truncate_binary_4 where partition_key in ('abcd', 'abcdef');
        """
        qt_truncate_binary_4_select1 """
            select * from truncate_binary_4 where partition_key = 'abcdef' order by id;
        """

        // Truncate INT(10)
        qt_truncate_int_10_cnt1 """
            select count(*) from truncate_int_10 where partition_key = 19;
        """
        qt_truncate_int_10_cnt2 """
            select count(*) from truncate_int_10 where partition_key in (7, 19);
        """
        qt_truncate_int_10_select1 """
            select * from truncate_int_10 where partition_key = 19 order by id;
        """

        // Truncate BIGINT(100)
        qt_truncate_bigint_100_cnt1 """
            select count(*) from truncate_bigint_100 where partition_key = 199;
        """
        qt_truncate_bigint_100_cnt2 """
            select count(*) from truncate_bigint_100 where partition_key in (7, 199);
        """
        qt_truncate_bigint_100_select1 """
            select * from truncate_bigint_100 where partition_key = 199 order by id;
        """

        // Truncate DECIMAL(10,2) width 10
        qt_truncate_decimal_10_cnt1 """
            select count(*) from truncate_decimal_10 where partition_key = 19.99;
        """
        qt_truncate_decimal_10_cnt2 """
            select count(*) from truncate_decimal_10 where partition_key in (9.99, 19.99);
        """
        qt_truncate_decimal_10_select1 """
            select * from truncate_decimal_10 where partition_key = 19.99 order by id;
        """
    }

    try {
        sql """ set time_zone = 'Asia/Shanghai'; """
        test_iceberg_transform_partitions()
    } finally {
        sql """ unset variable time_zone; """
    }
}
