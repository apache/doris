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

suite("test_iceberg_runtime_filter_partition_pruning_transform", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_runtime_filter_partition_pruning_transform"
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

    def test_runtime_filter_partition_pruning_transform = {
        // Bucket partitions
        qt_bucket_int_eq """
            select count(*) from bucket_int_4 where partition_key =
                (select partition_key from bucket_int_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_int_in """
            select count(*) from bucket_int_4 where partition_key in
                (select partition_key from bucket_int_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_bucket_bigint_eq """
            select count(*) from bucket_bigint_4 where partition_key =
                (select partition_key from bucket_bigint_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_bigint_in """
            select count(*) from bucket_bigint_4 where partition_key in
                (select partition_key from bucket_bigint_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_bucket_string_eq """
            select count(*) from bucket_string_4 where partition_key =
                (select partition_key from bucket_string_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_string_in """
            select count(*) from bucket_string_4 where partition_key in
                (select partition_key from bucket_string_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_bucket_date_eq """
            select count(*) from bucket_date_4 where partition_key =
                (select partition_key from bucket_date_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_date_in """
            select count(*) from bucket_date_4 where partition_key in
                (select partition_key from bucket_date_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_bucket_ts_eq """
            select count(*) from bucket_timestamp_4 where partition_key =
                (select partition_key from bucket_timestamp_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_ts_in """
            select count(*) from bucket_timestamp_4 where partition_key in
                (select partition_key from bucket_timestamp_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_bucket_tntz_eq """
            select count(*) from bucket_timestamp_ntz_4 where partition_key =
                (select partition_key from bucket_timestamp_ntz_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_tntz_in """
            select count(*) from bucket_timestamp_ntz_4 where partition_key in
                (select partition_key from bucket_timestamp_ntz_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_bucket_binary_eq """
            select count(*) from bucket_binary_4 where partition_key =
                (select partition_key from bucket_binary_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_bucket_binary_in """
            select count(*) from bucket_binary_4 where partition_key in
                (select partition_key from bucket_binary_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        // Truncate partitions
        qt_trunc_string_eq """
            select count(*) from truncate_string_3 where partition_key =
                (select partition_key from truncate_string_3
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_trunc_string_in """
            select count(*) from truncate_string_3 where partition_key in
                (select partition_key from truncate_string_3
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_trunc_binary_eq """
            select count(*) from truncate_binary_4 where partition_key =
                (select partition_key from truncate_binary_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_trunc_binary_in """
            select count(*) from truncate_binary_4 where partition_key in
                (select partition_key from truncate_binary_4
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_trunc_int_eq """
            select count(*) from truncate_int_10 where partition_key =
                (select partition_key from truncate_int_10
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_trunc_int_in """
            select count(*) from truncate_int_10 where partition_key in
                (select partition_key from truncate_int_10
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_trunc_bigint_eq """
            select count(*) from truncate_bigint_100 where partition_key =
                (select partition_key from truncate_bigint_100
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_trunc_bigint_in """
            select count(*) from truncate_bigint_100 where partition_key in
                (select partition_key from truncate_bigint_100
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """

        qt_trunc_decimal_eq """
            select count(*) from truncate_decimal_10 where partition_key =
                (select partition_key from truncate_decimal_10
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 1);
        """
        qt_trunc_decimal_in """
            select count(*) from truncate_decimal_10 where partition_key in
                (select partition_key from truncate_decimal_10
                 group by partition_key having count(*) > 0
                 order by partition_key desc limit 2);
        """
    }
    try {
        sql """ set time_zone = 'Asia/Shanghai'; """
        sql """ set enable_runtime_filter_partition_prune = false; """
        test_runtime_filter_partition_pruning_transform()
        sql """ set enable_runtime_filter_partition_prune = true; """
        test_runtime_filter_partition_pruning_transform()
    } finally {
        sql """ unset variable time_zone; """
        sql """ set enable_runtime_filter_partition_prune = true; """
    }
}
