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

suite("test_iceberg_runtime_filter_partition_pruning", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_runtime_filter_partition_pruning"
    String db_name = "partition_db"
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

    def test_runtime_filter_partition_pruning = {
        qt_runtime_filter_partition_pruning_decimal1 """
            select count(*) from decimal_partitioned where partition_key =
                (select partition_key from decimal_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_decimal2 """
            select count(*) from decimal_partitioned where partition_key in
                (select partition_key from decimal_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_decimal3 """
            select count(*) from decimal_partitioned where abs(partition_key) =
                (select partition_key from decimal_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_decimal_in_null """
            select count(*) from decimal_partitioned where partition_key in
                (select partition_key from decimal_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_int1 """
            select count(*) from int_partitioned where partition_key =
                (select partition_key from int_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_int2 """
            select count(*) from int_partitioned where partition_key in
                (select partition_key from int_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_int3 """
            select count(*) from int_partitioned where abs(partition_key) =
                (select partition_key from int_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_int_in_null """
            select count(*) from int_partitioned where partition_key in
                (select partition_key from int_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_string1 """
            select count(*) from string_partitioned where partition_key =
                (select partition_key from string_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_string2 """
            select count(*) from string_partitioned where partition_key in
                (select partition_key from string_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_string_in_null """
            select count(*) from string_partitioned where partition_key in
                (select partition_key from string_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_date1 """
            select count(*) from date_partitioned where partition_key =
                (select partition_key from date_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_date2 """
            select count(*) from date_partitioned where partition_key in
                (select partition_key from date_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_date_in_null """
            select count(*) from date_partitioned where partition_key in
                (select partition_key from date_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_timestamp1 """
            select count(*) from timestamp_partitioned where partition_key =
                (select partition_key from timestamp_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_timestamp2 """
            select count(*) from timestamp_partitioned where partition_key in
                (select partition_key from timestamp_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_timestamp_in_null """
            select count(*) from timestamp_partitioned where partition_key in
                (select partition_key from timestamp_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_boolean1 """
            select count(*) from boolean_partitioned where partition_key =
                (select partition_key from boolean_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_boolean2 """
            select count(*) from boolean_partitioned where partition_key in
                (select partition_key from boolean_partitioned
                group by partition_key having count(*) > 0);
        """
        qt_runtime_filter_partition_pruning_boolean_in_null """
            select count(*) from boolean_partitioned where partition_key in
                (select partition_key from boolean_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_float1 """
            select count(*) from float_partitioned where partition_key =
                (select partition_key from float_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_float2 """
            select count(*) from float_partitioned where partition_key in
                (select partition_key from float_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_float3 """
            select count(*) from float_partitioned where abs(partition_key) =
                (select partition_key from float_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_float_in_null """
            select count(*) from float_partitioned where partition_key in
                (select partition_key from float_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_timestamp_ntz1 """
            select count(*) from timestamp_ntz_partitioned where partition_key =
                (select partition_key from timestamp_ntz_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_timestamp_ntz2 """
            select count(*) from timestamp_ntz_partitioned where partition_key in
                (select partition_key from timestamp_ntz_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_timestamp_ntz_in_null """
            select count(*) from timestamp_ntz_partitioned where partition_key in
                (select partition_key from timestamp_ntz_partitioned
                order by id desc limit 2);
        """
        qt_runtime_filter_partition_pruning_binary1 """
            select count(*) from binary_partitioned where partition_key =
                (select partition_key from binary_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 1);
        """
        qt_runtime_filter_partition_pruning_binary2 """
            select count(*) from binary_partitioned where partition_key in
                (select partition_key from binary_partitioned
                group by partition_key having count(*) > 0
                order by partition_key desc limit 2);
        """
        qt_runtime_filter_partition_pruning_binary_in_null """
            select count(*) from binary_partitioned where partition_key in
                (select partition_key from binary_partitioned
                order by id desc limit 2);
        """
        qt_null_partition_1 """
            select * from null_str_partition_table where category = 'null';
        """
        qt_null_partition_2 """
            select * from null_str_partition_table where category = 'NULL';
        """
        qt_null_partition_3 """
            select count(*) from null_str_partition_table where category = '\\\\N';
        """
        qt_null_partition_4 """
            select * from null_str_partition_table where category is null;
        """
    }
    try {
        sql """ set time_zone = 'Asia/Shanghai'; """
        sql """ set enable_runtime_filter_partition_prune = false; """
        test_runtime_filter_partition_pruning()
        sql """ set enable_runtime_filter_partition_prune = true; """
        test_runtime_filter_partition_pruning()

    } finally {
        sql """ unset variable time_zone; """
        sql """ set enable_runtime_filter_partition_prune = true; """
    }

}
