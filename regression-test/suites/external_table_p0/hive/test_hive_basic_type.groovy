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

suite("test_hive_basic_type", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        for (boolean enable_filter_by_min_max : [true, false]) {
            String catalog_name = "test_${hivePrefix}_basic_type"
            String ex_db_name = "`default`"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

            sql """set enable_parquet_filter_by_min_max = ${enable_filter_by_min_max};"""

            sql """set enable_orc_filter_by_min_max = ${enable_filter_by_min_max};"""

            sql """drop catalog if exists ${catalog_name} """

            sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'hadoop.username' = 'hive'
            );"""

            sql """switch ${catalog_name}"""

            def res_dbs_log = sql "show databases;"
            for (int i = 0; i < res_dbs_log.size(); i++) {
                def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
                log.info("database = ${res_dbs_log[i][0]} => tables = " + tbs.toString())
            }
            try {
                order_qt_2 """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table order by l_orderkey limit 1;"""
                order_qt_3 """select * from ${catalog_name}.${ex_db_name}.parquet_delta_binary_packed order by int_value limit 1;"""
                order_qt_4 """select * from ${catalog_name}.${ex_db_name}.parquet_alltypes_tiny_pages  order by id desc  limit 5;"""
                order_qt_5 """select * from ${catalog_name}.${ex_db_name}.orc_all_types_partition order by bigint_col desc limit 3;"""
                order_qt_6 """select * from ${catalog_name}.${ex_db_name}.csv_partition_table order by k1 limit 1;"""
                order_qt_9 """select * from ${catalog_name}.${ex_db_name}.csv_all_types limit 1;"""
                order_qt_10 """select * from ${catalog_name}.${ex_db_name}.text_all_types limit 1;"""

                // parquet bloom
                order_qt_11 """select * from ${catalog_name}.${ex_db_name}.bloom_parquet_table limit 1;"""

                // orc bloom
                order_qt_12 """select * from ${catalog_name}.${ex_db_name}.bloom_orc_table limit 1;"""

                // orc predicate
                order_qt_13 """select * from ${catalog_name}.${ex_db_name}.orc_predicate_table where column_primitive_bigint = 6 limit 10;"""
                order_qt_14 """select count(1) from ${catalog_name}.${ex_db_name}.orc_predicate_table where column_primitive_bigint = 6;"""
                order_qt_15 """select * from ${catalog_name}.${ex_db_name}.orc_predicate_table where column_primitive_bigint = 1 limit 10;"""
                order_qt_16 """select count(1) from ${catalog_name}.${ex_db_name}.orc_predicate_table where column_primitive_bigint = 1;"""
                order_qt_17 """select * from ${catalog_name}.${ex_db_name}.orc_predicate_table where column_primitive_integer = 3 and column_primitive_bigint = 6 limit 10;"""

                // parquet predicate
                order_qt_18 """select * from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_bigint = 1 limit 10;"""
                order_qt_19 """select count(1) from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_bigint = 1;"""
                order_qt_20 """select * from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_integer = 3 limit 10;"""
                order_qt_21 """select count(1) from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_integer = 3;"""
                order_qt_22 """select * from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_integer = 1 limit 10;"""
                order_qt_23 """select count(1) from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_integer = 1;"""

                // only null parquet file test
                order_qt_24 """select * from ${catalog_name}.${ex_db_name}.only_null;"""
                order_qt_25 """select * from ${catalog_name}.${ex_db_name}.only_null where x is null;"""
                order_qt_26 """select * from ${catalog_name}.${ex_db_name}.only_null where x is not null;"""

                // parquet timestamp millis test
                order_qt_27 """desc ${catalog_name}.${ex_db_name}.parquet_timestamp_millis;"""
                order_qt_28 """select * from ${catalog_name}.${ex_db_name}.parquet_timestamp_millis order by test;"""

                // parquet timestamp micros test
                order_qt_29 """desc ${catalog_name}.${ex_db_name}.parquet_timestamp_micros;"""
                order_qt_30 """select * from ${catalog_name}.${ex_db_name}.parquet_timestamp_micros order by test;"""

                // parquet timestamp nanos test
                order_qt_31 """desc ${catalog_name}.${ex_db_name}.parquet_timestamp_nanos;"""
                order_qt_32 """select * from ${catalog_name}.${ex_db_name}.parquet_timestamp_nanos order by test;"""

                order_qt_7 """select * from ${catalog_name}.${ex_db_name}.orc_all_types_t limit 1;"""

                // parquet predicate
                order_qt_38 """select * from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_bigint = 6 limit 10;"""
                order_qt_39 """select count(1) from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_bigint = 6;"""
                order_qt_40 """select * from ${catalog_name}.${ex_db_name}.parquet_predicate_table where column_primitive_integer = 3 and column_primitive_bigint = 6 limit 10;"""

                order_qt_33 """select * from ${catalog_name}.${ex_db_name}.parquet_all_types limit 1;"""

                order_qt_36 """select * from ${catalog_name}.${ex_db_name}.parquet_gzip_all_types limit 1;"""
                order_qt_42 """select * from ${catalog_name}.${ex_db_name}.parquet_zstd_all_types limit 1;"""

                // hive tables of json classes do not necessarily support column separation to identify errors
                //order_qt_8 """select * from ${catalog_name}.${ex_db_name}.json_all_types limit 1;"""

                // At present, doris only supports three formats of orc parquet textfile, while others are not supported

                // hive tables in avro format are not supported
                //order_qt_34 """select * from ${catalog_name}.${ex_db_name}.avro_all_types limit 1;"""

                // hive tables in SEQUENCEFILE format are not supported
                //order_qt_35 """select * from ${catalog_name}.${ex_db_name}.sequence_all_types limit 1;"""

                // hive tables in rcbinary format are not supported
                //order_qt_37 """select * from ${catalog_name}.${ex_db_name}.rcbinary_all_types limit 1;"""

                // orc_all_types_t predicate test
                order_qt_41 """select * from ${catalog_name}.${ex_db_name}.orc_all_types_t where t_int = 3;"""

                //test parquet  byte_array_decimal and rle_bool
                order_qt_parquet """ select count(*) from ${catalog_name}.${ex_db_name}.parquet_decimal_bool """
                order_qt_parquet1 """ select * from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where decimals is not null and  bool_rle is not null  order by decimals,bool_rle limit 7 """
                order_qt_parquet2 """ select decimals from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where decimals is not null and decimals > 1  order by decimals limit 7 """
                order_qt_parquet3 """ select decimals from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where decimals = 123.456  order by decimals limit 7 """
                order_qt_parquet4 """ select decimals from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where decimals != -7871.416 and decimals is not null order by decimals limit 7 """

                order_qt_parquet5 """ select decimals from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where decimals is not null and decimals < 0  order by decimals limit 7 """

                order_qt_parquet7 """ select bool_rle from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where bool_rle is not null and bool_rle = 1 limit 7 """
                order_qt_parquet8 """ select bool_rle from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where bool_rle is not null and bool_rle = 1 limit 7 """
                order_qt_parquet9 """ select count(bool_rle) from ${catalog_name}.${ex_db_name}.parquet_decimal_bool; """
                order_qt_parquet10 """ select count(decimals) from ${catalog_name}.${ex_db_name}.parquet_decimal_bool; """
                order_qt_parquet11 """ select decimals from ${catalog_name}.${ex_db_name}.parquet_decimal_bool 
					where decimals is not null and decimals > 1  order by decimals limit 7 """
            } finally {
                res_dbs_log = sql "show databases;"
                for (int i = 0; i < res_dbs_log.size(); i++) {
                    def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
                    log
                            .info("database = ${res_dbs_log[i][0]} => tables  =  " + tbs.toString())
                }
            }
            //sql """drop catalog if exists ${catalog_name} """
        }
    }
}

