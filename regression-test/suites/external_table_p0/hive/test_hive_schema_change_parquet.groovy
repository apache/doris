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

suite("test_hive_schema_change_parquet", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive3"]) {
            setHivePrefix(hivePrefix)
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_hive_schema_change_parquet"
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
            """
            sql """ switch ${catalog_name} """

            sql """ use `schema_change` """

            try {
                qt_parquet_boolean_to_boolean """ select bool_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_boolean """ select tinyint_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_boolean """ select smallint_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_boolean """ select int_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_boolean """ select bigint_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_boolean """ select float_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_boolean """ select double_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_boolean """ select string_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_boolean """ select char1_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_boolean """ select char2_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_boolean """ select varchar_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_boolean """ select date_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_boolean """ select timestamp_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_boolean """ select decimal1_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_boolean """ select decimal2_col from parquet_primitive_types_to_boolean order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_smallint """ select bool_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_smallint """ select tinyint_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_smallint """ select smallint_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_smallint """ select int_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_smallint """ select bigint_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_smallint """ select float_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_smallint """ select double_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_smallint """ select string_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_smallint """ select char1_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_smallint """ select char2_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_smallint """ select varchar_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_smallint """ select date_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_smallint """ select timestamp_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_smallint """ select decimal1_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_smallint """ select decimal2_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_tinyint """ select bool_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_tinyint """ select tinyint_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_tinyint """ select smallint_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_tinyint """ select int_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_tinyint """ select bigint_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_tinyint """ select float_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_tinyint """ select double_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_tinyint """ select string_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_tinyint """ select char1_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_tinyint """ select char2_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_tinyint """ select varchar_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_tinyint """ select date_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_tinyint """ select timestamp_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_tinyint """ select decimal1_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_tinyint """ select decimal2_col from parquet_primitive_types_to_tinyint order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_smallint """ select bool_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_smallint """ select tinyint_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_smallint """ select smallint_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_smallint """ select int_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_smallint """ select bigint_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_smallint """ select float_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_smallint """ select double_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_smallint """ select string_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_smallint """ select char1_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_smallint """ select char2_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_smallint """ select varchar_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_smallint """ select date_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_smallint """ select timestamp_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_smallint """ select decimal1_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_smallint """ select decimal2_col from parquet_primitive_types_to_smallint order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_int """ select bool_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_int """ select tinyint_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_int """ select smallint_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_int """ select int_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_int """ select bigint_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_int """ select float_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_int """ select double_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_int """ select string_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_int """ select char1_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_int """ select char2_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_int """ select varchar_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_int """ select date_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_int """ select timestamp_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_int """ select decimal1_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_int """ select decimal2_col from parquet_primitive_types_to_int order by id """
            } catch (Exception e) {
            }



            try {
                qt_parquet_boolean_to_bigint """ select bool_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_bigint """ select tinyint_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_bigint """ select smallint_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_bigint """ select int_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_bigint """ select bigint_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_bigint """ select float_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_bigint """ select double_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_bigint """ select string_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_bigint """ select char1_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_bigint """ select char2_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_bigint """ select varchar_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_bigint """ select date_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_bigint """ select timestamp_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_bigint """ select decimal1_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_bigint """ select decimal2_col from parquet_primitive_types_to_bigint order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_float """ select bool_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_float """ select tinyint_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_float """ select smallint_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_float """ select int_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_float """ select bigint_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_float """ select float_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_float """ select double_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_float """ select string_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_float """ select char1_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_float """ select char2_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_float """ select varchar_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_float """ select date_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_float """ select timestamp_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_float """ select decimal1_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_float """ select decimal2_col from parquet_primitive_types_to_float order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_double """ select bool_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_double """ select tinyint_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_double """ select smallint_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_double """ select int_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_double """ select bigint_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_double """ select float_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_double """ select double_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_double """ select string_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_double """ select char1_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_double """ select char2_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_double """ select varchar_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_double """ select date_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_double """ select timestamp_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_double """ select decimal1_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_double """ select decimal2_col from parquet_primitive_types_to_double order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_string """ select bool_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_string """ select tinyint_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_string """ select smallint_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_string """ select int_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_string """ select bigint_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_string """ select float_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_string """ select double_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_string """ select string_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_string """ select char1_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_string """ select char2_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_string """ select varchar_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_string """ select date_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_string """ select timestamp_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_string """ select decimal1_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_string """ select decimal2_col from parquet_primitive_types_to_string order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_date """ select bool_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_date """ select tinyint_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_date """ select smallint_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_date """ select int_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_date """ select bigint_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_date """ select float_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_date """ select double_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_date """ select string_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_date """ select char1_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_date """ select char2_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_date """ select varchar_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_date """ select date_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_date """ select timestamp_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_date """ select decimal1_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_date """ select decimal2_col from parquet_primitive_types_to_date order by id """
            } catch (Exception e) {
            }



            try {
                qt_parquet_boolean_to_timestamp """ select bool_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_timestamp """ select tinyint_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_timestamp """ select smallint_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_timestamp """ select int_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_timestamp """ select bigint_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_timestamp """ select float_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_timestamp """ select double_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_timestamp """ select string_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_timestamp """ select char1_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_timestamp """ select char2_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_timestamp """ select varchar_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_timestamp """ select date_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_timestamp """ select timestamp_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_timestamp """ select decimal1_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_timestamp """ select decimal2_col from parquet_primitive_types_to_timestamp order by id """
            } catch (Exception e) {
            }


            try {
                qt_parquet_boolean_to_decimal1 """ select bool_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_decimal1 """ select tinyint_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_decimal1 """ select smallint_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_decimal1 """ select int_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_decimal1 """ select bigint_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_decimal1 """ select float_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_decimal1 """ select double_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_decimal1 """ select string_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_decimal1 """ select char1_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_decimal1 """ select char2_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_decimal1 """ select varchar_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_decimal1 """ select date_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_decimal1 """ select timestamp_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_decimal1 """ select decimal1_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_decimal1 """ select decimal2_col from parquet_primitive_types_to_decimal1 order by id """
            } catch (Exception e) {
            }



            try {
                qt_parquet_boolean_to_decimal2 """ select bool_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_tinyint_to_decimal2 """ select tinyint_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_smallint_to_decimal2 """ select smallint_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_int_to_decimal2 """ select int_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_bigint_to_decimal2 """ select bigint_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_float_to_decimal2 """ select float_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_double_to_decimal2 """ select double_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_string_to_decimal2 """ select string_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char1_to_decimal2 """ select char1_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_char2_to_decimal2 """ select char2_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_varchar_to_decimal2 """ select varchar_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_date_to_decimal2 """ select date_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_timestamp_to_decimal2 """ select timestamp_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal1_to_decimal2 """ select decimal1_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }

            try {
                qt_parquet_decimal2_to_decimal2 """ select decimal2_col from parquet_primitive_types_to_decimal2 order by id """
            } catch (Exception e) {
            }


            sql """ drop catalog ${catalog_name} """
        }
    }
}
