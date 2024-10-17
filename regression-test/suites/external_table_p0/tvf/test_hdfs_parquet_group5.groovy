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

suite("test_hdfs_parquet_group5","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/case_insensitive_column_matching.parquet"
            order_qt_test_0 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/array_int.parquet"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nulls.snappy.parquet"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/userdata3.parquet"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/binary.parquet"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/v0.7.1.all-named-index.parquet"
            order_qt_test_5 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nested_maps.snappy.parquet"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/userdata2.parquet"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nullable.impala.parquet"
            order_qt_test_8 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/alltypes_plain.snappy.parquet"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02725_data.parquet"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nullable_list.parquet"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02960_polygon_bound_bug.parquet"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/time64_test.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'timestamp' is not supported: INT64 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/non_hadoop_lz4_compressed.parquet"
            order_qt_test_14 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/fixed_length_decimal_1.parquet"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/fixed_length_decimal_legacy.parquet"
            order_qt_test_16 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/68131.parquet"
            order_qt_test_17 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/list_monotonically_increasing_offsets.parquet"
            order_qt_test_18 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/fixed_length_decimal.parquet"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02588_data.parquet"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02731.parquet"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/v0.7.1.column-metadata-handling.parquet"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nested_lists.snappy.parquet"
            order_qt_test_24 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02718_data.parquet"
            order_qt_test_25 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/alltypes_dictionary.parquet"
            order_qt_test_26 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/native_parquet_reader.parquet"
            order_qt_test_28 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/v0.7.1.some-named-index.parquet"
            order_qt_test_29 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/alltypes_list.parquet"
            order_qt_test_30 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/hadoop_lz4_compressed.parquet"
            order_qt_test_31 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/userdata1.parquet"
            order_qt_test_32 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/sample.parquet"
            order_qt_test_33 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nonnullable.impala.parquet"
            order_qt_test_34 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02876.parquet"
            order_qt_test_35 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/time32_test.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'timestamp' is not supported: INT32 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/userdata4.parquet"
            order_qt_test_37 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/datapage_v2.snappy.parquet"
            order_qt_test_38 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/int32_decimal.parquet"
            order_qt_test_39 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nested_lists.parquet"
            order_qt_test_40 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/datatype-date32.parquet"
            order_qt_test_41 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/byte_array_decimal.parquet"
            order_qt_test_42 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/list_columns.parquet"
            order_qt_test_43 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/array_float.parquet"
            order_qt_test_44 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/int64_decimal.parquet"
            order_qt_test_45 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/string_int_list_inconsistent_offset_multiple_batches.parquet"
            order_qt_test_46 """ select count(ints), count(strings) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/userdata5.parquet"
            order_qt_test_47 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/02716_data.parquet"
            order_qt_test_48 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/metadata_0.parquet"
            order_qt_test_49 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/int-list-zero-based-chunked-array.parquet"
            order_qt_test_50 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/single_nan.parquet"
            order_qt_test_51 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nested_nested_table.parquet"
            order_qt_test_52 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nine_byte_decimals_from_spark.parquet"
            order_qt_test_53 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/v0.7.1.parquet"
            order_qt_test_54 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/nested_table.parquet"
            order_qt_test_55 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/array_string.parquet"
            order_qt_test_56 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/delta_length_byte_array_encoding.parquet"
            order_qt_test_57 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group5/alltypes_plain.parquet"
            order_qt_test_58 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
        } finally {
        }
    }
}
