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

suite("test_hdfs_parquet_group6","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/upcase_field.parquet"
            order_qt_test_0 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/row-4k-id.parquet"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/timestamp_to_datetime.parquet"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/min-max-complex-types.parquet"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/float_double.parquet"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/blogs.parquet"
            order_qt_test_5 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested_array.parquet"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_8193.parquet"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/read_range_big_page_test.parquet"
            order_qt_test_8 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_8191.parquet"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested_array_test1.parquet"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_4096.parquet"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/issue_17693_1.parquet"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/page_index_big_page.parquet"
            order_qt_test_13 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_8192.parquet"
            order_qt_test_14 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/empty.parquet"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/0df0196b-f46f-43f5-8cf0-06fad7143af3-0_0-27-35_20230110191854854.parquet"
            order_qt_test_16 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/file_reader_test_binary.parquet"
            order_qt_test_17 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_4095.parquet"
            order_qt_test_18 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/dict_with_null_value.parquet"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_4097.parquet"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/col_not_null.parquet"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/small_row_group_data.parquet"
            order_qt_test_22 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested.parquet"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/map_data_only.parquet"
            order_qt_test_24 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/localfile.parquet"
            order_qt_test_25 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/schema4.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "Duplicated field name: col1"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested_array_test2.parquet"
            order_qt_test_27 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_8192.parquet"
            order_qt_test_28 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_1.parquet"
            order_qt_test_29 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_4095.parquet"
            order_qt_test_30 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/issue_17693_2.parquet"
            order_qt_test_31 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/no_min_max_statistics.parquet"
            order_qt_test_32 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_4097.parquet"
            order_qt_test_33 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/int96.parquet"
            order_qt_test_34 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/map_struct_subfield_required.parquet"
            order_qt_test_35 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_two_page_nullable.parquet"
            order_qt_test_36 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/large_page_header.parquet"
            order_qt_test_37 """ select count(myString), count(myInteger) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/parquet_delete_file.parquet"
            order_qt_test_38 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/00000000000000000030.checkpoint.parquet"
            order_qt_test_39 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/dict_two_page.parquet"
            order_qt_test_40 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/page_index_repeated_nodict.parquet"
            order_qt_test_41 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_8193.parquet"
            order_qt_test_42 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_8191.parquet"
            order_qt_test_43 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nullable_data_0.parquet"
            order_qt_test_44 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_4096.parquet"
            order_qt_test_45 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/struct_map_array.parquet"
            order_qt_test_46 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/float.parquet"
            order_qt_test_47 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/test_parquet_struct_in_struct.parquet"
            order_qt_test_48 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/array_struct.parquet"
            order_qt_test_49 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/int64_2_timestamp.parquet"
            order_qt_test_50 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/hudi_mor_two_level_nested_array.parquet"
            order_qt_test_51 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/type_mismatch_int96_string.parquet"
            order_qt_test_52 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/map_struct_subfield_optional.parquet"
            order_qt_test_53 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/issue_17822.parquet"
            order_qt_test_54 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/file_reader_test_map_char_key.parquet"
            order_qt_test_55 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/int32.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'time_millis' is not supported: INT32 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/hudi_array_map.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "Map element should have only one child(name='map', type='MAP_KEY_VALUE')"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/array_decode.parquet"
            order_qt_test_58 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/iceberg_string_map_string.parquet"
            order_qt_test_59 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/struct_array_null.parquet"
            order_qt_test_60 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_json.parquet"
            order_qt_test_61 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/schema2.parquet"
            order_qt_test_62 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/struct_data_only.parquet"
            order_qt_test_63 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/lzo_compression.parquet"
            order_qt_test_64 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/page_index_small_page.parquet"
            order_qt_test_65 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested_maps.parquet"
            order_qt_test_66 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/read_range_test.parquet"
            order_qt_test_67 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/dict_int.parquet"
            order_qt_test_68 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested_lists.parquet"
            order_qt_test_69 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            // uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/decimal.parquet"
            // test {
            //     sql """ select * from HDFS(
            //             "uri" = "${uri}",
            //             "hadoop.username" = "${hdfsUserName}",
            //             "format" = "parquet") limit 10; """
            //     exception "Out-of-bounds access in parquet data decoder"
            // }
            // AddressSanitizer: stack-buffer-overflow on address 0x7f251635d110 at pc 


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/schema3.parquet"
            order_qt_test_71 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/optional_map_key.parquet"
            order_qt_test_72 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/double.parquet"
            order_qt_test_73 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/byte_array.parquet"
            order_qt_test_74 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/07eeba07-b04d-42b5-9e31-35b1a22ea31d-0_0-89-83_20230208203804485.parquet"
            order_qt_test_75 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/boolean.parquet"
            order_qt_test_76 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/two_level_nested_array.parquet"
            order_qt_test_77 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/file_reader_test_struct_null.parquet"
            order_qt_test_78 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/datetime.parquet"
            order_qt_test_79 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            // uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/type_mismatch_decode_min_max.parquet"
            // order_qt_test_80 """ select * from HDFS(
            //             "uri" = "${uri}",
            //             "hadoop.username" = "${hdfsUserName}",
            //             "format" = "parquet") limit 10; """
            // doris::Status doris::vectorized::ByteArrayDictDecoder::_decode_values<false>(COW<doris::vectorized::IColumn>::mutable_ptr<doris::vectorized::IColumn>&, std::shared_ptr<doris::vectorized::IDataType const>&, doris::vectorized::ColumnSelectVector&, bool) in /mnt/disk2/suyiteng/output/be/lib/doris_be


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/nested_structs.parquet"
            order_qt_test_81 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/complex_subfield_not_null.parquet"
            order_qt_test_82 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/repeated_value.parquet"
            order_qt_test_83 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/schema1.parquet"
            order_qt_test_84 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/int96_timestamp_timezone.parquet"
            order_qt_test_85 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/map_key_is_struct.parquet"
            order_qt_test_86 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/test_parquet_time_type.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'c2' is not supported: INT64 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/json.parquet"
            order_qt_test_88 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_0.parquet"
            order_qt_test_89 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/ARROW-17100.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "Can't read byte array length from plain decoder"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/parquet_cpp_example.parquet"
            order_qt_test_91 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/push_broker_reader.parquet"
            order_qt_test_92 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/array_data_only.parquet"
            order_qt_test_93 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/file_reader_test_map.parquet"
            order_qt_test_94 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/map_null.parquet"
            order_qt_test_95 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_1.parquet"
            order_qt_test_96 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/bool_decoder.parquet"
            order_qt_test_97 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/fixed_len_byte_array.parquet"
            order_qt_test_98 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/int64.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'time_micros' is not supported: INT64 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/lz4_raw.parquet"
            order_qt_test_100 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/int96_timestamp.parquet"
            order_qt_test_101 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/data_null.parquet"
            order_qt_test_102 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/multi_stripes.parquet"
            order_qt_test_103 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet"
            order_qt_test_104 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/add_struct_subfield.parquet"
            order_qt_test_105 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group6/tpch_lineitem.parquet"
            order_qt_test_106 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
        } finally {
        }
    }
}
