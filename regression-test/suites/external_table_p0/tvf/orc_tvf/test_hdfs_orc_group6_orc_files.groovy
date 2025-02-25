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

suite("test_hdfs_orc_group6_orc_files","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/array_data_only.orc"
            order_qt_test_0 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_positional_column.orc"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/issue_16365.orc"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/boolean_type.orc"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/map_data_only.orc"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/data-bb44368c-b491-49ab-b81a-eea013f94132-0.orc"
            order_qt_test_5 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/empty_row_index.orc"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_varchar_column.orc"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_array_basic.orc"
            order_qt_test_8 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_struct_array_map_basic.orc"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/writer_tz_utc.orc"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_binary_column.orc"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/writer_at_shanghai.orc"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/struct_data_only.orc"
            order_qt_test_13 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/compound.orc"
            order_qt_test_14 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/data-32204456-8395-4f77-9347-b2d40939a5d5-0.orc"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/map_decimal_date.lz4.orc"
            order_qt_test_16 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/string-2-double.orc"
            order_qt_test_17 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/row-4k-id.orc"
            order_qt_test_18 """ select sum(id) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_struct_basic.orc"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/writer_tz_shanghai.orc"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/non_vec_orc_scanner.orc"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/tinyint.orc"
            order_qt_test_22 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_upper_case.orc"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_padding_char.orc"
            order_qt_test_24 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/date_type.orc"
            order_qt_test_25 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/scalar_types.orc"
            order_qt_test_26 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_test_time_column.orc"
            order_qt_test_27 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/two-strips-dict-and-nodict.orc"
            order_qt_test_28 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/data-07ea8d48-6012-4a76-a564-c422995189f2-0.orc"
            order_qt_test_29 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/type_mismatch.orc"
            order_qt_test_30 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/map_type_mismatched.orc"
            order_qt_test_31 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/decimal_and_timestamp.orc"
            order_qt_test_32 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/string-dict-column.orc"
            qt_test_33 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by col1 limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/orc_zero_size_stream.orc"
            order_qt_test_34 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/boolean_slot_ref.orc"
            order_qt_test_35 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/multi_stripes.orc"
            qt_test_36 """ select c1, sum(c0) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") group by c1 order by c1; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/padding_char_varchar_10k.orc"
            order_qt_test_37 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/map_filter_bug.orc"
            order_qt_test_38 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/timestamp.orc"
            order_qt_test_39 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group6/dec_orc.orc"
            order_qt_test_40 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
        } finally {
        }
    }
}
