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

suite("test_hdfs_parquet_group1","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/part.parquet"
            order_qt_test_0 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/web_sales.parquet"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/parquet_int_decimal_1.parquet"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/SingleFieldGroupInList.parquet"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/ThriftSingleFieldGroupInList.parquet"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/ThriftPrimitiveInList.parquet"
            order_qt_test_5 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/parquet_int_decimal_2.parquet"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/StringMapOfOptionalIntArray.parquet"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/hive_26612.parquet"
            order_qt_test_8 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/data_including_invalid_values.parquet"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/hive22670.parquet"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/UnannotatedListOfPrimitives.parquet"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/AvroPrimitiveInList.parquet"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/NestedMap.parquet"
            order_qt_test_13 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/HiveGroup.parquet"
            order_qt_test_14 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/data_with_valid_values.parquet"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/NewRequiredGroupInList.parquet"
            order_qt_test_16 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/employee_hive_3_1_3_us_pacific.parquet"
            order_qt_test_17 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/NewOptionalGroupInList.parquet"
            order_qt_test_18 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/MultiFieldGroupInList.parquet"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/escape_crlf.parquet"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/HiveRequiredGroupInList.parquet"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/test_uint.parquet"
            order_qt_test_22 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/UnannotatedListOfGroups.parquet"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group1/AvroSingleFieldGroupInList.parquet"
            order_qt_test_24 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
        } finally {
        }
    }
}
