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

suite("test_hdfs_orc_group0_orc_files","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/decimal.orc"
            order_qt_test_0 """ select sum(_col0) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testSargSkipPickupGroupWithoutIndexJava.orc"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/sample2.orc"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/sample1.orc"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.test1.orc"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testWithoutIndex.orc"
            order_qt_test_5 """ select count(*) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestVectorOrcFile.testLz4.orc"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by y limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testSargSkipPickupGroupWithoutIndexCPlusPlus.orc"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by x limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestVectorOrcFile.testZstd.0.12.orc"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by y limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestStringDictionary.testRowIndex.orc"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by str limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testPredicatePushdown.orc"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by int1 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.metaData.orc"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/col.dot.orc"
            order_qt_test_13 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/zero.orc"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.columnProjection.orc"
            order_qt_test_18 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by int1 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc_no_format.orc"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testStringAndBinaryStatistics.orc"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc_split_elim_cpp.orc"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by userid limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/demo-11-none.orc"
            order_qt_test_22 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by _col0 limit 100; """

            order_qt_test_22_2 """ select count(_col0) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testDate2038.orc"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by time limit 10; """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/nulls-at-end-snappy.orc"
            order_qt_test_25 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by _col0 DESC limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestVectorOrcFile.testLzo.orc"
            order_qt_test_26 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by y limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/decimal64_v2_cplusplus.orc"
            order_qt_test_27 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/complextypes_iceberg.orc"
            order_qt_test_28 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/bad_bloom_filter_1.6.11.orc"
            order_qt_test_30 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/demo-11-zlib.orc"
            order_qt_test_31 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by _col0 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/acid5k.orc"
            order_qt_test_32 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by rowid limit 100; """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc_index_int_string.orc"
            order_qt_test_34 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by _col0 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testMemoryManagementV12.orc"
            order_qt_test_35 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by int1 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testSnappy.orc"
            order_qt_test_36 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by int1 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/demo-12-zlib.orc"
            order_qt_test_37 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by _col0 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/over1k_bloom.orc"
            order_qt_test_38 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by _col0 DESC, _col1  DESC limit 98; """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc-file-no-timezone.orc"
            order_qt_test_41 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/decimal64_v2.orc"
            order_qt_test_42 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc-file-dst-no-timezone.orc"
            order_qt_test_43 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.emptyFile.orc"
            order_qt_test_44 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testSeek.orc"
            order_qt_test_45 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by int1 limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc-file-no-double-statistic.orc"
            order_qt_test_46 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc_split_elim.orc"
            order_qt_test_47 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by userid limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testStripeLevelStats.orc"
            order_qt_test_48 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/orc_split_elim_new.orc"
            order_qt_test_49 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by userid limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/bad_bloom_filter_1.6.0.orc"
            order_qt_test_50 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/TestOrcFile.testMemoryManagementV11.orc"
            order_qt_test_51 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc") order by int1 limit 100; """
        } finally {
        }
    }
}
