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

suite("test_hdfs_orc_group7_orc_files","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group7/decimal.orc"
            qt_test_0 """ select sum(_col0) from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group7/TestOrcFile.test1.orc"
            qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """


            // There are a timestamp problem in this case.
            // uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group7/TestOrcFile.testDate1900.orc"
            // qt_test_2 """ select * from HDFS(
            //             "uri" = "${uri}",
            //             "hadoop.username" = "${hdfsUserName}",
            //             "format" = "orc") order by time limit 100; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group7/TestOrcFile.emptyFile.orc"
            qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
        } finally {
        }
    }
}
