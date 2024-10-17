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

suite("test_tvf_upgrade_load", "p0,external,hive,external_docker,external_docker_hive,restart_fe,upgrade_case") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUserName = "doris"
    def format = "csv"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
            // test create view from tvf and alter view from tvf
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/all_types.csv"
            format = "csv"
            sql """ DROP VIEW IF EXISTS test_hdfs_tvf_create_view;"""
            sql """
                create view test_hdfs_tvf_create_view as
                select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "${format}") order by c1;
                """
            logger.info("View test_hdfs_tvf_create_view created")


            sql """
                alter view test_hdfs_tvf_create_view as
                select c1 from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "${format}") order by c1;
                """
            logger.info("View test_hdfs_tvf_create_view altered")
    }
}