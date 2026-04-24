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

suite("test_outfile_empty_data_with_broker", "tvf") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    // use to outfile to hdfs
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUserName = "doris"
    def format = "csv"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"

    String broker_name = "hdfs"

    def export_table_name = "outfile_empty_data_with_broker_test"

    def create_table = {table_name, column_define ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            ${column_define}
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_HDFS_with_broker = {
        // select ... into outfile ...
        def uuid = UUID.randomUUID().toString()

        def hdfs_outfile_path = "/user/doris/tmp_data/${uuid}"
        def uri = "${defaultFS}" + "${hdfs_outfile_path}/exp_"

        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "${uri}"
            FORMAT AS ${format}
            PROPERTIES (
                "broker.fs.defaultFS"="${defaultFS}",
                "broker.name"="${broker_name}",
                "broker.username" = "${hdfsUserName}"
            );
        """
        logger.info("outfile to hdfs with broker success path: " + res[0][3]);
        return res[0][3]
    }

    try {
        def doris_column_define = """
                                    `user_id` INT NOT NULL COMMENT "user id",
                                    `name` STRING NULL,
                                    `age` INT NULL"""
        // create table
        create_table(export_table_name, doris_column_define);
        // test outfile empty data to hdfs with broker
        def outfile_to_hdfs_with_broker_url = outfile_to_HDFS_with_broker()

        qt_select_tvf """ select * from HDFS(
                    "uri" = "${outfile_to_hdfs_with_broker_url}0.csv",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """

    } finally {
    }
}
