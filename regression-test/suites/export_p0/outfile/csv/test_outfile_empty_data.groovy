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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_empty_data", "external,hive,tvf,external_docker") {

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

    // use to outfile to s3
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    // broker
    String broker_name = "hdfs"

    def export_table_name = "outfile_empty_data_test"

    def create_table = {table_name, column_define ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            ${column_define}
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_HDFS_directly = {
        // select ... into outfile ...
        def uuid = UUID.randomUUID().toString()

        hdfs_outfile_path = "/user/doris/tmp_data/${uuid}"
        uri = "${defaultFS}" + "${hdfs_outfile_path}/exp_"

        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "${uri}"
            FORMAT AS ${format}
            PROPERTIES (
                "fs.defaultFS"="${defaultFS}",
                "hadoop.username" = "${hdfsUserName}"
            );
        """
        logger.info("outfile to hdfs direct success path: " + res[0][3]);
        return res[0][3]
    }

    def outfile_to_HDFS_with_broker = {
        // select ... into outfile ...
        def uuid = UUID.randomUUID().toString()

        hdfs_outfile_path = "/user/doris/tmp_data/${uuid}"
        uri = "${defaultFS}" + "${hdfs_outfile_path}/exp_"

        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "${uri}"
            FORMAT AS ${format}
            PROPERTIES (
                "broker.fs.defaultFS"="${defaultFS}",
                "broker.name"="hdfs",
                "broker.username" = "${hdfsUserName}"
            );
        """
        logger.info("outfile to hdfs with broker success path: " + res[0][3]);
        return res[0][3]
    }

    def outfile_to_S3_directly = {
        // select ... into outfile ...
        s3_outfile_path = "${bucket}/outfile/csv/test-outfile-empty/"
        uri = "s3://${s3_outfile_path}/exp_"

        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "${uri}"
            FORMAT AS csv
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        logger.info("outfile to s3 success path: " + res[0][3]);
        return res[0][3]
    }

    try {
        def doris_column_define = """
                                    `user_id` INT NOT NULL COMMENT "用户id",
                                    `name` STRING NULL,
                                    `age` INT NULL"""
        // create table
        create_table(export_table_name, doris_column_define);
        // test outfile empty data to hdfs directly
        def outfile_to_hdfs_directly_url = outfile_to_HDFS_directly()
        // test outfile empty data to hdfs with broker
        def outfile_to_hdfs_with_broker_url= outfile_to_HDFS_with_broker()
        // test outfile empty data to s3 directly
        def outfile_to_s3_directly_url = outfile_to_S3_directly()
        qt_select_base1 """ SELECT * FROM ${export_table_name} ORDER BY user_id; """ 

        qt_select_tvf1 """ select * from HDFS(
                    "uri" = "${outfile_to_hdfs_directly_url}0.csv",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """

        qt_select_tvf2 """ select * from HDFS(
                    "uri" = "${outfile_to_hdfs_with_broker_url}0.csv",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """
        
        qt_select_tvf3 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_to_s3_directly_url.substring(5 + bucket.length(), outfile_to_s3_directly_url.length())}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}",
                "use_path_style" = "false" -- aliyun does not support path_style
            );
            """

    } finally {
    }
}
