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

suite("test_hive_read_parquet_datetime", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)

        // open nereids
        sql """ set enable_nereids_planner=true """
        sql """ set enable_fallback_to_original_planner=false """

        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        // It's okay to use random `hdfsUser`, but can not be empty.
        def hdfsUserName = "doris"
        def format = "parquet"
        def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
        def defaultFS_with_postfix = "hdfs://${externalEnvIp}:${hdfs_port}/"
        def outfile_path = "/user/doris/tmp_data"
        def uri = "${defaultFS}" + "${outfile_path}/exp_"
        def export_table_name = "outfile_hive_read_parquet_datetime_type_test"
        def hive_database = "test_hive_read_parquet_datetime_type"
        def hive_table = "outfile_hive_read_parquet_datetime_type_test"

        def create_table = {table_name ->
            sql """ DROP TABLE IF EXISTS ${table_name} """
            sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `id` INT NOT NULL,
                `ts` DATETIME NOT NULL
                )
                DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
            """
        }

        def create_hive_table = {table_name ->
            def drop_table_str = """ drop table if exists ${hive_database}.${table_name} """
            def drop_database_str = """ drop database if exists ${hive_database}"""
            def create_database_str = """ create database ${hive_database}"""
            def create_table_str = """ CREATE EXTERNAL TABLE ${hive_database}.${table_name} (
                                            id INT,
                                            ts TIMESTAMP
                                        )
                                        stored as ${format}
                                        LOCATION "${outfile_path}"
                                    """

            logger.info("hive sql: " + drop_table_str)
            hive_docker """ ${drop_table_str} """

            logger.info("hive sql: " + drop_database_str)
            hive_docker """ ${drop_database_str} """

            logger.info("hive sql: " + create_database_str)
            hive_docker """ ${create_database_str}"""

            logger.info("hive sql: " + create_table_str)
            hive_docker """ ${create_table_str} """
        }

        def outfile_to_HDFS = { enable_int96 = false ->
            def uuid = UUID.randomUUID().toString()
            outfile_path = "/user/doris/tmp_data/${uuid}"
            uri = "${defaultFS}" + "${outfile_path}/exp_"

            def res = sql """
                SELECT * FROM ${export_table_name} t ORDER BY id
                INTO OUTFILE "${uri}"
                FORMAT AS ${format}
                PROPERTIES (
                    "hadoop.username" = "${hdfsUserName}",
                    "enable_int96_timestamps" = "${enable_int96}"
                );
            """
            logger.info("outfile success path: " + res[0][3]);
            return res[0][3]
        }

        try {
            // create doris table
            create_table(export_table_name)

            // insert 3 rows
            sql """ INSERT INTO ${export_table_name} VALUES
                (1, '2024-01-01 10:00:00'),
                (2, '2024-06-15 12:30:45'),
                (3, '2024-12-31 23:59:59');
            """

            // verify doris data
            qt_select_base_desc """ desc ${export_table_name}; """
            qt_select_base """ SELECT * FROM ${export_table_name} t ORDER BY id; """

            // outfile to hdfs with enable_int96_timestamps=true
            def outfile_url = outfile_to_HDFS(true)

            // create hive table
            create_hive_table(hive_table)

            qt_select_tvf_desc """ desc function HDFS(
                            "uri" = "${outfile_url}0.parquet",
                            "fs.defaultFS" = "${defaultFS}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}");
                            """
            // read back via tvf
            qt_select_tvf """ select * from HDFS(
                            "uri" = "${outfile_url}0.parquet",
                            "fs.defaultFS" = "${defaultFS}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}");
                            """

            qt_select_parquet_tvf """ select * except(file_name) from parquet_meta(
                "uri" = "${outfile_url}0.parquet",
                "fs.defaultFS" = "${defaultFS}",
                "hadoop.username" = "${hdfsUserName}",
                "mode" = "parquet_schema");
                """
            // read via hive
            qt_hive_docker """ SELECT * FROM ${hive_database}.${hive_table};"""

        } finally {
        }
    }
}
