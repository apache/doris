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

suite("test_hive_read_parquet_complex_type", "external,hive,external_docker") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
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


        def export_table_name = "outfile_hive_read_parquet_complex_type_test"
        def hive_database = "test_hive_read_parquet_complex_type"
        def hive_table = "outfile_hive_read_parquet_complex_type_test"

        def create_table = {table_name, column_define ->
            sql """ DROP TABLE IF EXISTS ${table_name} """
            sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `user_id` INT NOT NULL COMMENT "用户id",
                `name` STRING COMMENT "用户年龄",
                ${column_define}
                )
                DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
            """
        }

        def create_hive_table = {table_name, column_define ->
            def drop_table_str = """ drop table if exists ${hive_database}.${table_name} """
            def drop_database_str = """ drop database if exists ${hive_database}"""
            def create_database_str = """ create database ${hive_database}"""
            def create_table_str = """ CREATE EXTERNAL TABLE ${hive_database}.${table_name} (  
                                            user_id INT,
                                            name STRING,
                                            ${column_define}
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

        def outfile_to_HDFS = {
            // select ... into outfile ...
            def uuid = UUID.randomUUID().toString()

            outfile_path = "/user/doris/tmp_data/${uuid}"
            uri = "${defaultFS}" + "${outfile_path}/exp_"

            def res = sql """
                SELECT * FROM ${export_table_name} t ORDER BY user_id
                INTO OUTFILE "${uri}"
                FORMAT AS ${format}
                PROPERTIES (
                    "hadoop.username" = "${hdfsUserName}"
                );
            """
            logger.info("outfile success path: " + res[0][3]);
            return res[0][3]
        }

        // because for hive, `null` is null, and there is no space between two elements
        def handle_doris_space_and_NULL = {res -> 
            res = res.replaceAll(", ", ",");
            res = res.replaceAll("NULL", "null");
            return res
        }


        // 1. struct NULL type
        try {

            def doris_field_define = "`s_info` STRUCT<s_id:int(11), s_name:string, s_address:string> NULL"
            
            def hive_field_define = "`s_info` STRUCT<s_id:int, s_name:string, s_address:string>"
            

            // create table to export data
            create_table(export_table_name, doris_field_define)

            // insert data
            sql """ insert into ${export_table_name} values (1, 'doris1', {1, 'sn1', 'sa1'}); """
            sql """ insert into ${export_table_name} values (2, 'doris2', struct(2, 'sn2', 'sa2')); """
            sql """ insert into ${export_table_name} values (3, 'doris3', named_struct('s_id', 3, 's_name', 'sn3', 's_address', 'sa3')); """
            sql """ insert into ${export_table_name} values (4, 'doris4', null); """
            sql """ insert into ${export_table_name} values (5, 'doris5', struct(5, null, 'sa5')); """
            sql """ insert into ${export_table_name} values (6, 'doris6', struct(null, null, null)); """
            sql """ insert into ${export_table_name} values (7, null, struct(null, null, null)); """
            sql """ insert into ${export_table_name} values (8, null, null); """

            // test base data
            qt_select_base1 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

            // test outfile to hdfs
            def outfile_url = outfile_to_HDFS()

            // create hive table
            create_hive_table(hive_table, hive_field_define)

            qt_select_tvf1 """ select * from HDFS(
                            "uri" = "${outfile_url}0.parquet",
                            "fs.defaultFS" = "${defaultFS_with_postfix}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}");
                            """

            qt_hive_docker_02 """ SELECT * FROM ${hive_database}.${hive_table};"""
                
        } finally {
        }

        // 2. test Map
        try {
            def doris_field_define = "`m_info` Map<STRING, LARGEINT> NULL"
            
            def hive_field_define = "`m_info` Map<STRING, STRING>"
            

            // create table to export data
            create_table(export_table_name, doris_field_define)

            // insert data
            sql """ insert into ${export_table_name} values (1, 'doris1', {'a': 100, 'b': 111}), (2, 'doris2', {'a': 200, 'b': 222}); """
            sql """ insert into ${export_table_name} values (3, 'doris3', {'a': null, 'b': 333, 'c':399, 'd':399999999999999}); """
            sql """ insert into ${export_table_name} values (4, 'doris4', {}); """
            sql """ insert into ${export_table_name} values (5, 'doris5', {'b': null}); """
            sql """ insert into ${export_table_name} values (6, null, null); """
            sql """ insert into ${export_table_name} values (7, 'doris7', null); """

            // test base data
            qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

            // test outfile to hdfs
            def outfile_url = outfile_to_HDFS()

            // create hive table
            create_hive_table(hive_table, hive_field_define)

            qt_select_tvf2 """ select * from HDFS(
                            "uri" = "${outfile_url}0.parquet",
                            "fs.defaultFS" = "${defaultFS}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}");
                            """

            qt_hive_docker_02 """ SELECT * FROM ${hive_database}.${hive_table};"""

        } finally {
        }

        // 3. test ARRAY
        try {
            def doris_field_define = "`a_info` ARRAY<int> NULL"
            
            def hive_field_define = "`a_info` ARRAY<int>"


            // create table to export data
            create_table(export_table_name, doris_field_define)


            // insert data
            sql """ insert into ${export_table_name} values (1, 'doris1', [9, 99, 999]), (2, 'doris2', [8, 88]); """
            sql """ insert into ${export_table_name} values (3, 'doris3', []); """
            sql """ insert into ${export_table_name} values (4, 'doris4', null); """
            sql """ insert into ${export_table_name} values (5, 'doris5', [1, null, 2]); """
            sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
            sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, 1, 2, 999999, 111111]); """
            sql """ insert into ${export_table_name} values (8, 'doris8', null); """

            // test base data
            qt_select_base3 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

            // test outfile to hdfs
            def outfile_url = outfile_to_HDFS()

            // create hive table
            create_hive_table(hive_table, hive_field_define)

            qt_select_tvf3 """ select * from HDFS(
                            "uri" = "${outfile_url}0.parquet",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}");
                            """

            qt_hive_docker_03 """ SELECT * FROM ${hive_database}.${hive_table};"""

        } finally {
        }

        // 4. test struct with all type
        try {
            def doris_field_define = "`s_info` STRUCT<user_id:INT, date:DATE, datetime:DATETIME, city:VARCHAR(20), age:SMALLINT, sex:TINYINT, bool_col:BOOLEAN, int_col:INT, bigint_col:BIGINT, largeint_col:LARGEINT, float_col:FLOAT, double_col:DOUBLE, char_col:CHAR(10), decimal_col:DECIMAL> NULL"
            
            def hive_field_define = "`s_info` STRUCT<user_id:INT, `date`:DATE, `datetime`:TIMESTAMP, city:VARCHAR(20), age:SMALLINT, sex:TINYINT, bool_col:BOOLEAN, int_col:INT, bigint_col:BIGINT, largeint_col:STRING, float_col:FLOAT, double_col:DOUBLE, char_col:CHAR(10), decimal_col:DECIMAL>"


            // create table to export data
            create_table(export_table_name, doris_field_define)


            // insert data
            StringBuilder sb = new StringBuilder()
            int i = 1
            for (; i < 10; i ++) {
                sb.append("""
                    (${i}, 'doris_${i}', {${i}, '2017-10-01', '2017-10-01 00:00:00', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}_1234', ${i}}),
                """)
            }
            sb.append("""
                (${i}, 'doris_${i}', {${i}, '2017-10-01', '2017-10-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL})
            """)

            sql """ INSERT INTO ${export_table_name} VALUES ${sb.toString()} """

            // test base data
            qt_select_base4 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

            // test outfile to hdfs
            def outfile_url = outfile_to_HDFS()

            // create hive table
            create_hive_table(hive_table, hive_field_define)

            qt_select_tvf4 """ select * from HDFS(
                            "uri" = "${outfile_url}0.parquet",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}");
                            """

            qt_hive_docker_04 """ SELECT * FROM ${hive_database}.${hive_table};"""

        } finally {
        }
    }
}
