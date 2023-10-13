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

suite("test_hive_read_orc", "external,hive,external_docker") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }


    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """


    String hdfs_port = context.config.otherConfigs.get("hdfs_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUserName = "doris"
    def format = "orc"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def outfile_path = "/user/doris/tmp_data"
    def uri = "${defaultFS}" + "${outfile_path}/exp_"


    def export_table_name = "outfile_hive_read_orc_test"
    def hive_database = "test_hive_read_orc"
    def hive_table = "outfile_hive_read_orc_test"

    def create_table = {table_name, column_define ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            ${column_define}
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
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
                "fs.defaultFS"="${defaultFS}",
                "hadoop.username" = "${hdfsUserName}"
            );
        """
        logger.info("outfile success path: " + res[0][3]);
        return res[0][3]
    }

    def create_hive_table = {table_name, column_define ->
        def drop_table_str = """ drop table if exists ${hive_database}.${table_name} """
        def drop_database_str = """ drop database if exists ${hive_database}"""
        def create_database_str = """ create database ${hive_database}"""
        def create_table_str = """ CREATE EXTERNAL TABLE ${hive_database}.${table_name} (  
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

    // test INT, String type
    try {
        def doris_column_define = """
                                    `user_id` INT NOT NULL COMMENT "用户id",
                                    `name` STRING NULL,
                                    `age` INT NULL"""

        def hive_column_define = """
                                    user_id INT,
                                    name STRING,
                                    age INT"""

        // create table
        create_table(export_table_name, doris_column_define);
        

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', 18); """
        sql """ insert into ${export_table_name} values (2, 'doris2', 40); """
        sql """ insert into ${export_table_name} values (3, null, null); """
        sql """ insert into ${export_table_name} values (4, 'doris4', ${Integer.MIN_VALUE}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', ${Integer.MAX_VALUE}); """
        sql """ insert into ${export_table_name} values (6, null, ${Integer.MIN_VALUE}); """
        sql """ insert into ${export_table_name} values (7, null, 0); """
        sql """ insert into ${export_table_name} values (8, "nereids", null); """

        qt_select_base1 """ SELECT * FROM ${export_table_name} ORDER BY user_id; """ 

        // test outfile to hdfs
        def outfile_url = outfile_to_HDFS()

        // create hive table
        create_hive_table(hive_table, hive_column_define)

        qt_select_tvf1 """ select * from HDFS(
                    "uri" = "${outfile_url}0.orc",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """
        def tvf_res = sql """ select * from HDFS(
                    "uri" = "${outfile_url}0.orc",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """
        def hive_res = hive_docker """ SELECT * FROM ${hive_database}.${hive_table} ORDER BY user_id;"""
        
        logger.info("The result of tvf select: " + tvf_res.toString());
        logger.info("The result of hive select: " + hive_res.toString());
        assertEquals(tvf_res, hive_res)

    } finally {
    }

    // test all types
    try {
        def doris_column_define = """
                                    `user_id` INT NOT NULL COMMENT "用户id",
                                    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                                    `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间2",
                                    `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
                                    `datetimev2_1` DATETIMEV2 NOT NULL COMMENT "数据灌入日期时间",
                                    `datetimev2_2` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
                                    `datetimev2_3` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
                                    `city` VARCHAR(20) COMMENT "用户所在城市",
                                    `street` STRING COMMENT "用户所在街道",
                                    `age` SMALLINT COMMENT "用户年龄",
                                    `sex` TINYINT COMMENT "用户性别",
                                    `bool_col` boolean COMMENT "",
                                    `int_col` int COMMENT "",
                                    `bigint_col` bigint COMMENT "",
                                    `largeint_col` largeint COMMENT "",
                                    `float_col` float COMMENT "",
                                    `double_col` double COMMENT "",
                                    `char_col` CHAR(10) COMMENT "",
                                    `decimal_col` decimal COMMENT "",
                                    `decimalv3_col` decimalv3 COMMENT "",
                                    `decimalv3_col2` decimalv3(1,0) COMMENT "",
                                    `decimalv3_col3` decimalv3(1,1) COMMENT "",
                                    `decimalv3_col4` decimalv3(9,8) COMMENT "",
                                    `decimalv3_col5` decimalv3(20,10) COMMENT "",
                                    `decimalv3_col6` decimalv3(38,0) COMMENT "",
                                    `decimalv3_col7` decimalv3(38,37) COMMENT "",
                                    `decimalv3_col8` decimalv3(38,38) COMMENT ""
                                """

        def hive_column_define = """
                                    user_id INT,
                                    `date` STRING,
                                    datev2 STRING,
                                    `datetime` STRING,
                                    datetimev2_1 STRING,
                                    datetimev2_2 STRING,
                                    datetimev2_3 STRING,
                                    city STRING,
                                    street STRING,
                                    age SMALLINT,
                                    sex TINYINT,
                                    bool_col boolean,
                                    int_col INT,
                                    bigint_col BIGINT,
                                    largeint_col STRING,
                                    `float_col` float,
                                    `double_col` double,
                                    `char_col` char(5),
                                    `decimal_col` decimal ,
                                    `decimalv3_col` decimal ,
                                    `decimalv3_col2` decimal(1,0) ,
                                    `decimalv3_col3` decimal(1,1) ,
                                    `decimalv3_col4` decimal(9,8) ,
                                    `decimalv3_col5` decimal(20,10) ,
                                    `decimalv3_col6` decimal(38,0) ,
                                    `decimalv3_col7` decimal(38,37) ,
                                    `decimalv3_col8` decimal(38,38)
                                """

        // create table
        create_table(export_table_name, doris_column_define);
        

        StringBuilder sb = new StringBuilder()
        int i = 1
        sb.append("""
                (${i}, '2023-04-20', '2023-04-20', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00',
                'Beijing', 'Haidian',
                ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}',
                ${i}, ${i}, ${i}, 0.${i}, ${i}, ${i}, ${i}, ${i}, 0.${i}),
            """)

        sb.append("""
            (${++i}, '9999-12-31', '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59', '2023-04-20 00:00:00.12', '2023-04-20 00:00:00.3344',
            '', 'Haidian',
            ${Short.MIN_VALUE}, ${Byte.MIN_VALUE}, true, ${Integer.MIN_VALUE}, ${Long.MIN_VALUE}, -170141183460469231731687303715884105728, ${Float.MIN_VALUE}, ${Double.MIN_VALUE}, 'char${i}',
            100000000, 100000000, 4, 0.1, 0.99999999, 9999999999.9999999999, 99999999999999999999999999999999999999, 9.9999999999999999999999999999999999999, 0.99999999999999999999999999999999999999),
        """)
        
        sb.append("""
                (${++i}, '2023-04-21', '2023-04-21', '2023-04-20 12:34:56', '2023-04-20 00:00:00', '2023-04-20 00:00:00.123', '2023-04-20 00:00:00.123456',
                'Beijing', '', 
                ${Short.MAX_VALUE}, ${Byte.MAX_VALUE}, true, ${Integer.MAX_VALUE}, ${Long.MAX_VALUE}, 170141183460469231731687303715884105727, ${Float.MAX_VALUE}, ${Double.MAX_VALUE}, 'char${i}',
                999999999, 999999999, 9, 0.9, 9.99999999, 1234567890.0123456789, 12345678901234567890123456789012345678, 1.2345678901234567890123456789012345678, 0.12345678901234567890123456789012345678),
            """)

        sb.append("""
                (${++i}, '0000-01-01', '0000-01-01', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00',
                'Beijing', 'Haidian',
                ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}',
                ${i}, ${i}, ${i}, 0.${i}, ${i}, ${i}, ${i}, ${i}, 0.${i})
            """)

        
        sql """ INSERT INTO ${export_table_name} VALUES
                ${sb.toString()}
            """
    
        def insert_res = sql "show last insert;"
        logger.info("insert result: " + insert_res.toString())
        qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """    

        // test outfile to hdfs
        def outfile_url = outfile_to_HDFS()
        // create hive table
        create_hive_table(hive_table, hive_column_define)

        qt_select_tvf2 """ select * from HDFS(
                    "uri" = "${outfile_url}0.orc",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """

        def tvf_res = sql """ select * from HDFS(
                    "uri" = "${outfile_url}0.orc",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                    """
        def hive_res = hive_docker """ SELECT * FROM ${hive_database}.${hive_table} ORDER BY user_id;"""
        
        logger.info("The result of tvf select: " + tvf_res.toString());
        logger.info("The result of hive select: " + hive_res.toString());

        for (int row = 0; row < 4; ++row) {
            for (int j = 0; j < 27; ++j) {
                // There are some problem when type is float in test framework
                if (j == 15) {
                    continue;
                }
                assertEquals(tvf_res[row][j], hive_res[row][j])
            }
        }

    } finally {
    }
}
