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

suite("test_outfile_orc_max_file_size", "p2") {
    String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
    String hdfsPort = context.config.otherConfigs.get("extHdfsPort")
    String fs = "hdfs://${nameNodeHost}:${hdfsPort}"
    String user_name = context.config.otherConfigs.get("extHiveHmsUser")

    // the path used to load data
    def load_data_path = "/user/export_test/test_orc_max_file_size.orc"
    // the path used to export data
    def outFilePath = """/user/export_test/test_max_file_size/test_orc/exp_"""
    
    def create_table = {table_name -> 
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` INT COMMENT "用户年龄",
                `sex` INT COMMENT "用户性别",
                `bool_col` boolean COMMENT "",
                `int_col` int COMMENT "",
                `bigint_col` bigint COMMENT "",
                `largeint_col` largeint COMMENT "",
                `float_col` float COMMENT "",
                `double_col` double COMMENT "",
                `char_col` CHAR(10) COMMENT "",
                `decimal_col` decimal COMMENT ""
                )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    def table_export_name = "test_export_max_file_size"

    create_table(table_export_name)

    // load data
    sql """ 
            insert into ${table_export_name}
            select * from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}${load_data_path}",
            "fs.defaultFS" = "${fs}",
            "hadoop.username" = "${user_name}",
            "format" = "orc");
        """

    def test_outfile_orc_success = {maxFileSize, isDelete, fileNumber, totalRows -> 
        table = sql """
            select * from ${table_export_name}
            into outfile "${fs}${outFilePath}"
            FORMAT AS ORC
            PROPERTIES(
                "fs.defaultFS"="${fs}",
                "hadoop.username" = "${user_name}",
                "max_file_size" = "${maxFileSize}",
                "delete_existing_files"="${isDelete}"
            );
        """
        assertTrue(table.size() == 1)
        assertTrue(table[0].size == 4)
        log.info("outfile result = " + table[0])
        assertEquals(table[0][0], fileNumber)
        assertEquals(table[0][1], totalRows)
    }

    def test_outfile_orc_fail = {maxFileSize, isDelete -> 
        test {
            sql """
                select * from ${table_export_name}
                into outfile "${fs}${outFilePath}"
                FORMAT AS ORC
                PROPERTIES(
                    "fs.defaultFS"="${fs}",
                    "hadoop.username" = "${user_name}",
                    "max_file_size" = "${maxFileSize}",
                    "delete_existing_files"="${isDelete}"
                );
            """

            // other check will not work because already declared a check callback
            exception "max file size should between 5MB and 2GB"

            // callback
            check { result, exception, startTime, endTime ->
                assertTrue(exception != null)
            }
        }
    }

    test_outfile_orc_fail('3MB', true)
    test_outfile_orc_fail('2.1GB', true)
    test_outfile_orc_success('5MB', true, 3, 2000000)
    test_outfile_orc_success('63MB', true, 3, 2000000)
    test_outfile_orc_success('64MB', true, 3, 2000000)
    test_outfile_orc_success('80MB', true, 2, 2000000)
}
