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

suite("test_export_max_file_size", "p2") {
    String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
    String hdfsPort = context.config.otherConfigs.get("extHdfsPort")
    String fs = "hdfs://${nameNodeHost}:${hdfsPort}"
    String user_name = context.config.otherConfigs.get("extHiveHmsUser")


    def table_export_name = "test_export_max_file_size"
    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
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

    // Used to store the data exported before
    def table_load_name = "test_load"
    sql """ DROP TABLE IF EXISTS ${table_load_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_load_name} (
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

    def load_data_path = "/user/export_test/exp_max_file_size.csv"
    sql """ 
            insert into ${table_export_name}
            select * from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}${load_data_path}",
            "hadoop.username" = "${user_name}",
            "column_separator" = ",",
            "format" = "csv");
        """

    
    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                return res[0][11]
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    def outFilePath = """/user/export_test/test_max_file_size/exp_"""

    // 1. csv test
    def test_export = {format, file_suffix, isDelete ->
        def uuid = UUID.randomUUID().toString()
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${fs}${outFilePath}"
            PROPERTIES(
                "label" = "${uuid}",
                "format" = "${format}",
                "column_separator"=",",
                "max_file_size" = "5MB",
                "delete_existing_files"="${isDelete}"
            )
            with HDFS (
                "fs.defaultFS"="${fs}",
                "hadoop.username" = "${user_name}"
            );
        """

        def outfile_info = waiting_export.call(uuid)
        def json = parseJson(outfile_info)
        assert json instanceof List
        assertEquals("3", json.fileNumber[0])
        def outfile_url = json.url[0]

        for (int j = 0; j < json.fileNumber[0].toInteger(); ++j ) {
            // check data correctness
            sql """ 
                insert into ${table_load_name}
                select * from hdfs(
                "uri" = "${outfile_url}${j}.csv",
                "hadoop.username" = "${user_name}",
                "column_separator" = ",",
                "format" = "csv");
            """
        }
    }

    // begin test
    test_export('csv', 'csv', true);
    order_qt_select """ select * from ${table_load_name} order by user_id limit 1000"""

}
