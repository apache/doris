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

suite("test_outfile_orc_max_file_size", "p2,external") {
    String enabled = "true";
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // open nereids
        sql """ set enable_nereids_planner=true """
        sql """ set enable_fallback_to_original_planner=false """

        String ak = getS3AK()
        String sk = getS3SK()
        String s3_endpoint = getS3Endpoint()
        String region = getS3Region()
        String bucket = context.config.otherConfigs.get("s3BucketName")

        // the path used to load data
        def load_data_path = "export_test/test_orc_max_file_size.orc"
        // the path used to export data
        def outFilePath = """${bucket}/export/test_max_file_size/test_orc/exp_"""
        
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

        def table_export_name = "test_outfile_orc_max_file_size"

        create_table(table_export_name)

        // load data
        sql """ 
                insert into ${table_export_name}
                select 
                    number as user_id,
                    date_add('2024-01-01', interval cast(rand() * 365 as int) day) as date,
                    date_add('2024-01-01 00:00:00', interval cast(rand() * 365 * 24 * 3600 as int) second) as datetime,
                    concat('City_', cast(cast(rand() * 100 as int) as string)) as city,
                    cast(rand() * 80 + 18 as int) as age,
                    cast(rand() * 2 as int) as sex,
                    if(rand() > 0.5, true, false) as bool_col,
                    cast(rand() * 1000000 as int) as int_col,
                    cast(rand() * 10000000000 as bigint) as bigint_col,
                    cast(rand() * 100000000000000 as largeint) as largeint_col,
                    cast(rand() * 1000 as float) as float_col,
                    rand() * 10000 as double_col,
                    concat('char_', cast(cast(rand() * 10000 as int) as string)) as char_col,
                    cast(rand() * 1000 as decimal(10, 2)) as decimal_col
                from numbers("number" = "2000000");
            """

        def test_outfile_orc_success = {maxFileSize, isDelete, fileNumber, totalRows -> 
            def table = sql """
                select * from ${table_export_name}
                into outfile "s3://${outFilePath}"
                FORMAT AS ORC
                PROPERTIES(
                    "max_file_size" = "${maxFileSize}",
                    "delete_existing_files"="${isDelete}",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.secret_key"="${sk}",
                    "s3.access_key" = "${ak}",
                    "provider" = "${getS3Provider()}"
                );
            """

            log.info("table = " + table);
            // assertTrue(table.size() == 1)
            // assertTrue(table[0].size() == 4)
            log.info("outfile result = " + table[0])
            assertEquals(table[0][0], fileNumber)
            assertEquals(table[0][1], totalRows)
        }

        def test_outfile_orc_fail = {maxFileSize, isDelete -> 
            test {
                sql """
                    select * from ${table_export_name}
                    into outfile "s3://${outFilePath}"
                    FORMAT AS ORC
                    PROPERTIES(
                        "max_file_size" = "${maxFileSize}",
                        "delete_existing_files"="${isDelete}",
                        "s3.endpoint" = "${s3_endpoint}",
                        "s3.region" = "${region}",
                        "s3.secret_key"="${sk}",
                        "s3.access_key" = "${ak}",
                        "provider" = "${getS3Provider()}"
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
        test_outfile_orc_success('32MB', true, 2, 2000000)
        test_outfile_orc_success('65MB', true, 1, 2000000)
    }
    
}
