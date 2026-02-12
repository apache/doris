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

suite("test_export_max_file_size", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // open nereids
        sql """ set enable_nereids_planner=true """
        sql """ set enable_fallback_to_original_planner=false """

        String ak = getS3AK()
        String sk = getS3SK()
        String s3_endpoint = getS3Endpoint()
        String region = getS3Region()
        String bucket = context.config.otherConfigs.get("s3BucketName")

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
                from numbers("number" = "1000000");
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

        def outFilePath = """${bucket}/export/test_max_file_size/exp_"""

        // 1. csv test
        def test_export = {format, file_suffix, isDelete ->
            def uuid = UUID.randomUUID().toString()
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}"
                PROPERTIES(
                    "label" = "${uuid}",
                    "format" = "${format}",
                    "max_file_size" = "5MB",
                    "delete_existing_files"="${isDelete}"
                )
                WITH s3 (
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.secret_key"="${sk}",
                    "s3.access_key" = "${ak}",
                    "provider" = "${getS3Provider()}"
                );
            """

            def outfile_info = waiting_export.call(uuid)
            def json = parseJson(outfile_info)
            assert json instanceof List
            assertEquals("25", json.fileNumber[0][0])
            def outfile_url = json.url[0][0]

            qt_sql_count """ 
                select count(*) from s3(
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length())}.csv",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "csv",
                    "provider" = "${getS3Provider()}",
                    "region" = "${region}"
                );
            """

            // check data correctness
            sql """ 
                insert into ${table_load_name}
                select * from s3(
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length())}.csv",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "csv",
                    "provider" = "${getS3Provider()}",
                    "region" = "${region}"
                );
            """

            order_qt_select """ select user_id from ${table_load_name} order by user_id limit 100 """
            order_qt_select_cnt """ select count(*) from ${table_load_name} """
        }

        // begin test
        test_export('csv', 'csv', true);
    }
}
