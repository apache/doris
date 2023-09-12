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

suite("test_seq_load", "load_p0") {

    def tableName = "uniq_tbl_basic"

    sql new File("""${ context.file.parent }/ddl/uniq_tbl_basic_drop.sql""").text
    sql new File("""${ context.file.parent }/ddl/uniq_tbl_basic.sql""").text

    def label = UUID.randomUUID().toString().replace("-", "0")
    def path = "s3://doris-build-1308700295/regression/load/data/basic_data.csv"
    def format_str = "CSV"
    def ak = getS3AK()
    def sk = getS3SK()
    def seq_column = "K01"

    def sql_str = """
            LOAD LABEL $label (
                DATA INFILE("$path")
                INTO TABLE $tableName
                COLUMNS TERMINATED BY "|"
                FORMAT AS $format_str
                (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
                ORDER BY $seq_column
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "cos.ap-beijing.myqcloud.com",
                "AWS_REGION" = "ap-beijing"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""
    logger.info("Submit load with lable: $label, table: $tableName, path: $path")

    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED " + label)
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            def reason = result[0][7]
            logger.info("load failed, reason:$reason")
            assertTrue(1 == 2)
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(1 == 2, "load Timeout: $label")
        }
    }

    qt_sql """ SELECT COUNT(*) FROM ${tableName} """
}