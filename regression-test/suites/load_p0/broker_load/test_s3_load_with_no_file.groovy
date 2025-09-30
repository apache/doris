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

suite("test_s3_load_with_no_file", "load_p0") {
    def tableName = "tbl_with_no_file"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
             user_id            BIGINT       NOT NULL COMMENT "用户 ID",
             name               VARCHAR(20)           COMMENT "用户姓名",
             age                INT                   COMMENT "用户年龄"
         ) DUPLICATE KEY(user_id)
         DISTRIBUTED BY HASH(user_id) BUCKETS 1
         PROPERTIES (
             "replication_num" = "1"
         )
    """

    def label = UUID.randomUUID().toString().replace("-", "0")

    def sql_str = """
            LOAD LABEL $label (
                DATA INFILE("s3://${s3BucketName}/regression/load/data/xxxx_no_file.csv")
                INTO TABLE $tableName
                COLUMNS TERMINATED BY ","
            )
            WITH S3 (
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "PROVIDER" = "${getS3Provider()}",
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}"
            )
            """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""

    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
        logger.info("show load result: " + result[0])
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED " + label)
            assertTrue(1 == 2)
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            def reason = result[0][7]
            logger.info("load failed, reason:$reason")
            assertTrue(reason.contains("No source files found"))
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(1 == 2, "load Timeout: $label")
        }
    }
}
