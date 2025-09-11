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

suite("test_bulk_load_empty_field_as_null", "p0") {
    def tableName = "test_bulk_load_empty_field_as_null"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` string NULL,
            `v1` date  NULL,
            `v2` string  NULL,
            `v3` datetime  NULL,
            `v4` string  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    def label = UUID.randomUUID().toString().replace("-", "0")

    def sql_str = """
            LOAD LABEL $label (
                DATA INFILE("s3://${s3BucketName}/regression/load/data/empty_field_as_null.csv")
                INTO TABLE $tableName
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                PROPERTIES (
                    "empty_field_as_null" = "true"
                )
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "provider" = "${getS3Provider()}"
            );
            """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""

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

    qt_sql """ SELECT * FROM ${tableName} order by k1 """

}
