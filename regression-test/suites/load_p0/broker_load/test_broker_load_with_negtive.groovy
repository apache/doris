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

suite("test_broker_load_with_negtive", "load_p0") {
    // define a sql table
    def testTable = "tbl_broker_load_with_negtive"

    sql """ DROP TABLE IF EXISTS ${testTable} """
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` BIGINT NOT NULL,
            `k2` DATE NULL,
            `k3` INT(11) NOT NULL,
            `k4` INT(11) NOT NULL,
            `v5` BIGINT SUM NULL DEFAULT "0"
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    def check_load_result = {checklabel, testTablex ->
        def max_try_milli_secs = 30000
        while (max_try_milli_secs) {
            def result = sql "show load where label = '${checklabel}'"
            log.info("result: ${result}")
            if(result[0][2] == "FINISHED") {
                qt_select "select * from ${testTablex} order by k1, k2, k3, k4"
                break
            } else {
                sleep(1000) // wait 1 second every time
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    assertEquals(1, 2)
                }
            }
        }
    }

    // first load
    def label = UUID.randomUUID().toString().replace("-", "0")

    def sql_str = """
            LOAD LABEL $label (
                DATA INFILE("s3://${s3BucketName}/regression/load/data/broker_load_with_merge.csv")
                INTO TABLE $testTable
                COLUMNS TERMINATED BY ","
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "PROVIDER" = "${getS3Provider()}"
            )
            """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""

    check_load_result.call(label, testTable)

    // second load with negative
    label = UUID.randomUUID().toString().replace("-", "1")
    sql_str = """
            LOAD LABEL $label (
                DATA INFILE("s3://${s3BucketName}/regression/load/data/broker_load_with_merge.csv")
                NEGATIVE
                INTO TABLE $testTable
                COLUMNS TERMINATED BY ","
                (k1, k2, k3, k4, tmp)
                set(v5 = tmp + 2)
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "PROVIDER" = "${getS3Provider()}"
            )
            """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""

    check_load_result.call(label, testTable)

    // third load with negative
    label = UUID.randomUUID().toString().replace("-", "2")
    sql_str = """
            LOAD LABEL $label (
                DATA INFILE("s3://${s3BucketName}/regression/load/data/broker_load_with_merge.csv")
                NEGATIVE
                INTO TABLE $testTable
                COLUMNS TERMINATED BY ","
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "PROVIDER" = "${getS3Provider()}"
            )
            """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""

    check_load_result.call(label, testTable)

    test {
        sql """
            LOAD LABEL $label (
                DELETE
                DATA INFILE("s3://${s3BucketName}/regression/load/data/broker_load_with_merge.csv")
                NEGATIVE
                INTO TABLE $testTable
                COLUMNS TERMINATED BY ","
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "PROVIDER" = "${getS3Provider()}"
            )
            """
        exception "not support MERGE or DELETE with NEGATIVE."
    }
}