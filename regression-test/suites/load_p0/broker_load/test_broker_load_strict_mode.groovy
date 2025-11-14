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

suite("test_broker_load_strict_mode", "load_p0") {
    def testTable = "test_broker_load_strict_mode"
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    def label1 = "test_broker_load_strict_mode_" + UUID.randomUUID().toString().replaceAll("-", "")
    def label2 = "test_broker_load_strict_mode_" + UUID.randomUUID().toString().replaceAll("-", "")

    sql """ DROP TABLE IF EXISTS ${testTable} """
    sql """
        create table ${testTable} (
            c1 varchar(4)
        )
        DUPLICATE KEY(c1)
        DISTRIBUTED BY HASH(c1) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """

        // should error
        sql """
            LOAD LABEL ${label1} (
                DATA INFILE("s3://${s3BucketName}/load/load23111.csv")
                INTO TABLE ${testTable}
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                (c1)
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            PROPERTIES (
                "strict_mode" = "true"
            );
            """

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="${label1}" order by createtime desc limit 1; """
            logger.info("show load result: " + result[0])
            if (result[0][2].equals("CANCELLED")) {
                def errorMsg = result[0][7]
                def firstErrorMsg = result[0][result[0].length - 1]
                logger.info("load failed, errorMsg:$errorMsg, firstErrorMsg:$firstErrorMsg")
                assertTrue(errorMsg.contains("ETL_QUALITY_UNSATISFIED"))
                assertTrue(firstErrorMsg.contains("the length of input is too long than schema"))
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }

        sql """
            LOAD LABEL ${label2} (
                DATA INFILE("s3://${s3BucketName}/load/load23111.csv")
                INTO TABLE ${testTable}
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                (c1)
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            )
            PROPERTIES (
                "strict_mode" = "false"
            );
            """

        max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="${label2}" order by createtime desc limit 1; """
            logger.info("show load result: " + result[0])
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + label2)
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }
}