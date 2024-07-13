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

suite("test_update_rows_and_partition_first_load", "p2") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    String ak = getS3AK()
    String sk = getS3SK()
    String enabled = context.config.otherConfigs.get("enableBrokerLoad")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        sql """DROP DATABASE IF EXISTS test_update_rows_and_partition_first_load"""
        sql """CREATE DATABASE test_update_rows_and_partition_first_load"""
        sql """use test_update_rows_and_partition_first_load"""
        sql """
                CREATE TABLE update_rows_test1 (
                    id int NULL,
                    name String NULL
                )ENGINE=OLAP
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1"
                );
           """

        sql """
                CREATE TABLE update_rows_test2 (
                    id int NULL,
                    name String NULL
                )ENGINE=OLAP
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1"
                );
           """

        sql """
             CREATE TABLE `partition_test1` (
                `id` INT NOT NULL,
                `name` VARCHAR(25) NOT NULL
              ) ENGINE=OLAP
              DUPLICATE KEY(`id`)
                COMMENT 'OLAP'
                PARTITION BY RANGE(`id`)
              (PARTITION p1 VALUES [("0"), ("100")),
               PARTITION p2 VALUES [("100"), ("200")),
               PARTITION p3 VALUES [("200"), ("300")))
              DISTRIBUTED BY HASH(`id`) BUCKETS 1
              PROPERTIES (
               "replication_num" = "1");
             """

        sql """
             CREATE TABLE `partition_test2` (
                `id` INT NOT NULL,
                `name` VARCHAR(25) NOT NULL
              ) ENGINE=OLAP
              DUPLICATE KEY(`id`)
                COMMENT 'OLAP'
                PARTITION BY RANGE(`id`)
              (PARTITION p1 VALUES [("0"), ("100")),
               PARTITION p2 VALUES [("100"), ("200")),
               PARTITION p3 VALUES [("200"), ("300")))
              DISTRIBUTED BY HASH(`id`) BUCKETS 1
              PROPERTIES (
               "replication_num" = "1");
             """

        sql """analyze table update_rows_test1 with sync"""
        sql """analyze table update_rows_test2 with sync"""
        sql """analyze table partition_test1 with sync"""
        sql """analyze table partition_test2 with sync"""

        def label = "part_" + UUID.randomUUID().toString().replace("-", "0")
        sql """
                LOAD LABEL ${label} (
                    DATA INFILE("s3://${s3BucketName}/regression/load/data/update_rows_1.csv")
                    INTO TABLE update_rows_test1
                    COLUMNS TERMINATED BY ",",
                    DATA INFILE("s3://${s3BucketName}/regression/load/data/update_rows_2.csv")
                    INTO TABLE update_rows_test2
                    COLUMNS TERMINATED BY ",",
                    DATA INFILE("s3://${s3BucketName}/regression/load/data/update_rows_1.csv")
                    INTO TABLE partition_test1
                    COLUMNS TERMINATED BY ",",
                    DATA INFILE("s3://${s3BucketName}/regression/load/data/update_rows_2.csv")
                    INTO TABLE partition_test2
                    COLUMNS TERMINATED BY ","
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${s3Endpoint}",
                    "AWS_REGION" = "${s3Region}",
                    "provider" = "${getS3Provider()}"
                );
        """

        boolean finished = false;
        for(int i = 0; i < 120; i++) {
            def result = sql """show load where label = "$label" """
            if (result[0][2] == "FINISHED") {
                finished = true;
                break;
            }
            logger.info("Load not finished, wait one second.")
            Thread.sleep(1000)
        }
        if (finished) {
            def result = sql """show table stats update_rows_test1"""
            assertEquals("5", result[0][0])
            result = sql """show table stats update_rows_test2"""
            assertEquals("6", result[0][0])
            result = sql """show table stats partition_test1"""
            assertEquals("5", result[0][0])
            assertEquals("true", result[0][6])
            result = sql """show table stats partition_test2"""
            assertEquals("true", result[0][6])
            assertEquals("6", result[0][0])
        }
        sql """DROP DATABASE IF EXISTS test_update_rows_and_partition_first_load"""
    }
}

