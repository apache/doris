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

suite("test_broker_load_multi_filegroup", "p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()

    // pr 22666
    def tbl_22666 = "part_22666"
    sql """drop table if exists ${tbl_22666} force"""
    sql """
        CREATE TABLE ${tbl_22666} (
            p_partkey          int NULL,
            p_name        VARCHAR(55) NULL,
            p_mfgr        VARCHAR(25) NULL,
            p_brand       VARCHAR(50) NULL,
            p_type        VARCHAR(25) NULL,
            p_size        int NULL,
            p_container   VARCHAR(10) NULL,
            p_retailprice decimal(15, 2) NULL,
            p_comment     VARCHAR(23) NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`p_partkey`)
        DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    def label_22666 = "part_" + UUID.randomUUID().toString().replace("-", "0")
    sql """
        LOAD LABEL ${label_22666} (
            DATA INFILE("s3://${s3BucketName}/regression/load/data/part0.parquet")
            INTO TABLE ${tbl_22666}
            FORMAT AS "PARQUET"
            (p_partkey, p_name, p_mfgr),
            DATA INFILE("s3://${s3BucketName}/regression/load/data/part1.parquet")
            INTO TABLE ${tbl_22666}
            FORMAT AS "PARQUET"
            (p_partkey, p_brand, p_type)
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "${getS3AK()}",
            "AWS_SECRET_KEY" = "${getS3SK()}",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        );
    """

    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        def String[][] result = sql """ show load where label="$label_22666" order by createtime desc limit 1; """
        logger.info("Load status: " + result[0])
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED " + label_22666)
            break;
        }
        if (result[0][2].equals("CANCELLED")) {
            assertTrue(false, "load failed: $result")
            break;
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(1 == 2, "load Timeout: $label_22666")
        }
    }

    order_qt_pr22666_1 """ select count(*) from ${tbl_22666} where p_brand is not null limit 10;"""
    order_qt_pr22666_2 """ select count(*) from ${tbl_22666} where p_name is not null limit 10;"""

}

