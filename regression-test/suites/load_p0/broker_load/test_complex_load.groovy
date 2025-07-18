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

suite("test_complex_load", "load_p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def path = "s3://${s3BucketName}/regression/load/data/basic_data.csv"
    def format_str = "CSV"
    def ak = getS3AK()
    def sk = getS3SK()

    def tableName = "test_complex_load"

    sql """ DROP TABLE IF EXISTS test_complex_load """
    sql """ DROP TABLE IF EXISTS test_complex_load_uniq"""

    sql """
        CREATE TABLE test_complex_load
        (
            k00 INT             NOT NULL,
            k01 DATE            NOT NULL,
            k02 BOOLEAN         NULL,
            k03 TINYINT         NULL,
            k04 SMALLINT        NULL,
            k05 INT             NULL,
            k06 BIGINT          NULL,
            k07 LARGEINT        NULL,
            k08 FLOAT           NULL,
            k09 DOUBLE          NULL,
            k10 DECIMAL(9,1)         NULL,
            k11 DECIMALV3(9,1)       NULL,
            k12 DATETIME        NULL,
            k13 DATEV2          NULL,
            k14 DATETIMEV2      NULL,
            k15 CHAR            NULL,
            k16 VARCHAR         NULL,
            k17 STRING          NULL,
            k18 JSON            NULL,
            kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
            kd02 TINYINT         NOT NULL DEFAULT "1",
            kd03 SMALLINT        NOT NULL DEFAULT "2",
            kd04 INT             NOT NULL DEFAULT "3",
            kd05 BIGINT          NOT NULL DEFAULT "4",
            kd06 LARGEINT        NOT NULL DEFAULT "5",
            kd07 FLOAT           NOT NULL DEFAULT "6.0",
            kd08 DOUBLE          NOT NULL DEFAULT "7.0",
            kd09 DECIMAL         NOT NULL DEFAULT "888888888",
            kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
            kd11 DATE            NOT NULL DEFAULT "2023-08-24",
            kd12 DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
            kd14 DATETIMEV2      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd18 JSON            NULL,
        
            INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
            INDEX idx_bitmap_k110 (`kd01`) USING BITMAP,
            INDEX idx_bitmap_k113 (`k13`) USING BITMAP,
            INDEX idx_bitmap_k114 (`k14`) USING BITMAP,
            INDEX idx_bitmap_k117 (`k17`) USING BITMAP
        )
        DUPLICATE KEY(k00,k01)
        PARTITION BY RANGE(k01)
        (
            PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
            PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
            PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
        )
        DISTRIBUTED BY HASH(k00) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE test_complex_load_uniq
        (
            k00 INT             NOT NULL,
            k01 DATE            NOT NULL,
            k02 BOOLEAN         NULL,
            k03 TINYINT         NULL,
            k04 SMALLINT        NULL,
            k05 INT             NULL,
            k06 BIGINT          NULL,
            k07 LARGEINT        NULL,
            k08 FLOAT           NULL,
            k09 DOUBLE          NULL,
            k10 DECIMAL(9,1)         NULL,
            k11 DECIMALV3(9,1)       NULL,
            k12 DATETIME        NULL,
            k13 DATEV2          NULL,
            k14 DATETIMEV2      NULL,
            k15 CHAR            NULL,
            k16 VARCHAR         NULL,
            k17 STRING          NULL,
            k18 JSON            NULL,
            kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
            kd02 TINYINT         NOT NULL DEFAULT "1",
            kd03 SMALLINT        NOT NULL DEFAULT "2",
            kd04 INT             NOT NULL DEFAULT "3",
            kd05 BIGINT          NOT NULL DEFAULT "4",
            kd06 LARGEINT        NOT NULL DEFAULT "5",
            kd07 FLOAT           NOT NULL DEFAULT "6.0",
            kd08 DOUBLE          NOT NULL DEFAULT "7.0",
            kd09 DECIMAL         NOT NULL DEFAULT "888888888",
            kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
            kd11 DATE            NOT NULL DEFAULT "2023-08-24",
            kd12 DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
            kd14 DATETIMEV2      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd18 JSON            NULL,
            INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
            INDEX idx_bitmap_k110 (`kd01`) USING BITMAP,
            INDEX idx_bitmap_k113 (`k13`) USING BITMAP,
            INDEX idx_bitmap_k114 (`k14`) USING BITMAP,
            INDEX idx_bitmap_k117 (`k17`) USING BITMAP
        )
        UNIQUE KEY(k00,k01)
        PARTITION BY RANGE(k01)
        (
            PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
            PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
            PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
        )
        DISTRIBUTED BY HASH(k00) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // load with partition
    def label = UUID.randomUUID().toString().replace("-", "0")
    sql """
        LOAD LABEL ${label} (
            DATA INFILE("${path}")
            INTO TABLE test_complex_load
            PARTITION(p1)
            COLUMNS TERMINATED BY "|"
            FORMAT AS CSV
            (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
        "max_filter_ratio" = "1.0"
    );
    """
    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED") || result[0][2].equals("CANCELLED")) {
            logger.info("Load result " + result)
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(1 == 2, "load Timeout: $label")
        }
    }
    def res1 = sql """select * from test_complex_load"""
    assertEquals(3, res1.size())

    // load with delete
    def label2 = UUID.randomUUID().toString().replace("-", "0")
    sql "truncate table test_complex_load_uniq"
    sql """
        LOAD LABEL ${label2} (
            WITH MERGE
            DATA INFILE("${path}")
            INTO TABLE test_complex_load_uniq
            PARTITION(p1)
            COLUMNS TERMINATED BY "|"
            FORMAT AS CSV
            (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
            DELETE ON k01 = '2023-08-08'
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
        "max_filter_ratio" = "1.0"
    );
    """
    def max_try_milli_secs1 = 600000
    while (max_try_milli_secs1 > 0) {
        String[][] result = sql """ show load where label="$label2" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED") || result[0][2].equals("CANCELLED")) {
            logger.info("Load result " + result)
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs1 -= 1000
        if(max_try_milli_secs1 <= 0) {
            assertTrue(1 == 2, "load Timeout: $label2")
        }
    }
    def res2 = sql """select * from test_complex_load_uniq"""
    assertEquals(2, res2.size())

    // load with colum mapping
    def label3 = UUID.randomUUID().toString().replace("-", "0")
    sql "truncate table test_complex_load"
    sql """
        LOAD LABEL ${label3} (
            DATA INFILE("${path}")
            INTO TABLE test_complex_load
            PARTITION(p1)
            COLUMNS TERMINATED BY "|"
            FORMAT AS CSV
            (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
            SET (k00 = k00 + 1)
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
        "max_filter_ratio" = "1.0"
    );
    """
    def max_try_milli_secs2 = 600000
    while (max_try_milli_secs2 > 0) {
        String[][] result = sql """ show load where label="$label3" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED") || result[0][2].equals("CANCELLED")) {
            logger.info("Load result " + result)
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs2 -= 1000
        if(max_try_milli_secs2 <= 0) {
            assertTrue(1 == 2, "load Timeout: $label3")
        }
    }
    def res3 = sql """select * from test_complex_load order by k01 desc"""
    assertEquals(3, res3.size())
    assertEquals(66, res3.get(0).get(0))

    // load with pre filter
    def label4 = UUID.randomUUID().toString().replace("-", "0")
    sql "truncate table test_complex_load"
    sql """
        LOAD LABEL ${label4} (
            DATA INFILE("${path}")
            INTO TABLE test_complex_load
            PARTITION(p1)
            COLUMNS TERMINATED BY "|"
            FORMAT AS CSV
            (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
            SET (k03 = k03 - 30)
            PRECEDING FILTER k03 > 90
            WHERE k03 <= k00
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
        "max_filter_ratio" = "1.0"
    );
    """
    def max_try_milli_secs3 = 600000
    while (max_try_milli_secs3 > 0) {
        String[][] result = sql """ show load where label="$label4" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED") || result[0][2].equals("CANCELLED")) {
            logger.info("Load result " + result)
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs3 -= 1000
        if(max_try_milli_secs3 <= 0) {
            assertTrue(1 == 2, "load Timeout: $label4")
        }
    }
    def res4 = sql """select * from test_complex_load order by k01 desc"""
    assertEquals(1, res4.size())
    assertEquals(65, res4.get(0).get(0))
}
