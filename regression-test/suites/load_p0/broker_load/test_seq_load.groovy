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

    def tableName = "uniq_tbl_basic_seq"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName}
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
            kd15 CHAR(300)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
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
            "function_column.sequence_col" = "k12",
            "replication_num" = "1"
        );
    """

    def label = UUID.randomUUID().toString().replace("-", "0")
    def path = "s3://doris-build-1308700295/regression/load/data/basic_data.csv"
    def format_str = "CSV"
    def ak = getS3AK()
    def sk = getS3SK()
    def seq_column = "K12"

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