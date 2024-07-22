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

suite("test_multi_table_load", "load_p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    
    def tableName = "test_multi_table_load"

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

    def path = "s3://${s3BucketName}/regression/load/data/basic_data.csv"
    def format_str = "CSV"
    def ak = getS3AK()
    def sk = getS3SK()

    def data_desc_prefix = """DATA INFILE("$path")
               INTO TABLE $tableName """

    def data_desc_suffix = """COLUMNS TERMINATED BY "|"
               FORMAT AS $format_str
               (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18) """

    def data_desces = [
        new Tuple("""$data_desc_prefix
               PARTITION(p1)
               $data_desc_suffix """, "FINISHED"),
        new Tuple("""$data_desc_prefix
               $data_desc_suffix,

               $data_desc_prefix
               $data_desc_suffix """, "FINISHED"),
        new Tuple( """$data_desc_prefix
               $data_desc_suffix,

               $data_desc_prefix
               PARTITION(p1)
               $data_desc_suffix """, "CANCELLED"),
        new Tuple( """$data_desc_prefix
               PARTITION(p1)
               $data_desc_suffix,

               $data_desc_prefix
               $data_desc_suffix """, "CANCELLED")
    ]

    for (def tuple : data_desces) {
        def data_desc = tuple.get(0)
        def load_result = tuple.get(1)

        sql """ truncate table ${tableName} """
        def label = UUID.randomUUID().toString().replace("-", "0")
        def sql_str = """
            LOAD LABEL $label (
                $data_desc
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "provider" = "${getS3Provider()}"
            )
            properties(
                "use_new_load_scan_node" = "true",
                "max_filter_ratio" = "1.0"
            )
            """
        try {
            sql """${sql_str}"""
        } catch (Exception e) {
            logger.info("submit load failed", e)
            assertEquals("CANCELLED", load_result)
            assertTrue(e.getMessage().contains("There are overlapping partitions"))
            continue
        }

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED") || result[0][2].equals("CANCELLED")) {
                logger.info("Load result " + result)
                assertEquals(load_result, result[0][2])
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

    // test cancel load
    def tuple = data_desces[0]
    def data_desc = tuple.get(0)
    def load_result = tuple.get(1)

    def label = UUID.randomUUID().toString().replace("-", "0")
    def sql_str = """
        LOAD LABEL $label (
            $data_desc
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
            "use_new_load_scan_node" = "true",
            "max_filter_ratio" = "1.0"
        )
        """
    try {
        sql """${sql_str}"""
        sql """cancel load where label = "$label";"""
    } catch (Exception e) {
        logger.info("xx cancel load failed", e)
        assertFalse(true);
    }

    String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
    assertEquals("CANCELLED", result[0][2])
}
