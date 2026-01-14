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


suite("test_tvf_lz4_compress") {
    def table1 = "test_tvf_lz4_compress_streamload"
    def table2 = "test_tvf_lz4_compress_s3load"
    def table3 = "test_tvf_lz4_compress_tvfload"

    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()

    // stream load test 'lz4' and 'lz4frame'
    /* test_compress.csv.lz4
        1,2
        3,4
        5,6
        7,8
        9,10
        11,12
        13,14
        15,16
        17,18
        19,20
    */
    try {
        sql """
            CREATE TABLE IF NOT EXISTS ${table1} (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """

        // stream load use 'lz4'
        streamLoad {
            table "${table1}"
            set 'format', 'csv'
            set 'column_separator', ','
            set 'columns', 'a,b'
            set 'compress_type', 'lz4'
            file 'test_compress.csv.lz4'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 10)
            }
        }
        sql """ truncate table ${table1}; """
        // stream load use 'lz4frame'
        streamLoad {
            table "${table1}"
            set 'format', 'csv'
            set 'column_separator', ','
            set 'columns', 'a,b'
            set 'compress_type', 'lz4frame'
            file 'test_compress.csv.lz4'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 10)
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table1}")
    }

    // with S3 load test
    try {
        sql """
            CREATE TABLE IF NOT EXISTS ${table2} (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """

        // S3 load use 'lz4'
        def label1 = "test_s3_load_lz4_" + System.currentTimeMillis()
        sql """
            LOAD LABEL ${label1} (
                DATA INFILE("s3://${s3BucketName}/load/tvf_compress.csv.lz4")
                INTO TABLE ${table2}
                COLUMNS TERMINATED BY ","
                FORMAT AS "csv"
                (a, b)
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${ak}",
                "AWS_SECRET_KEY" = "${sk}",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "compress_type" = "lz4"
            )
            """

        def max_try_milli_secs = 60000
        while (max_try_milli_secs > 0) {
            def load = sql """ show load where label = "${label1}" """
            log.info("load info : ${load}")
            if (load.size() > 0 && load[0][2] == "FINISHED") {
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if (max_try_milli_secs <= 0) {
                assertTrue(false, "S3 load timeout: ${label1}")
            }
        }

        sql """ truncate table ${table2}; """

        // S3 load use 'lz4frame'
        def label2 = "test_s3_load_lz4frame_" + System.currentTimeMillis()
        sql """
            LOAD LABEL ${label2} (
                DATA INFILE("s3://${s3BucketName}/load/tvf_compress.csv.lz4")
                INTO TABLE ${table2}
                COLUMNS TERMINATED BY ","
                FORMAT AS "csv"
                (a, b)
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${ak}",
                "AWS_SECRET_KEY" = "${sk}",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "compress_type" = "lz4frame"
            )
            """

        max_try_milli_secs = 60000
        while (max_try_milli_secs > 0) {
            def load = sql """ show load where label = "${label2}" """
            log.info("load info : ${load}")
            if (load.size() > 0 && load[0][2] == "FINISHED") {
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if (max_try_milli_secs <= 0) {
                assertTrue(false, "S3 load timeout: ${label2}")
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table2}")
    }

    // tvf s3 load test
    try {
        sql """
            CREATE TABLE IF NOT EXISTS ${table3} (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """

        // TVF S3 load use 'lz4'
        sql """
            INSERT INTO ${table3} 
            SELECT CAST(split_part(c1, ',', 1) AS INT) AS a, CAST(split_part(c1, ',', 2) AS INT) AS b FROM S3 (
                "uri" = "s3://${s3BucketName}/load/tvf_compress.csv.lz4",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "csv",
                "compress_type" = "lz4"
            )
            """

        qt_tvf_lz4 """ SELECT count(*) FROM ${table3}; """
        qt_tvf_lz4_data """ SELECT * FROM ${table3} ORDER BY a; """

        sql """ truncate table ${table3}; """

        // TVF S3 load use 'lz4frame'
        sql """
            INSERT INTO ${table3} 
            SELECT CAST(split_part(c1, ',', 1) AS INT) AS a, CAST(split_part(c1, ',', 2) AS INT) AS b FROM S3 (
                "uri" = "s3://${s3BucketName}/load/tvf_compress.csv.lz4",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "csv",
                "compress_type" = "lz4frame"
            )
            """

        qt_tvf_lz4frame """ SELECT count(*) FROM ${table3}; """
        qt_tvf_lz4frame_data """ SELECT * FROM ${table3} ORDER BY a; """

    } finally {
        try_sql("DROP TABLE IF EXISTS ${table3}")
    }

}