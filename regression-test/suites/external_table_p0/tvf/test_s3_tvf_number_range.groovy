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

suite("test_s3_tvf_number_range", "p0,external") {

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String test_table = "test_s3_tvf_number_range_table"

    sql """ DROP TABLE IF EXISTS ${test_table} """
    
    sql """
        CREATE TABLE ${test_table} (
            a INT,
            b INT
        )
        DUPLICATE KEY(a) 
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    // Test 1: Single range expansion {1..3} - should load {1,2,3}
    try {
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{1..3}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test1_data """ SELECT * FROM ${test_table} """
        
    } finally {
    }

    // Test 2: Single range expansion {3..1} - should load {1,2,3}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{3..1}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test2_data """ SELECT * FROM ${test_table} """
        
    } finally {
    }

    // Test 3: Single range expansion {2..2} - should load {2}
    try {
        sql """ TRUNCATE TABLE ${test_table} """

        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{2..2}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test3_data """ SELECT * FROM ${test_table} """
        
    } finally {
    }

    // Test 4: Single range expansion {-1..1} - should load 0 files
    try {
        sql """ TRUNCATE TABLE ${test_table} """

        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{-1..1}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test4_data """ SELECT * FROM ${test_table} """
        
    } finally {
    }

    // Test 5: Multiple ranges in one path {1..2}_{1..2} - should load {11,12,21,22}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{1..2}_{1..2}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test5_data """ SELECT * FROM ${test_table} """
        
    } finally {
    }

    // Test 6: Multiple ranges in one path build a nums {0..9}{0..9} - should load all files in 00~99
    // the test cover the case : load the files only existing - {11,12,21,22}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{0..9}{0..9}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test6_data """ SELECT * FROM ${test_table} ORDER BY a, b """
        
    } finally {
    }

    // Test 7：Multiple ranges in one path with single num {1..2,3,1..3} - should load {1,2,3}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{1..2,3,1..3}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test7_data """ SELECT * FROM ${test_table} ORDER BY a, b """
        
    } finally {
    }

    // Test 8: Multiple ranges in one path with single num {3..1,2,1..2} - shoud load {1,2,3}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{3..1,2,1..2}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test8_data """ SELECT * FROM ${test_table} ORDER BY a, b """
        
    } finally {
    }

    ////////////////////////////////////
    // Test with invalid character
    ///////////////////////////////////
    
    // Test 9: has negative number {-1..2,1..3} - should load {1,2,3}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{-1..2,1..3}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test9_data """ SELECT * FROM ${test_table} ORDER BY a, b """
        
    } finally {
    }

    // Test 10: has char {Refrain,1..3} - should load {1,2,3}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        sql """ INSERT INTO ${test_table}
                SELECT a, b FROM S3
                (
                    "uri" = "s3://${bucket}/load/tvf_data/data_{Refrain,1..3}.csv",
                    "format" = "csv",
                    "column_separator" = ",",
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "csv_schema" = "a:int;b:int"
                );
            """
        qt_test10_data """ SELECT * FROM ${test_table} ORDER BY a, b """
        
    } finally {
    }

    // Test 11: bcause BrokerLoad uses the same code path, so we just test it easily
    // {3..1,2,1..2} - shoud load {1,2,3}
    try {
        sql """ TRUNCATE TABLE ${test_table} """
        
        def label = "test_broker_load_number_range_" + System.currentTimeMillis()
        
        sql """
            LOAD LABEL ${label} (
                DATA INFILE("s3://${bucket}/load/tvf_data/data_{3..1,2,1..2}.csv")
                INTO TABLE ${test_table}
                COLUMNS TERMINATED BY ","
                FORMAT AS "csv"
                (a, b)
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}"
            );
        """
        
        // Wait for load to complete
        def max_try_time = 60
        def success = false
        while (max_try_time > 0) {
            def result = sql """ SHOW LOAD WHERE LABEL = '${label}' ORDER BY CreateTime DESC LIMIT 1; """
            if (result.size() > 0) {
                def state = result[0][2]  // State column
                if (state == "FINISHED") {
                    success = true
                    break
                } else if (state == "CANCELLED") {
                    logger.error("Load job ${label} was cancelled: ${result[0]}")
                    break
                }
            }
            Thread.sleep(1000)
            max_try_time--
        }
        
        if (!success) {
            def result = sql """ SHOW LOAD WHERE LABEL = '${label}' ORDER BY CreateTime DESC LIMIT 1; """
            logger.error("Load job ${label} failed or timeout. Status: ${result}")
        }
        
        qt_test11_data """ SELECT * FROM ${test_table} ORDER BY a, b """
        
    } finally {
    }

    sql """ DROP TABLE IF EXISTS ${test_table} """
}