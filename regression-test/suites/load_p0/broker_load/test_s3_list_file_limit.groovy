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

suite("test_s3_list_file_limit", "load_p0") {
    def tableName = "test_s3_list_file_limit_table"
    def bucket = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            a INT,
            b INT
        ) DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // Test 1: TVF with limit=15, should succeed (12 files < 15)
    sql """ SET max_s3_list_objects_count = 15; """
    sql """
        INSERT INTO ${tableName}
        SELECT * FROM S3
        (
            "uri" = "s3://${bucket}/load/tvf_data/data_*.csv",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${region}",
            "format" = "csv",
            "column_separator" = ","
        );
    """
    qt_sql """ SELECT COUNT(*) FROM ${tableName}; """
    
    sql """ TRUNCATE TABLE ${tableName}; """

    // Test 2: Broker Load with limit=15, should succeed (12 files < 15)
    sql """ SET max_s3_list_objects_count = 15; """
    def test2_label = "test_s3_list_load_positive_" + UUID.randomUUID().toString().replace("-", "0")
    sql """
        LOAD LABEL ${test2_label} (
            DATA INFILE("s3://${bucket}/load/tvf_data/data_*.csv")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
            FORMAT AS "CSV"
        )
        WITH S3 (
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${region}",
            "AWS_ACCESS_KEY" = "${ak}",
            "AWS_SECRET_KEY" = "${sk}"
        )
    """
    
    def max_try_milli_secs = 120000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ SHOW LOAD WHERE label="${test2_label}" ORDER BY createtime DESC LIMIT 1; """
        if (result.length > 0) {
            if (result[0][2].equals("FINISHED")) {
                def count = sql """ SELECT COUNT(*) FROM ${tableName}; """
                assertTrue(count[0][0] > 0, "Test 2: broker load should succeed")
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                assertTrue(false, "Test 2 failed: ${reason}")
            }
        }
        Thread.sleep(5000)
        max_try_milli_secs -= 5000
        if (max_try_milli_secs <= 0) {
            assertTrue(false, "Test 2 timeout")
        }
    }
    
    qt_sql """ SELECT COUNT(*) FROM ${tableName}; """
    sql """ TRUNCATE TABLE ${tableName}; """

    // Test 3: TVF with limit=10, should fail (12 files > 10)
    sql """ SET max_s3_list_objects_count = 10; """
    try {
        sql """
            INSERT INTO ${tableName}
            SELECT * FROM S3
            (
                "uri" = "s3://${bucket}/load/tvf_data/data_*.csv",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${region}",
                "format" = "csv",
                "column_separator" = ","
            );
        """
        assertTrue(false, "Test 3: should fail but succeeded")
    } catch (Exception e) {
        def errorMsg = e.getMessage()
        assertTrue(errorMsg.contains("Too many files"), 
                   "Test 3: error should contain 'Too many files', actual: ${errorMsg}")
    }
    
    qt_sql """ SELECT COUNT(*) FROM ${tableName}; """
    
    // Test 4: Broker Load with limit=10, should fail (12 files > 10)
    sql """ SET max_s3_list_objects_count = 10; """
    def test4_label = "test_s3_list_load_negative_" + UUID.randomUUID().toString().replace("-", "0")
    sql """
        LOAD LABEL ${test4_label} (
            DATA INFILE("s3://${bucket}/load/tvf_data/data_*.csv")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
            FORMAT AS "CSV"
        )
        WITH S3 (
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${region}",
            "AWS_ACCESS_KEY" = "${ak}",
            "AWS_SECRET_KEY" = "${sk}"
        )
    """
    
    max_try_milli_secs = 120000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ SHOW LOAD WHERE label="${test4_label}" ORDER BY createtime DESC LIMIT 1; """
        if (result.length > 0) {
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                assertTrue(reason.contains("Too many files"), "Test 4: should fail with 'Too many files' error")
                assertTrue(reason.contains("10"), "Test 4: error should mention limit value")
                break
            }
            if (result[0][2].equals("FINISHED")) {
                assertTrue(false, "Test 4: should fail but succeeded")
            }
        }
        Thread.sleep(5000)
        max_try_milli_secs -= 5000
        if (max_try_milli_secs <= 0) {
            assertTrue(false, "Test 4 timeout")
        }
    }
    
    qt_sql """ SELECT COUNT(*) FROM ${tableName}; """

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
