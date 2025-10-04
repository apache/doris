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

    // Test 1: Single range expansion {1..3} - should load 3 files
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

    // Test 2: Multiple ranges in one path {1..2}_{1..2} - should load 4 files
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
        qt_test2_data """ SELECT * FROM ${test_table} """
        
    } finally {
        sql """ DROP TABLE IF EXISTS ${test_table} """
    }
}