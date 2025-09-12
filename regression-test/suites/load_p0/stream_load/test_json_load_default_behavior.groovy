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

suite("test_json_load_default_behavior", "p0") {

    // case1 test streamload with default config
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table1 (
                id INT DEFAULT '10',
                city VARCHAR(32) DEFAULT '',
                code BIGINT SUM DEFAULT '0'
            )
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """

        streamLoad {
            table "test_table1"
            set 'label', "behavior_check" + UUID.randomUUID().toString()
            set 'format', 'json'
            set 'columns', 'Id, cIty, CodE'
            file 'case_sensitive_json.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 2)
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table1")
    }

    // case2 test streamload with strip_outer_array config
    try {        
        sql """
            CREATE TABLE IF NOT EXISTS test_table2 (
                id INT,
                city VARCHAR(32),
                code INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        streamLoad {
            table "test_table2"
            set 'format', 'json'
            set 'strip_outer_array', 'true'
            file 'simple_json.json'

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
        try_sql("DROP TABLE IF EXISTS test_table2")
    }

    // case3 test tvf s3 load with default config
    // we just need to test SELECT * FROM S3
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()

    // default is read_json_by_line
    def res1 = sql """
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${s3BucketName}/read_by_line.json",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "${s3Endpoint}",
                        "s3.region" = "${s3Region}",
                        "format" = "json"
                    );
                    """
    log.info("select frm s3 result: ${res1}".toString())
    assertTrue(res1[0].size() == 3)

    // [DATA_QUALITY_ERROR]JSON data is array-object, `strip_outer_array` must be TRUE
    test {
        sql """
            SELECT * FROM S3
            (
                "uri" = "s3://${s3BucketName}/outer_array.json",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "json"
            );
            """
        exception "JSON data is array-object, `strip_outer_array` must be TRUE"
    }

    // set strip_outer_array = true
    def res2 = sql """
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${s3BucketName}/outer_array.json",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "${s3Endpoint}",
                        "s3.region" = "${s3Region}",
                        "format" = "json"
                    );
                    """
    log.info("select frm s3 result: ${res2}".toString())
    assertTrue(res2[0].size() == 3)
}
