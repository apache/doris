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

suite("test_s3_read_transient_error_retry", "p0,external,nonConcurrent") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    if (!ak || !sk || !s3_endpoint || !bucket) {
        logger.info("S3 not configured, skip test")
        return
    }

    def outFilePath = "${bucket}/test_s3_transient_error_retry/data_"

    // Clean up before test; keep table after test for debugging
    sql """ DROP TABLE IF EXISTS test_s3_transient_error_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_s3_transient_error_src (
            `id` INT NOT NULL,
            `name` STRING,
            `value` INT
        )
        DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
    """
    sql """ INSERT INTO test_s3_transient_error_src VALUES (1,'a',100),(2,'b',200),(3,'c',300) """

    def outfile_url = ""
    def res = sql """
        SELECT * FROM test_s3_transient_error_src
        INTO OUTFILE "s3://${outFilePath}"
        FORMAT AS CSV
        PROPERTIES (
            "s3.endpoint" = "${s3_endpoint}",
            "s3.region" = "${region}",
            "s3.secret_key" = "${sk}",
            "s3.access_key" = "${ak}",
            "column_separator" = ","
        );
    """
    outfile_url = res[0][3]
    logger.info("Exported to: ${outfile_url}")

    // Test 1: transient error retry succeeds (inject_count=1, first read fails, retry succeeds)
    try {
        GetDebugPoint().enableDebugPointForAllBEs(
            "s3_file_reader.transient_error", ["inject_count": "1"])

        def result = sql """
            SELECT count(*) FROM s3(
                'uri' = '${outfile_url}.csv',
                'format' = 'csv',
                's3.access_key' = '${ak}',
                's3.secret_key' = '${sk}',
                's3.endpoint' = '${s3_endpoint}',
                's3.region' = '${region}',
                'column_separator' = ','
            );
        """
        logger.info("Query with transient error injection succeeded: ${result}")
        assertEquals(3, result[0][0] as int)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("s3_file_reader.transient_error")
    }

    // Test 2: transient error retry exhausted (inject_count=10 > max_transient_retries=3, should fail)
    try {
        GetDebugPoint().enableDebugPointForAllBEs(
            "s3_file_reader.transient_error", ["inject_count": "10"])

        def failed = false
        try {
            sql """
                SELECT count(*) FROM s3(
                    'uri' = '${outfile_url}.csv',
                    'format' = 'csv',
                    's3.access_key' = '${ak}',
                    's3.secret_key' = '${sk}',
                    's3.endpoint' = '${s3_endpoint}',
                    's3.region' = '${region}',
                    'column_separator' = ','
                );
            """
        } catch (Exception e) {
            logger.info("Query failed as expected after retry exhaustion: ${e.getMessage()}")
            assertTrue(e.getMessage().contains("Failed to flush response stream")
                    || e.getMessage().contains("injected"))
            failed = true
        }
        assertTrue(failed, "Query should have failed after transient retry exhaustion")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("s3_file_reader.transient_error")
    }

}
