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

// Crash regression: value/null-map semantics are checked by ParquetColumnConvertTest.
// Materialize the mixed-schema TVF column here rather than using metadata-only COUNT(*).
suite("test_parquet_invalid_string_cast", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    if (!ak || !sk) {
        logger.info("S3 not configured, skip")
        return
    }
    String prefix = "s3://${getS3BucketName()}/regression/parquet_invalid_string_cast/${UUID.randomUUID()}"
    String storageProperties = """
        "s3.endpoint" = "${getS3Endpoint()}",
        "s3.region" = "${getS3Region()}",
        "s3.access_key" = "${ak}",
        "s3.secret_key" = "${sk}"
    """
    sql """SELECT CAST(1 AS BIGINT) AS id, CAST('seed' AS STRING) AS name
           INTO OUTFILE "${prefix}/00_seed/part_" FORMAT AS parquet
           PROPERTIES (${storageProperties})"""
    sql """SELECT CAST(CASE number % 4
                          WHEN 0 THEN 'xx'
                          WHEN 1 THEN '5'
                          WHEN 2 THEN NULL
                          ELSE '9' END AS STRING) AS id,
                  CAST(number AS STRING) AS name
           FROM numbers("number" = "4096")
           INTO OUTFILE "${prefix}/01_strings/part_" FORMAT AS parquet
           PROPERTIES (${storageProperties})"""

    def previousSettings = sql "SELECT @@enable_file_scanner_v2, @@batch_size"
    try {
        // This converter belongs to the original Parquet reader; force that path and
        // read enough rows to exercise multiple batches, including invalid casts and NULLs.
        sql "SET enable_file_scanner_v2=false"
        sql "SET batch_size=1024"
        // S3 glob expansion lists the two disjoint prefixes in sorted order, so schema
        // inference sees the BIGINT seed before it scans the STRING file.
        sql """SELECT id, name FROM S3(
                   "uri" = "${prefix}/{00_seed,01_strings}/*.parquet",
                   "format" = "parquet",
                   ${storageProperties}) ORDER BY name"""
    } finally {
        sql "SET enable_file_scanner_v2=${previousSettings[0][0]}"
        sql "SET batch_size=${previousSettings[0][1]}"
    }
}
