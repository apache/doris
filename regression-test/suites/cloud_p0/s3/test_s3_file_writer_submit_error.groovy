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

suite("test_s3_file_writer_submit_error", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def submitUploadBufferErrorPoint = "S3FileWriter.submit_upload_buffer.inject_error"
    def asyncCloseSubmitErrorPoint = "S3FileWriter.close.submit_async_close.inject_error"
    def debugPoints = [submitUploadBufferErrorPoint, asyncCloseSubmitErrorPoint]

    def disableDebugPoints = {
        debugPoints.each { point ->
            GetDebugPoint().disableDebugPointForAllBEs(point)
        }
    }

    def insertAndCheck = { String label, boolean expectSuccess ->
        def insertSql = "INSERT INTO test_s3_file_writer_submit_error " +
                "VALUES (1, '${label}'), (2, '${label}')"
        if (expectSuccess) {
            sql insertSql
        } else {
            test {
                sql insertSql
                exception "S3FileWriter.submit_upload_buffer.inject_error"
            }
        }
    }

    def runWithDebugPoint = { String label, boolean expectSuccess, String point ->
        GetDebugPoint().enableDebugPointForAllBEs(point)
        try {
            insertAndCheck(label, expectSuccess)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(point)
        }
    }

    sql """ DROP TABLE IF EXISTS test_s3_file_writer_submit_error """
    sql """
        CREATE TABLE IF NOT EXISTS test_s3_file_writer_submit_error (
            `k1` int NULL,
            `v1` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    setBeConfigTemporary([
        "enable_file_cache_adaptive_write": "false",
        "enable_packed_file": "true",
        "small_file_threshold_bytes": "1"
    ]) {
        try {
            sql """ SET disable_file_cache = true """
            disableDebugPoints()

            runWithDebugPoint("submit_err", false, submitUploadBufferErrorPoint)
            runWithDebugPoint("close_submit_err", true, asyncCloseSubmitErrorPoint)
        } finally {
            sql """ SET disable_file_cache = false """
            disableDebugPoints()
        }
    }
}
