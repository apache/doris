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

    def streamLoadAndCheck = { String label, String expectedStatus ->
        streamLoad {
            table "test_s3_file_writer_submit_error"
            set "column_separator", ","
            inputText "1,${label}\n2,${label}\n"
            time 120000

            check { result, exception, startTime, endTime ->
                def msg = exception == null ? result : exception.getMessage()
                logger.info("stream load result for ${label}: ${msg}")
                assertTrue(exception == null,
                        "stream load should return json result for ${label}: ${msg}")
                def json = parseJson(result)
                assertEquals(expectedStatus, json.Status.toLowerCase())
            }
        }
    }

    def runWithDebugPoint = { String label, String expectedStatus, String point ->
        GetDebugPoint().enableDebugPointForAllBEs(point)
        try {
            streamLoadAndCheck(label, expectedStatus)
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
        "enable_packed_file": "false"
    ]) {
        try {
            sql """ SET enable_file_cache = false """
            disableDebugPoints()

            runWithDebugPoint("submit_err", "fail", submitUploadBufferErrorPoint)
            runWithDebugPoint("close_submit_err", "success", asyncCloseSubmitErrorPoint)
        } finally {
            disableDebugPoints()
        }
    }
}
