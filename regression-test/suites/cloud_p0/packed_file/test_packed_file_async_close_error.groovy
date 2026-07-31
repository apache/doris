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

suite("test_packed_file_async_close_error", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def closeErrorPoint = "S3FileWriter._close_impl.inject_error"
    def afterFinalizeSleepPoint = "SegmentFlusher._flush_segment_writer.after_finalize.sleep"
    def waitDatClosedPoint = "SegmentFileCollection.close.wait_dat_closed"

    sql """ DROP TABLE IF EXISTS test_packed_file_async_close_error """
    sql """
        CREATE TABLE IF NOT EXISTS test_packed_file_async_close_error (
            `k1` int NULL,
            `v1` varchar(32) NULL,
            INDEX idx_v1 (`v1`) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    setBeConfigTemporary([
        "enable_packed_file": "false",
        "enable_vertical_segment_writer": "true",
        "small_file_threshold_bytes": "1048576",
        "packed_file_size_threshold_bytes": "1048576",
        "packed_file_time_threshold_ms": "1"
    ]) {
        try {
            sql """ SET enable_file_cache = false """
            GetDebugPoint().disableDebugPointForAllBEs(closeErrorPoint)
            GetDebugPoint().disableDebugPointForAllBEs(afterFinalizeSleepPoint)
            GetDebugPoint().disableDebugPointForAllBEs(waitDatClosedPoint)
            GetDebugPoint().enableDebugPointForAllBEs(closeErrorPoint)
            GetDebugPoint().enableDebugPointForAllBEs(afterFinalizeSleepPoint)
            GetDebugPoint().enableDebugPointForAllBEs(waitDatClosedPoint)

            streamLoad {
                table "test_packed_file_async_close_error"
                set "column_separator", ","
                inputText "1,a\n2,b\n"
                time 120000

                check { result, exception, startTime, endTime ->
                    def msg = exception == null ? result : exception.getMessage()
                    logger.info("stream load result with injected S3 close error: ${msg}")
                    assertTrue(exception == null, "stream load should succeed before async packed file upload fails: ${msg}")
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                }
            }

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(closeErrorPoint)
            GetDebugPoint().disableDebugPointForAllBEs(afterFinalizeSleepPoint)
            GetDebugPoint().disableDebugPointForAllBEs(waitDatClosedPoint)
        }
    }
}
