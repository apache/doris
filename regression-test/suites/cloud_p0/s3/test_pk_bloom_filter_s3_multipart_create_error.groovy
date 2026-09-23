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

import groovy.json.JsonSlurper

// Reproduce: the first CreateMultipartUpload of a segment fails while the primary key bloom
// filter page fills the first S3 multipart buffer (5 MiB).
//
// Debug points:
//   PrimaryKeyBloomFilterIndexWriterImpl::finish.pad_before_bloom_filter
//       pad unreferenced bytes so that the PK bloom filter starts at 5,203,052
//       (s3_write_buffer_size - 39,828), i.e. the same offset as the production case.
//   S3FileWriter._create_multi_upload_request.inject_error_once
//       the first CreateMultipartUpload of the segment fails, the retry succeeds.
//   PrimaryKeyBloomFilterIndexWriterImpl::finish.ignore_add_error      (legacy only)
//       emulate the legacy caller which ignored the Status of bf_writer.add().
//   S3FileWriter.appendv.legacy_data_size                              (legacy only)
//       emulate the legacy appendv which used capacity - size of the pending buffer.
//
// Case 1 (current code): the load must fail with the injected error.
// Case 2 (legacy emulation): the load succeeds, but the uploaded object is 39,828 bytes larger
//        than segments_file_size in rowset meta, so reading the segment reports corruption.
suite("test_pk_bloom_filter_s3_multipart_create_error", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    // 600,000 distinct keys => bloom filter of 1 MiB + 1 byte, larger than the 1 MiB
    // IndexedColumnWriter data page, so the page is written inside bf_writer.add().
    def numRows = 600000L
    def expectedSum = numRows * (numRows - 1) / 2

    def padPoint = "PrimaryKeyBloomFilterIndexWriterImpl::finish.pad_before_bloom_filter"
    def createErrorPoint = "S3FileWriter._create_multi_upload_request.inject_error_once"
    def ignoreAddErrorPoint = "PrimaryKeyBloomFilterIndexWriterImpl::finish.ignore_add_error"
    def legacyDataSizePoint = "S3FileWriter.appendv.legacy_data_size"
    def allPoints = [padPoint, createErrorPoint, ignoreAddErrorPoint, legacyDataSizePoint]

    def disableAllPoints = {
        allPoints.each { GetDebugPoint().disableDebugPointForAllBEs(it) }
    }

    def createTable = { String tableName ->
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
        sql """
            CREATE TABLE ${tableName} (
                `k` BIGINT NOT NULL,
                `v` BIGINT NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            )
        """
        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assertEquals(1, tablets.size())
        return tablets[0].TabletId.toString()
    }

    def enablePoints = { List<String> points, String tabletId ->
        // Segment path: data/{tablet_id}/{rowset_id}_{seg}.dat or data/{shard}/{tablet_id}/...
        def params = ["path_contains": "/${tabletId}/".toString()]
        points.each { GetDebugPoint().enableDebugPointForAllBEs(it, params) }
    }

    def insertSql = { String tableName ->
        return "INSERT INTO ${tableName} SELECT number, number FROM numbers(\"number\" = \"${numRows}\")"
    }

    def clearFileCacheOnAllBackends = {
        def backends = sql """ SHOW BACKENDS """
        for (be in backends) {
            def url = "http://${be[1]}:${be[4]}/api/file_cache?op=clear&sync=true"
            def json = new JsonSlurper().parseText(new URL(url).text)
            if (json.status != "OK") {
                throw new RuntimeException("clear file cache on ${be[1]}:${be[4]} failed: ${json}")
            }
        }
        sleep(5000)
    }

    def logRowsets = { String tableName, String tabletId ->
        def rowsets = sql """
            SELECT ROWSET_ID, NUM_SEGMENTS, ROWSET_NUM_ROWS, DATA_DISK_SIZE
            FROM information_schema.rowsets WHERE TABLET_ID = ${tabletId}
        """
        logger.info("rowsets of ${tableName} (tablet ${tabletId}): ${rowsets}")
        return rowsets
    }

    setBeConfigTemporary([
        "enable_file_cache_adaptive_write": "false",
        "enable_packed_file": "false"
    ]) {
        try {
            sql """ SET disable_file_cache = true """
            disableAllPoints()

            // Case 0: padding alone must not change the result, the padding is unreferenced.
            def normalTable = "test_pk_bf_s3_mpu_normal"
            def normalTablet = createTable(normalTable)
            enablePoints([padPoint], normalTablet)
            try {
                sql insertSql(normalTable)
            } finally {
                disableAllPoints()
            }
            clearFileCacheOnAllBackends()
            def res = sql """ SELECT count(*), sum(k), sum(v) FROM ${normalTable} """
            assertEquals(numRows, res[0][0] as long)
            assertEquals(expectedSum, res[0][1] as long)
            assertEquals(expectedSum, res[0][2] as long)
            // The data should be written in one segment, otherwise tune numRows.
            logRowsets(normalTable, normalTablet)

            // Case 1: current code propagates the CreateMultipartUpload error from
            // bf_writer.add(), the load fails and nothing becomes visible.
            def currentTable = "test_pk_bf_s3_mpu_current"
            def currentTablet = createTable(currentTable)
            enablePoints([padPoint, createErrorPoint], currentTablet)
            try {
                test {
                    sql insertSql(currentTable)
                    exception "inject CreateMultipartUpload failure"
                }
            } finally {
                disableAllPoints()
            }
            res = sql """ SELECT count(*) FROM ${currentTable} """
            assertEquals(0L, res[0][0] as long)

            // Case 2: legacy behavior. The size check after upload did not catch it in the
            // affected version, disable it here (it would also DCHECK in debug builds).
            setBeConfigTemporary(["enable_s3_object_check_after_upload": "false"]) {
                def legacyTable = "test_pk_bf_s3_mpu_legacy"
                def legacyTablet = createTable(legacyTable)
                enablePoints([padPoint, createErrorPoint, ignoreAddErrorPoint, legacyDataSizePoint],
                        legacyTablet)
                try {
                    // The corruption is silent: the load reports success.
                    sql insertSql(legacyTable)
                } finally {
                    disableAllPoints()
                }
                logRowsets(legacyTable, legacyTablet)
                logger.info("segment object of tablet ${legacyTablet} should be 39828 bytes larger " +
                        "than segments_file_size in rowset meta, check BE log for the debug points above")

                // Read from object storage instead of the write-through file cache.
                clearFileCacheOnAllBackends()
                def readError = null
                def readResult = null
                try {
                    readResult = sql """ SELECT count(*), sum(k), sum(v) FROM ${legacyTable} """
                } catch (Throwable t) {
                    readError = t
                }
                logger.info("read legacy table result=${readResult}, error=${readError?.message}")
                if (readError == null) {
                    assertTrue(readResult[0][0] as long != numRows
                            || readResult[0][1] as long != expectedSum
                            || readResult[0][2] as long != expectedSum,
                            "expect corrupted data in legacy case, but got ${readResult}")
                } else {
                    def msg = readError.message.toLowerCase()
                    assertTrue(msg.contains("corrupt") || msg.contains("magic")
                            || msg.contains("checksum") || msg.contains("bad segment")
                            || msg.contains("footer"),
                            "unexpected error: ${readError.message}")
                }
            }
        } finally {
            sql """ SET disable_file_cache = false """
            disableAllPoints()
        }
    }
}
