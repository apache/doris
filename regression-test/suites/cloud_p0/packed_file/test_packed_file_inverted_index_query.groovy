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

// Regression test for reading a packed inverted index file after a file cache miss.
//
// When `enable_packed_file` is on, the inverted index file (`{rowset}_{seg}.idx`) of the first
// segment is packed into a shared file and is NOT written as a standalone object. At read time the
// inverted index reader must look the `.idx` up in PackedFileSystem's index map, whose keys are the
// relative segment paths (e.g. "data/{tablet}/{rowset}_{seg}.idx"). Previously the reader derived
// the `.idx` path from `_file_reader->path()`, which the remote file system normalizes to an
// absolute path (e.g. "s3://bucket/prefix/data/..."). That absolute path did not match the relative
// keys, so the lookup missed and the `.idx` was read as a (non-existent) standalone object, failing
// with `CLuceneError ... read past EOF`.
//
// The failure is masked by the local file cache (its key is filename based), so it only shows up on
// a cache miss. This test loads small data so the `.idx` gets packed, clears the file cache, then
// runs inverted-index-backed queries which must succeed.
suite("test_packed_file_inverted_index_query", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def clearFileCacheOnAllBackends = {
        def backends = sql """SHOW BACKENDS"""
        for (be in backends) {
            def ip = be[1]
            def port = be[4]
            def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
            def response = new URL(url).text
            def json = new JsonSlurper().parseText(response)
            if (json.status != "OK") {
                throw new RuntimeException("Clear cache on ${ip}:${port} failed: ${json.status}")
            }
        }
        // Wait for the async part of the clear to settle.
        sleep(5000)
    }

    setBeConfigTemporary([
        "enable_packed_file": "true",
        "small_file_threshold_bytes": "1048576"  // 1MB threshold, small loads below are packed
    ]) {
        def tableName = "test_packed_file_inverted_index_query"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` int NOT NULL,
                `name` string NULL,
                `score` int NULL,
                INDEX idx_name (`name`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for name',
                INDEX idx_score (`score`) USING INVERTED COMMENT 'inverted index for score'
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
        """

        // Load small batches so each rowset's first-segment `.idx` is packed into a shared file.
        def rowsPerBatch = 600
        def batches = 3
        def totalRows = rowsPerBatch * batches
        for (int b = 0; b < batches; b++) {
            def data = new StringBuilder()
            for (int j = 0; j < rowsPerBatch; j++) {
                def id = b * rowsPerBatch + j
                def name = (id % 2 == 0) ? "apple" : "banana"
                def score = id % 100
                data.append("${id},${name},${score}\n")
            }
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'columns', 'id, name, score'
                inputText data.toString()
                time 30000
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(rowsPerBatch, json.NumberLoadedRows as int)
                }
            }
        }

        // Sanity check before clearing cache.
        def totalBefore = sql "select count(*) from ${tableName}"
        assertEquals(totalRows, totalBefore[0][0] as int)

        // Force a file cache miss so the packed `.idx` must be resolved through PackedFileSystem.
        clearFileCacheOnAllBackends()

        // These queries are backed by the inverted indexes and read the packed `.idx`.
        // Before the fix they failed with "CLuceneError ... read past EOF" on cache miss.
        sql "set enable_inverted_index_query = true"

        // Numeric inverted index (BKD), equality predicate.
        def expectedScore5 = (0..<totalRows).count { (it % 100) == 5 }
        def scoreResult = sql "select count(*) from ${tableName} where score = 5"
        assertEquals(expectedScore5, scoreResult[0][0] as int,
                "score = 5 query (inverted index, after cache clear) returned wrong count")

        // String inverted index, full-text match predicate.
        def expectedApple = (0..<totalRows).count { (it % 2) == 0 }
        def matchResult = sql "select count(*) from ${tableName} where name match_any 'apple'"
        assertEquals(expectedApple, matchResult[0][0] as int,
                "name match_any 'apple' query (inverted index, after cache clear) returned wrong count")

        // Clear once more and run again to cover repeated cache-miss reads.
        clearFileCacheOnAllBackends()
        def matchResult2 = sql "select count(*) from ${tableName} where name match_any 'banana'"
        assertEquals(totalRows - expectedApple, matchResult2[0][0] as int,
                "name match_any 'banana' query (inverted index, after cache clear) returned wrong count")

        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
