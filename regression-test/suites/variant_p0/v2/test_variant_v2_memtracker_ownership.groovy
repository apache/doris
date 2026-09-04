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

suite("test_variant_v2_memtracker_ownership", "nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        assertTrue(getFeConfig("enable_variant_v2").toBoolean())

        setBeConfigTemporary([
                crash_in_memory_tracker_inaccurate: true,
                write_buffer_size: 10240
        ]) {
            sql "DROP TABLE IF EXISTS test_variant_v2_memtracker_ownership"
            sql """
                CREATE TABLE test_variant_v2_memtracker_ownership (
                    k BIGINT,
                    v VARIANT
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "deprecated_variant_enable_flatten_nested" = "false"
                )
            """

            File dataFile = File.createTempFile("variant_v2_memtracker_ownership", ".json")
            dataFile.deleteOnExit()
            try {
                dataFile.withWriter("UTF-8") { writer ->
                    for (int i = 1; i <= 2000; i++) {
                        writer.writeLine("""{"k":${i},"v":{"id":${i},"name":"row-${i}","nested":{"flag":true}}}""")
                    }
                }

                streamLoad {
                    table "test_variant_v2_memtracker_ownership"
                    set "format", "json"
                    set "read_json_by_line", "true"
                    file dataFile.absolutePath
                    time 60000

                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        def json = parseJson(result)
                        assertEquals("success", json.Status.toLowerCase())
                        assertEquals(2000, json.NumberTotalRows)
                        assertEquals(2000, json.NumberLoadedRows)
                    }
                }
            } finally {
                dataFile.delete()
            }

            qt_loaded_rows """
                SELECT count(*), sum(k), min(cast(v['id'] AS BIGINT)), max(cast(v['id'] AS BIGINT))
                FROM test_variant_v2_memtracker_ownership
            """
        }
    }
}
