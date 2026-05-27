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

suite("test_variant_stream_load_key_error_url", "variant_type,p0") {
    sql "DROP TABLE IF EXISTS test_variant_stream_load_key_error_url"
    sql """
        CREATE TABLE test_variant_stream_load_key_error_url (
            k INT,
            v VARIANT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    String longKey = "a" * 256
    File dataFile = new File("/tmp/test_variant_stream_load_key_error_url.json")
    dataFile.withWriter { writer ->
        writer.writeLine('{"k":1,"v":{"short_key":1}}')
        writer.writeLine("""{"k":2,"v":{"${longKey}":2}}""")
    }

    streamLoad {
        table "test_variant_stream_load_key_error_url"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        set 'max_filter_ratio', '0.9'
        file dataFile.absolutePath
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(1, json.NumberLoadedRows)
            assertEquals(1, json.NumberFilteredRows)
            assertTrue(json.ErrorURL != null && json.ErrorURL != "N/A")

            def (code, out, err) = curl("GET", json.ErrorURL)
            assertEquals(0, code)
            assertTrue(out.contains("Failed to parse variant column `v`"))
            assertTrue(out.contains("Key length exceeds maximum allowed size of 255 bytes"))
            assertTrue(out.contains("key length: 256"))
            assertTrue(out.contains('"k":2'))
        }
    }
}
