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

import groovy.json.JsonOutput

suite("test_file_type_text_load_errors") {
    // Six-field format input, including binary 00 ff 41. SQL CAST intentionally cannot
    // carry inline, so keep this fixture on the real format-reader/stream-load path.
    def good = [uri: "urn:good", offset: 0, size: 3,
                content_type: "application/octet-stream", checksum: null, inline: "AP9B"]
    def bad = good + [uri: "urn:bad", inline: "!!!!"]
    def empty = good + [uri: "urn:empty", size: 0, inline: ""]
    def remote = good + [uri: "urn:remote", inline: null]
    def values = [good, bad, null, empty,
                  good + [inline: "Zg"], good + [inline: "Zh=="], remote]

    def loadRows = { String target, String format, boolean strict, List rows, int filtered,
                     boolean arrayDocuments = false ->
        def input = rows.collect { JsonOutput.toJson(it) }.join('\n') + '\n'
        if (arrayDocuments) {
            // The first document below has only rejected rows in strict mode. Its empty
            // output must not be counted as an additional source row.
            input = rows.collate(2).collect { JsonOutput.toJson(it) }.join('\n') + '\n'
        }
        streamLoad {
            table target
            set "format", format
            set "strict_mode", strict.toString()
            set "max_filter_ratio", "1"
            set "columns", rows[0].keySet().join(',')
            if (format == "json") {
                set "read_json_by_line", "true"
                set "strip_outer_array", arrayDocuments.toString()
            }
            inputText input
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def response = parseJson(result)
                assertEquals("success", response.Status.toLowerCase(), result)
                assertEquals(rows.size() as long, response.NumberTotalRows as long, result)
                assertEquals(filtered as long, response.NumberFilteredRows as long, result)
                assertEquals((rows.size() - filtered) as long, response.NumberLoadedRows as long, result)
            }
        }
    }

    ["json"].each { format ->
        [false, true].each { strict ->
            [false, true].each { nullable ->
                def tag = "${format}_${strict}_${nullable}"
                def target = "test_file_text_load_${tag}"
                sql "DROP TABLE IF EXISTS ${target}"
                sql """
                    CREATE TABLE ${target} (
                        id INT NOT NULL, before_file STRING, f FILE ${nullable ? 'NULL' : 'NOT NULL'},
                        after_file STRING)
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1")
                """
                def rows = values.withIndex().collect { value, index ->
                    if (format == "json" && index % 2 == 1) {
                        // JSON source order differs from the source slot order. A bad FILE
                        // must also roll back a later slot inserted before the first slot.
                        return [after_file: "after${index + 1}", f: value,
                                before_file: "before${index + 1}", id: index + 1]
                    }
                    return [id: index + 1, before_file: "before${index + 1}", f: value,
                            after_file: "after${index + 1}"]
                }
                // Three invalid Base64 cells; genuine source NULL remains legal only for
                // nullable targets. Filtering must preserve both adjacent scalar columns.
                int filtered = (strict || !nullable ? 3 : 0) + (nullable ? 0 : 1)
                loadRows(target, format, strict, rows, filtered)
                "qt_top_${tag}" """
                    SELECT id, before_file, f IS NULL, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'offset'),
                           ELEMENT_AT(f, 'size'), ELEMENT_AT(f, 'content_type'), ELEMENT_AT(f, 'checksum'),
                           __file_data_size(f), after_file
                    FROM ${target} ORDER BY id
                """
            }

            def target = "test_file_text_load_nested_${format}_${strict}"
            sql "DROP TABLE IF EXISTS ${target}"
            sql """
                CREATE TABLE ${target} (
                    id INT NOT NULL, s STRUCT<f:FILE,n:INT>, a ARRAY<FILE>, m MAP<STRING,FILE>)
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1")
            """
            loadRows(target, format, strict, [
                [id: 1, s: [f: good, n: 7], a: [good, empty], m: [bad: good, good: empty]],
                [id: 2, s: [f: bad, n: 9], a: [bad, good], m: [bad: bad, good: good]],
                [id: 3, s: null, a: null, m: null]
            ], 0)
            "qt_nested_${format}_${strict}" """
                SELECT id, s IS NULL, s, a, m,
                       __file_data_size(element_at(s, 'f')),
                       __file_data_size(a[2]), __file_data_size(m['good'])
                FROM ${target} ORDER BY id
            """

            if (format == "json") {
                def arrayTarget = "test_file_text_load_json_array_${strict}"
                sql "DROP TABLE IF EXISTS ${arrayTarget}"
                sql """
                    CREATE TABLE ${arrayTarget} (id INT NOT NULL, f FILE)
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1")
                """
                loadRows(arrayTarget, "json", strict,
                        [[id: 1, f: bad], [id: 2, f: bad], [id: 3, f: good], [id: 4, f: null]],
                        strict ? 2 : 0, true)
                "qt_array_documents_${strict}" """
                    SELECT id, f, __file_data_size(f) FROM ${arrayTarget} ORDER BY id
                """
            }
        }
    }
}
