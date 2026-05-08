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

// Regression test for OPENSOURCE-374: when loading JSON into a STRUCT column, sub-fields must
// be matched by field name. Out-of-order JSON keys in Stream Load used to turn the whole struct
// column into NULL; a missing field is now filled with NULL and an unknown field is ignored
// (consistent with PostgreSQL / Spark / Trino). Struct-to-struct CAST stays positional, which
// also matches those engines.
suite("test_struct_field_align") {
    def tableName = "test_struct_field_align"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id          INT                                  NOT NULL,
            c_struct    STRUCT<f1:INT,f2:FLOAT,f3:STRING>    NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    def doStreamLoad = { fileName ->
        streamLoad {
            table tableName
            set 'format', 'json'
            set 'strip_outer_array', 'true'
            set 'columns', 'id, c_struct'
            file fileName
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(0, json.NumberFilteredRows)
                assertTrue(json.NumberLoadedRows > 0)
            }
        }
    }

    // 1) ordered keys (baseline)
    doStreamLoad "test_struct_field_align_ordered.json"
    // 2) keys whose order differs from the DDL (the core bug) and a row that omits f3
    doStreamLoad "test_struct_field_align_swapped.json"

    sql "sync"

    def rows = sql "SELECT id, c_struct FROM ${tableName} ORDER BY id"
    def actual = [:]
    for (row in rows) {
        actual[row[0] as int] = (row[1] == null ? "NULL" : row[1].toString())
    }

    def expected = [
        1  : '{"f1":10, "f2":3.1400001, "f3":"Emily"}',
        2  : '{"f1":4, "f2":1.5, "f3":null}',
        3  : '{"f1":7, "f2":null, "f3":"Benjamin"}',
        4  : '{"f1":null, "f2":null, "f3":null}',
        5  : 'NULL',
        // swapped JSON keys must align by name instead of producing NULL
        20 : '{"f1":4, "f2":1.5, "f3":null}',
        21 : '{"f1":9, "f2":2.5, "f3":"Tom"}',
        // f3 omitted -> filled with NULL
        22 : '{"f1":1, "f2":8.5, "f3":null}',
        // an unknown field (f4) is ignored, the matched fields still align by name
        23 : '{"f1":7, "f2":3.5, "f3":"Z"}',
        // upper-case JSON keys are matched case-insensitively to the lower-cased schema names
        24 : '{"f1":5, "f2":6.5, "f3":"U"}',
    ]

    assertEquals(expected.size(), actual.size())
    for (e in expected) {
        assertEquals(e.value, actual[e.key], "row id=${e.key} mismatch".toString())
    }
}
