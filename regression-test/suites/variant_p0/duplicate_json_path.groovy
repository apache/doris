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

suite("duplicate_json_path", "p0") {
    def customBeConfig = [
        variant_enable_duplicate_json_path_check: true
    ]
    setBeConfigTemporary(customBeConfig) {
        sql "DROP TABLE IF EXISTS duplicate_json_path"
        sql """
            CREATE TABLE duplicate_json_path (
                k int,
                v variant
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "group_commit_interval_ms" = "2000",
                "disable_auto_compaction" = "true"
            );
        """

        sql """insert into duplicate_json_path values (1, '{"a":42,"a":{"b":42}}')"""
        sql """insert into duplicate_json_path values (2, '{"a" : 123, "a" : "123"}')"""
        sql """insert into duplicate_json_path values (3, '{"a.b":1,"a":{"b":2}}')"""
        sql """insert into duplicate_json_path values (4, '{"a":{"b":3},"a.b":4}')"""
        sql """insert into duplicate_json_path values (5, '{"a":{"b":5},"a":{"c":6}}')"""
        sql """insert into duplicate_json_path values (6, '{"a":[1],"a":2}')"""
        sql """insert into duplicate_json_path values (7, '{"a":2,"a":[1]}')"""

        streamLoad {
            table "duplicate_json_path"
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'group_commit', 'async_mode'
            unset 'label'
            file 'duplicate_json_path.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(7, json.NumberTotalRows)
                assertEquals(7, json.NumberLoadedRows)
            }
        }

        for (int i = 0; i < 30; i++) {
            def count = sql "select count(*) from duplicate_json_path"
            if (count[0][0] == 14) {
                break
            }
            sleep(1000)
        }
        def totalRows = sql "select count(*) from duplicate_json_path"
        assertEquals(14, totalRows[0][0])

        def expectedResult = [
                [1, "{\"b\":42}", "42", null],
                [2, "123", null, null],
                [3, "{\"b\":1}", "1", null],
                [4, "{\"b\":3}", "3", null],
                [5, "{\"b\":5,\"c\":6}", "5", "6"],
                [6, "[1]", null, null],
                [7, "2", null, null],
                [8, "{\"b\":42}", "42", null],
                [9, "123", null, null],
                [10, "{\"b\":8}", "8", null],
                [11, "{\"b\":10}", "10", null],
                [12, "{\"b\":11,\"c\":12}", "11", "12"],
                [13, "[13]", null, null],
                [14, "14", null, null]
        ]

        def queryResult = {
            sql """
            select k, cast(v['a'] as string), cast(v['a']['b'] as string), cast(v['a']['c'] as string)
            from duplicate_json_path
            order by k
            """
        }
        assertEquals(expectedResult, queryResult())

        trigger_and_wait_compaction("duplicate_json_path", "full")
        assertEquals(expectedResult, queryResult())
    }
}
