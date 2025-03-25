
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

suite("test_csv_with_enclose_and_escape", "p0") {

    def tableName = "test_csv_with_enclose_and_escape"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` string NULL,
            `v1` date  NULL,
            `v2` string  NULL,
            `v3` datetime  NULL,
            `v4` string  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    def normalCases = [
            'enclose_normal.csv',
            'enclose_with_escape.csv',
            'enclose_wrong_position.csv',
            'enclose_empty_values.csv'
    ]

    for (i in 0..<normalCases.size()) {
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'trim_double_quotes', 'true'
            set 'enclose', "\""
            set 'escape', '\\'

            file "${normalCases[i]}"
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'trim_double_quotes', 'true'
        set 'enclose', "\""
        set 'escape', '\\'
        set 'max_filter_ratio', '0.5'

        file "enclose_incomplete.csv"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'trim_double_quotes', 'true'
        set 'enclose', "\""
        set 'escape', '\\'

        file "enclose_without_escape.csv"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def json = parseJson(result)
                assertEquals("Success", json.Status)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '@@'
        set 'line_delimiter', '$$$'
        set 'trim_double_quotes', 'true'
        set 'enclose', "\""
        set 'escape', '\\'

        file "enclose_multi_char_delimiter.csv"
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'trim_double_quotes', 'false'
        set 'enclose', "\""
        set 'escape', '\\'

        file "enclose_not_trim_quotes.csv"
    }

    sql "sync"
    qt_select """
        SELECT * FROM ${tableName} ORDER BY k1, k2 
    """
}
