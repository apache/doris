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

import java.util.Random;

suite("test_http_stream_json", "p0") {

    def db = "regression_test_load_p0_http_stream"
    // 1. test json
    def tableName1 = "test_http_stream_simple_object_json"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
            id int,
            city VARCHAR(10),
            code int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName1} (id, city, code) select id, city, code from http_stream(
                        "format"="json",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true"
                        )
                    """
            time 10000
            file '../stream_load/simple_object_json.json'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(12, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql1 "select id, city, code from ${tableName1} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }

    // 2. test json
    def tableName2 = "test_http_stream_one_array_json"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
            id int,
            city VARCHAR(10),
            code int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName2} (id, city, code) select * from http_stream(
                        "format"="json",
                        "strip_outer_array" = "true",
                        "read_json_by_line" = "false"
                        )
                    """
            time 10000
            file 'one_array_json.json'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(10, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql2 "select id, city, code from ${tableName2} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName2}"
    }

    // 3. test json
    def tableName3 = "test_http_stream_nest_json"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName3} (
            id int,
            city VARCHAR(10),
            code int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName3} (id, city, code) select cast(id as INT) as id, city, cast(code as INT) as code from http_stream(
                        "format"="json",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true",
                         "json_root" = "\$.item"
                        )
                    """
            time 10000
            file '../stream_load/nest_json.json'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(5, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql3 "select id, city, code from ${tableName3} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName3}"
    }

    // 4. test json
    def tableName4 = "test_http_stream_simple_object_json"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName4} (
            id int,
            city VARCHAR(10),
            code int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName4} (id, code) select cast(id as INT) as id, cast(code as INT) as code from http_stream(
                        "format"="json",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true",
                        "jsonpaths" = "[\\"\$.id\\", \\"\$.code\\"]"
                        )
                    """
            time 10000
            file '../stream_load/simple_object_json.json'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(12, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql4 "select id, code from ${tableName4} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName4}"
    }

    // 5.test num as string
    def tableName5 = "test_http_stream_num_as_string"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName5} (
        `k1` int(11) NULL,
        `k2` float NULL,
        `k3` double NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName5} (k1,k2,k3) select k1,k2,k3 from http_stream(
                        "format"="json",
                        "strip_outer_array" = "true",
                        "num_as_string" = "true"
                        )
                    """
            time 10000
            file '../stream_load/num_as_string.json'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(3, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql_num_as_string "select * from ${tableName5} order by k1"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName5}"
    }
}

