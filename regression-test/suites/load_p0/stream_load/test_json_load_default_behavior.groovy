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

suite("test_json_load_default_behavior", "p0") {
    // case1 test read_json_by_line = true (true, _) - read json by line like case2
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table1 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        streamLoad {
            table "test_table1"
            set 'format', 'json'
            set 'columns', 'a,b'
            set 'read_json_by_line', 'true'
            file 'data_by_line.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 4)  // 成功读入4行数据
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table1")
    }
    // case2: test default behavior (_ _) - check default behavior is read json by line
    /*
    {"a": 1, "b": 11}
    {"a": 2, "b": 12}
    {"a": 3, "b": 13}
    {"a": 4, "b": 14}
    */
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table2 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        streamLoad {
            table "test_table2"
            set 'format', 'json'
            set 'columns', 'a,b'
            file 'data_by_line.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 4)  // 成功读入4行数据
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table2")
    }
    // case3: test strip_outer_array=true (_ true)
    /*
    [{"a": 1, "b": 11},{"a": 2, "b": 12},{"a": 3, "b": 13},{"a": 4, "b": 14}]
    */
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table3 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        streamLoad {
            table "test_table3"
            set 'format', 'json'
            set 'columns', 'a,b'
            set 'strip_outer_array', 'true'
            file 'data_by_array_oneLine.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 4)
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table3")
    }
    // case4: test strip_outer_array=true (_ true)
    /*
    [
        {"a": 1, "b": 11},
        {"a": 2, "b": 12},
        {"a": 3, "b": 13},
        {"a": 4, "b": 14}
    ]
    */
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table4 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        streamLoad {
            table "test_table4"
            set 'format', 'json'
            set 'columns', 'a,b'
            set 'strip_outer_array', 'true'
            file 'data_by_array_MultiLine.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 4)
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table4")
    }

    // case5: test read_json_by_line=true and strip_outer_array=true (true true)
    /*
    [{"a": 1, "b": 11},{"a": 2, "b": 12}]
    [{"a": 3, "b": 13},{"a": 4, "b": 14}]
    */
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table5 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        streamLoad {
            table "test_table5"
            set 'format', 'json'
            set 'columns', 'a,b'
            set 'read_json_by_line', 'true'
            set 'strip_outer_array', 'true'
            file 'data_by_multiArray.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 4)  // 两行，每行2个对象，共4条数据
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table5")
    }

    // ============================== S3 tvf =========================================
    // ============== we just need to test SELECT * FROM S3 ==========================
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()

    // case6 test default behavior (_ _)
    def res1 = sql """
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${s3BucketName}/load/data_by_line.json",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "${s3Endpoint}",
                        "s3.region" = "${s3Region}",
                        "format" = "json"
                    );
                    """
    log.info("select frm s3 result: ${res1}".toString())
    assertTrue(res1.size() == 4)  // 检查结果集有4行数据

    // case7 test error
    // [DATA_QUALITY_ERROR]JSON data is array-object, `strip_outer_array` must be TRUE
    test {
        sql """
            SELECT * FROM S3
            (
                "uri" = "s3://${s3BucketName}/load/data_by_array_oneLine.json",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "json"
            );
            """
        exception "JSON data is array-object, `strip_outer_array` must be TRUE"
    }

    // case8 test strip_outer_array=true (_ true)
    def res2 = sql """
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${s3BucketName}/load/data_by_array_oneLine.json",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "${s3Endpoint}",
                        "s3.region" = "${s3Region}",
                        "format" = "json",
                        "strip_outer_array" = "true"
                    );
                    """
    log.info("select frm s3 result: ${res2}".toString())
    assertTrue(res2.size() == 4)  // 检查结果集有4行数据

    // case9 test strip_outer_array=true (_ true)
    def res3 = sql """
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${s3BucketName}/load/data_by_array_MultiLine.json",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "${s3Endpoint}",
                        "s3.region" = "${s3Region}",
                        "format" = "json",
                        "strip_outer_array" = "true"
                    );
                    """
    log.info("select frm s3 result: ${res3}".toString())
    assertTrue(res3.size() == 4)  // 检查结果集有4行数据

    // ============================== S3 load =========================================
    
    // case10: S3 load test default behavior (_ _) - check default behavior is read json by line
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table_s3_1 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        def label10 = "s3_load_default_" + UUID.randomUUID().toString().replaceAll("-", "")
        sql """
            LOAD LABEL ${label10} (
                DATA INFILE("s3://${s3BucketName}/load/data_by_line.json")
                INTO TABLE test_table_s3_1
                FORMAT AS "json"
                (a, b)
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            );
            """
        
        // Wait for load to complete
        def max_try_time = 60000
        while (max_try_time > 0) {
            def result = sql "SHOW LOAD WHERE label = '${label10}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                def count = sql "SELECT COUNT(*) FROM test_table_s3_1"
                assertEquals(4, count[0][0])
                break
            } else if (result[0][2] == "CANCELLED") {
                throw new Exception("Load job cancelled: " + result[0][7])
            }
            Thread.sleep(1000)
            max_try_time -= 1000
            if (max_try_time <= 0) {
                throw new Exception("Load job timeout")
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table_s3_1")
    }

    // case11: S3 load test strip_outer_array=true (_ true) - one line array
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table_s3_2 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        def label11 = "s3_load_strip_array_" + UUID.randomUUID().toString().replaceAll("-", "")
        sql """
            LOAD LABEL ${label11} (
                DATA INFILE("s3://${s3BucketName}/load/data_by_array_oneLine.json")
                INTO TABLE test_table_s3_2
                FORMAT AS "json"
                (a, b)
                PROPERTIES(
                    "strip_outer_array" = "true"
                )
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            );
            """
        
        // Wait for load to complete
        def max_try_time = 60000
        while (max_try_time > 0) {
            def result = sql "SHOW LOAD WHERE label = '${label11}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                def count = sql "SELECT COUNT(*) FROM test_table_s3_2"
                assertEquals(4, count[0][0])
                break
            } else if (result[0][2] == "CANCELLED") {
                throw new Exception("Load job cancelled: " + result[0][7])
            }
            Thread.sleep(1000)
            max_try_time -= 1000
            if (max_try_time <= 0) {
                throw new Exception("Load job timeout")
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table_s3_2")
    }

    // case12: S3 load test strip_outer_array=true (_ true) - multi line array
    // error S3 load must use read_json_by_line = true

    // case13: S3 load test read_json_by_line=true and strip_outer_array=true (true true)
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table_s3_4 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        def label13 = "s3_load_both_true_" + UUID.randomUUID().toString().replaceAll("-", "")
        sql """
            LOAD LABEL ${label13} (
                DATA INFILE("s3://${s3BucketName}/load/data_by_multiArray.json")
                INTO TABLE test_table_s3_4
                FORMAT AS "json"
                (a, b)
                PROPERTIES(
                    "read_json_by_line" = "true",
                    "strip_outer_array" = "true"
                )
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            );
            """
        
        // Wait for load to complete
        def max_try_time = 60000
        while (max_try_time > 0) {
            def result = sql "SHOW LOAD WHERE label = '${label13}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                def count = sql "SELECT COUNT(*) FROM test_table_s3_4"
                assertEquals(4, count[0][0])  // 两行，每行2个对象，共4条数据
                break
            } else if (result[0][2] == "CANCELLED") {
                throw new Exception("Load job cancelled: " + result[0][7])
            }
            Thread.sleep(1000)
            max_try_time -= 1000
            if (max_try_time <= 0) {
                throw new Exception("Load job timeout")
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table_s3_4")
    }

    // case14: S3 load test read_json_by_line=true (true _) - explicit read json by line
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table_s3_5 (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        def label14 = "s3_load_read_by_line_" + UUID.randomUUID().toString().replaceAll("-", "")
        sql """
            LOAD LABEL ${label14} (
                DATA INFILE("s3://${s3BucketName}/load/data_by_line.json")
                INTO TABLE test_table_s3_5
                FORMAT AS "json"
                (a, b)
                PROPERTIES(
                    "read_json_by_line" = "true"
                )
            )
            WITH S3 (
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}"
            );
            """
        
        // Wait for load to complete
        def max_try_time = 60000
        while (max_try_time > 0) {
            def result = sql "SHOW LOAD WHERE label = '${label14}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                def count = sql "SELECT COUNT(*) FROM test_table_s3_5"
                assertEquals(4, count[0][0])
                break
            } else if (result[0][2] == "CANCELLED") {
                throw new Exception("Load job cancelled: " + result[0][7])
            }
            Thread.sleep(1000)
            max_try_time -= 1000
            if (max_try_time <= 0) {
                throw new Exception("Load job timeout")
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table_s3_5")
    }
}
