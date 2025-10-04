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

suite("test_json_root", "p0") {
    def testTable = "t"
    def dataFile = "test_json_root.json"
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    
    sql "DROP TABLE IF EXISTS ${testTable}"
    
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            c1 INT,
            c2 INT,
            c3 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(c1)
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    
    // case1: use "$." in jsonpaths
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'jsonpaths', '["$.record.c1","$.record.c2","$.record.c3"]'
        set 'columns', 'c1,c2,c3'
        
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(json.NumberLoadedRows > 0)
        }
    }
    
    sql """ sync; """

    qt_select_streamload_dollar_dot "SELECT * FROM ${testTable} ORDER BY c1"

    sql "TRUNCATE TABLE ${testTable}"
    
    // case2: use "$" in jsonpaths
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'jsonpaths', '["$record.c1","$record.c2","$record.c3"]'
        set 'columns', 'c1,c2,c3'
        
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(json.NumberLoadedRows > 0)
        }
    }
    
    sql """ sync; """
    
    qt_select_streamload_dollar "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"
    
    // case3: use "$." in json_root
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'json_root', '$.record'
        set 'jsonpaths', '["$.c1","$.c2","$.c3"]'
        set 'columns', 'c1,c2,c3'
        
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(json.NumberLoadedRows > 0)
        }
    }
    
    sql """ sync; """
    
    qt_select_jsonroot_dollar_dot "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"
    
    // case4: use "$" in json_root
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'json_root', '$record'
        set 'jsonpaths', '["$.c1","$.c2","$.c3"]'
        set 'columns', 'c1,c2,c3'
        
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(json.NumberLoadedRows > 0)
        }
    }
    
    sql """ sync; """
    
    qt_select_jsonroot_dollar "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"
    
    // case5: use "$" in json_root and jsonpaths
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'json_root', '$record'
        set 'jsonpaths', '["$c1","$c2","$c3"]'
        set 'columns', 'c1,c2,c3'
        
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(json.NumberLoadedRows > 0)
        }
    }
    
    sql """ sync; """
    
    qt_select_jsonroot_jsonpaths_dollar "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"
    
    // case6: Broker Load
    def brokerLoadLabel = "test_broker_load_jsonpaths_${System.currentTimeMillis()}"
    sql """
        LOAD LABEL ${brokerLoadLabel}(
            DATA INFILE("s3://${s3BucketName}/load/simple_nest.json")
            INTO TABLE ${testTable}
            COLUMNS TERMINATED BY ","
            FORMAT AS "json"
            (c1, c2, c3)
            PROPERTIES(
              "read_json_by_line" = "true",
              "jsonpaths" = '[\"\$record.c1\",\"\$record.c2\",\"\$record.c3\"]'
            )
        )
        WITH S3 (
                "AWS_ACCESS_KEY" = "${ak}",
                "AWS_SECRET_KEY" = "${sk}",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}"
        );
        """
    
    def maxRetry = 30
    def retryCount = 0
    while (retryCount < maxRetry) {
        def result = sql "SHOW LOAD WHERE LABEL = '${brokerLoadLabel}'"
        if (result.size() > 0) {
            def status = result[0][2].toString().toLowerCase()
            if (status == "cancelled" || status == "finished") {
                assertTrue(status == "finished", "Broker Load should succeed")
                break
            }
        }
        sleep(1000)
        retryCount++
    }
    
    sql """ sync; """
    
    qt_select_broker_load "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"
    
    // case7: insert into 
    sql """
        INSERT INTO ${testTable}(c1, c2, c3)
        SELECT c1, c2, c3
        FROM S3 (
            "uri" = "s3://${s3BucketName}/load/simple_nest.json",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
            "format" = "json",
            "read_json_by_line" = "true",
            "jsonpaths" = '[\"\$record.c1\",\"\$record.c2\",\"\$record.c3\"]'
        );
        """
    
    sql """ sync; """
    
    qt_select_tvf_success "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"

    // case8: insert into 
    sql """
        INSERT INTO ${testTable}(c1, c2, c3)
        SELECT c1, c2, c3
        FROM S3 (
            "uri" = "s3://${s3BucketName}/load/simple_nest.json",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
            "format" = "json",
            "read_json_by_line" = "true",
            "json_root" = "\$.record",
            "jsonpaths" = '[\"\$c1\",\"\$c2\",\"\$c3\"]'
        );
        """
    
    sql """ sync; """
    
    qt_select_tvf_success "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "DROP TABLE IF EXISTS ${testTable}"
}