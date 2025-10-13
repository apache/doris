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
    def dataFile = "simple_json.json"
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    
    sql "DROP TABLE IF EXISTS ${testTable}"
    
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
        id INT DEFAULT '10',
        city VARCHAR(32) DEFAULT '',
        code BIGINT SUM DEFAULT '0')
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // case1: use "$." in json_root
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'json_root', '$.'
        set 'columns', 'id,city,code'
        
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
    
    qt_select_jsonroot_dollar_dot "SELECT * FROM ${testTable} ORDER BY id"
    
    sql "TRUNCATE TABLE ${testTable}"
    
    // case2: use "$" in json_root
    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'json_root', '$'
        set 'columns', 'id,city,code'
        
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
    
    qt_select_jsonroot_dollar "SELECT * FROM ${testTable} ORDER BY id"

    sql "DROP TABLE IF EXISTS ${testTable}"

    testTable = "t_with_json"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable}(
            c1 INT DEFAULT '10',
            c2 Json
        )
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // case3: use "$" in json_path

    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'jsonpaths', '["$.id", "$"]'
        set 'columns', 'c1,c2'
        
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
    
    qt_select_jsonpath_dollar "SELECT * FROM ${testTable} ORDER BY c1"
    
    sql "TRUNCATE TABLE ${testTable}"


    // case4: use "$." in json_path

    streamLoad {
        table testTable
        file dataFile
        time 10000
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        set 'jsonpaths', '["$.id", "$."]'
        set 'columns', 'c1,c2'
        
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
    
    qt_select_jsonpath_dollar_dot "SELECT * FROM ${testTable} ORDER BY c1"
        
    sql "DROP TABLE IF EXISTS ${testTable}"
}