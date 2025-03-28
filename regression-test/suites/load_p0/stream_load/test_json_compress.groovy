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

suite("test_json_compress", "p0") { 
    def tableName = "test_json_compress_tbl"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}
        (
            k00 INT             NOT NULL,
            k01 DATE            NOT NULL,
            k02 BOOLEAN         NULL,
            k03 TINYINT         NULL,
            k04 SMALLINT        NULL,
            k05 INT             NULL,
            k06 BIGINT          NULL,
            k07 LARGEINT        NULL,
            k08 FLOAT           NULL,
            k09 DOUBLE          NULL,
            k10 DECIMAL(9,1)    NULL,
            k11 DECIMALV3(9,1)  NULL,
            k12 DATETIME        NULL,
            k13 DATEV2          NULL,
            k14 DATETIMEV2      NULL,
            k15 CHAR            NULL,
            k16 VARCHAR         NULL,
            k17 STRING          NULL,
            k18 JSON            NULL
    
        )
        DUPLICATE KEY(k00)
        DISTRIBUTED BY HASH(k00) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    streamLoad {
        set 'format', 'json'
        set 'compress_type', 'gz'
        set 'read_json_by_line', 'true'
        table "${tableName}"
        time 10000
        file """${getS3Url()}/regression/load/data/basic_data_by_line.json.gz"""
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    streamLoad {
        set 'format', 'json'
        set 'compress_type', 'bz2'
        set 'read_json_by_line', 'true'
        table "${tableName}"
        time 10000
        file """${getS3Url()}/regression/load/data/basic_data_by_line.json.bz2"""
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    streamLoad {
        set 'format', 'json'
        set 'compress_type', 'lz4'
        set 'read_json_by_line', 'true'
        table "${tableName}"
        time 10000
        file """${getS3Url()}/regression/load/data/basic_data_by_line.json.lz4"""
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    streamLoad {
        set 'format', 'json'
        set 'compress_type', 'lzop'
        set 'read_json_by_line', 'true'
        table "${tableName}"
        time 10000
        file """${getS3Url()}/regression/load/data/basic_data_by_line.json.lzo"""
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    streamLoad {
        set 'format', 'json'
        set 'compress_type', 'deflate'
        set 'read_json_by_line', 'true'
        table "${tableName}"
        time 10000
        file """${getS3Url()}/regression/load/data/basic_data_by_line.json.deflate"""
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    qt_sql "select * from ${tableName} order by k00, k01"

}
