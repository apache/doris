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

suite("test_compress_type", "load_p0") {
    def tableName = "basic_data"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    // GZ/LZO/BZ2/LZ4FRAME/DEFLATE/LZOP
    sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
    // sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'format', "CSV"
        set 'compress_type', 'GZ'

        file "basic_data.csv.gz"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(20, json.NumberTotalRows)
                assertEquals(20, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'format', "CSV"
        set 'compress_type', 'BZ2'

        file "basic_data.csv.bz2"
        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(20, json.NumberTotalRows)
                assertEquals(20, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'format', 'csv'
        set 'compress_type', 'LZ4'

        file "basic_data.csv.lz4"
        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(20, json.NumberTotalRows)
                assertEquals(20, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'compress_type', 'GZ'

        file "basic_data.csv.gz"
        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(20, json.NumberTotalRows)
                assertEquals(20, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'compress_type', 'BZ2'

        file "basic_data.csv.bz2"
        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(20, json.NumberTotalRows)
                assertEquals(20, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'compress_type', 'LZ4'

        file "basic_data.csv.lz4"
        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(20, json.NumberTotalRows)
                assertEquals(20, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                assertTrue(json.LoadBytes > 0)
        }       
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'format', "CSV"
        file "basic_data.csv.gz"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(13, json.NumberTotalRows)
                assertEquals(0, json.NumberLoadedRows)
                assertEquals(13, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'format', "CSV"
        file "basic_data.csv.bz2"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(9, json.NumberTotalRows)
                assertEquals(0, json.NumberLoadedRows)
                assertEquals(9, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        set 'format', "CSV"
        file "basic_data.csv.lz4"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(31, json.NumberTotalRows)
                assertEquals(0, json.NumberLoadedRows)
                assertEquals(31, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        file "basic_data.csv.gz"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(13, json.NumberTotalRows)
                assertEquals(0, json.NumberLoadedRows)
                assertEquals(13, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        file "basic_data.csv.bz2"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(9, json.NumberTotalRows)
                assertEquals(0, json.NumberLoadedRows)
                assertEquals(9, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'trim_double_quotes', 'true'
        file "basic_data.csv.lz4"

        check {
            result, exception, startTime, endTime ->
                assertTrue(exception == null)
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Fail", json.Status)
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(31, json.NumberTotalRows)
                assertEquals(0, json.NumberLoadedRows)
                assertEquals(31, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
        }
    }

    qt_sql """ select count(*) from ${tableName} """
}