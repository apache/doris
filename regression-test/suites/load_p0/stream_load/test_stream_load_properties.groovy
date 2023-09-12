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

suite("test_stream_load_properties", "p0") {

    def tables = [
                  "dup_tbl_basic",
                  "uniq_tbl_basic",
                  "mow_tbl_basic",
                  "agg_tbl_basic",
                  "dup_tbl_array",
                  "uniq_tbl_array",
                  "mow_tbl_array",
                 ]

    def columns = [ 
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                  ]

    def timezoneColumns = 
                  [ 
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                  ]

    def files = [
                  "basic_data.csv",
                  "basic_data.csv",
                  "basic_data.csv",
                  "basic_data.csv",
                  "basic_array_data.csv",
                  "basic_array_data.csv",
                  "basic_array_data.csv"
                ]

    def timezoneFiles = [
                  "basic_data_timezone.csv",
                  "basic_data_timezone.csv",
                  "basic_data_timezone.csv",
                  "basic_data_timezone.csv",
                  "basic_array_data_timezone.csv",
                  "basic_array_data_timezone.csv",
                  "basic_array_data_timezone.csv",
                ]

    def errorFiles = [
                  "basic_data_with_errors.csv",
                  "basic_data_with_errors.csv",
                  "basic_data_with_errors.csv",
                  "basic_data_with_errors.csv",
                  "basic_array_data_with_errors.csv",
                  "basic_array_data_with_errors.csv",
                  "basic_array_data_with_errors.csv",
                ]

    def compress_type = [
                  "gz",
                  "bz2",
                  "lz4",
                  "deflate",
                  "lzo",
                ]

    def compress_files = [
                  "basic_data.csv.gz",
                  "basic_data.csv.bz2",
                  "basic_data.csv.lz4",
                  "basic_data.csv.deflate",
                  "basic_data.csv.lzo",
                  "basic_array_data.csv.gz",
                  "basic_array_data.csv.bz2",
                  "basic_array_data.csv.lz4",
                  "basic_array_data.csv.deflate",
                  "basic_array_data.csv.lzo",
                ]

    def loadedRows = [0,0,0,0,17,17,17]

    def filteredRows = [20,20,20,20,3,3,3]

    def maxFilterRatio = [1,1,1,1,0.15,0.15,0.15]

    // exec_mem_limit
    def i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'exec_mem_limit', '1'
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            i++
        }
    } finally {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
        }
    }

    // timezone
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', timezoneColumns[i]
                set 'timezone', 'Asia/Shanghai'
                file timezoneFiles[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_timezone_shanghai "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_timezone_shanghai "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', timezoneColumns[i]
                set 'timezone', 'Africa/Abidjan'
                file timezoneFiles[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_timezone_Abidjan "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_timezone_Abidjan "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    // strict_mode
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'strict_mode', 'true'
                file errorFiles[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(loadedRows[i], json.NumberLoadedRows)
                    assertEquals(filteredRows[i], json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_strict_mode "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_strict_mode "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    // max_filter_ratio
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'strict_mode', 'true'
                set 'max_filter_ratio', "${maxFilterRatio[i]}"
                file errorFiles[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(loadedRows[i], json.NumberLoadedRows)
                    assertEquals(filteredRows[i], json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_max_filter_ratio "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_max_filter_ratio "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    // load_to_single_tablet
    try {
            sql new File("""${context.file.parent}/ddl/dup_tbl_basic_drop_random_bucket.sql""").text
            sql new File("""${context.file.parent}/ddl/dup_tbl_basic_create_random_bucket.sql""").text

            streamLoad {
                table 'stream_load_dup_tbl_basic_random_bucket'
                set 'column_separator', '|'
                set 'columns', columns[0]
                set 'load_to_single_tablet', 'true'
                file files[0]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            // def res = sql "show tablets from stream_load_dup_tbl_basic_random_bucket"
            // assertEquals(res[0][10].toString(), "20")
    } finally {
        sql new File("""${context.file.parent}/ddl/dup_tbl_basic_drop_random_bucket.sql""").text
    }

    // compress_type 
    // gz/bz2/lz4
    // todo lzo/deflate
    i = 0
    try {
        for (String tableName in tables) {
            for (int j = 0; j < 3 ; j++) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                streamLoad {
                    table "stream_load_" + tableName
                    set 'column_separator', '|'
                    set 'columns', columns[i]
                    set 'compress_type', compress_type[j]
                    if (i <= 3) {
                        file compress_files[0+j]
                    }else{
                        file compress_files[5+j]
                    }
                    time 10000 // limit inflight 10s

                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        log.info("Stream load result: ${tableName}".toString())
                        def json = parseJson(result)
                        assertEquals("success", json.Status.toLowerCase())
                        assertEquals(20, json.NumberTotalRows)
                        assertEquals(20, json.NumberLoadedRows)
                        assertEquals(0, json.NumberFilteredRows)
                        assertEquals(0, json.NumberUnselectedRows)
                    }
                }
                def tableName1 =  "stream_load_" + tableName
                if (i <= 3) {
                    qt_sql_compress_type "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_compress_type "select * from ${tableName1} order by k00"
                }
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    // skip_lines
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'skip_lines', '2'
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(18, json.NumberTotalRows)
                    assertEquals(18, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_skip_lines "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_skip_lines "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    // column_separator
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', ','
                set 'columns', columns[i]
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(0, json.NumberLoadedRows)
                    assertEquals(20, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }

    // line_delimiter
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'line_delimiter', 'line_delimiter'
                set 'columns', columns[i]
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertEquals(1, json.NumberTotalRows)
                    assertEquals(0, json.NumberLoadedRows)
                    assertEquals(1, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
        }
    }
}
