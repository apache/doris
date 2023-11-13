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

suite("test_http_stream_properties", "p0") {

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
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19",
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19",
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19",
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,to_bitmap(c5) c19,HLL_HASH(c5) c20,TO_QUANTILE_STATE(c5,1.0) c21,to_bitmap(c6) c22,HLL_HASH(c6) c23,TO_QUANTILE_STATE(c6,1.0) c24",
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18",
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18",
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18",
                  ]

    def target_columns = [ 
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                  ]

    def timezoneColumns = [
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c1",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c1",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c1",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c1,k19=to_bitmap(c5),k20=HLL_HASH(c5),k21=TO_QUANTILE_STATE(c5,1.0),kd19=to_bitmap(c6),kd20=HLL_HASH(c6),kd21=TO_QUANTILE_STATE(c6,1.0)",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18",
                  ]

    def files = [
                  "../stream_load/basic_data.csv",
                  "../stream_load/basic_data.csv",
                  "../stream_load/basic_data.csv",
                  "../stream_load/basic_data.csv",
                  "../stream_load/basic_array_data.csv",
                  "../stream_load/basic_array_data.csv",
                  "../stream_load/basic_array_data.csv"
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

    // def compress_type = [
    //               "gz",
    //               "bz2",
    //               "lz4",
    //               "deflate",
    //               "lzo",
    //             ]

    // def compress_files = [
    //               "basic_data.csv.gz",
    //               "basic_data.csv.bz2",
    //               "basic_data.csv.lz4",
    //               "basic_data.csv.deflate",
    //               "basic_data.csv.lzo",
    //               "basic_array_data.csv.gz",
    //               "basic_array_data.csv.bz2",
    //               "basic_array_data.csv.lz4",
    //               "basic_array_data.csv.deflate",
    //               "basic_array_data.csv.lzo",
    //             ]
    def compress_files = [
                        "dup_tbl_basic": [
                            ["../stream_load/basic_data.csv.gz", "gz"],
                            ["../stream_load/basic_data.csv.bz2", "bz2"],
                        ],
                        "uniq_tbl_basic": [
                            ["../stream_load/basic_data.csv.gz", "gz"],
                            ["../stream_load/basic_data.csv.bz2", "bz2"],
                        ],
                        "mow_tbl_basic": [
                            ["../stream_load/basic_data.csv.gz", "gz"],
                            ["../stream_load/basic_data.csv.bz2", "bz2"],
                        ],
                        "agg_tbl_basic": [
                            ["../stream_load/basic_data.csv.gz", "gz"],
                            ["../stream_load/basic_data.csv.bz2", "bz2"],
                        ],
                        "dup_tbl_array": [
                            ["../stream_load/basic_array_data.csv.gz", "gz"],
                            ["../stream_load/basic_array_data.csv.bz2", "bz2"],
                        ],
                        "uniq_tbl_array": [
                            ["../stream_load/basic_array_data.csv.gz", "gz"],
                            ["../stream_load/basic_array_data.csv.bz2", "bz2"],
                        ],
                        "mow_tbl_array": [
                            ["../stream_load/basic_array_data.csv.gz", "gz"],
                            ["../stream_load/basic_array_data.csv.bz2", "bz2"],
                        ],
                    ]

    def loadedRows = [12,12,12,12,8,8,15]

    def filteredRows = [8,8,8,8,12,12,5]

    def maxFilterRatio = [0.4,0.4,0.4,0.4,0.6,0.6,0.6]

    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

    def i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_create.sql""").text
            
            def tableNm = "stream_load_" + tableName

            streamLoad {
                set 'version', '1'
                set 'sql', """
                        insert into ${db}.${tableNm}(${target_columns[i]}) select ${columns[i]} from http_stream("format"="csv", "column_separator"="|")
                        """
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
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_drop.sql""").text
        }
    }
    
    // TODO timezone

    // TODO strict_mode

    // TODO max_filter_ratio

    // sequence
    try {
            sql new File("""${context.file.parent}/../stream_load/ddl/uniq_tbl_basic_drop_sequence.sql""").text
            sql new File("""${context.file.parent}/../stream_load/ddl//uniq_tbl_basic_create_sequence.sql""").text

            String tableNm = "stream_load_uniq_tbl_basic_sequence"

            streamLoad {
                set 'version', '1'
                set 'sql', """
                        insert into ${db}.${tableNm}(${target_columns[0]}) select ${columns[0]} from http_stream("format"="CSV", "column_separator"="|")
                        """
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
            qt_sql_squence "select * from stream_load_uniq_tbl_basic_sequence order by k00,k01"
    } finally {
        sql new File("""${context.file.parent}/../stream_load/ddl/uniq_tbl_basic_drop_sequence.sql""").text
    }

    // TODO merge type

    // TODO two_phase_commit

    // compress_type
    // gz/bz2
    // TODO lzo/deflate/lz4
    i = 0
    try {
        for (String tableName in tables) {
            compress_files[tableName].each { fileName, type -> {
                    sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_drop.sql""").text
                    sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_create.sql""").text
                    def tableNm = "stream_load_" + tableName
                    streamLoad {
                        set 'version', '1'
                        set 'sql', """
                            insert into ${db}.${tableNm}(${target_columns[i]}) select ${columns[i]} from http_stream("format"="CSV", "column_separator"="|", "compress_type"="${type}")
                            """
                        file fileName
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
                } 
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${table}_drop.sql""").text
        }
    }

    // skip_lines
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_create.sql""").text

            def tableNm = "stream_load_" + tableName

            streamLoad {
                set 'version', '1'
                set 'sql', """
                        insert into ${db}.${tableNm}(${target_columns[i]}) select ${columns[i]} from http_stream("format"="CSV", "column_separator"="|", "skip_lines"="2")
                        """
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
            if (i <= 3) {
                qt_sql_skip_lines "select * from ${tableNm} order by k00,k01"
            } else {
                qt_sql_skip_lines "select * from ${tableNm} order by k00"
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${table}_drop.sql""").text
        }
    }

    // column_separator
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_create.sql""").text
            def tableNm = "stream_load_" + tableName
            streamLoad {
                set 'version', '1'
                set 'sql', """
                        insert into ${db}.${tableNm}(${target_columns[i]}) select ${columns[i]} from http_stream("format"="CSV", "column_separator"=",")
                        """
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    // assertEquals(0, json.NumberTotalRows)
                    // assertEquals(0, json.NumberLoadedRows)
                    // assertEquals(0, json.NumberFilteredRows)
                    // assertEquals(0, json.NumberUnselectedRows)
                }
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${table}_drop.sql""").text
        }
    }

    // line_delimiter
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/../stream_load/ddl/${tableName}_create.sql""").text
            def tableNm = "stream_load_" + tableName
            streamLoad {
                set 'version', '1'
                set 'sql', """
                        insert into ${db}.${tableNm}(${target_columns[i]}) select ${columns[i]} from http_stream("format"="CSV", "column_separator"=",", "line_delimiter"=",")
                        """
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    // assertEquals(1, json.NumberTotalRows)
                    // assertEquals(0, json.NumberLoadedRows)
                    // assertEquals(1, json.NumberFilteredRows)
                    // assertEquals(0, json.NumberUnselectedRows)
                }
            }
            i++
        }
    } finally {
        for (String table in tables) {
            sql new File("""${context.file.parent}/../stream_load/ddl/${table}_drop.sql""").text
        }
    }
}

