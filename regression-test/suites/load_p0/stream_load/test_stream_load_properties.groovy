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

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.impl.client.LaxRedirectStrategy;

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

    def json_files = [
                 "basic_data.json",
                 "basic_array_data.json",
                ]

    def json_by_line_files = [
                 "basic_data_by_line.json",
                 "basic_array_data_by_line.json",
                ]

    def loadedRows = [12,12,12,12,8,8,15]

    def jsonLoadedRows = [20,20,20,20,18,18,18]

    def filteredRows = [8,8,8,8,12,12,5]

    def maxFilterRatio = [0.4,0.4,0.4,0.4,0.6,0.6,0.6]

    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

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

    // sequence
    try {
            sql new File("""${context.file.parent}/ddl/uniq_tbl_basic_drop_sequence.sql""").text
            sql new File("""${context.file.parent}/ddl/uniq_tbl_basic_create_sequence.sql""").text

            streamLoad {
                table 'stream_load_uniq_tbl_basic_sequence'
                set 'column_separator', '|'
                set 'columns', columns[0]
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
        sql new File("""${context.file.parent}/ddl/uniq_tbl_basic_drop_sequence.sql""").text
    }

    // merge type
    i = 0
    try {
        def tableName = "mow_tbl_basic"
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

        streamLoad {
            table "stream_load_" + tableName
            set 'column_separator', '|'
            set 'columns', columns[i]
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

        streamLoad {
            table "stream_load_" + tableName
            set 'column_separator', '|'
            set 'columns', columns[i]
            set 'merge_type', 'DELETE'
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
        def tableName1 = "stream_load_" + tableName
        qt_sql_merge_type "select * from ${tableName1} order by k00,k01"                  
    } finally {
        sql new File("""${context.file.parent}/ddl/mow_tbl_basic_drop.sql""").text
    }

    // two_phase_commit
    def do_streamload_2pc = { txn_id, txn_operation, tableName->
        HttpClients.createDefault().withCloseable { client ->
            RequestBuilder requestBuilder = RequestBuilder.put("http://${address.hostString}:${address.port}/api/${db}/${tableName}/_stream_load_2pc")
            String encoding = Base64.getEncoder()
                .encodeToString((user + ":" + (password == null ? "" : password)).getBytes("UTF-8"))
            requestBuilder.setHeader("Authorization", "Basic ${encoding}")
            requestBuilder.setHeader("Expect", "100-Continue")
            requestBuilder.setHeader("txn_id", "${txn_id}")
            requestBuilder.setHeader("txn_operation", "${txn_operation}")

            String backendStreamLoadUri = null
            client.execute(requestBuilder.build()).withCloseable { resp ->
                resp.withCloseable {
                    String body = EntityUtils.toString(resp.getEntity())
                    def respCode = resp.getStatusLine().getStatusCode()
                    // should redirect to backend
                    if (respCode != 307) {
                        throw new IllegalStateException("Expect frontend stream load response code is 307, " +
                                "but meet ${respCode}\nbody: ${body}")
                    }
                    backendStreamLoadUri = resp.getFirstHeader("location").getValue()
                }
            }

            requestBuilder.setUri(backendStreamLoadUri)
            try{
                client.execute(requestBuilder.build()).withCloseable { resp ->
                    resp.withCloseable {
                        String body = EntityUtils.toString(resp.getEntity())
                        def respCode = resp.getStatusLine().getStatusCode()
                        if (respCode != 200) {
                            throw new IllegalStateException("Expect backend stream load response code is 200, " +
                                    "but meet ${respCode}\nbody: ${body}")
                        }
                    }
                }
            } catch (Throwable t) {
                log.info("StreamLoad Exception: ", t)
            }
        }
    }

    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            String txnId
            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'two_phase_commit', 'true'
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    txnId = json.TxnId
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }

            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_2pc "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_2pc "select * from ${tableName1} order by k00"
            }

            do_streamload_2pc.call(txnId, "abort", tableName1)

            if (i <= 3) {
                qt_sql_2pc_abort "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_2pc_abort "select * from ${tableName1} order by k00"
            }

            streamLoad {
                table "stream_load_" + tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'two_phase_commit', 'true'
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    txnId = json.TxnId
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }

            do_streamload_2pc.call(txnId, "commit", tableName1)

            def count = 0
            while (true) {
                def res
                if (i <= 3) {
                    res = sql "select count(*) from ${tableName1}"
                } else {
                    res = sql "select count(*) from ${tableName1}"
                }
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 60) {
                    log.error("stream load commit can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(1000)
                count++
            }
            
            if (i <= 3) {
                qt_sql_2pc_commit "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_2pc_commit "select * from ${tableName1} order by k00"
            }

            i++
        }
    } finally {
        for (String tableName in tables) {
            def tableName1 =  "stream_load_" + tableName
            sql "DROP TABLE IF EXISTS ${tableName1} FORCE"
        }
    }

    // compress_type 
    // gz/bz2/lz4
    // todo lzo/deflate
    // i = 0
    // try {
    //     for (String tableName in tables) {
    //         for (int j = 0; j < 3 ; j++) {
    //             sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    //             sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

    //             streamLoad {
    //                 table "stream_load_" + tableName
    //                 set 'column_separator', '|'
    //                 set 'columns', columns[i]
    //                 set 'compress_type', compress_type[j]
    //                 if (i <= 3) {
    //                     file compress_files[0+j]
    //                 }else{
    //                     file compress_files[5+j]
    //                 }
    //                 time 10000 // limit inflight 10s

    //                 check { result, exception, startTime, endTime ->
    //                     if (exception != null) {
    //                         throw exception
    //                     }
    //                     log.info("Stream load result: ${tableName}".toString())
    //                     def json = parseJson(result)
    //                     assertEquals("success", json.Status.toLowerCase())
    //                     assertEquals(20, json.NumberTotalRows)
    //                     assertEquals(20, json.NumberLoadedRows)
    //                     assertEquals(0, json.NumberFilteredRows)
    //                     assertEquals(0, json.NumberUnselectedRows)
    //                 }
    //             }
    //             def tableName1 =  "stream_load_" + tableName
    //             if (i <= 3) {
    //                 qt_sql_compress_type "select * from ${tableName1} order by k00,k01"
    //             } else {
    //                 qt_sql_compress_type "select * from ${tableName1} order by k00"
    //             }
    //         }
    //         i++
    //     }
    // } finally {
    //     for (String table in tables) {
    //         sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
    //     }
    // }

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

    //json
    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'format', 'json'
                set 'columns', columns[i]
                set 'strip_outer_array', 'true'
                set 'fuzzy_parse', 'true'
                if (i <= 3) {
                    file json_files[0]
                } else {
                    file json_files[1]
                }
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(jsonLoadedRows[i], json.NumberTotalRows)
                    assertEquals(jsonLoadedRows[i], json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_json_strip_outer_array "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_json_strip_outer_array "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
        }
    }

    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'format', 'json'
                set 'columns', columns[i]
                set 'strip_outer_array', 'true'
                set 'jsonpath', '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\", \"$.k18\"]'
                if (i <= 3) {
                    file json_files[0]
                } else {
                    file json_files[1]
                }
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(jsonLoadedRows[i], json.NumberTotalRows)
                    assertEquals(jsonLoadedRows[i], json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_json_jsonpath "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_json_jsonpath "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
        }
    }

    i = 0
    try {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

            streamLoad {
                table "stream_load_" + tableName
                set 'format', 'json'
                set 'columns', columns[i]
                set 'read_json_by_line', 'true'
                if (i <= 3) {
                    file json_by_line_files[0]
                } else {
                    file json_by_line_files[1]
                }
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(jsonLoadedRows[i], json.NumberTotalRows)
                    assertEquals(jsonLoadedRows[i], json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }
            def tableName1 =  "stream_load_" + tableName
            if (i <= 3) {
                qt_sql_json_read_by_line "select * from ${tableName1} order by k00,k01"
            } else {
                qt_sql_json_read_json_by_line "select * from ${tableName1} order by k00"
            }
            i++
        }
    } finally {
        for (String tableName in tables) {
            sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
        }
    }
}
