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

import groovy.io.FileType
import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File

suite("stress_test_diff_date_list", "p2,nonConcurrent") {

    sql """ADMIN SET FRONTEND CONFIG ('max_auto_partition_num' = '10000000')"""

    // get doris-db from s3
    def dirPath = context.file.parent
    def fileName = "doris-dbgen"
    def fileUrl = "http://${getS3BucketName()}.${getS3Endpoint()}/regression/doris-dbgen-23-10-18/doris-dbgen-23-10-20/doris-dbgen"
    def filePath = Paths.get(dirPath, fileName)
    if (!Files.exists(filePath)) {
        new URL(fileUrl).withInputStream { inputStream ->
            Files.copy(inputStream, filePath)
        }
        def file = new File(dirPath + "/" + fileName)
        file.setExecutable(true)
    }

    def data_count = 2
    def cur_rows = 1000000

    // use doris-dbgen product data file
    def doris_dbgen_create_data = { db_name, tb_name, part_type, i ->
        def rows = cur_rows // total rows to load
        def bulkSize = rows
        def tableName = tb_name

        def jdbcUrl = context.config.jdbcUrl
        def urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
        def sql_port
        if (urlWithoutSchema.indexOf("/") >= 0) {
            // e.g: jdbc:mysql://locahost:8080/?a=b
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
        } else {
            // e.g: jdbc:mysql://locahost:8080
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
        }
        String feHttpAddress = context.config.feHttpAddress
        def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

        String realDb = db_name
        String user = context.config.jdbcUser
        String password = context.config.jdbcPassword


        def cm
        if (password) {
            cm = """${context.file.parent}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb} --table ${tableName} --rows ${rows} --bulk-size ${bulkSize} --http-port ${http_port} --config ${context.file.parent}/../doris_dbgen_conf/two_stream_load_conflict.yaml --save-to-dir ${context.file.parent}/${part_type}_${i}/"""
        } else {
            cm = """${context.file.parent}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb} --table ${tableName} --rows ${rows} --bulk-size ${bulkSize} --http-port ${http_port} --config ${context.file.parent}/../doris_dbgen_conf/two_stream_load_conflict.yaml --save-to-dir ${context.file.parent}/${part_type}_${i}/"""
        }
        logger.info("command is: " + cm)
        def proc = cm.execute()
        def sout = new StringBuilder(), serr = new StringBuilder()
        proc.consumeProcessOutput(sout, serr)
        proc.waitForOrKill(7200000)
        logger.info("std out: " + sout + "std err: " + serr)
    }

    def write_to_file = { cur_path, content ->
        File file = new File(cur_path)
        file.write(content)
    }

    def load_result
    def doris_dbgen_stream_load_data = { db_name, tb_name, part_type, i ->
        def list = []
        def dir = new File("""${context.file.parent}""" + "/" + part_type + "_" + i)
        dir.eachFileRecurse (FileType.FILES) { file ->
            list << file
        }
        logger.info(list[0].toString())

        streamLoad {
            db "${db_name}"
            table "${tb_name}"

            set 'column_separator', '|'

            file """${list[0].toString()}"""
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                if (json.Status.toLowerCase() != "success" || 0 != json.NumberFilteredRows) {
                    load_result = result
                }
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    def data_delete = { part_type ->

        for (int i = 1; i <= data_count; i++) {
            def list = []
            def dir = new File("""${context.file.parent}""" + "/" + part_type + "_" + i)
            if (dir.exists()) {
                dir.eachFileRecurse (FileType.FILES) { file ->
                    list << file
                }
                logger.info("rm -rf " + dir)
                ("rm -rf " + dir).execute().text
            }
        }
    }

    def append_to_file = { content ->

        def list = []
        def dir = new File("""${context.file.parent}""" + "/list_1")
        dir.eachFileRecurse (FileType.FILES) { file ->
            list << file
        }

        File file = list[0]
        file.append(content)
    }

    String db = context.config.getDbNameByFile(context.file)
    def database_name = db
    def tb_name1 = "two_streamload_list1"
    def tb_name2 = "two_streamload_list2"

    sql """create database if not exists ${database_name};"""
    sql """use ${database_name};"""
    sql """drop table if exists ${tb_name1};"""
    sql """drop table if exists ${tb_name2};"""
    sql new File("""${context.file.parent}/../ddl/two_streamload_table1.sql""").text
    sql new File("""${context.file.parent}/../ddl/two_streamload_table2.sql""").text

    data_delete("list")
    doris_dbgen_create_data(database_name, tb_name1, "list", 1)
    def add_one_row = "2|1|TRUE|109|2022-12-10"
    append_to_file(add_one_row)
    doris_dbgen_create_data(database_name, tb_name2, "list", 2)

    def thread3 = Thread.start {
        doris_dbgen_stream_load_data(database_name, tb_name1, "list", 1)
    }
    def thread4 = Thread.start {
        doris_dbgen_stream_load_data(database_name, tb_name1, "list", 2)
    }
    thread3.join()
    thread4.join()

    if (load_result != null) {
        def json = parseJson(load_result)
        log.info("Stream load failed. ${load_result}".toString())
        assertEquals("success", json.Status.toLowerCase())
        assertEquals(0, json.NumberFilteredRows)
    }

    // TEST-BODY
    def origin_count = sql """select count(*) from ${database_name}.${tb_name1}"""
    def origin_part = sql """show partitions from ${database_name}.${tb_name1}"""
    def count_rows = origin_count[0][0]
    // check data count
    assertTrue(count_rows == cur_rows * data_count + 1)
    assertTrue(origin_part.size() == 2)
}
