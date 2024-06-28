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

suite("multi_thread_load", "p1,nonConcurrent") { // stress case should use resource fully```
    // get doris-db from s3
    def dirPath = context.file.parent
    def fatherPath = context.file.parentFile.parentFile.getPath()
    def fileName = "doris-dbgen"
    def fileUrl = "${getS3Url()}/regression/doris-dbgen-23-10-18/doris-dbgen-23-10-20/doris-dbgen"
    def filePath = Paths.get(dirPath, fileName)
    if (!Files.exists(filePath)) {
        new URL(fileUrl).withInputStream { inputStream ->
            Files.copy(inputStream, filePath)
        }
        def file = new File(dirPath + "/" + fileName)
        file.setExecutable(true)
    }

    def dir_file_exist = { String cur_dir ->
        assertTrue(new File(cur_dir).exists())
        def subFolders = Files.list(Paths.get(cur_dir)).filter {Files.isDirectory(it)}.toList()
        for (def folder_it : subFolders) {
            logger.info("folder_if: " + folder_it)
            def folder = new File(folder_it.toString())
            def files = folder.listFiles()
            assertTrue(files.length==1)
            files.each {file ->
                logger.info("file.name: " + folder_it.toString() + "/" + file.name)
            }
        }
    }


    def data_count = 20 // number of load tasks and threads
    def rows = 100  // total rows to load

    // generate datafiles via doris-dbgen
    def doris_dbgen_create_data = { db_name, tb_name, part_type ->
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

        for (int i = 0; i < data_count; i++) {
            def cm
            if (password) {
                cm = """${dirPath}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb} --table ${tableName} --rows ${rows} --bulk-size ${bulkSize} --http-port ${http_port} --config ${fatherPath}/doris_dbgen_conf/two_stream_load_conflict.yaml --save-to-dir ${dirPath}/${part_type}/${part_type}_${i}/"""
            } else {
                cm = """${dirPath}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb} --table ${tableName} --rows ${rows} --bulk-size ${bulkSize} --http-port ${http_port} --config ${fatherPath}/doris_dbgen_conf/two_stream_load_conflict.yaml --save-to-dir ${dirPath}/${part_type}/${part_type}_${i}/"""
            }
            logger.info("datagen: " + cm)
            def proc = cm.execute()
            def sout = new StringBuilder(), serr = new StringBuilder()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(7200000)
            logger.info("std out: " + sout + ", std err: " + serr)
        }

        def table_exist = sql """select * from information_schema.tables where  TABLE_SCHEMA = "${realDb}" and TABLE_NAME = "${tableName}";"""
        assertTrue(table_exist.size == 1)
        dir_file_exist("""${dirPath}/${part_type}""")

        for (int i = 0; i < data_count; i++) {
            def dir_name = """${dirPath}/${part_type}/${part_type}_${i}"""
            def directory = new File(dir_name)
            assertTrue(directory.exists())
            assertTrue(directory.isDirectory())

            def files = directory.listFiles()
            assertTrue(files.length == 1)
            assertTrue(files[0].isFile())
            logger.info("The file name is: + ${dirPath}/${part_type}/${part_type}_${i}/${files[0].name}")
        }
    }

    def write_to_file = { cur_path, content ->
        File file = new File(cur_path)
        file.write(content)
    }

    def cm_list = []
    def doris_dbgen_load_data = { db_name, tb_name, part_type ->
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

        for (int i = 0; i < data_count; i++) {
            def cm = ""
            def list = []
            def dir = new File("""${dirPath}""" + "/" + part_type + "/" + part_type + "_" + i)
            dir.eachFileRecurse (FileType.FILES) { file ->
                list << file
            }

            if (password) {
                cm = """curl --location-trusted -u ${user}:${password} -H "column_separator:|" -T ${list[0]} http://${sql_ip}:${http_port}/api/${realDb}/${tableName}/_stream_load"""
            } else {
                cm = """curl --location-trusted -u root: -H "column_separator:|" -T ${list[0]} http://${sql_ip}:${http_port}/api/${realDb}/${tableName}/_stream_load"""
            }
            logger.info("load data: " + cm)

            def load_path = """${dirPath}/range/thread_load_${i}.sh"""
            write_to_file(load_path, cm)
            cm_list.add("""bash ${dirPath}/range/thread_load_${i}.sh""")
        }
    }

    def data_delete = { part_type ->
        def sql_cm = """rm -rf ${dirPath}/${part_type}"""
        sql_cm.execute()
    }

    def database_name = "regression_test_auto_partition_concurrent"
    def table_name = "concurrent"

    sql """create database if not exists ${database_name};"""
    sql """use ${database_name};"""
    sql """drop table if exists ${table_name};"""
    sql new File("""${fatherPath}/ddl/concurrent.sql""").text

    data_delete("range")
    doris_dbgen_create_data(database_name, table_name, "range")
    doris_dbgen_load_data(database_name, table_name, "range")

    def load_threads = []
    def concurrent_load = { str ->
        logger.info("load start:" + str)
        def proc = str.execute()
        def sout = new StringBuilder(), serr = new StringBuilder()
        proc.consumeProcessOutput(sout, serr)
        proc.waitForOrKill(600000) // 10 minutes
    }

    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[0])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[1])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[2])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[3])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[4])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[5])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[6])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[7])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[8])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[9])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[10])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[11])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[12])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[13])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[14])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[15])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[16])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[17])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[18])})
    load_threads.add(Thread.startDaemon{concurrent_load(cm_list[19])})

    // wait them for finishing
    for (Thread th in load_threads) {
        th.join()
    }

    // check data count
    def row_count_range = sql """select count() from ${table_name};"""
    assertTrue(data_count*rows == row_count_range[0][0], "${data_count*rows}, ${row_count_range[0][0]}")
    // check there's no intersect in partitions
    def partition_res_range = sql_return_maparray """show partitions from ${table_name} order by PartitionName;"""
    for (int i = 0; i < partition_res_range.size(); i++) {
        for (int j = i+1; j < partition_res_range.size(); j++) {
            if (partition_res_range[i].Range == partition_res_range[j].Range) {
                assertTrue(false, "$i, $j")
            }
        }
    }
}
