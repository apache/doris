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

suite("stress_test_insert_into", "p1,nonConcurrent") { // stress case should use resource fully
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

    def rows = 100000

    // load data via doris-dbgen
    def doris_dbgen_create_data = { db_name, tb_name ->
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
            cm = """
                ${dirPath}/doris-dbgen gen
                    --host ${sql_ip}
                    --sql-port ${sql_port}
                    --user ${user}
                    --password ${password}
                    --database ${realDb}
                    --table ${tableName}
                    --rows ${rows}
                    --http-port ${http_port}
                    --config ${fatherPath}/doris_dbgen_conf/stress_test_insert_into.yaml
                """
        } else {
            cm = """
                ${dirPath}/doris-dbgen gen
                    --host ${sql_ip}
                    --sql-port ${sql_port}
                    --user ${user}
                    --database ${realDb}
                    --table ${tableName}
                    --rows ${rows}
                    --http-port ${http_port}
                    --config ${fatherPath}/doris_dbgen_conf/stress_test_insert_into.yaml
                """
        }

        logger.info("datagen: " + cm)
        def proc = cm.execute()
        def sout = new StringBuilder(), serr = new StringBuilder()
        proc.consumeProcessOutput(sout, serr)
        proc.waitForOrKill(1800000)
        logger.info("std out: " + sout + ", std err: " + serr)
    }

    def database_name = "regression_test_auto_partition_concurrent"
    def table_src = "stress_source"
    def table_dest = "stress_destination"

    sql """create database if not exists ${database_name};"""
    sql """use ${database_name};"""
    sql """drop table if exists ${table_src};"""
    sql """drop table if exists ${table_dest};"""
    sql new File("""${fatherPath}/ddl/stress_source.sql""").text
    sql new File("""${fatherPath}/ddl/stress_destination.sql""").text
    doris_dbgen_create_data(database_name, table_src)

    // TEST-BODY
    def count_src = sql " select count() from ${table_src}; "
    sql " insert into ${table_dest} select * from ${table_src} "
    def count_dest = sql " select count() from ${table_dest}; "
    // check data count
    assertTrue(count_src[0][0] > 0)
    assertEquals(count_src[0][0], count_dest[0][0])
    logger.info("got rows: ${count_src[0][0]}")
}
