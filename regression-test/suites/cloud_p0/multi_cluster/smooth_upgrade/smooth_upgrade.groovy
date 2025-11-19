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

/* smooth-upgrade regression
 * Objective:
 *   1. old BE finishes inflight load jobs and quit eventually
 *   2. all load jobs finishes successfully
 *
 * Limitations:
 *   1. only support single BE test
 *   2. cluster must be created before test
 *   3. new BE process must be created (no need to add) before test
 */

import groovy.json.JsonOutput

suite("smooth_upgrade") {
    // def tables = [nation: 25, customer: 15000000, lineitem: 600037902, orders: 150000000, part: 20000000, partsupp: 80000000, region: 5, supplier: 1000000]
    def tables = [nation: 25, customer: 15000000, region: 5, supplier: 1000000]

    // new be info (TODO: get from config):
    def ip = context.config.upgradeNewBeIp
    def hbPort = context.config.upgradeNewBeHbPort
    def httpPort = context.config.upgradeNewBeHttpPort
    def beUniqueId = context.config.upgradeNewBeUniqueId

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()

    def create_table_and_start_load = { ->
        def s3BucketName = getS3BucketName()
        def s3WithProperties = """WITH S3 (
            |"AWS_ACCESS_KEY" = "${getS3AK()}",
            |"AWS_SECRET_KEY" = "${getS3SK()}",
            |"AWS_ENDPOINT" = "${getS3Endpoint()}",
            |"AWS_REGION" = "${getS3Region()}")
            |PROPERTIES(
            |"exec_mem_limit" = "8589934592",
            |"load_parallelism" = "3")""".stripMargin()
        
        

        tables.each { table, rows ->
            sql """ DROP TABLE IF EXISTS $table """
            // create table if not exists
            sql new File("""${context.file.parentFile}/ddl/${table}.sql""").text
            def loadLabel = table + "_" + uniqueID
            // load data from cos
            def loadSql = new File("""${context.file.parentFile}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
            loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
            sql loadSql
        }
    }

    def add_new_be = { ->
        println("the ip is " + ip)
        println("the heartbeat port is " + hbPort)
        println("the http port is " + httpPort)
        println("the be unique id is " + beUniqueId)

        addNodeForSmoothUpgrade(beUniqueId, ip, hbPort, "selectdb_cloud_dev_smooth_upgrade_asan_name_cluster0", "selectdb_cloud_dev_smooth_upgrade_asan_id_cluster0")

        sleep(20000)

        result  = sql "show clusters"
        assertTrue(result.size() == 1)
    }

    def del_new_be = { ->
        println("delete be unique id is " + beUniqueId)

        d_node.call(beUniqueId, ip, hbPort, "selectdb_cloud_dev_smooth_upgrade_asan_name_cluster0", "selectdb_cloud_dev_smooth_upgrade_asan_id_cluster0")

        sleep(20000)

        result  = sql "show clusters"
        for (row : result) {
            println row
        }
        assertTrue(result.size() == 1)
    }

    def wait_and_check_load = { ->
        tables.each { table, rows ->
            while (true) {
                def loadLabel = table + "_" + uniqueID
                def stateResult = sql "show load where Label = '${loadLabel}'"
                def loadState = stateResult[stateResult.size() - 1][2].toString()
                if ("CANCELLED".equalsIgnoreCase(loadState)) {
                    throw new IllegalStateException("load ${loadLabel} failed.")
                } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                    sql 'sync'
                    for (int i = 1; i <= 5; i++) {
                        def loadRowCount = sql "select count(1) from ${table}"
                        logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                        assertTrue(loadRowCount[0][0] == rows)
                    }
                    sql new File("""${context.file.parentFile}/ddl/${table}_delete.sql""").text
                    for (int i = 1; i <= 5; i++) {
                        def loadRowCount = sql "select count(1) from ${table}"
                        logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                        assertTrue(loadRowCount[0][0] == 0)
                    }
                    break
                }
                sleep(5000)
            }
        }
    }

    def wait_old_be_inactive = { ->
        def sub = '"isActive":false'
        def max_try_milli_secs = 1800000
        def is_active = true
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show backends; """
            for (row : result) {
                println row
                if (row[22].contains(sub)) {
                    is_active = false
                    break
                }
            }

            if (is_active == false) {
                break
            }

            Thread.sleep(5000)
            max_try_milli_secs -= 5000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "wait old BE timeout")
            }
        }
    }

    sleep(10000)
    try {
        create_table_and_start_load.call()

        sleep(10000)

        add_new_be.call()

        wait_old_be_inactive.call()

        wait_and_check_load.call()
    } finally {
        del_new_be.call()
    }
}

