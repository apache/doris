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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq

import org.apache.doris.regression.util.DebugPoint

import org.apache.doris.regression.util.NodeType

suite('test_schema_change_with_compaction2', 'nonConcurrent') {
    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()

    // set fe configuration
    sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"
    sql new File("""${context.file.parent}/../ddl/date_delete.sql""").text
    def load_date_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    sql new File("""${context.file.parent}/../ddl/date_create.sql""").text
    def injectName = 'CloudSchemaChangeJob.process_alter_tablet.sleep'
    def injectBe = null
    def backends = sql_return_maparray('show backends')
    def array = sql_return_maparray("SHOW TABLETS FROM date")
    def injectBeId = array[0].BackendId
    def originTabletId = array[0].TabletId
    injectBe = backends.stream().filter(be -> be.BackendId == injectBeId).findFirst().orElse(null)
    assertNotNull(injectBe)

    def load_delete_compaction = {
        load_date_once("date");
        sql "delete from date where d_datekey < 19900000"
        sql "select count(*) from date"
        // cu compaction
        logger.info("run compaction:" + originTabletId)
        (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, originTabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        boolean running = true
        do {
            Thread.sleep(100)
            (code, out, err) = be_get_compaction_status(injectBe.Host, injectBe.HttpPort, originTabletId)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    try {
        load_delete_compaction()
        load_delete_compaction()
        load_delete_compaction()
        

        sleep(1000)

        DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, injectName)
        sql "ALTER TABLE date MODIFY COLUMN d_holidayfl bigint(11)"
        sleep(5000) 
        array = sql_return_maparray("SHOW TABLETS FROM date")


        // base compaction
        logger.info("run compaction:" + originTabletId)
        (code, out, err) = be_run_base_compaction(injectBe.Host, injectBe.HttpPort, originTabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)


        // wait for all compactions done
        boolean running = true
        while (running) {
            Thread.sleep(100)
            (code, out, err) = be_get_compaction_status(injectBe.Host, injectBe.HttpPort, originTabletId)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        }
        def newTabletId = array[1].TabletId
        logger.info("run compaction:" + newTabletId)
        (code, out, err) = be_run_base_compaction(injectBe.Host, injectBe.HttpPort, newTabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertTrue(out.contains("invalid tablet state."))
        
    } finally {
        if (injectBe != null) {
            DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, injectName)
        }
        int max_try_time = 3000
        while (max_try_time--){
            result = getJobState("date")
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }
        for (int i = 0; i < 5; i++) {
            load_date_once("date");
        }
        def count = sql """ select count(*) from date; """
        assertEquals(count[0][0], 20448);
        // check rowsets
        logger.info("run show:" + originTabletId)
        (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, originTabletId)
        logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
        assertTrue(out.contains("[0-1]"))
        assertTrue(out.contains("[2-7]"))

        logger.info("run show:" + newTabletId)
        (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, newTabletId)
        logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
        assertTrue(out.contains("[0-1]"))
        assertTrue(out.contains("[2-2]"))
        assertTrue(out.contains("[7-7]"))
        
        // base compaction
        logger.info("run compaction:" + newTabletId)
        (code, out, err) = be_run_base_compaction(injectBe.Host, injectBe.HttpPort, newTabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)


        // wait for all compactions done
        boolean running = true
        while (running) {
            Thread.sleep(100)
            (code, out, err) = be_get_compaction_status(injectBe.Host, injectBe.HttpPort, newTabletId)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        }

        logger.info("run compaction:" + newTabletId)
        (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, newTabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)

        running = true
        while (running) {
            Thread.sleep(100)
            (code, out, err) = be_get_compaction_status(injectBe.Host, injectBe.HttpPort, newTabletId)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        }

        logger.info("run show:" + newTabletId)
        (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, newTabletId)
        logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
        assertTrue(out.contains("[0-1]"))
        assertTrue(out.contains("[2-7]"))
        assertTrue(out.contains("[8-12]"))
    }

}