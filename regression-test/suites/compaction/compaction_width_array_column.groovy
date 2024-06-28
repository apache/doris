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

suite('compaction_width_array_column', "p2") {
    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]
    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))

    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    def s3BucketName = getS3BucketName()
    def random = new Random();

    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()

    // set fe configuration
    sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

    def tableName = "column_witdh_array"

    def table_create_task = { table_name ->
        // drop table if exists
        sql """drop table if exists ${table_name}"""
        // create table
        def create_table = new File("""${context.file.parent}/ddl/${table_name}.sql""").text
        create_table = create_table.replaceAll("\\\$\\{table\\_name\\}", table_name)
        sql create_table
    }

    def table_load_task = { table_name ->
        uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        loadLabel = table_name + "_" + uniqueID
        //loadLabel = table_name + '_load_5'
        loadSql = new File("""${context.file.parent}/ddl/${table_name}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel)
        loadSql = loadSql.replaceAll("\\\$\\{table\\_name\\}", table_name)
        nowloadSql = loadSql + s3WithProperties
        try_sql nowloadSql

        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            logger.info("load result is ${stateResult}")
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    table_create_task(tableName)
    table_load_task(tableName)

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    boolean isOverLap = true
    int tryCnt = 0;
    while (isOverLap && tryCnt < 3) {
        isOverLap = false

        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
        }

        // wait for all compactions done
        for (def tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                logger.info("rowset info" + rowset)
                String overLappingStr = rowset.split(" ")[3]
                if (overLappingStr == "OVERLAPPING") {
                    isOverLap = true;
                }
                logger.info("is over lap " + isOverLap + " " + overLappingStr)
            }
        }
        tryCnt++;
    }

    assertFalse(isOverLap);
}
