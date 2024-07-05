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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_base_compaction_with_dup_key_max_file_size_limit", "p2") {
    def tableName = "test_base_compaction_with_dup_key_max_file_size_limit"
 
    // use customer table of tpch_sf100
    def rows = 15000000
    def load_tpch_sf100_customer = { 
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString() 
        def rowCount = sql "select count(*) from ${tableName}"
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
        sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"
        if (rowCount[0][0] != rows) {
            def loadLabel = tableName + "_" + uniqueID
            // load data from cos
            def loadSql = """
            LOAD LABEL ${loadLabel}(
                DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/customer.tbl")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY "|"
                (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
            )
            """
            loadSql = loadSql + s3WithProperties
            sql loadSql

            // check load state
            while (true) {
                def stateResult = sql "show load where Label = '${loadLabel}'"
                logger.info("load result is ${stateResult}")
                def loadState = stateResult[stateResult.size() - 1][2].toString()
                if ("CANCELLED".equalsIgnoreCase(loadState)) {
                    throw new IllegalStateException("load ${loadLabel} failed.")
                } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                    rows += 15000000
                    break
                }
                sleep(5000)
            }
        } 
    }
    try {
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

        boolean disableAutoCompaction = true
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        def triggerCompaction = { be_host, be_http_port, compact_type, tablet_id ->
            // trigger compactions for all tablets in ${tableName}
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=${compact_type}")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", disableAutoCompaction " + disableAutoCompaction + ", err=" + err)
            if (!disableAutoCompaction) {
                return "Success, " + out
            }
            assertEquals(code, 0)
            return out
        } 

        def waitForCompaction = { be_host, be_http_port, tablet_id ->
            // wait for all compactions done
            boolean running = true
            do {
                Thread.sleep(1000)
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X GET http://${be_host}:${be_http_port}")
                sb.append("/api/compaction/run_status?tablet_id=")
                sb.append(tablet_id)

                String command = sb.toString()
                logger.info(command)
                process = command.execute()
                code = process.waitFor()
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1", "disable_auto_compaction" = "true"
            )
        """ 

        def tablet = (sql_return_maparray """ show tablets from ${tableName}; """)[0]
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G overlapping
        // cp: -1
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G nooverlapping
        // cp: 3
        assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"));
        waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)


        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G nooverlapping
        //      [3-3] 1G overlapping
        // cp: 3
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G nooverlapping
        //      [3-3] 1G nooverlapping
        // cp: 4 
        assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"));
        waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)


        // The conditions for base compaction have been satisfied.
        // Since the size of first input rowset is 0, there is no file size limitation. (maybe fix it?)
        // rowsets:
        //      [0-3] 2G nooverlapping
        // cp: 4 
        assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "base", tablet_id).contains("Success"));
        waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id) 


        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G overlapping
        // cp: 4
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G nooverlapping
        // cp: 5
        assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"));
        waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)


        // Due to the limit of config::base_compaction_dup_key_max_file_size_mbytes(1G),
        // can not do base compaction, return E-808
        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G nooverlapping
        // cp: 5
        assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "base", tablet_id).contains("E-808"));

        def rowCount = sql "select count(*) from ${tableName}"
        assertTrue(rowCount[0][0] != rows)
    } finally {
    }
}
