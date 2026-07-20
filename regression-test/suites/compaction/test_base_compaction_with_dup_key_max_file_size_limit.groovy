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

suite("test_base_compaction_with_dup_key_max_file_size_limit", "p2,nonConcurrent") {
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
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (rcode, rout, rerr) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(rout.contains("OK"))
        }
    }

    def reset_be_param = { paramName ->
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def originalValue = backendId_to_params.get(id).get(paramName)
            def (rcode, rout, rerr) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, originalValue))
            assertTrue(rout.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (rcode, rout, rerr) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertEquals(rcode, 0)
            assertTrue(rout.contains(paramName))
            def resultList = parseJson(rout)[0]
            assertTrue(resultList.size() == 4)
            backendId_to_params.get(id, [:]).put(paramName, resultList[2])
        }
    }

    get_be_param("disable_auto_compaction")
    get_be_param("base_compaction_dup_key_max_file_size_mbytes")

    try {
        // This test expects manual base compaction to be rejected with E-808 after building a
        // single large base rowset. Background auto compaction can reshape those rowsets, so keep
        // it disabled cluster-wide while the case is running.
        set_be_param("disable_auto_compaction", "true")
        // The encoded size of the 15M-row TPCH customer load can be below the default 1GB limit.
        // Lower the limit so the generated base rowset deterministically hits the size gate.
        set_be_param("base_compaction_dup_key_max_file_size_mbytes", "100")

        def triggerCompaction = { be_host, be_http_port, compact_type, tablet_id ->
            // trigger compactions for all tablets in ${tableName}
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=${compact_type}")

            String command = sb.toString()
            logger.info(command)
            def process = command.execute()
            def ccode = process.waitFor()
            def cerr = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            def cout = process.getText()
            logger.info("Run compaction: code=" + ccode + ", out=" + cout + ", err=" + cerr)
            assertEquals(ccode, 0)
            return cout
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
        trigger_and_wait_compaction(tableName, "cumulative")

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
        trigger_and_wait_compaction(tableName, "cumulative")

        // The conditions for base compaction have been satisfied.
        // Since the size of first input rowset is 0, there is no file size limitation. (maybe fix it?)
        // rowsets:
        //      [0-3] 2G nooverlapping
        // cp: 4
        trigger_and_wait_compaction(tableName, "base")

        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G overlapping
        // cp: 4
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G nooverlapping
        // cp: 5
        trigger_and_wait_compaction(tableName, "cumulative")

        // Due to the limit of config::base_compaction_dup_key_max_file_size_mbytes (100MB, set
        // above), the first input rowset exceeds it, so base compaction can not run and returns
        // E-808.
        // rowsets:
        //      [0-3] nooverlapping
        //      [4-4] nooverlapping
        // cp: 5
        // WHAT: replace with plugin and handle fail?
        assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "base", tablet_id).contains("E-808"));

        def rowCount = sql "select count(*) from ${tableName}"
        assertTrue(rowCount[0][0] != rows)
    } finally {
        reset_be_param("disable_auto_compaction")
        reset_be_param("base_compaction_dup_key_max_file_size_mbytes")
    }
}
