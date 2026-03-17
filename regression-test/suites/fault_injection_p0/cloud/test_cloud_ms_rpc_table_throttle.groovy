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

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cloud_ms_rpc_table_throttle', 'docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.beConfigs += [
        'enable_ms_backpressure_handling=true',
        // Short intervals for faster test feedback
        'ms_backpressure_upgrade_interval_ms=2000',
        'ms_backpressure_downgrade_interval_ms=5000',
        'ms_backpressure_upgrade_top_k=2',
        'ms_backpressure_throttle_ratio=0.5',
        'ms_rpc_table_qps_limit_floor=1.0',
    ]

    def token = "greedisgood9999"

    def ms_inject_suite = { msHttpPort, suiteName ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=apply_suite&name=${suiteName}"
            check { respCode, body ->
                log.info("apply suite ${suiteName} resp: ${body} ${respCode}".toString())
            }
        }
    }

    def ms_inject_enable = { msHttpPort ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=enable"
            check { respCode, body ->
                log.info("enable inject resp: ${body} ${respCode}".toString())
            }
        }
    }

    def ms_inject_change_args = { msHttpPort, key, value ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=set&name=${key}&behavior=change_args&value=${value}"
            check { respCode, body ->
                log.info("set inject args ${key}=${value} resp: ${body} ${respCode}".toString())
            }
        }
    }

    def ms_inject_clear = { msHttpPort ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=clear"
            check { respCode, body ->
                log.info("clear inject resp: ${body} ${respCode}".toString())
            }
        }
    }

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms addr={}, port={}, endpoint={}", ms.host, ms.httpPort, msHttpPort)

        def tableName1 = "throttle_test_tbl1"
        def tableName2 = "throttle_test_tbl2"

        sql """ DROP TABLE IF EXISTS ${tableName1} FORCE """
        sql """ DROP TABLE IF EXISTS ${tableName2} FORCE """
        sql """
            CREATE TABLE ${tableName1} (
                `k` INT NOT NULL,
                `v` INT NOT NULL
            ) DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 5
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE TABLE ${tableName2} (
                `k` INT NOT NULL,
                `v` INT NOT NULL
            ) DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 5
            PROPERTIES ("replication_num" = "1")
        """

        // Phase 0: Initially no throttlers
        def result = sql_return_maparray """
            SELECT * FROM information_schema.backend_ms_rpc_table_throttlers
        """
        logger.info("Phase 0 - initial throttlers: ${result}")
        assertEquals(0, result.size())

        // Generate RPC traffic so bvar QPS counters have data
        for (int i = 0; i < 5; i++) {
            sql """ INSERT INTO ${tableName1} VALUES (${i}, ${i * 10}) """
        }
        for (int i = 0; i < 5; i++) {
            sql """ INSERT INTO ${tableName2} VALUES (${i}, ${i * 20}) """
        }
        sleep(1000)

        // Phase 1: Enable MS injection to return MAX_QPS_LIMIT
        logger.info("Phase 1 - enabling MS injection to trigger backpressure upgrade")
        ms_inject_suite.call(msHttpPort, "MetaServiceProxy::inject_max_qps_limit")
        ms_inject_enable.call(msHttpPort)
        // Set injection probability to 100% for deterministic testing
        ms_inject_change_args.call(msHttpPort,
                "MetaServiceProxy::call_impl::inject_max_qps_limit.set_p",
                URLEncoder.encode('[0.5]', "UTF-8"))

        // Generate traffic to trigger MS_BUSY -> upgrade
        def insertThreads = []
        for (int i = 100; i < 106; i++) {
            def val = i
            insertThreads << Thread.start {
                try {
                    sql """ INSERT INTO ${tableName1} VALUES (${val}, ${val}) """
                } catch (Exception e) {
                    logger.info("Insert failed (expected during throttle): ${e.getMessage()}")
                }
            }
        }
        for (int i = 100; i < 106; i++) {
            def val = i
            insertThreads << Thread.start {
                try {
                    sql """ INSERT INTO ${tableName2} VALUES (${val}, ${val}) """
                } catch (Exception e) {
                    logger.info("Insert failed (expected during throttle): ${e.getMessage()}")
                }
            }
        }
        sleep(3000)

        // Disable MS injection so subsequent operations succeed
        ms_inject_clear.call(msHttpPort)

        // Phase 2: Verify throttlers exist after upgrade
        def upgraded = false
        for (int attempt = 0; attempt < 5; attempt++) {
            result = sql_return_maparray """
                SELECT * FROM information_schema.backend_ms_rpc_table_throttlers
            """
            logger.info("Phase 2 - attempt ${attempt}, throttlers: ${result}")
            if (result.size() > 0) {
                upgraded = true
                break
            }
            sleep(2000)
        }
        assertTrue(upgraded, "Expected throttle entries after backpressure upgrade")

        for (def row : result) {
            logger.info("  Throttled: BE_ID=${row.BE_ID}, TABLE_ID=${row.TABLE_ID}, " +
                        "RPC_TYPE=${row.RPC_TYPE}, QPS_LIMIT=${row.QPS_LIMIT}, " +
                        "CURRENT_QPS=${row.CURRENT_QPS}")
            assertTrue(Double.parseDouble(row.QPS_LIMIT.toString()) > 0,
                       "QPS_LIMIT should be positive")
        }

        insertThreads.each { it.join() }

        // Phase 3: Wait for downgrade (no more MS_BUSY)
        logger.info("Phase 3 - waiting for throttle downgrade...")
        def downgraded = false
        for (int attempt = 0; attempt < 10; attempt++) {
            sleep(3000)
            result = sql_return_maparray """
                SELECT * FROM information_schema.backend_ms_rpc_table_throttlers
            """
            logger.info("Phase 3 - attempt ${attempt}, throttlers: ${result}")
            if (result.size() == 0) {
                downgraded = true
                break
            }
        }
        assertTrue(downgraded, "Expected throttle entries to be removed after downgrade")
        logger.info("Phase 3 - downgrade confirmed, all throttle entries removed")

        // Phase 4: Verify normal operation resumes
        sql """ INSERT INTO ${tableName1} VALUES (999, 999) """
        sql """ INSERT INTO ${tableName2} VALUES (999, 999) """
        def cnt1 = sql """ SELECT COUNT(*) FROM ${tableName1} """
        def cnt2 = sql """ SELECT COUNT(*) FROM ${tableName2} """
        logger.info("Phase 4 - final row counts: tbl1=${cnt1[0][0]}, tbl2=${cnt2[0][0]}")
        assertTrue(Integer.parseInt(cnt1[0][0].toString()) > 0)
        assertTrue(Integer.parseInt(cnt2[0][0].toString()) > 0)
    }
}
