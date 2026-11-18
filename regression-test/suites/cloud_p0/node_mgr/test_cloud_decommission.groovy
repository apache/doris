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
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions
import java.util.concurrent.TimeUnit

suite("cloud_decommission", 'p0, docker') {
    if (!isCloudMode()) {
        return
    }

    def checkStatus = { ms, decommissionBeUniqueId, decommissionBe ->
        boolean found = false
        awaitUntil(600) {
            found = false
            def resp = get_cluster.call(decommissionBeUniqueId, ms)
            resp.each { cluster ->
                cluster.nodes.each { node ->
                    if (node.ip as String == decommissionBe.Host as String && node.heartbeat_port as Integer == decommissionBe.HeartbeatPort as Integer && node.status as String == "NODE_STATUS_DECOMMISSIONED") {
                        found = true
                    }
                }
            }
            found
        }

        assertTrue(found)
    }

    def dropAndCheckBe = { host, heartbeatPort ->
        sql """ ALTER SYSTEM DROPP BACKEND "${host}:${heartbeatPort}" """
        awaitUntil(600) {
            def result = sql_return_maparray """ SHOW BACKENDS """ 
            log.info("show backends result {}", result)
            def ret = result.find {it.Host == host && it.HeartbeatPort == heartbeatPort}
            ret == null
        }
    }

    def checkDecommission = { Closure beforeDecommissionActionSupplier, Closure afterDecommissionActionSupplier, int beNum ->
        def begin = System.currentTimeMillis()
        setFeConfig("cloud_balance_tablet_percent_per_run", 0.5)

        // in docker,be's cluster name
        sql """ use @compute_cluster """

        def result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM decommission_table """
        assertEquals(result.size(), beNum)
        awaitUntil(600) {
            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM decommission_table """
            if (beNum == 3) {
                result.every { Integer.valueOf((String) it.ReplicaNum) >= 15 && Integer.valueOf((String) it.ReplicaNum) <= 17 }
            } else {
                // beNum == 2
                result.every { Integer.valueOf((String) it.ReplicaNum) >= 23 && Integer.valueOf((String) it.ReplicaNum) <= 25 }
            }
        }

        if (beforeDecommissionActionSupplier) {
            beforeDecommissionActionSupplier()
        }
        // idx = 1, http decommission one be 
        def decommissionBeFirstIdx = 1
        def decommissionBeFirst = cluster.getBeByIndex(decommissionBeFirstIdx)
        log.info(" decommissionBeFirst: {} ", decommissionBeFirst.host)
        def showBes = sql_return_maparray """SHOW BACKENDS"""
        log.info(" showBes: {} ", showBes)
        def firstDecommissionBe = showBes.find {
            it.Host == decommissionBeFirst.host
        }
        assertNotNull(firstDecommissionBe)
        log.info("first decommission be {}", firstDecommissionBe)
        def jsonSlurper = new JsonSlurper()
        def jsonObject = jsonSlurper.parseText(firstDecommissionBe.Tag)
        String firstDecommissionBeCloudClusterId = jsonObject.compute_group_id
        String firstDecommissionBeUniqueId = jsonObject.cloud_unique_id
        String firstDecommissionBeClusterName = jsonObject.compute_group_name

        def ms = cluster.getAllMetaservices().get(0)
        logger.info("ms addr={}, port={}", ms.host, ms.httpPort)
        d_node.call(firstDecommissionBeUniqueId, firstDecommissionBe.Host, firstDecommissionBe.HeartbeatPort,
                firstDecommissionBeClusterName, firstDecommissionBeCloudClusterId, ms)

        awaitUntil(600) {
            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM decommission_table """ 
            result.any { Integer.valueOf((String) it.ReplicaNum) == 0 }
        }

        checkStatus(ms, firstDecommissionBeUniqueId, firstDecommissionBe)

        result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM decommission_table """ 
        assertEquals(result.size(), beNum)
        for (row : result) {
            log.info("replica distribution: ${row} ".toString())
        }
        if (afterDecommissionActionSupplier) {
            afterDecommissionActionSupplier()
        }

        // Drop the selected backend
        dropAndCheckBe(firstDecommissionBe.Host, firstDecommissionBe.HeartbeatPort)

        if (beforeDecommissionActionSupplier) {
            beforeDecommissionActionSupplier()
        }
        // idx = 2, sql node decommission one be
        def decommissionBeSecondIdx = 2
        def secondDecommissionBe = cluster.getBeByIndex(decommissionBeSecondIdx)

        // Decommission the selected backend
        sql """ ALTER SYSTEM DECOMMISSION BACKEND "$secondDecommissionBe.Host:$secondDecommissionBe.HeartbeatPort" """

        result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM decommission_table """ 
        assertEquals(result.size(), beNum - 1)

        awaitUntil(600) {
            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM decommission_table """ 
            log.info("show replica result {}", result)
            def ret = result.findAll { Integer.valueOf((String) it.ReplicaNum) == 0 }
            log.info("ret {}", ret)
            // be has been droped
            ret.size() == beNum - 2
        }
        def secondDecommissionBeUniqueId = firstDecommissionBeUniqueId
        checkStatus(ms, secondDecommissionBeUniqueId, secondDecommissionBe)

        for (row : result) {
            log.info("replica distribution: ${row} ".toString())
        }
        if (afterDecommissionActionSupplier) {
            afterDecommissionActionSupplier()
        }
        // Drop the selected backend
        dropAndCheckBe(secondDecommissionBe.Host, secondDecommissionBe.HeartbeatPort)

        def showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
        log.info("show compute group {}", showComputeGroup)
        // when 2 bes are decommissioned, the compute group will be deleted
        def bes = sql_return_maparray """ SHOW BACKENDS """
        if (bes.size() == 0) {
            assertEquals(0, showComputeGroup.size())
        }

        System.currentTimeMillis() - begin
    }

    def checkDifferentAction = { Closure beforeDecommissionActionSupplier, Closure afterDecommissionActionSupplier, int atLeastCost, int waitTime, int beNum ->
        def begin = System.currentTimeMillis()
        def cost = checkDecommission.call(beforeDecommissionActionSupplier, afterDecommissionActionSupplier, beNum)
        log.info("in check, inner cost {}", cost)
        cost = System.currentTimeMillis() - begin
        log.info("in check, outter cost {}", cost)
        // assertTrue(waitTime > atLeastCost)
        // decommission 2 bes
        assertTrue(cost >= 2 * waitTime)
        cost
    }

    def checkWal = { int atLeastCost, int beNum -> 
        def futrue = null
        // 25s
        def waitTime = 25 * 1000
        
        Closure beforeClosure = {
            log.info("before wal closure")
            GetDebugPoint().enableDebugPointForAllFEs("FE.GET_ALL_WAL_QUEUE_SIZE", [value:5])
            futrue = thread {
                Thread.sleep(waitTime)
                cluster.clearFrontendDebugPoints()
            }
        } as Closure

        Closure afterClosure = {
            log.info("after wal closure")
            assertNotNull(futrue)
            futrue.get()
        } as Closure

        checkDifferentAction(beforeClosure, afterClosure, atLeastCost as int, waitTime as int, beNum)
    }

    def checkTxnNotFinish = { int atLeastCost, int beNum -> 
        def futrue = null
        def waitTime = 30 * 1000
        // check txn not finish 
        Closure beforeClosure = {
            log.info("before insert closure")
            // after waitTime insert finish
            futrue = thread {
                // insert waitTime seconds
                for (int i = 1; i <= waitTime / 1000; i++) {
                    Thread.sleep(1 * 1000)
                    sql """insert into decommission_table values ($i + 1, $i * 2, $i + 3)"""
                }
            }
        }
        Closure afterClosure = { ->
            log.info("after insert closure")
            assertNotNull(futrue)
            futrue.get()
        }
        checkDifferentAction(beforeClosure, afterClosure, atLeastCost as int, waitTime as int, beNum)
    }

    def createTestTable = { ->
        sql """
            CREATE TABLE decommission_table (
            class INT,
            id INT,
            score INT SUM
            )
            AGGREGATE KEY(class, id)
            DISTRIBUTED BY HASH(class) BUCKETS 48
        """ 
    }

    def checkFENormalAfterRestart = {
        cluster.restartFrontends()
        def reconnectFe = {
            sleep(5000)
            logger.info("Reconnecting to a new frontend...")
            def newFe = cluster.getMasterFe()
            if (newFe) {
                logger.info("New frontend found: ${newFe.host}:${newFe.httpPort}")
                def url = String.format(
                        "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                        newFe.host, newFe.queryPort)
                url = context.config.buildUrlWithDb(url, context.dbName)
                context.connectTo(url, context.config.jdbcUser, context.config.jdbcPassword)
                logger.info("Successfully reconnected to the new frontend")
            } else {
                logger.error("No new frontend found to reconnect")
            }
        }
        reconnectFe()
        def ret = sql """show frontends;"""
        assertEquals(2, ret.size())
    }

    // Keep the target RPC in flight while a node-state update is published.
    // Both builds use the same gates: the old one publishes a missing node,
    // while the fixed one publishes the same node with its new status.
    for (String operation : ["decommission_node", "notify_decommissioned"]) {
        def atomicOptions = new ClusterOptions()
        atomicOptions.setFeNum(1)
        atomicOptions.setBeNum(1)
        atomicOptions.cloudMode = true
        atomicOptions.feConfigs += ['cloud_tablet_rebalancer_interval_second=3600']
        docker(atomicOptions) {
            sql "USE @compute_cluster"
            sql "DROP TABLE IF EXISTS decommission_atomic_table"
            sql """CREATE TABLE decommission_atomic_table (id INT)
                   DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num"="1")"""
            // Wait for the automatic decommission checker to finish its first round.
            // Its next round is an hour away, so only this test changes node status.
            def feLog = new File(cluster.getFeByIndex(1).getLogFilePath())
            awaitUntil(60) { feLog.text.contains("rebalancer sche interval 3600 s") }

            def be = (sql_return_maparray("SHOW BACKENDS"))[0]
            def tag = new JsonSlurper().parseText(be.Tag as String)
            def ms = cluster.getAllMetaservices().get(0)
            def msLog = new File(ms.getLogFilePath())
            def nodePoint = "ResourceManager::node_update:after_refresh"
            def commitPoint = "commit_rowset::before_get_instance_id:${tag.cloud_unique_id}"
            def inject = { String action, String point = "" ->
                httpTest {
                    op "get"
                    endpoint "${ms.host}:${ms.httpPort}"
                    uri "/MetaService/http/v1/injection_point?token=${token}&op=${action}&name=${point}&behavior=wait"
                    check { code, response ->
                        assertEquals(200, code)
                        assertEquals("OK", parseJson(response).code)
                    }
                }
            }
            def waitForPoint = { String point, long offset ->
                awaitUntil(60) {
                    def log = new RandomAccessFile(msLog, "r")
                    try {
                        log.seek(offset)
                        byte[] bytes = new byte[(int) (log.length() - offset)]
                        log.readFully(bytes)
                        new String(bytes, "UTF-8").contains("injection point waiting, point=${point}")
                    } finally {
                        log.close()
                    }
                }
                logger.info("Reached MS injection point {}", point)
            }
            def alterStatus = { String action ->
                httpTest {
                    endpoint "${ms.host}:${ms.httpPort}"
                    uri "/MetaService/http/${action}?token=${token}"
                    body JsonOutput.toJson([
                        instance_id: context.config.instanceId,
                        cluster: [
                            type: "COMPUTE",
                            cluster_id: tag.compute_group_id,
                            cluster_name: tag.compute_group_name,
                            nodes: [[cloud_unique_id: tag.cloud_unique_id,
                                     ip: be.Host, heartbeat_port: be.HeartbeatPort as int]]
                        ]
                    ])
                    check { code, response ->
                        assertEquals(200, code)
                        assertEquals("OK", parseJson(response).code)
                    }
                }
            }
            def load = null
            def alter = null
            inject("enable")
            try {
                long loadOffset = msLog.length()
                inject("set", commitPoint)
                load = thread { sql "INSERT INTO decommission_atomic_table VALUES (1)" }
                waitForPoint(commitPoint, loadOffset)
                if (operation == "notify_decommissioned") {
                    alterStatus("decommission_node")
                }

                long updateOffset = msLog.length()
                inject("set", nodePoint)
                alter = thread { alterStatus(operation) }
                waitForPoint(nodePoint, updateOffset)
                // The node-state request stays blocked until the load finishes.
                inject("clear", commitPoint)
                load.get(60, TimeUnit.SECONDS)
                if (operation == "decommission_node") {
                    qt_decommission_atomic "SELECT count(*), sum(id) FROM decommission_atomic_table"
                } else {
                    qt_decommissioned_atomic "SELECT count(*), sum(id) FROM decommission_atomic_table"
                }
            } finally {
                inject("clear", commitPoint)
                inject("clear", nodePoint)
                inject("disable")
                if (load != null && !load.isDone()) {
                    load.get(60, TimeUnit.SECONDS)
                }
                if (alter != null) {
                    alter.get(60, TimeUnit.SECONDS)
                }
            }
        }
    }

    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
    ]

    for (int i = 0; i < clusterOptions.size(); i++) {
        log.info("begin {} step", i + 1)
        clusterOptions[i].feConfigs += [
            'sys_log_verbose_modules=org',
            'heartbeat_interval_second=1',
            'cloud_tablet_rebalancer_interval_second=1',
            'cloud_cluster_check_interval_second=1'
        ]
        clusterOptions[i].beConfigs += [
            'sys_log_verbose_modules=*',
        ]
        clusterOptions[i].setFeNum(2)
        // cluster has 3 bes
        // cluster has 2 bes, after decommission 2 nodes, and drop 2 nodes, compute group name will be delete from fe
        int beNum = i == 0 ? 3 : 2
        clusterOptions[i].setBeNum(beNum)
        clusterOptions[i].cloudMode = true
        // clusterOptions[i].connectToFollower = true
        clusterOptions[i].enableDebugPoints()

        def noWalCheckCost = 0
        docker(clusterOptions[i]) {
            createTestTable.call()
            noWalCheckCost = checkDecommission(null, null, beNum)
            checkFENormalAfterRestart.call()
        }
        log.info("no wal check cost {}", noWalCheckCost)

        def walCheckCost = 0
        docker(clusterOptions[i]) {
            createTestTable.call()
            walCheckCost = checkWal(noWalCheckCost as int, beNum as int)
            checkFENormalAfterRestart.call()
        }
        log.info("wal check cost {}", walCheckCost)

        def txnCheckCost = 0
        docker(clusterOptions[i]) {
            createTestTable.call()
            txnCheckCost = checkTxnNotFinish(noWalCheckCost as int, beNum as int)
            checkFENormalAfterRestart.call()
        } 
        log.info("txn check cost {}", txnCheckCost)
        log.info("finish {} step", i + 1)
    }
}
