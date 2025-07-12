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
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

// 1 create two physical cluster c1, c2, every cluster contains 2 be
// 2 create vcg, c1, c2 are sub compute group of vcg, adn c1 is active cg
// 3 use vcg
// 4 stop 2 bes of c1
// 5 start 2 bes of c1
// 6 long-term stop 2 bes of c1

suite('vcg_auto_failover', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    String tableName = "test_all_vcluster"
    String tbl = "test_virtual_compute_group_tbl"

    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
    ]
    options.feNum = 3
    options.cloudMode = true

    def add_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/add_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def alter_cluster_info_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/alter_vcluster_info?token=$token"
            body request_body
            check check_func
        }
    }

    options.connectToFollower = false

    for (def j = 0; j < 2; j++) {
        docker(options) {
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

            def clusterName1 = "newcluster1"
            // add cluster newcluster1
            cluster.addBackend(2, clusterName1)

            def clusterName2 = "newcluster2"
            // add cluster newcluster2
            cluster.addBackend(2, clusterName2)

            // add vcluster
            def normalVclusterName = "normalVirtualClusterName"
            def normalVclusterId = "normalVirtualClusterId"
            def vcgClusterNames = [clusterName1, clusterName2]
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName1}", standby_cluster_names: ["${clusterName2}"], failover_failure_threshold: 10]
            clusterMap = [cluster_name: "${normalVclusterName}", cluster_id:"${normalVclusterId}", type:"VIRTUAL", cluster_names:vcgClusterNames, cluster_policy:clusterPolicy]
            def normalInstance = [instance_id: "${instance_id}", cluster: clusterMap]
            jsonOutput = new JsonOutput()
            def normalVcgBody = jsonOutput.toJson(normalInstance)
            add_cluster_api.call(msHttpPort, normalVcgBody) {
                respCode, body ->
                    log.info("add normal vitural compute group http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            // show cluster
            sleep(5000)
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            def vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            assertTrue(vcgInShow.Policy.contains("activeComputeGroup='newcluster1', standbyComputeGroup='newcluster2'"))

            showResult = sql "show clusters"
            for (row : showResult) {
                println row
            }
            showResult = sql "show backends"
            for (row : showResult) {
                println row
            }

            // get be ip of clusterName1
            def jsonSlurper = new JsonSlurper()
            def cluster1Ips = showResult.findAll { entry ->
                def raw = entry[19]
                def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw
                info.compute_group_name == clusterName1
            }.collect { entry ->
                entry[1]
            }
            log.info("backends of cluster1: ${clusterName1} ${cluster1Ips}".toString())

            def cluster2Ips = showResult.findAll { entry ->
                def raw = entry[19]
                def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw
                info.compute_group_name == clusterName2
            }.collect { entry ->
                entry[1]
            }
            log.info("backends of cluster2: ${clusterName2} ${cluster2Ips}".toString())

            sql """use @${normalVclusterName}"""
            sql """ drop table if exists ${tableName} """

            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                  `k1` int(11) NULL,
                  `k2` tinyint(4) NULL,
                  `k3` smallint(6) NULL,
                  `k4` bigint(20) NULL,
                  `k5` largeint(40) NULL,
                  `k6` float NULL,
                  `k7` double NULL,
                  `k8` decimal(9, 0) NULL,
                  `k9` char(10) NULL,
                  `k10` varchar(1024) NULL,
                  `k11` text NULL,
                  `k12` date NULL,
                  `k13` datetime NULL
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            """

            sql """
                CREATE TABLE ${tbl} (
                  `k1` int(11) NULL,
                  `k2` char(5) NULL
                )
                DUPLICATE KEY(`k1`, `k2`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                PROPERTIES (
                "replication_num"="1"
                )
            """

            sql """ set enable_profile = true """

            cluster.stopBackends(4, 5)

            before_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
            log.info("before_cluster2_be0_load_rows : ${before_cluster2_be0_load_rows}".toString())
            before_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
            log.info("before_cluster2_be0_flush : ${before_cluster2_be0_flush}".toString())

            before_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
            log.info("before_cluster2_be1_load_rows : ${before_cluster2_be1_load_rows}".toString())
            before_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
            log.info("before_cluster2_be1_flush : ${before_cluster2_be1_flush}".toString())

            txnId = -1;
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'cloud_cluster', 'normalVirtualClusterName'

                file 'all_types.csv'
                time 10000 // limit inflight 10s
                setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

                check { loadResult, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${loadResult}".toString())
                    def json = parseJson(loadResult)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                    txnId = json.TxnId
                }
            }

            sql """
                insert into ${tbl} (k1, k2) values (1, "10");
            """

            sql "sync"
            order_qt_all11 "SELECT count(*) FROM ${tableName}" // 20
            order_qt_all12 "SELECT count(*) FROM ${tableName} where k1 <= 10"  // 11

            after_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
            log.info("after_cluster2_be0_load_rows : ${after_cluster2_be0_load_rows}".toString())
            after_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
            log.info("after_cluster2_be0_flush : ${after_cluster2_be0_flush}".toString())

            after_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
            log.info("after_cluster2_be1_load_rows : ${after_cluster2_be1_load_rows}".toString())
            after_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
            log.info("after_cluster2_be1_flush : ${after_cluster2_be1_flush}".toString())

            assertTrue(before_cluster2_be0_load_rows < after_cluster2_be0_load_rows || before_cluster2_be1_load_rows < after_cluster2_be1_load_rows)
            assertTrue(before_cluster2_be0_flush < after_cluster2_be0_flush || before_cluster2_be1_flush < after_cluster2_be1_flush)

            set = [cluster2Ips[0] + ":" + "8060", cluster2Ips[1] + ":" + "8060"] as Set
            sql """ select count(k2) AS theCount, k3 from test_all_vcluster group by k3 order by theCount limit 1 """
            checkProfileNew.call(set)

            sleep(16000)
            // show cluster
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            log.info("policy {}", vcgInShow.Policy)
            assertTrue(vcgInShow.Policy.contains("activeComputeGroup='newcluster1', standbyComputeGroup='newcluster2'"))

            cluster.startBackends(4, 5)

            before_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
            log.info("before_cluster1_be0_load_rows : ${before_cluster1_be0_load_rows}".toString())
            before_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
            log.info("before_cluster1_be0_flush : ${before_cluster1_be0_flush}".toString())

            before_cluster1_be1_load_rows = get_be_metric(cluster1Ips[1], "8040", "load_rows");
            log.info("before_cluster1_be1_load_rows : ${before_cluster1_be1_load_rows}".toString())
            before_cluster1_be1_flush = get_be_metric(cluster1Ips[1], "8040", "memtable_flush_total");
            log.info("before_cluster1_be1_flush : ${before_cluster1_be1_flush}".toString())

            before_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
            log.info("before_cluster2_be0_load_rows : ${before_cluster2_be0_load_rows}".toString())
            before_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
            log.info("before_cluster2_be0_flush : ${before_cluster2_be0_flush}".toString())

            before_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
            log.info("before_cluster2_be1_load_rows : ${before_cluster2_be1_load_rows}".toString())
            before_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
            log.info("before_cluster2_be1_flush : ${before_cluster2_be1_flush}".toString())

            txnId = -1;
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'cloud_cluster', 'normalVirtualClusterName'

                file 'all_types.csv'
                time 10000 // limit inflight 10s
                setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

                check { loadResult, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${loadResult}".toString())
                    def json = parseJson(loadResult)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                    txnId = json.TxnId
                }
            }

            sql """
                insert into ${tbl} (k1, k2) values (1, "10");
            """

            sql "sync"
            order_qt_all11 "SELECT count(*) FROM ${tableName}" // 20
            order_qt_all12 "SELECT count(*) FROM ${tableName} where k1 <= 10"  // 11

            after_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
            log.info("after_cluster1_be0_load_rows : ${after_cluster1_be0_load_rows}".toString())
            after_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
            log.info("after_cluster1_be0_flush : ${after_cluster1_be0_flush}".toString())

            after_cluster1_be1_load_rows = get_be_metric(cluster1Ips[1], "8040", "load_rows");
            log.info("after_cluster1_be1_load_rows : ${after_cluster1_be1_load_rows}".toString())
            after_cluster1_be1_flush = get_be_metric(cluster1Ips[1], "8040", "memtable_flush_total");
            log.info("after_cluster1_be1_flush : ${after_cluster1_be1_flush}".toString())

            after_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
            log.info("after_cluster2_be0_load_rows : ${after_cluster2_be0_load_rows}".toString())
            after_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
            log.info("after_cluster2_be0_flush : ${after_cluster2_be0_flush}".toString())

            after_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
            log.info("after_cluster2_be1_load_rows : ${after_cluster2_be1_load_rows}".toString())
            after_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
            log.info("after_cluster2_be1_flush : ${after_cluster2_be1_flush}".toString())

            assertTrue(before_cluster1_be0_load_rows < after_cluster1_be0_load_rows || before_cluster1_be1_load_rows < after_cluster1_be1_load_rows)
            assertTrue(before_cluster1_be0_flush < after_cluster1_be0_flush || before_cluster1_be1_flush < after_cluster1_be1_flush)

            assertTrue(before_cluster2_be0_load_rows == after_cluster2_be0_load_rows)
            assertTrue(before_cluster2_be0_flush == after_cluster2_be0_flush)
            assertTrue(before_cluster2_be1_load_rows == after_cluster2_be1_load_rows)
            assertTrue(before_cluster2_be1_flush == after_cluster2_be1_flush)

            set = [cluster1Ips[0] + ":" + "8060", cluster1Ips[1] + ":" + "8060"] as Set
            sql """ select count(k2) AS theCount, k3 from test_all_vcluster group by k3 order by theCount limit 1 """
            checkProfileNew.call(set)

            cluster.stopBackends(4, 5)

            before_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
            log.info("before_cluster2_be0_load_rows : ${before_cluster2_be0_load_rows}".toString())
            before_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
            log.info("before_cluster2_be0_flush : ${before_cluster2_be0_flush}".toString())

            before_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
            log.info("before_cluster2_be1_load_rows : ${before_cluster2_be1_load_rows}".toString())
            before_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
            log.info("before_cluster2_be1_flush : ${before_cluster2_be1_flush}".toString())

            txnId = -1;
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'cloud_cluster', 'normalVirtualClusterName'

                file 'all_types.csv'
                time 10000 // limit inflight 10s
                setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

                check { loadResult, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${loadResult}".toString())
                    def json = parseJson(loadResult)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                    txnId = json.TxnId
                }
            }

            sql """
                insert into ${tbl} (k1, k2) values (1, "10");
            """

            sql "sync"
            order_qt_all11 "SELECT count(*) FROM ${tableName}" // 20
            order_qt_all12 "SELECT count(*) FROM ${tableName} where k1 <= 10"  // 11

            after_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
            log.info("after_cluster2_be0_load_rows : ${after_cluster2_be0_load_rows}".toString())
            after_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
            log.info("after_cluster2_be0_flush : ${after_cluster2_be0_flush}".toString())

            after_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
            log.info("after_cluster2_be1_load_rows : ${after_cluster2_be1_load_rows}".toString())
            after_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
            log.info("after_cluster2_be1_flush : ${after_cluster2_be1_flush}".toString())

            assertTrue(before_cluster2_be0_load_rows < after_cluster2_be0_load_rows || before_cluster2_be1_load_rows < after_cluster2_be1_load_rows)
            assertTrue(before_cluster2_be0_flush < after_cluster2_be0_flush || before_cluster2_be1_flush < after_cluster2_be1_flush)

            set = [cluster2Ips[0] + ":" + "8060", cluster2Ips[1] + ":" + "8060"] as Set
            sql """ select count(k2) AS theCount, k3 from test_all_vcluster group by k3 order by theCount limit 1 """
            checkProfileNew.call(set)

            sleep(60000)
            sql """
                insert into ${tbl} (k1, k2) values (1, "10");
            """

            // show cluster
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            assertTrue(vcgInShow.Policy.contains("activeComputeGroup='newcluster2', standbyComputeGroup='newcluster1'"))
        }
        // connect to follower, run again
        //options.connectToFollower = true
    }
}
