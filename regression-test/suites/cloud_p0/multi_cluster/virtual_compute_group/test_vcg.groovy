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

suite('test_vcg', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'fetch_cluster_cache_hotspot_interval_ms=600000',
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

    def checkWarmupJob = { job, warmUpIds, expectedSrc, expectedDst ->
        assertTrue(job.SyncMode.contains("EVENT_DRIVEN (LOAD)") || job.SyncMode.contains("PERIODIC (600s)"))
        if (warmUpIds.contains(job.JobId)) {
            // old
            assertTrue(job.Status.contains("CANCELLED") && (job.ErrMsg.contains("user cancel") || job.ErrMsg.contains("vcg cancel")))
        } else {
            // new
            assertTrue(job.SrcComputeGroup.contains(expectedSrc) && job.DstComputeGroup.contains(expectedDst))
            warmUpIds.add(job.JobId)
        }
    }

    options.connectToFollower = false
    def tbl = "test_virtual_compute_group_tbl"

    for (def j = 0; j < 2; j++) {
        docker(options) {
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

            def clusterName1 = "newcluster1"
            // 添加一个新的cluster newcluster1
            cluster.addBackend(2, clusterName1)

            def clusterName2 = "newcluster2"
            // 添加一个新的cluster newcluster2
            cluster.addBackend(1, clusterName2) 

            def clusterName3 = "newcluster3"
            // 添加一个新的cluster newcluster3
            cluster.addBackend(3, clusterName3) 

            // create test user
            def user = "regression_test_cloud_user"
            sql """create user ${user} identified by 'Cloud12345'"""
            sql """grant select_priv,load_priv on *.*.* to ${user}"""

            // add empty vcg, test not show in fe
            /*
                curl '175.43.101.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
                    "instance_id":"default_instance_id",
                    "cluster":{
                        "type":"VIRTUAL",
                        "cluster_name":"emptyClusterName",
                        "cluster_id":"emptyClusterId"
                    }
                }'
            */
            def vClusterNameEmpty = "emptyClusterName"
            def vClusterIdEmpty = "emptyClusterId"
            def instance_id = "default_instance_id"
            def clusterMap = [cluster_name: "${vClusterNameEmpty}", cluster_id:"${vClusterIdEmpty}", type:"VIRTUAL"]
            def instance = [instance_id: "${instance_id}", cluster: clusterMap]
            def jsonOutput = new JsonOutput()
            def addEmptyVComputeGroupBody  = jsonOutput.toJson(instance)
            add_cluster_api.call(msHttpPort, addEmptyVComputeGroupBody) {
                respCode, body ->
                    log.info("add empty vitural compute group http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            }
            // empty vcg, add succ, but not show in fe
            sleep(5000)
            def showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            def find = showComputeGroup.any { it.Name == vClusterNameEmpty }
            assertFalse(find)
            

            // add one vcg, test normal vcg
            // contain newCluster1, newCluster2

            /*
            curl '175.43.101.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
                "instance_id": "default_instance_id",
                "cluster": {
                    "type": "VIRTUAL",
                    "cluster_name": "normalVirtualClusterName",
                    "cluster_id": "normalVirtualClusterId",
                    "cluster_names": [
                        "newcluster1",
                        "newcluster2"
                    ],
                    "cluster_policy": {
                        "type": "ActiveStandby",
                        "active_cluster_name": "newcluster1",
                        "standby_cluster_names": [
                            "newcluster2"
                        ]
                    }
                }
            }'
            */
            
            def normalVclusterName = "normalVirtualClusterName"
            def normalVclusterId = "normalVirtualClusterId"
            def vcgClusterNames = [clusterName1, clusterName2]
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName1}", standby_cluster_names: ["${clusterName2}"]]
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
            /*
            [{IsCurrent=TRUE, BackendNum=3, Policy=, SubClusters=, Users=, Name=compute_cluster}, {IsCurrent=FALSE, BackendNum=2, Policy=, SubClu
    sters=, Users=, Name=newcluster1}, {IsCurrent=FALSE, BackendNum=1, Policy=, SubClusters=, Users=, Name=newcluster2}, {IsCurrent=FALSE, BackendNum=3, Policy=, SubClusters=, Users=, Name=newcluster3}, {IsCurrent=FALSE, BackendNum=0
    , Policy={policyType=ActiveStandby, activeComputeGroup='newcluster1', standbyComputeGroup='newcluster2', failoverFailureThreshold=0, unhealthyNodeThresholdPercent=0}, SubClusters=newcluster1, newcluster2, Users=, Name=normalVirtualClusterName}]
            */
            def vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }

            assertNotNull(vcgInShow)
            // {policyType=ActiveStandby, activeComputeGroup='newcluster1', standbyComputeGroup='newcluster2', failoverFailureThreshold=0, unhealthyNodeThresholdPercent=0}
            assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster1","standbyComputeGroup":"newcluster2"'))

            def showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
            log.info("show warm up after create {}", showWarmup)
            def warmUpIds = []
/*
 [{Status=RUNNING, AllBatch=0, Type=CLUSTER, FinishTime=null, ErrMsg=
       │ , DstComputeGroup=newcluster2, CreateTime=2025-05-11 20:38:48.479, SrcComputeGroup=newcluster1, StartTime=2025-05-11 20:38:48.479, FinishBatch=0, JobId=1746966973833, Sync
       │ Mode=EVENT_DRIVEN (LOAD)}, {Status=WAITING, AllBatch=0, Type=CLUSTER, FinishTime=2025-05-11 20:38:50.720, ErrMsg=, DstComputeGroup=newcluster2, CreateTime=2025-05-11 20:38
       │ :48.437, SrcComputeGroup=newcluster1, StartTime=2025-05-11 20:38:48.437, FinishBatch=1, JobId=1746966973832, SyncMode=PERIODIC (600s)}]
       */
            assertEquals(showWarmup.size(), 2)
            showWarmup.each {
                assertTrue(it.SyncMode.contains("EVENT_DRIVEN (LOAD)") || it.SyncMode.contains("PERIODIC (600s)"))
                if (it.SyncMode.contains("EVENT_DRIVEN (LOAD)")) {
                    assertTrue(it.SrcComputeGroup.contains("newcluster1") && it.DstComputeGroup.contains("newcluster2"))
                    warmUpIds.add(it.JobId)
                } else if (it.SyncMode.contains("PERIODIC (600s)")) {
                    assertTrue(it.SrcComputeGroup.contains("newcluster1") && it.DstComputeGroup.contains("newcluster2"))
                    warmUpIds.add(it.JobId)
                } else {
                    assertTrue(false)
                }
            }
            assertEquals(warmUpIds.size(), 2)

            // test manual cancel warm up job, generate new jobs
            sql """CANCEL WARM UP JOB WHERE ID=${showWarmup[0].JobId}"""

            awaitUntil(50, 3) {
                showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
                // cancel 2, generate 2
                showWarmup.size() == 4
            }

            showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
            assertEquals(showWarmup.size(), 4)
            showWarmup.each { job ->
                checkWarmupJob(job, warmUpIds, "newcluster1", "newcluster2")
            }
            assertEquals(warmUpIds.size(), 4)

            // grant usage_priv on cluster vcg to user
            sql """GRANT USAGE_PRIV ON CLUSTER '${normalVclusterName}' TO '${user}'"""
            // show grant
            def result = sql_return_maparray """show grants for '${user}'"""
            log.info("show grant for ${user}, ret={}", result)
            def ret = result.find {it.CloudClusterPrivs == "normalVirtualClusterName: Cluster_usage_priv"}
            // more auth test, use fe ut, see org.apache.doris.mysql.privilege.CloudAuthTest
            
            // use vcg
            
            sql """use @${normalVclusterName}"""
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
            );
            """

            sql """
                insert into ${tbl} (k1, k2) values (1, "10");
            """

            result = sql """select count(*) from ${tbl}"""
            log.info("result = {}", result)
            assertEquals(result.size(), 1)

            def db = context.dbName
            connectInDocker(user, 'Cloud12345') {
                sql """insert into ${db}.${tbl} (k1, k2) values (2, "20")"""
                result = sql """select * from ${db}.${tbl}"""
                assertEquals(result.size(), 2)
            }

            try {
                sql """WARM UP COMPUTE GROUP ${normalVclusterName} WITH TABLE ${tbl} """
                assertTrue(false)
            } catch (Exception e) {
                logger.info("exception: {}", e.getMessage())
                assertTrue(e.getMessage().contains("not support"))
            } 

            try {
                sql """WARM UP COMPUTE GROUP ${clusterName1} WITH COMPUTE GROUP ${clusterName2}
                    PROPERTIES (
                    "sync_mode" = "periodic",   -- 周期性的 warm up job
                    "sync_interval_sec" = "600" -- 单位是秒
                    )"""
                assertTrue(false)
            } catch (Exception e) {
                logger.info("exception: {}", e.getMessage())
                assertTrue(e.getMessage().contains("not support"))
            }

            try {
                sql """WARM UP COMPUTE GROUP ${clusterName1} WITH COMPUTE GROUP ${clusterName2}
                    PROPERTIES (
                        "sync_mode" = "event_driven",  -- 事件触发的 warm up job
                        "sync_event" = "load"          -- load 包含 compaction 和 sc，后续可能扩展 query
                    )"""
                assertTrue(false)
            } catch (Exception e) {
                logger.info("exception: {}", e.getMessage())
                assertTrue(e.getMessage().contains("not support"))
            }

            // alter cluster info, change standbyComputeGroup to newcluster1, activeComputeGroup to newcluster2
            clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName2}", standby_cluster_names: ["${clusterName1}"]]
            clusterMap = [cluster_name: "${normalVclusterName}", cluster_id:"${normalVclusterId}", type:"VIRTUAL", cluster_policy:clusterPolicy]
            normalInstance = [instance_id: "${instance_id}", cluster: clusterMap]
            jsonOutput = new JsonOutput()
            normalVcgBody = jsonOutput.toJson(normalInstance)
            alter_cluster_info_api(msHttpPort, normalVcgBody) {
                respCode, body ->
                    log.info("alter virtual cluster result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            awaitUntil(20) {
                showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                log.info("show compute group after alter {}", showComputeGroup)
                vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
                vcgInShow != null
            }
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            // {Policy={policyType=ActiveStandby, activeComputeGroup='newcluster2', standbyComputeGroup='newcluster1', failoverFailureThreshold=3, unhealthyNodeThresholdPercent=100}, SubClusters=newcluster2, newcluster1, Users=, Name=normalVirtualClusterName}
            assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster2","standbyComputeGroup":"newcluster1"'))
            // alter active -> sync to fe -> cancel old jobs -> generate new jobs
            // -> sync to ms -> sync to fe -> save new jobs
            awaitUntil(50, 3) { 
                showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
                // cancel 2, generate 2
                showWarmup.size() == 6
            }
            showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
            log.info("show warm up after alter {}", showWarmup)

            assertEquals(showWarmup.size(), 6)
            showWarmup.each { job ->
                checkWarmupJob(job, warmUpIds, "newcluster2", "newcluster1")
            }
            assertEquals(warmUpIds.size(), 6)

            // test switch active cluster, use fe switch
            // stop 6, stop clusterName2
            cluster.stopBackends(6)

            // test warm up job destory and generate new jobs
            awaitUntil(50, 3) { 
                sql """USE @${normalVclusterName}"""
                sql """select count(*) from ${tbl}"""
                showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                vcgInShow = showComputeGroup.find { it.Name == normalVclusterName } 
                vcgInShow.Policy.contains('"activeComputeGroup":"newcluster1","standbyComputeGroup":"newcluster2"')
            }
            showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
            assertEquals(showWarmup.size(), 6)

            cluster.startBackends(6)
            awaitUntil(50, 3) {
                showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
                showWarmup.size() == 8
            }
            showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
            showWarmup.each { job ->
                checkWarmupJob(job, warmUpIds, "newcluster1", "newcluster2")
            }
            assertEquals(warmUpIds.size(), 8)
        
            // test rename vcg
            def newNormalVclusterName = "newNormalVirtualClusterName"
            rename_cloud_cluster.call(newNormalVclusterName, normalVclusterId, ms)
            awaitUntil(20) {
                showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                log.info("show compute group after rename {}", showComputeGroup)
                vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
                vcgInShow == null
            } 
            // show cluster again
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNull(vcgInShow)
            vcgInShow = showComputeGroup.find { it.Name == newNormalVclusterName }
            assertNotNull(vcgInShow)

            // rename back to 
            rename_cloud_cluster.call(normalVclusterName, normalVclusterId, ms)
            awaitUntil(20) {
                showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                log.info("show compute group after rename back {}", showComputeGroup)
                vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
                vcgInShow != null
            } 
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            vcgInShow = showComputeGroup.find { it.Name == newNormalVclusterName }
            assertNull(vcgInShow)

            // drop vcg
            drop_cluster(normalVclusterName, normalVclusterId, ms)

            // drop_cluster, drop vcg succ
            def tag = getCloudBeTagByName(clusterName1)
            logger.info("tag1 = {}", tag)

            def jsonSlurper = new JsonSlurper()
            def jsonObject = jsonSlurper.parseText(tag)
            def cloudClusterId = jsonObject.compute_group_id
            drop_cluster(clusterName1, cloudClusterId, ms)

            tag = getCloudBeTagByName(clusterName2)
            logger.info("tag2 = {}", tag)
            jsonSlurper = new JsonSlurper()
            jsonObject = jsonSlurper.parseText(tag)
            cloudClusterId = jsonObject.compute_group_id
            drop_cluster(clusterName2, cloudClusterId, ms)
            awaitUntil(20) {
                def showRet = sql """SHOW COMPUTE GROUPS"""
                log.info("show cgs: {}", showRet)
                showRet.size() == 2
            }

            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group after drop {}", showComputeGroup)
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNull(vcgInShow)
            def cg1InShow = showComputeGroup.find { it.Name == clusterName1 }
            assertNull(cg1InShow)
            def cg2InShow = showComputeGroup.find { it.Name == clusterName2 }
            assertNull(cg1InShow)

            showWarmup = sql_return_maparray """SHOW WARM UP JOB"""
            log.info("show warm up after drop vcg {}", showWarmup)
            assertEquals(showWarmup.size(), 8)
            showWarmup.each {
                assertTrue(it.SyncMode.contains("EVENT_DRIVEN (LOAD)") || it.SyncMode.contains("PERIODIC (600s)"))
                assertTrue(warmUpIds.contains(it.JobId))
                if (it.SyncMode.contains("EVENT_DRIVEN (LOAD)")) {
                    assertTrue(it.Status.contains("CANCELLED") && (it.ErrMsg.contains("user cancel") || it.ErrMsg.contains("vcg cancel")))
                } else if (it.SyncMode.contains("PERIODIC (600s)")) {
                    assertTrue(it.Status.contains("CANCELLED") && it.ErrMsg.contains("vcg cancel"))
                } else {
                    assertTrue(false)
                }
            }
        }
        // connect to follower, run again
        options.connectToFollower = true 
        logger.info("Successfully run {} times", j + 1)
    }
}
