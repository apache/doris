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
import org.awaitility.Awaitility;
import org.apache.doris.regression.util.Http
import static java.util.concurrent.TimeUnit.SECONDS;

suite('test_auto_start_in_cloud', 'multi_cluster') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_pre_heating_time_limit_sec=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(3)
    options.setBeNum(3)
    options.cloudMode = true
    options.connectToFollower = true

    def getClusterFragementStatus = { def fe -> 
        def (feHost, feHttpPort) = fe.getHttpAddress()
        // curl -X GET -u root: '128.1.1.1:8030/rest/v2/manager/cluster/cluster_info/cloud_cluster_status'
        def url = 'http://' + feHost + ':' + feHttpPort + '/rest/v2/manager/cluster/cluster_info/cloud_cluster_status'
        def result = Http.GET(url, true)
        result
    }


    def set_cluster_status = { String unique_id , String cluster_id, String status, def ms ->
        def jsonOutput = new JsonOutput()
        def reqBody = [
                    cloud_unique_id: unique_id,
                    cluster : [
                        cluster_id : cluster_id,
                        cluster_status : status
                    ]
                ]
        def js = jsonOutput.toJson(reqBody)
        log.info("drop cluster req: ${js} ".toString())

        def set_cluster_status_api = { request_body, check_func ->
            httpTest {
                endpoint ms.host+':'+ms.httpPort
                uri "/MetaService/http/set_cluster_status?token=greedisgood9999"
                body request_body
                check check_func
            }
        }

        set_cluster_status_api.call(js) {
            respCode, body ->
                log.info("set cluster status resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }
    }

    docker(options) {
        sql """
            CREATE TABLE table1 (
            class INT,
            id INT,
            score INT SUM
            )
            AGGREGATE KEY(class, id)
            DISTRIBUTED BY HASH(class) BUCKETS 48
        """

        sql """INSERT INTO table1 VALUES (1, 1, 100)"""
        // master
        def fe1 = cluster.getFeByIndex(1)
        // ms
        def ms = cluster.getAllMetaservices().get(0)

        def result = sql_return_maparray """SHOW CLUSTERS"""
        String clusterName = result[0].cluster
        def tag = getCloudBeTagByName(clusterName)
        logger.info("tag = {}", tag)

        def jsonSlurper = new JsonSlurper()
        def jsonObject = jsonSlurper.parseText(tag)
        String cloudClusterId = jsonObject.cloud_cluster_id
        String uniqueId = jsonObject.cloud_unique_id

        sleep(5 * 1000)

        Map<String, Long> fragmentUpdateTimeMap = [:]

        // no read,write,sc, 20s suspend cluster
        boolean clusterCanSuspend = true
        for (int i = 0; i < 20; i++) {
            result = getClusterFragementStatus(fe1)
            result.data.compute_cluster_id.each {
                if (fragmentUpdateTimeMap[it.host] == null) {
                    fragmentUpdateTimeMap[it.host] = it.lastFragmentUpdateTime
                } else if (fragmentUpdateTimeMap[it.host] != it.lastFragmentUpdateTime) {
                    log.info("fragment update time changed be: {} old time: {} new time: {}", it.host, fragmentUpdateTimeMap[it.host], it.lastFragmentUpdateTime)
                    clusterCanSuspend = false
                }
            }
            sleep(1 * 1000)
        }
        assertTrue(clusterCanSuspend)

        // cloud control set cluster status SUSPENDED
        set_cluster_status(uniqueId, cloudClusterId, "SUSPENDED", ms)

        dockerAwaitUntil(5) {
            tag = getCloudBeTagByName(clusterName)
            logger.info("tag = {}", tag) 
            jsonObject = jsonSlurper.parseText(tag)
            String cluster_status = jsonObject.cloud_cluster_status
            cluster_status == "SUSPENDED"
        }

        cluster.stopBackends(1,2,3)

        // select
        future1 = thread {
            def begin = System.currentTimeMillis();
            // root cant resume, due to deamon thread use root
            def connInfo = context.threadLocalConn.get()
            result = connect(user = 'admin', password = '', url = connInfo.conn.getMetaData().getURL()) {
                sql 'SELECT * FROM table1'
            }
            def cost = System.currentTimeMillis() - begin;
            log.info("result {} time cost: {}", result, cost)
            assertTrue(cost > 5000)
            assertEquals(1, result.size())
        }
        // insert
   
        // cloud control
        future2 = thread {
            // check cluster "TO_RESUME"
            dockerAwaitUntil(5) {
                tag = getCloudBeTagByName(clusterName)
                logger.info("tag = {}", tag) 
                jsonObject = jsonSlurper.parseText(tag)
                String cluster_status = jsonObject.cloud_cluster_status
                cluster_status == "TO_RESUME"
            }
            sleep(5 * 1000)
            cluster.startBackends(1,2,3)
            set_cluster_status(uniqueId, cloudClusterId, "NORMAL", ms)
        } 

        future1.get()
        future2.get()
    }
}
