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

suite('test_tvf_in_cloud', 'multi_cluster,docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.cloudMode = true

    def testCase = {
        for (def i = 0; i < 100; i++) {
            def ret = sql """select * from numbers("number" = "100")"""
            assertEquals(ret.size(), 100)
            test {
                // current cloud not implement it
                sql """select START_VERSION,END_VERSION from information_schema.rowsets"""
                exception "_get_all_rowsets is not implemented"
            }
        }
    }

    docker(options) {
        def clusterName = "newcluster1"
        // 添加一个新的cluster add_new_cluster
        cluster.addBackend(3, clusterName)
       
        def result = sql """show clusters"""
        logger.info("show cluster1 : {}", result)
        def tag = getCloudBeTagByName(clusterName)
        logger.info("tag = {}", tag)

        def jsonSlurper = new JsonSlurper()
        def jsonObject = jsonSlurper.parseText(tag)
        def cloudClusterId = jsonObject.cloud_cluster_id
        // multi cluster env

        // current cluster
        testCase.call()
        // use other cluster
        def ret = sql_return_maparray """show clusters"""
        def currentCluster = ret.stream().filter(cluster -> cluster.is_current == "TRUE").findFirst().orElse(null)
        def otherCluster = ret.stream().filter(cluster -> cluster.is_current == "FALSE").findFirst().orElse(null)
        assertTrue(otherCluster != null)
        sql """use @${otherCluster.cluster}"""
        testCase.call()
        
        // 调用http api 将add_new_cluster 下掉
        def ms = cluster.getAllMetaservices().get(0)
        logger.info("ms addr={}, port={}", ms.host, ms.httpPort)
        drop_cluster(clusterName, cloudClusterId, ms)
        Thread.sleep(5000)
        result = sql """show clusters"""
        logger.info("show cluster2 : {}", result)

        // single cluster env
        // use old clusterName, has been droped
        test {
            sql """select * from numbers("number" = "100")"""
            exception "in cloud maybe this cluster has been dropped" 
        }
        // switch to old cluster
        sql """use @${currentCluster.cluster}"""
        testCase.call()
    }
}
