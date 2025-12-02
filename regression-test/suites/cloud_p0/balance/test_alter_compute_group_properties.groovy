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
import org.codehaus.groovy.runtime.IOGroovyMethods

suite('test_alter_compute_group_properties', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def findComputeGroup = { clusterName ->
        def showComputeGroups = sql_return_maparray """SHOW COMPUTE GROUPS"""
        log.info("SHOW COMPUTE GROUPS result: {}", showComputeGroups)
        showComputeGroups.find { it.Name == clusterName }
    }

    def findCluster = { clusterName ->
        def showCg = sql_return_maparray """SHOW CLUSTERS"""
        log.info("SHOW CLUSTERS result: {}", showCg)
        showCg.find { it.cluster == clusterName }
    }

    docker(options) {
        String clusterName = "compute_cluster"
        def showComputeGroup = findComputeGroup(clusterName)
        def showclusters = findCluster(clusterName)
        assertTrue(showComputeGroup.Properties.contains('balance_type=async_warmup, balance_warm_up_task_timeout=300'))
        assertTrue(showclusters.properties.contains('balance_type=async_warmup, balance_warm_up_task_timeout=300'))
        sql """ALTER COMPUTE GROUP $clusterName PROPERTIES ('balance_type'='without_warmup')"""
        sleep(3 * 1000)
        showComputeGroup = findComputeGroup(clusterName)
        showclusters = findCluster(clusterName)
        assertTrue(showComputeGroup.Properties.contains('balance_type=without_warmup'))
        assertTrue(showclusters.properties.contains('balance_type=without_warmup'))
        try {
            //  errCode = 2, detailMessage = Property balance_warm_up_task_timeout cannot be set when current balance_type is without_warmup. Only async_warmup type supports timeout setting.
            sql """ALTER COMPUTE GROUP $clusterName PROPERTIES ('balance_warm_up_task_timeout'='6000')"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("Property balance_warm_up_task_timeout cannot be set when current"))
        } 
        sql """ALTER COMPUTE GROUP $clusterName PROPERTIES ('balance_type'='async_warmup', 'balance_warm_up_task_timeout'='6000')"""
        sleep(3 * 1000)
        showComputeGroup = findComputeGroup(clusterName)
        showclusters = findCluster(clusterName)
        assertTrue(showComputeGroup.Properties.contains('balance_type=async_warmup, balance_warm_up_task_timeout=6000'))
        assertTrue(showclusters.properties.contains('balance_type=async_warmup, balance_warm_up_task_timeout=6000'))
        sql """ALTER COMPUTE GROUP $clusterName PROPERTIES ('balance_type'='without_warmup')"""
        sleep(3 * 1000)
        showComputeGroup = findComputeGroup(clusterName)
        showclusters = findCluster(clusterName)
        assertTrue(showComputeGroup.Properties.contains('balance_type=without_warmup'))
        assertTrue(showclusters.properties.contains('balance_type=without_warmup'))
        sql """ALTER COMPUTE GROUP $clusterName PROPERTIES ('balance_type'='async_warmup')"""
        sleep(3 * 1000)
        showComputeGroup = findComputeGroup(clusterName)
        showclusters = findCluster(clusterName)
        assertTrue(showComputeGroup.Properties.contains('balance_type=async_warmup, balance_warm_up_task_timeout=300'))
        assertTrue(showclusters.properties.contains('balance_type=async_warmup, balance_warm_up_task_timeout=300'))
        sql """ALTER COMPUTE GROUP $clusterName PROPERTIES ('balance_type'='sync_warmup')"""
        sleep(3 * 1000)
        showComputeGroup = findComputeGroup(clusterName)
        showclusters = findCluster(clusterName)
        assertTrue(showComputeGroup.Properties.contains('balance_type=sync_warmup'))
        assertTrue(showclusters.properties.contains('balance_type=sync_warmup'))
    }
}
