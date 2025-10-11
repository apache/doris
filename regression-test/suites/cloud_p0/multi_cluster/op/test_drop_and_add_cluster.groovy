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

suite("test_drop_and_add_node_and_cluster", "op, nonConcurrent") {

    def clusters = [:]
    def nodes = [:]

    //decommission node
    def OpTimeout = 3600
    def DeltaTime = 30
    def UseTime = 0

    def wait_for_decommision_finish = { ip ->
        def finish_tag = false
        for(int t = DeltaTime; t <= OpTimeout; t += DeltaTime){
            def conn = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
                def backends = sql_return_maparray "show backends;"
                for (def be : backends) {
                    def nowTmpNodeStatus = be.Status
                    Map nowNodeStatus = new groovy.json.JsonSlurper().parseText(nowTmpNodeStatus)
                    if (be.Host == ip) {
                        if ( !nowNodeStatus.isActive.toBoolean() && be.SystemDecommissioned.toBoolean() ) {
                            log.info("${ip} finish decommission!")
                            finish_tag = true
                            break
                        }
                        log.info("${ip} not finish decommission, node info: " + be)
                    }
                }
            }
            if (finish_tag) {
                break
            }
            UseTime = t
            sleep(DeltaTime*1000)
        }
        assertTrue(UseTime <= OpTimeout, "decommission ${ip} timeout")
    }

    def is_add_to_backends = { ip ->
        def is_add = false
        def conn = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
            def backends = sql_return_maparray "show backends;"
            for (def be : backends) {
                def nowTmpNodeStatus = be.Status
                Map nowNodeStatus = new groovy.json.JsonSlurper().parseText(nowTmpNodeStatus)
                log.info("be info: " + be)
                log.info("nowNodeStatus: " + nowNodeStatus)
                log.info("ip: ${ip}, be.Host: ${be.Host}, Alive status: ${be.Alive}, nowNodeStatus.isActive: ${nowNodeStatus.isActive}, is_add: ${is_add.toString()}")
                if ( be.Host == ip && be.Alive.toBoolean() && nowNodeStatus.isActive.toBoolean() ) {
                    is_add = true
                    break
                }
            }
        }
        return is_add
    }

    def is_add_to_clusters = { cluster_name ->
        def is_add = false
        def conn = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
            def res = sql_return_maparray "show clusters;"
            for (def cluster : res) {
                if (cluster.cluster == cluster_name) {
                    is_add = true
                    break
                }
            }
        }
        return is_add
    }

    //do cluster operate in compacibility test at below code
    // setp1. init nodes and clusters

    // op case only run in root, get cluster basic info
    def connRes = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
        // get cluster info, record
        def res = sql_return_maparray "show clusters;"
        // cluster_res = [[cluster:CloudCluster0, is_current:TRUE, users:root], [cluster:CloudCluster1, is_current:FALSE, users:admin]]
        // if single cluster just not run
        if ( res.size() < 2 ) {
           log.info("not multi-cluster, not run. res: ${res}")
           System.exit(0)
        }
        if ( context.config.otherConfigs.get("multiClusterInstance") == null && context.config.otherConfigs.get("metaServiceToken") == null ) {
           log.info("multiClusterInstance and metaServiceToken not configed in regression-test.groovy, plz config!")
           System.exit(1)
        }
        for (def cluster : res) {
            clusters[cluster.cluster] = new ArrayList<>()
            log.info("clusters :" + clusters)
        }
        // get node info, record
        def backends = sql_return_maparray "show backends;"
        for (def be : backends) {
            log.info("be :" + be)
            def TmpCloudInfo = be.Tag
            Map CloudTag = new groovy.json.JsonSlurper().parseText(TmpCloudInfo)
            node_cluster = CloudTag.cloud_cluster_name
            log.info("clusters :" + clusters)
            clusters[node_cluster].add(be.Host)

            nodes[be.Host] = [:]
            nodes[be.Host].heartbeat_port = be.HeartbeatPort
            nodes[be.Host].http_port = be.HttpPort
            nodes[be.Host].cloud_unique_id = CloudTag.cloud_unique_id
            nodes[be.Host].cloud_cluster_name = CloudTag.cloud_cluster_name
            nodes[be.Host].cloud_cluster_id = CloudTag.cloud_cluster_id
        }

        log.info("nodes: " + nodes)
        log.info("clusterList: " + clusters);
    }
    /*
    demo
    clusters = ["CloudCluster0":["172.20.48.164", "172.20.48.165"], "CloudCluster1":["172.20.48.167", "172.20.48.166"]]
    nodes = [
        "172.20.48.164":["heartbeat_port":"9056", "http_port":"8046", "cloud_unique_id":"1:CloudCluster0:CLOUD_COMPUTE_ID", "cloud_cluster_name":"CloudCluster0", "cloud_cluster_id":"CLOUD_COMPUTE_ID_CLUSTER0"],
        "172.20.48.165":["heartbeat_port":"9056", "http_port":"8046", "cloud_unique_id":"1:CloudCluster0:CLOUD_COMPUTE_ID", "cloud_cluster_name":"CloudCluster0", "cloud_cluster_id":"CLOUD_COMPUTE_ID_CLUSTER0"],
        "172.20.48.166":["heartbeat_port":"9167", "http_port":"8157", "cloud_unique_id":"1:CloudCluster1:CLOUD_COMPUTE_ID", "cloud_cluster_name":"CloudCluster1", "cloud_cluster_id":"CLOUD_COMPUTE_ID_CLUSTER1"],
        "172.20.48.167":["heartbeat_port":"9167", "http_port":"8157", "cloud_unique_id":"1:CloudCluster1:CLOUD_COMPUTE_ID", "cloud_cluster_name":"CloudCluster1", "cloud_cluster_id":"CLOUD_COMPUTE_ID_CLUSTER1"]
    ]
    */

    def pick_to_op_node_ip = ''
    def pick_to_op_cluster_name = ''
    clusters.forEach { key, value ->
        pick_to_op_cluster_name = key
        for ( be in value ) {
            pick_to_op_node_ip = be
            break
        }
    }
    log.info("pick_to_op_node_ip: " + pick_to_op_node_ip)
    log.info("pick_to_op_cluster_name: " + pick_to_op_cluster_name)

    pick_to_op_node_ip = "172.20.48.166"
    pick_to_op_cluster_name = "CloudCluster1"
    //decommsion node
    d_node.call(
        nodes[pick_to_op_node_ip].cloud_unique_id,
        pick_to_op_node_ip,
        nodes[pick_to_op_node_ip].heartbeat_port,
        nodes[pick_to_op_node_ip].cloud_cluster_name,
        nodes[pick_to_op_node_ip].cloud_cluster_id
    )
    wait_for_decommision_finish(pick_to_op_node_ip)
    //drop node
    drop_node.call(
        nodes[pick_to_op_node_ip].cloud_unique_id,
        pick_to_op_node_ip,
        nodes[pick_to_op_node_ip].heartbeat_port,
        nodes[pick_to_op_node_ip].cloud_cluster_name,
        nodes[pick_to_op_node_ip].cloud_cluster_id
    )
    // wait 1min
    sleep(60000)
    assertTrue( !is_add_to_backends(pick_to_op_node_ip), "${pick_to_op_node_ip} drop fail")
    //add node
    add_node.call(
        nodes[pick_to_op_node_ip].cloud_unique_id,
        pick_to_op_node_ip,
        nodes[pick_to_op_node_ip].heartbeat_port,
        nodes[pick_to_op_node_ip].cloud_cluster_name,
        nodes[pick_to_op_node_ip].cloud_cluster_id
    )
    // wait 1min
    sleep(60000)
    assertTrue( is_add_to_backends(pick_to_op_node_ip), "${pick_to_op_node_ip} add fail")

    //drop_cluster
    drop_cluster.call(pick_to_op_cluster_name, nodes[pick_to_op_node_ip].cloud_cluster_id)
    sleep(60000)
    assertTrue( !is_add_to_clusters(pick_to_op_cluster_name), "${pick_to_op_cluster_name} drop fail")

    //add_cluster
    nodes.forEach { node_ip, node_info ->
        if (node_info.cloud_cluster_name == pick_to_op_cluster_name){
            // if cluster not add, so add_cluster
            if ( !is_add_to_clusters(pick_to_op_cluster_name) ) {

                add_cluster.call(
                    node_info.cloud_unique_id,
                    node_ip,
                    node_info.heartbeat_port,
                    node_info.cloud_cluster_name,
                    node_info.cloud_cluster_id);
            } else {
                // cluster already add, but node not add, add_node
                log.info("node ${node_ip} not add to cluster ${pick_to_op_cluster_name}, add now!")
                if ( !is_add_to_backends(node_ip)) {
                    add_node.call(
                        node_info.cloud_unique_id,
                        node_ip,
                        node_info.heartbeat_port,
                        node_info.cloud_cluster_name,
                        node_info.cloud_cluster_id)
                }
            }
        }
    }
    assertTrue( is_add_to_clusters(pick_to_op_cluster_name), "${pick_to_op_cluster_name} add fail")

}