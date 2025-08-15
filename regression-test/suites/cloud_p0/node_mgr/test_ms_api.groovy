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
import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions

suite('test_ms_api', 'p0, docker') {
    if (!isCloudMode()) {
        return;
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def create_instance_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/create_instance?token=$token"
            body request_body
            check check_func
        }
    }

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=${token}&instance_id=${instance_id}"
            check check_func
        }
    }


    def enable_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=enable"
            check check_func
        }
    }
    
    // curl "175.43.101.1:5000/MetaService/http/v1/injection_point?token=greedisgood9999&op=set&name=resource_manager::set_safe_drop_time&behavior=change_args&value=[-1]"
    def inject_to_ms_api = { msHttpPort, key, value, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=set&name=${key}&behavior=change_args&value=${value}"
            check check_func
        }
    }

    // curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=clear"
    def clear_ms_inject_api = { msHttpPort, key, value, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=clear"
            check check_func
        }
    }

    def drop_cluster_api = { msHttpPort, request_body, check_func ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/drop_cluster?token=$token"
                body request_body
                check check_func
            }
    }


    // drop instance
    def drop_instance_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_instance?token=greedisgood9999"
            body request_body
            check check_func
        }
    }

    def add_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/add_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def get_obj_store_info_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/get_obj_store_info?token=$token"
            body request_body
            check check_func
        }
    }

    def update_ak_sk_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/update_ak_sk?token=$token"
            body request_body
            check check_func
        }
    }

    def add_obj_info_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/add_obj_info?token=$token"
            body request_body
            check check_func
        }
    }

    def get_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/get_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def rename_node_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/rename_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def add_node_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/add_node?token=$token"
            body request_body
            check check_func
        }
    }

    def drop_node_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_node?token=$token"
            body request_body
            check check_func
        }
    }

    // old case
    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

        // Inventory function test
        def token = "greedisgood9999"
        def instance_id = "instance_id_test_in_docker"
        def name = "user_1"
        def user_id = "10000"

        // create instance
        /*
        curl -X GET '127.0.0.1:5000/MetaService/http/create_instance?token=greedisgood9999' -d '{
            "instance_id": "instance_id_deadbeef",
            "name": "user_1",
            "user_id": "10000",
            "obj_info": {
                "ak": "test-ak1",
                "sk": "test-sk1",
                "bucket": "test-bucket",
                "prefix": "test-prefix",
                "endpoint": "test-endpoint",
                "region": "test-region",
                "provider" : "BOS",
                "external_endpoint" : "endpoint"
            }
        }'
        */
        def jsonOutput = new JsonOutput()
        def s3 = [
                    ak: "test-ak1",
                    sk : "test-sk1",
                    bucket : "test-bucket",
                    prefix: "test-prefix",
                    endpoint: "test-endpoint",
                    region: "test-region",
                    provider : "BOS",
                    'external_endpoint': "test-external-endpoint"
                ]
        def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", obj_info: s3]
        def instance_body = jsonOutput.toJson(map)

        create_instance_api.call(msHttpPort, instance_body) {
                respCode, body ->
                    log.info("http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        
        def otherInstancemap = [instance_id: "instance_id_test_in_docker_other", name: "${name}", user_id: "${user_id}", obj_info: s3]
        def otherInstanceBody = jsonOutput.toJson(otherInstancemap)

        create_instance_api.call(msHttpPort, otherInstanceBody) {
            respCode, body ->
                log.info("create other instance http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // create again failed
        create_instance_api.call(msHttpPort, otherInstanceBody) {
            respCode, body ->
                log.info("create other instance again http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
        }

        jsonOutput = new JsonOutput()
        def instance = [instance_id: "instance_id_test_in_docker_other"]
        def dropInstanceBody = jsonOutput.toJson(instance)

        drop_instance_api.call(msHttpPort, dropInstanceBody) {
            respCode, body ->
                log.info("drop instance http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // add compute group 1
        def clusterName = "compute_name_1"
        def clusterId = "compute_id_1"
        def cloudUniqueId = "1:${instance_id}:xxxxx"
        def compute_ip1 = "182.0.0.1"
        def heartbeatPort = 9050

        // add no be cluster
        /*
            curl -X GET http://127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999 -d '{
                "instance_id": "instance_id_deadbeef",
                "cluster": {
                    "cluster_name": "cluster_name1",
                    "cluster_id": "cluster_id1",
                    "type": "COMPUTE",
                    "nodes": []
                }
            }'
        */
        def nodeList = []
        def clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def addEmptyComputeGroup  = jsonOutput.toJson(instance)
        add_cluster_api.call(msHttpPort, addEmptyComputeGroup) {
            respCode, body ->
                log.info("add empty compute group http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        } 

        jsonOutput = new JsonOutput()
        def dropEmptyComputeGroup  = jsonOutput.toJson(instance)
        // drop empty cluster
        /*
            curl -X GET http://127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999 -d '{
                "instance_id": "instance_id_deadbeef",
                "cluster": {
                    "cluster_name": "cluster_name1",
                    "cluster_id": "cluster_id1"
                }
            }'
        */
        drop_cluster_api.call(msHttpPort, dropEmptyComputeGroup) {
            respCode, body ->
                log.info("drop empty compute group http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        def nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${compute_ip1}", heartbeat_port: "${heartbeatPort}"]
        nodeList = [nodeMap]
        clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        def addComputeGroupBody = jsonOutput.toJson(instance)
        // add_cluster has one node
        /*
            curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
                "instance_id":"instance_id_deadbeef",
                "cluster":{
                    "cluster_name":"cluster_name1",
                    "cluster_id":"cluster_id1",
                    "type" : "COMPUTE",
                    "nodes":[
                        {
                            "cloud_unique_id":"cloud_unique_id_compute_node0",
                            "ip":"172.0.0.10",
                            "heartbeat_port":9050
                        }
                    ]
                }
            }'
        */
        add_cluster_api.call(msHttpPort, addComputeGroupBody) {
            respCode, body ->
                log.info("add one compute node http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        } 

        // add sql group
        // see Config.java 
        // public static String cloud_sql_server_cluster_id = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER";
        // public static String cloud_sql_server_cluster_name = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER";
        def feClusterId = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
        def feClusterName = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER"
        def ip1 = "162.0.0.1"
        def ip2 = "162.0.0.2"
        def ip3 = "162.0.0.3"
        def edit_log_port = 8050
        def feNodeMap1 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip1}", edit_log_port: "${edit_log_port}", node_type:"FE_MASTER"]
        def feNodeMap2 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip2}", edit_log_port: "${edit_log_port}", node_type:"FE_OBSERVER"]
        def feNodeMap3 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip3}", edit_log_port: "${edit_log_port}", node_type:"FE_OBSERVER"]
        def feNodeList = [feNodeMap1, feNodeMap2, feNodeMap3]
        def feClusterMap = [cluster_name: "${feClusterName}", cluster_id:"${feClusterId}", type:"SQL", nodes:feNodeList]
        instance = [instance_id: "${instance_id}", cluster: feClusterMap]
        jsonOutput = new JsonOutput()
        def addSqlGroupBody = jsonOutput.toJson(instance) 

        add_cluster_api.call(msHttpPort, addSqlGroupBody) {
                respCode, body ->
                    log.info("http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        
        def json
        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        json.result


        // get instance's s3 info
        /*
            curl '127.0.0.1:5000/MetaService/http/get_obj_store_info?token=greedisgood9999' -d '{
                "cloud_unique_id":"cloud_unique_id_compute_node0"
            }'
        */
        def get_obj_store_info_api_body = [cloud_unique_id:"${cloudUniqueId}"]
        jsonOutput = new JsonOutput()
        def getObjStoreInfo = jsonOutput.toJson(get_obj_store_info_api_body)

        get_obj_store_info_api.call(msHttpPort, getObjStoreInfo) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // update instance's s3 info
        /*
            curl '127.0.0.1:5000/MetaService/http/update_ak_sk?token=greedisgood9999' -d '{
                "instance_id": "cloud_unique_id_compute_node0",
                "internal_bucket_user": [
                    "user_id": "1-userid",
                    "ak": "test-ak1-updated",
                    "sk": "test-sk1-updated"
                ],
            }'
        */
        def internal_bucket_user = [[user_id:"1-userid", ak:"test-ak1-updated", sk:"test-sk1-updated"]]
        def update_ak_sk_api_body = [instance_id:"${instance_id}", internal_bucket_user:internal_bucket_user]
        jsonOutput = new JsonOutput()
        def upDateAKSKBody = jsonOutput.toJson(update_ak_sk_api_body)


        update_ak_sk_api.call(msHttpPort, upDateAKSKBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // add s3 info to instance
        /*
            curl '127.0.0.1:5000/MetaService/http/add_obj_info?token=greedisgood9999' -d '{
                "cloud_unique_id": "cloud_unique_id_compute_node0",
                "obj": {
                    "ak": "test-ak2",
                    "sk": "test-sk2",
                    "bucket": "test-bucket",
                    "prefix": "test-prefix",
                    "endpoint": "test-endpoint",
                    "region": "test-region",
                    "provider": "COS"
                }
            }'
        */

        def add_obj_info_api_body = [cloud_unique_id:"${cloudUniqueId}",
                                    obj:[ak:"test-ak2", sk:"test-sk2", bucket:"test-bucket",
                                        prefix: "test-prefix", endpoint: "test-endpoint", region:"test-region", provider:"COS"]]
        jsonOutput = new JsonOutput()
        def addObjInfoBody = jsonOutput.toJson(add_obj_info_api_body)


        add_obj_info_api.call(msHttpPort, addObjInfoBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // add again, failed
        add_cluster_api.call(msHttpPort, addComputeGroupBody) {
            respCode, body ->
                log.info("add again http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
                assertTrue(json.msg.startsWith("try to add a existing cluster id"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        } 

        // get_cluster by cluster name
        /*
            curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{
                "cluster_name": "cluster_name1",
                "cloud_unique_id": "cloud_unique_id_compute_node0"
            }'
        */
        def get_cluster_by_name = [cluster_name: "${clusterName}", cloud_unique_id: "${cloudUniqueId}"]
        jsonOutput = new JsonOutput()
        def getClusterByNameBody = jsonOutput.toJson(get_cluster_by_name)

        get_cluster_api.call(msHttpPort, getClusterByNameBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // get_cluster by cluster id
        /*
            curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{
                "cluster_id": "cluster_id1",
                "cloud_unique_id": "cloud_unique_id_compute_node0"
            }'
        */
        def get_cluster_by_id = [cluster_id: "${clusterId}", cloud_unique_id: "${cloudUniqueId}"]
        jsonOutput = new JsonOutput()
        def getClusterByIdBody = jsonOutput.toJson(get_cluster_by_id)

        get_cluster_api.call(msHttpPort, getClusterByIdBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // add nodes
        /*
            curl '127.0.0.1:5000/MetaService/http/add_node?token=greedisgood9999' -d '{
                "instance_id": "instance_id_deadbeef",
                "cluster": {
                    "cluster_name": "cluster_name1",
                    "cluster_id": "cluster_id1",
                    "type": "COMPUTE",
                    "nodes": [
                        {
                            "cloud_unique_id": "cloud_unique_id_compute_node1",
                            "ip": "172.0.0.11",
                            "heartbeat_port": 9050
                        },
                        {
                            "cloud_unique_id": "cloud_unique_id_compute_node2",
                            "ip": "172.0.0.12",
                            "heartbeat_port": 9050
                        }
                    ]
                }
            }'
        */
        def compute_ip2 = "182.0.0.2"
        def compute_ip3 = "182.0.0.3"
        def node1 = [cloud_unique_id: "${cloudUniqueId}", ip : "${compute_ip2}", heartbeat_port: 9050]
        def node2 = [cloud_unique_id: "${cloudUniqueId}", ip : "${compute_ip3}", heartbeat_port: 9050]
        def add_nodes = [node1, node2]
        def add_nodes_cluster = [cluster_name: "${clusterName}", cluster_id: "${clusterId}", type: "COMPUTE", nodes: add_nodes]
        def add_nodes_body = [instance_id: "${instance_id}", cluster: add_nodes_cluster]
        jsonOutput = new JsonOutput()
        def addTwoComputeNodeBody = jsonOutput.toJson(add_nodes_body)

        add_node_api.call(msHttpPort, addTwoComputeNodeBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("add two compute nodes http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop nodes
        /*
            curl '127.0.0.1:5000/MetaService/http/drop_node?token=greedisgood9999' -d '{
                "instance_id": "instance_id_deadbeef",
                "cluster": {
                    "cluster_name": "cluster_name1",
                    "cluster_id": "cluster_id1",
                    "type": "COMPUTE",
                    "nodes": [
                        {
                            "cloud_unique_id": "cloud_unique_id_compute_node1",
                            "ip": "172.0.0.11",
                            "heartbeat_port": 9050
                        },
                        {
                            "cloud_unique_id": "cloud_unique_id_compute_node2",
                            "ip": "172.0.0.12",
                            "heartbeat_port": 9050
                        }
                    ]
                }
            }'
        */
        def del_nodes = [node1, node2]
        def del_nodes_cluster = [cluster_name: "${clusterName}", cluster_id: "${clusterId}", type: "COMPUTE", nodes: del_nodes]
        def del_nodes_body = [instance_id: "${instance_id}", cluster: del_nodes_cluster]
        jsonOutput = new JsonOutput()
        def delTwoNodesBody = jsonOutput.toJson(del_nodes_body)


        drop_node_api.call(msHttpPort, delTwoNodesBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("drop two nodes http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // rename cluster
        /*
            curl '127.0.0.1:5000/MetaService/http/rename_cluster?token=greedisgood9999' -d '{
                "instance_id":"instance_id_deadbeef",
                "cluster":{
                    "cluster_name":"compute_name_1",
                    "cluster_id":"compute_id_1"
                }
            }'
        */

        clusterMap = [cluster_name: "compute_name_1_renamed", cluster_id:"${clusterId}"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def renmaeClusterBody = jsonOutput.toJson(instance)
        rename_node_api.call(msHttpPort, renmaeClusterBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("rename cluster http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def renameClusterBackBody = jsonOutput.toJson(instance)
        rename_node_api.call(msHttpPort, renameClusterBackBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("rename cluster back http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        def clusterName2 = "cluster_name2"
        def clusterId2 = "cluster_id2"
        def nodeList1 = [node1]
        def clusterMap1 = [cluster_name: "${clusterName2}", cluster_id:"${clusterId2}", type:"COMPUTE", nodes:nodeList1]
        instance = [instance_id: "${instance_id}", cluster: clusterMap1]
        jsonOutput = new JsonOutput()
        def addNewComputeGroupBody = jsonOutput.toJson(instance)
        // add_cluster has one node
        /*
            curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
                "instance_id":"instance_id_deadbeef",
                "cluster":{
                    "cluster_name":"cluster_name2",
                    "cluster_id":"cluster_id2",
                    "type" : "COMPUTE",
                    "nodes":[
                        {
                            "cloud_unique_id":"cloud_unique_id_compute_node0",
                            "ip":"172.0.0.11",
                            "heartbeat_port":9050
                        }
                    ]
                }
            }'
        */

        add_cluster_api.call(msHttpPort, addNewComputeGroupBody) {
            respCode, body ->
                log.info("add new compute group http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // "code": "INVALID_ARGUMENT",
        // "msg": "failed to drop instance, instance has clusters"
        def dropInstanceFailedDueHasCuster = addNewComputeGroupBody
        drop_instance_api.call(msHttpPort, dropInstanceFailedDueHasCuster) {
            respCode, body ->
                log.info("drop instance failed due to has cluster http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
        }

        // failed, failed to rename cluster, a cluster with the same name already exists in this instance
        clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId2}"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def renameFailedBody = jsonOutput.toJson(instance)
        rename_node_api.call(msHttpPort, renameFailedBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("rename failed http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
                assertTrue(json.msg.contains("failed to rename cluster, a cluster with the same name already exists in this instance"))
        }

        // get cluster status
        /*
            curl '127.0.0.1:5000/MetaService/http/get_cluster_status?token=greedisgood9999' -d '{
                "instance_ids":["instance_id_deadbeef"]
            }'
        */
        def get_cluster_status = { request_body, check_func ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/get_cluster_status?token=$token"
                body request_body
                check check_func
            }
        }

        // set cluster status
        /*
            curl '127.0.0.1:5000/MetaService/http/set_cluster_status?token=greedisgood9999' -d '{
                "instance_id": "instance_id_deadbeef",
                "cluster": {
                    "cluster_id": "test_cluster_1_id1",
                    "cluster_status":"STOPPED"
                }
            }'
        */
        def set_cluster_status = { request_body, check_func ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/set_cluster_status?token=$token"
                body request_body
                check check_func
            }
        }

        def getClusterInstance = [instance_ids: ["${instance_id}"]]
        jsonOutput = new JsonOutput()
        def getClusterStatusBody = jsonOutput.toJson(getClusterInstance)
        get_cluster_status.call(getClusterStatusBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("get cluster status http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        clusterMap = [cluster_id:"${clusterId2}", cluster_status:"SUSPENDED"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def setClusterStatusBody = jsonOutput.toJson(instance)
        set_cluster_status.call(setClusterStatusBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("set cluster status http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // failed to set cluster status, status eq original status, original cluster is NORMAL
        set_cluster_status.call(setClusterStatusBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("set cluster status again failed http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
        }

        // failed to set cluster status, original cluster is SUSPENDED and want set UNKNOWN
        clusterMap = [cluster_id:"${clusterId2}", cluster_status:"UNKNOWN"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def setClusterInvalidStatusBody = jsonOutput.toJson(instance)
        set_cluster_status.call(setClusterInvalidStatusBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("set cluster status invalid status http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
        }

        // drop cluster
        /*
            curl '127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999' -d '{
                "instance_id":"instance_id_deadbeef",
                "cluster":{
                    "cluster_name":"cluster_name1",
                    "cluster_id":"cluster_id1"
                }
            }'
        */
        clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def dropCluster1Body = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropCluster1Body) {
            respCode, body ->
                log.info("drop cluster 1 http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop cluster_id2
        clusterMap = [cluster_id:"${clusterId2}"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def dropCluster2Body = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropCluster2Body) {
            respCode, body ->
                log.info("drop cluster 2 http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop not exist cluster, falied
        /*
            curl '127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999' -d '{
                "instance_id":"instance_id_deadbeef",
                "cluster":{
                    "cluster_name":"not_exist_cluster_name",
                    "cluster_id":"not_exist_cluster_name"
                }
            }'
        */
        clusterMap = [cluster_name: "not_exist_cluster_name", cluster_id:"not_exist_cluster_id", nodes:nodeList]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def dropNotExistClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropNotExistClusterBody) {
            respCode, body ->
                log.info("drop not exist cluster http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("NOT_FOUND"))
        }

        // after drop, get cluster again, failed
        instance = [cloud_unique_id: "${cloudUniqueId}", cluster_id: "${clusterId}"]
        jsonOutput = new JsonOutput()
        def afterDropAndGetBody = jsonOutput.toJson(instance)
        get_cluster_api.call(msHttpPort, afterDropAndGetBody) {
            respCode, body ->
                json = parseJson(body)
                log.info("after drop and get again http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("NOT_FOUND"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // add node to another cluster, compute_ip1 not owned by any cluster
        /*
            curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
                "instance_id":"instance_id_deadbeef",
                "cluster":{
                    "cluster_name":"cluster_name2",
                    "cluster_id":"cluster_name2",
                    "type" : "COMPUTE",
                    "nodes":[
                        {
                            "cloud_unique_id":"cloud_unique_id_compute_node0",
                            "ip":"172.0.0.10",
                            "heartbeat_port":9050
                        }
                    ]
                }
            }'
        */
        clusterName = "compute_group_name2"
        clusterId = "compute_group_id2"
        nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${compute_ip1}", heartbeat_port: "${heartbeatPort}"]
        nodeList = [nodeMap]
        clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList, type:"COMPUTE"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def addNodeToOtherBody = jsonOutput.toJson(instance)
        add_cluster_api.call(msHttpPort, addNodeToOtherBody) {
            respCode, body ->
                log.info("add node to other compute group http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // drop cluster
        /*
            curl -X GET http://127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999 -d '{
                "instance_id": "instance_id_deadbeef",
                "cluster": {
                    "cluster_name": "cluster_name2",
                    "cluster_id": "cluster_id2"
                }
            }'
        */
        clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}"]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        jsonOutput = new JsonOutput()
        def dropClusterErrBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropClusterErrBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("after drop other cluster get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // add back compute group
        add_cluster_api.call(msHttpPort, addNewComputeGroupBody) {
            respCode, body ->
                log.info("add new compute group again again http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        add_cluster_api.call(msHttpPort, addComputeGroupBody) {
            respCode, body ->
                log.info("add again again http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("after add back fe cluster get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        // custom instance PB
        /*
        {
            "code": "OK",
            "msg": "",
            "result": {
                "user_id": "10000",
                "instance_id": "instance_id_test_in_docker",
                "name": "user_1",
                "clusters": [
                    {
                        "cluster_id": "RESERVED_CLUSTER_ID_FOR_SQL_SERVER",
                        "cluster_name": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
                        "type": "SQL",
                            "nodes": [
                            {
                                "cloud_unique_id": "1:instance_id_test_in_docker:xxxxx",
                                "ip": "162.0.0.1",
                                "ctime": "1733650571",
                                "mtime": "1733650571",
                                "edit_log_port": 8050,
                                "node_type": "FE_FOLLOWER"
                            },
                            {
                                "cloud_unique_id": "1:instance_id_test_in_docker:xxxxx",
                                "ip": "162.0.0.2",
                                "ctime": "1733650571",
                                "mtime": "1733650571",
                                "edit_log_port": 8050,
                                "node_type": "FE_OBSERVER"
                            },
                            {
                                "cloud_unique_id": "1:instance_id_test_in_docker:xxxxx",
                                "ip": "162.0.0.3",
                                "ctime": "1733650571",
                                "mtime": "1733650571",
                                "edit_log_port": 8050,
                                "node_type": "FE_OBSERVER"
                            }
                        ]
                    },
                    {
                        "cluster_id": "cluster_id2",
                        "cluster_name": "cluster_name2",
                        "type": "COMPUTE",
                        "nodes": [
                            {
                                "cloud_unique_id": "1:instance_id_test_in_docker:xxxxx",
                                "ip": "182.0.0.2",
                                "ctime": "1733653263",
                                "mtime": "1733653263",
                                "heartbeat_port": 9050
                            }
                        ],
                        "cluster_status": "NORMAL"
                    },
                    {
                        "cluster_id": "compute_id_1",
                        "cluster_name": "compute_name_1",
                        "type": "COMPUTE",
                        "nodes": [
                            {
                                "cloud_unique_id": "1:instance_id_test_in_docker:xxxxx",
                                "ip": "182.0.0.1",
                                "ctime": "1733653263",
                                "mtime": "1733653263",
                                "heartbeat_port": 9050
                            }
                        ],
                        "cluster_status": "NORMAL"
                    }
                ],
                "obj_info": [
                    {
                        "ctime": "1733650571",
                        "mtime": "1733650571",
                        "id": "1",
                        "ak": "test-ak1-updated",
                        "sk": "test-sk1-updated",
                        "bucket": "test-bucket",
                        "prefix": "test-prefix",
                        "endpoint": "test-endpoint",
                        "region": "test-region",
                        "provider": "BOS",
                        "external_endpoint": "test-external-endpoint",
                        "user_id": "1-userid",
                        "encryption_info": {
                            "encryption_method": "AES_256_ECB",
                            "key_id": "1"
                        },
                        "sse_enabled": false
                    },
                    {
                        "ctime": "1733650571",
                        "mtime": "1733650571",
                        "id": "2",
                        "ak": "test-ak2",
                        "sk": "test-sk2",
                        "bucket": "test-bucket",
                        "prefix": "test-prefix",
                        "endpoint": "test-endpoint",
                        "region": "test-region",
                        "provider": "COS",
                        "external_endpoint": "",
                        "encryption_info": {
                            "encryption_method": "AES_256_ECB",
                            "key_id": "1"
                        },
                        "sse_enabled": false
                    }
                ],
                        "status": "NORMAL",
                "iam_user": {
                    "user_id": "",
                    "ak": "",
                    "sk": "",
                    "external_id": "instance_id_test_in_docker"
                },
                "sse_enabled": false,
                "enable_storage_vault": false
            }
        }
        */
    }

    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
    ]

    for (def cos in clusterOptions) {
        cos.cloudMode = true
        cos.feNum = 1
        cos.beNum = 1
        cos.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'sys_log_verbose_modules=org',
            'heartbeat_interval_second=1',
            ]
    }
    

    for (def i = 0; i < clusterOptions.size(); i++) {
        // 1. Test that a node cannot be repeatedly added to multiple clusters
        // 1.1 compute node
        // 2. Test API supports switching from master observer mode to multi follower mode
        docker(clusterOptions[i]) {
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            logger.info("ms2 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

            def token = "greedisgood9999"
            def instance_id = "instance_id_test_in_docker_1"
            def name = "user_1"
            def user_id = "10000"
            def clusterName = "compute_name_1"
            def clusterId = "compute_id_1"
            def cloudUniqueId = "1:${instance_id}:xxxxx"
            // create instance
            def jsonOutput = new JsonOutput()
            def s3 = [
                        ak: "test-ak1",
                        sk : "test-sk1",
                        bucket : "test-bucket",
                        prefix: "test-prefix",
                        endpoint: "test-endpoint",
                        region: "test-region",
                        provider : "BOS",
                        'external_endpoint': "test-external-endpoint"
                    ]
            def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", obj_info: s3]
            def instance_body = jsonOutput.toJson(map)

            create_instance_api.call(msHttpPort, instance_body) {
                    respCode, body ->
                        log.info("http cli result: ${body} ${respCode}".toString())
                        def json = parseJson(body)
                        assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def compute_ip1 = "182.0.0.1" 
            def heartbeatPort = 9050
            def nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${compute_ip1}", heartbeat_port: "${heartbeatPort}"]
            def nodeList = [nodeMap]
            def clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
            def instance = [instance_id: "${instance_id}", cluster: clusterMap]
            def addComputeGroupBody = jsonOutput.toJson(instance)
            add_cluster_api.call(msHttpPort, addComputeGroupBody) {
                respCode, body ->
                    log.info("add one compute node http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            } 
            
            // 1. Test that a node cannot be repeatedly added to multiple clusters
            // 1.1 compute node
            def node1 = [cloud_unique_id: "${cloudUniqueId}", ip : "${compute_ip1}", heartbeat_port: 9050]
            def add_nodes = [node1]
            def otherClusterName = "compute_name_1_other"
            def otherClusterId = "compute_id_1_other"
            def add_nodes_cluster = [cluster_name: "${otherClusterName}", cluster_id: "${otherClusterId}", type: "COMPUTE", nodes: add_nodes]
            def addNodeToOtherCluster = [instance_id: "${instance_id}", cluster: add_nodes_cluster]
            jsonOutput = new JsonOutput()
            def addNodeToOtherClusterbody = jsonOutput.toJson(addNodeToOtherCluster)
            add_cluster_api.call(msHttpPort, addNodeToOtherClusterbody) {
                respCode, body ->
                    log.info("add node to other compute group http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
                    assertTrue(json.msg.contains("compute node endpoint has been added"))
            }

            // 1.2 sql node
            def feClusterId = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
            def feClusterName = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER"
            def ip1 = "162.0.0.1"
            def ip2 = "162.0.0.2"
            def ip3 = "162.0.0.3"
            def edit_log_port = 8050
            def feNodeMap1 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip1}", edit_log_port: "${edit_log_port}", node_type:"FE_MASTER"]
            def feNodeMap2 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip2}", edit_log_port: "${edit_log_port}", node_type:"FE_OBSERVER"]
            def feNodeMap3 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip3}", edit_log_port: "${edit_log_port}", node_type:"FE_OBSERVER"]
            def feNodeList = [feNodeMap1, feNodeMap2, feNodeMap3]
            def feClusterMap = [cluster_name: "${feClusterName}", cluster_id:"${feClusterId}", type:"SQL", nodes:feNodeList]
            instance = [instance_id: "${instance_id}", cluster: feClusterMap]
            jsonOutput = new JsonOutput()
            def addSqlGroupBody = jsonOutput.toJson(instance) 

            add_cluster_api.call(msHttpPort, addSqlGroupBody) {
                    respCode, body ->
                        log.info("http cli result: ${body} ${respCode}".toString())
                        def json = parseJson(body)
                        assertTrue(json.code.equalsIgnoreCase("OK"))
            }
            
            def node_fe_other = [cloud_unique_id: "${cloudUniqueId}", ip : "${ip3}", edit_log_port: 8050, node_type:"FE_FOLLOWER"]
            add_nodes = [node_fe_other]
            otherClusterName = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER_OTHER"
            otherClusterId = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER_OTHER"
            add_nodes_cluster = [cluster_name: "${otherClusterName}", cluster_id: "${otherClusterId}", type:"SQL", nodes: add_nodes]
            def addNodeToOtherClusterFE = [instance_id: "${instance_id}", cluster: add_nodes_cluster]
            jsonOutput = new JsonOutput()
            def addNodeToOtherFEClusterbody = jsonOutput.toJson(addNodeToOtherClusterFE)
            add_cluster_api.call(msHttpPort, addNodeToOtherFEClusterbody) {
                respCode, body ->
                    log.info("add node to other compute group http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
                    assertTrue(json.msg.contains("sql node endpoint has been added"))
            }

            // 2. Test API supports switching from master observer mode to multi follower mode
            def newTestSQLIP1 = "152.0.0.1"
            def newTestSQLIP2 = "152.0.0.2"
            def newNodefeTestForUnlimit1Failed = [cloud_unique_id: "${cloudUniqueId}", ip : "${newTestSQLIP1}", edit_log_port: 8050, node_type:"FE_OBSERVER"]
            def newNodefeTestForUnlimit2 = [cloud_unique_id: "${cloudUniqueId}", ip : "${newTestSQLIP2}", edit_log_port: 8050, node_type:"FE_OBSERVER"]
            def addNodesFailed = [newNodefeTestForUnlimit1Failed, newNodefeTestForUnlimit2]

            // two FE_OBSERVER Failed to add cluster
            def addTwoObNodesClusterFailed = [cluster_name: "${otherClusterName}", cluster_id: "${otherClusterId}", type:"SQL", nodes: addNodesFailed]
            def addNodesClusterInstanceFailed = [instance_id: "${instance_id}", cluster: addTwoObNodesClusterFailed]
            jsonOutput = new JsonOutput()
            def addNodesClusterFailedBody = jsonOutput.toJson(addNodesClusterInstanceFailed)
            add_cluster_api.call(msHttpPort, addNodesClusterFailedBody) {
                respCode, body ->
                    log.info("add two observer fe failed test http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
                    assertTrue(json.msg.contains("cluster is SQL type, must have only one master node, now master count: 0"))
            }

            def ip4 = "162.0.0.4"
            def ip5 = "162.0.0.5"
            def ip6 = "162.0.0.6"
            def feNode4 = [cloud_unique_id: "${cloudUniqueId}", ip : "${ip4}", edit_log_port: "${edit_log_port}", node_type:"FE_MASTER"]
            def feNode5 = [cloud_unique_id: "${cloudUniqueId}", ip : "${ip5}", edit_log_port: "${edit_log_port}", node_type:"FE_OBSERVER"]
            def feNode6 = [cloud_unique_id: "${cloudUniqueId}", ip : "${ip6}", edit_log_port: "${edit_log_port}", node_type:"FE_FOLLOWER"]
            def addNodesClusterFailed = [cluster_name: "${feClusterName}", cluster_id: "${feClusterId}", type: "SQL", nodes: [feNode4, feNode5, feNode6]]
            def dropAllFeNodesFailed = [cluster_name: "${feClusterName}", cluster_id: "${feClusterId}", type: "SQL", nodes: [feNodeMap1, feNodeMap2, feNodeMap3, feNode5, feNode6]]
            def addNodesClusterSucc = [cluster_name: "${feClusterName}", cluster_id: "${feClusterId}", type: "SQL", nodes: [feNode5, feNode6]]
            def addNodesFailedBody = [instance_id: "${instance_id}", cluster: addNodesClusterFailed]
            def dropAllFeNodesClusterBody = [instance_id: "${instance_id}", cluster: dropAllFeNodesFailed]
            def addNodesBodySuccBody= [instance_id: "${instance_id}", cluster: addNodesClusterSucc]
            jsonOutput = new JsonOutput()
            def addSomeFENodesFailed = jsonOutput.toJson(addNodesFailedBody)
            def addSomeFENodesSucc = jsonOutput.toJson(addNodesBodySuccBody)
            def dropAllFeNodesFailedJson = jsonOutput.toJson(dropAllFeNodesClusterBody)

            add_node_api.call(msHttpPort, addSomeFENodesFailed) {
                respCode, body ->
                    def json = parseJson(body)
                    // failed, due to two master node
                    // if force_change_to_multi_follower_mode == false, check type not changed, FE_MASTER
                    log.info("add some fe failed nodes http cli result: ${body} ${respCode} ${json}".toString())
                    assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
                    assertTrue(json.msg.contains("instance invalid, cant modify, plz check"))
            }

            add_node_api.call(msHttpPort, addSomeFENodesSucc) {
                respCode, body ->
                    def json = parseJson(body)
                    log.info("add some fe nodes http cli result: ${body} ${respCode} ${json}".toString())
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            // inject point, to change MetaServiceImpl_get_cluster_set_config
            inject_to_ms_api.call(msHttpPort, "resource_manager::set_safe_drop_time", URLEncoder.encode('[-1]', "UTF-8")) {
                respCode, body ->
                    log.info("inject resource_manager::set_safe_drop_time resp: ${body} ${respCode}".toString()) 
            }

            enable_ms_inject_api.call(msHttpPort) {
                respCode, body ->
                log.info("enable inject resp: ${body} ${respCode}".toString()) 
            }

            drop_node_api.call(msHttpPort, dropAllFeNodesFailedJson) {
                respCode, body ->
                    def json = parseJson(body)
                    log.info("drop all fe nodes failed http cli result: ${body} ${respCode} ${json}".toString())
                    assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
                    assertTrue(json.msg.contains("instance invalid, cant modify, plz check")) 
            }

            get_instance_api.call(msHttpPort, instance_id) {
                respCode, body ->
                    log.info("add Master-observer mode get instance resp: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
                    def result = json.result
                    def FECluster = result.clusters.find {
                        it.cluster_id == "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
                    }
                    log.info("find it cluster ${FECluster}")
                    assertNotNull(FECluster)
                    def checkFENode = FECluster.nodes.find {
                        it.ip == ip1
                    }
                    def followerFeNode = FECluster.nodes.find {
                        it.ip == ip6
                    }
                    log.info("find it node fe1: ${checkFENode}")
                    log.info("find it node fe6: ${followerFeNode}")
                    assertNotNull(checkFENode)
                    assertNotNull(followerFeNode)
                    assertEquals("FE_MASTER", checkFENode.node_type)
                    assertEquals("FE_FOLLOWER", followerFeNode.node_type)
            }
        }
    }

    // 3. fe get cluster, change FE_MASTER to FE_FOLLOWER
    // Upgrade compatibility for existing master-observers models
    // when call get_cluster api, FE_MASTER type will be changed to FE_FOLLOWER
    def optionsForUpgrade = new ClusterOptions()
    optionsForUpgrade.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    optionsForUpgrade.setFeNum(1)
    optionsForUpgrade.setBeNum(1)
    optionsForUpgrade.cloudMode = true

    docker(optionsForUpgrade) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms3 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

        def token = "greedisgood9999"
        def instance_id = "instance_id_test_in_docker_2"
        def name = "user_1"
        def user_id = "10000"
        def clusterName = "compute_name_1"
        def clusterId = "compute_id_1"
        def cloudUniqueId = "1:${instance_id}:xxxxx"
        // create instance
        def jsonOutput = new JsonOutput()
        def s3 = [
                    ak: "test-ak1",
                    sk : "test-sk1",
                    bucket : "test-bucket",
                    prefix: "test-prefix",
                    endpoint: "test-endpoint",
                    region: "test-region",
                    provider : "BOS",
                    'external_endpoint': "test-external-endpoint"
                ]
        def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", obj_info: s3]
        def instance_body = jsonOutput.toJson(map)

        create_instance_api.call(msHttpPort, instance_body) {
                respCode, body ->
                    log.info("http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        def feClusterId = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
        def feClusterName = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER"
        def ip1 = "162.0.0.1"
        def ip2 = "162.0.0.2"
        def edit_log_port = 8050
        def feNodeMap1 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip1}", edit_log_port: "${edit_log_port}", node_type:"FE_MASTER"]
        def feNodeMap2 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip2}", edit_log_port: "${edit_log_port}", node_type:"FE_OBSERVER"]
        def feNodeList = [feNodeMap1, feNodeMap2]
        def feClusterMap = [cluster_name: "${feClusterName}", cluster_id:"${feClusterId}", type:"SQL", nodes:feNodeList]
        def instance = [instance_id: "${instance_id}", cluster: feClusterMap]
        jsonOutput = new JsonOutput()
        def addSqlGroupBody = jsonOutput.toJson(instance) 

        add_cluster_api.call(msHttpPort, addSqlGroupBody) {
                respCode, body ->
                    log.info("http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
        }
        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("add Master-observer mode get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def result = json.result
                def FECluster = result.clusters.find {
                    it.cluster_id == "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
                }
                log.info("find it cluster ${FECluster}")
                assertNotNull(FECluster)
                def checkMasterNode = FECluster.nodes.find {
                    it.ip == ip1
                }
                assertNotNull(checkMasterNode) 
                assertEquals("FE_MASTER", checkMasterNode.node_type)
        }


        enable_ms_inject_api.call(msHttpPort) {
            respCode, body ->
               log.info("enable inject resp: ${body} ${respCode}".toString()) 
        }

        def getFEClusterByName = [cluster_name: "${feClusterName}", cluster_id:"${feClusterId}", cloud_unique_id: "${cloudUniqueId}"]
        jsonOutput = new JsonOutput()
        def getClusterByNameBody = jsonOutput.toJson(getFEClusterByName)

        get_cluster_api.call(msHttpPort, getClusterByNameBody) {
            respCode, body ->
                def json = parseJson(body)
                log.info("get FE cluster http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def result = json.result
                def checkFollowerNode = result.nodes.find {
                    it.ip == ip1
                }
                assertNotNull(checkFollowerNode) 
                assertEquals("FE_MASTER", checkFollowerNode.node_type)
        }

        // check instance
        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("after get cluster get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def result = json.result
                def FECluster = result.clusters.find {
                    it.cluster_id == "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
                }
                log.info("find it cluster ${FECluster}")
                assertNotNull(FECluster)
                def checkFollowerNode = FECluster.nodes.find {
                    it.ip == ip1
                }
                assertNotNull(checkFollowerNode) 
                assertEquals("FE_MASTER", checkFollowerNode.node_type)
        } 

        // 4. Test use HTTP API add fe, drop fe node in protection time failed, excced protection time succ
        // test drop fe observer node
        def del_nodes = [feNodeMap2]
        def del_nodes_cluster = [cluster_name: "${feClusterName}", cluster_id: "${feClusterId}", type: "SQL", nodes: del_nodes]
        def del_nodes_body = [instance_id: "${instance_id}", cluster: del_nodes_cluster]
        jsonOutput = new JsonOutput()
        def delFeObserverNodesBody = jsonOutput.toJson(del_nodes_body)

        drop_node_api.call(msHttpPort, delFeObserverNodesBody) {
            respCode, body ->
                def json = parseJson(body)
                log.info("drop fe observer node http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
                assertTrue(json.msg.contains("drop fe node not in safe time, try later"))
        }

        // test drop fe cluster, can drop, without protection
        // fe checker will drop fe node,
        // non-master node will exit, and master node, whill throw error, can't drop itself succ
        // feClusterMap = [cluster_name: "${feClusterName}", cluster_id:"${feClusterId}"]
        feClusterMap = [cluster_id:"${feClusterId}"]
        instance = [instance_id: "${instance_id}", cluster: feClusterMap]
        jsonOutput = new JsonOutput()
        def dropFeClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropFeClusterBody) {
            respCode, body ->
                log.info("drop fe cluster http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("NOT_FOUND"))
                assertTrue(json.msg.contains("drop fe cluster not in safe time, try later"))
        }

        get_cluster_api.call(msHttpPort, getClusterByNameBody) {
            respCode, body ->
                def json = parseJson(body)
                log.info("get FE cluster after drop observer http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def result = json.result
                def checkObserverNotBeenDropedNode = result.nodes.find {
                    it.ip == ip2
                }
                assertNotNull(checkObserverNotBeenDropedNode)
        }

        // inject point, to change MetaServiceImpl_get_cluster_set_config
        inject_to_ms_api.call(msHttpPort, "resource_manager::set_safe_drop_time", URLEncoder.encode('[-1]', "UTF-8")) {
            respCode, body ->
                log.info("inject resource_manager::set_safe_drop_time resp: ${body} ${respCode}".toString()) 
        }

        enable_ms_inject_api.call(msHttpPort) {
            respCode, body ->
                log.info("enable inject resp: ${body} ${respCode}".toString()) 
        }

        // after inject, drop fe node, drop fe cluster all succ
        drop_node_api.call(msHttpPort, delFeObserverNodesBody) {
            respCode, body ->
                def json = parseJson(body)
                log.info("after inject drop fe observer nodeshttp cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
        } 

        get_cluster_api.call(msHttpPort, getClusterByNameBody) {
            respCode, body ->
                def json = parseJson(body)
                log.info("get FE cluster after drop observer http cli result: ${body} ${respCode} ${json}".toString())
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def result = json.result
                def checkObserverBeenDropedNode = result.nodes.find {
                    it.ip == ip2
                }
                assertNull(checkObserverBeenDropedNode)
        }

        drop_cluster_api.call(msHttpPort, dropFeClusterBody) {
            respCode, body ->
                log.info("drop fe cluster http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("after get cluster get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def result = json.result
                def FECluster = result.clusters.find {
                    it.cluster_id == "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
                }
                assertNull(FECluster)
        } 

        // 5. Test Drop node, unable to find node message HTTP code return 404
        def compute_ip1 = "182.0.0.1" 
        def heartbeatPort = 9050
        def nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${compute_ip1}", heartbeat_port: "${heartbeatPort}"]
        def nodeList = [nodeMap]
        def clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
        instance = [instance_id: "${instance_id}", cluster: clusterMap]
        def addComputeGroupBody = jsonOutput.toJson(instance)
        add_cluster_api.call(msHttpPort, addComputeGroupBody) {
                respCode, body ->
                    log.info("http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("after get cluster get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        } 

        // 182.0.0.3 not in instance, can't find to drop, return code 404
        def node1 = [cloud_unique_id: "${cloudUniqueId}", ip : "182.0.0.3", heartbeat_port: 9050] 
        def del_compute_nodes = [node1]
        def del_compute_nodes_cluster = [cluster_name: "${clusterName}", cluster_id: "${clusterId}", type: "COMPUTE", nodes: del_compute_nodes]
        def del_compute_nodes_body = [instance_id: "${instance_id}", cluster: del_compute_nodes_cluster]
        jsonOutput = new JsonOutput()
        def delComputeNodesBody = jsonOutput.toJson(del_compute_nodes_body)

        drop_node_api.call(msHttpPort, delComputeNodesBody) {
            respCode, body ->
                log.info("drop compute group http cli result: ${body} ${respCode}".toString())
                assertEquals(404, respCode)
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("NOT_FOUND"))
        }
    }

    // 6. check 127.0.0.1 ms exit
    def optionsForMs = new ClusterOptions()
    optionsForMs.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    optionsForMs.setFeNum(1)
    optionsForMs.setBeNum(1)
    optionsForMs.setMsNum(2)
    optionsForMs.cloudMode = true

    docker(optionsForMs) {
        log.info("in test ms docker env")
        def mss = cluster.getAllMetaservices()
        cluster.addRWPermToAllFiles()
        def ms2 = cluster.getMetaservices().get(1)
        assertNotNull(ms2)
        // change ms2 conf, and restart it
        def confFile = ms2.getConfFilePath()
        log.info("ms2 conf file: {}", confFile)
        def writer = new PrintWriter(new FileWriter(confFile, true))  // true 表示 append 模式
        writer.println("priority_networks=127.0.0.1/32")
        writer.flush()
        writer.close()

        cluster.restartMs(ms2.index)
        // check ms2 exit, exit need some time
        sleep(15000)
        ms2 = cluster.getMetaservices().get(1)
        def ms2Alive = ms2.alive
        assertFalse(ms2Alive)
    }
}
