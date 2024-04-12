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
import org.apache.doris.regression.suite.Suite

Suite.metaClass.add_cluster = { be_unique_id, ip, port, cluster_name, cluster_id ->
    def jsonOutput = new JsonOutput()
    def s3 = [
                     type: 'COMPUTE',
                     cluster_name : cluster_name,
                     cluster_id : cluster_id,
                     nodes: [
                         [
                             cloud_unique_id: be_unique_id,
                             ip: ip,
                             heartbeat_port: port
                         ],
                     ]
                 ]
    def map = [instance_id: "${instance_id}", cluster: s3]
    def js = jsonOutput.toJson(map)
    log.info("add cluster req: ${js} ".toString())

    def add_cluster_api = { request_body, check_func ->
        httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/add_cluster?token=${token}"
                body request_body
                check check_func
        }
    }

    add_cluster_api.call(js) {
            respCode, body ->
                log.info("add cluster resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase('OK') || json.code.equalsIgnoreCase('ALREADY_EXISTED'))
    }
}

Suite.metaClass.get_cluster = { be_unique_id ->
    def jsonOutput = new JsonOutput()
    def map = [instance_id: "${instance_id}", cloud_unique_id: "${be_unique_id}" ]
    def js = jsonOutput.toJson(map)
    log.info("get cluster req: ${js} ".toString())

    def add_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/get_cluster?token=${token}"
            body request_body
            check check_func
        }
    }

    def json
    add_cluster_api.call(js) {
        respCode, body ->
            log.info("get cluster resp: ${body} ${respCode}".toString())
            json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase('OK') || json.code.equalsIgnoreCase('ALREADY_EXISTED'))
    }
    json.result.cluster
}

Suite.metaClass.drop_cluster = { cluster_name, cluster_id ->
    def jsonOutput = new JsonOutput()
    def reqBody = [
                 type: 'COMPUTE',
                 cluster_name : cluster_name,
                 cluster_id : cluster_id,
                 nodes: [
                 ]
             ]
    def map = [instance_id: "${instance_id}", cluster: reqBody]
    def js = jsonOutput.toJson(map)
    log.info("drop cluster req: ${js} ".toString())

    def drop_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/drop_cluster?token=${token}"
            body request_body
            check check_func
        }
    }

    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("dorp cluster resp: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase('OK') || json.code.equalsIgnoreCase('ALREADY_EXISTED'))
    }
}

Suite.metaClass.add_node = { be_unique_id, ip, port, cluster_name, cluster_id ->
    def jsonOutput = new JsonOutput()
    def clusterInfo = [
                 type: 'COMPUTE',
                 cluster_name : cluster_name,
                 cluster_id : cluster_id,
                 nodes: [
                     [
                         cloud_unique_id: be_unique_id,
                         ip: ip,
                         heartbeat_port: port
                     ],
                 ]
             ]
    def map = [instance_id: "${instance_id}", cluster: clusterInfo]
    def js = jsonOutput.toJson(map)
    log.info("add node req: ${js} ".toString())

    def add_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/add_node?token=${token}"
            body request_body
            check check_func
        }
    }

    add_cluster_api.call(js) {
        respCode, body ->
            log.info("add node resp: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase('OK') || json.code.equalsIgnoreCase('ALREADY_EXISTED'))
    }
}

Suite.metaClass.d_node = { be_unique_id, ip, port, cluster_name, cluster_id ->
    def jsonOutput = new JsonOutput()
    def clusterInfo = [
                 type: 'COMPUTE',
                 cluster_name : cluster_name,
                 cluster_id : cluster_id,
                 nodes: [
                     [
                         cloud_unique_id: be_unique_id,
                         ip: ip,
                         heartbeat_port: port
                     ],
                 ]
             ]
    def map = [instance_id: "${instance_id}", cluster: clusterInfo]
    def js = jsonOutput.toJson(map)
    log.info("decommission node req: ${js} ".toString())

    def d_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/decommission_node?token=${token}"
            body request_body
            check check_func
        }
    }

    d_cluster_api.call(js) {
        respCode, body ->
            log.info("decommission node resp: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase('OK') || json.code.equalsIgnoreCase('ALREADY_EXISTED'))
    }
}

