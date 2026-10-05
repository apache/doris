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

Suite.metaClass.addNodeForSmoothUpgrade = { String be_unique_id, String ip, String port, String cluster_name, String cluster_id ->

    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance

    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    def jsonOutput = new JsonOutput()
    def clusterInfo = [
                 type: "COMPUTE",
                 cluster_name : cluster_name,
                 cluster_id : cluster_id,
                 nodes: [
                     [
                         cloud_unique_id: be_unique_id,
                         ip: ip,
                         heartbeat_port: port,
                         is_smooth_upgrade: true
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
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }
}

logger.info("Added 'addNodeForSmoothUpgrade' function to Suite")

