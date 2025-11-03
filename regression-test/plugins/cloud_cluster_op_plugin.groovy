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

Suite.metaClass.addClusterLb = { be_unique_id, ip, port, cluster_name, cluster_id, public_endpoint, private_endpoint ->
    def jsonOutput = new JsonOutput()
    def s3 = [
                 type: "COMPUTE",
                 cluster_name : cluster_name,
                 cluster_id : cluster_id,
                 public_endpoint: public_endpoint,
                 private_endpoint: private_endpoint,
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
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }
}

Suite.metaClass.updateClusterEndpoint = { cluster_id, public_endpoint, private_endpoint ->
    def jsonOutput = new JsonOutput()
    def s3 = [
                 cluster_id : cluster_id,
                 public_endpoint: public_endpoint,
                 private_endpoint: private_endpoint,
             ]
    def map = [instance_id: "${instance_id}", cluster: s3]
    def js = jsonOutput.toJson(map)
    log.info("add cluster req: ${js} ".toString())

    def add_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/update_cluster_endpoint?token=${token}"
            body request_body
            check check_func
        }
    }

    add_cluster_api.call(js) {
        respCode, body ->
            log.info("update cluster endpoint: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }
}

logger.info("Added 'cloud cluster op' function to Suite")

