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

suite('test_ms_alter_obj_info', 'p0, docker') {
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

    def alter_obj_info_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/alter_obj_info?token=$token"
            body request_body
            check check_func
        }
    }

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

        // Inventory function test
        def token = "greedisgood9999"
        def instance_id = "instance_id_test_in_docker"
        def name = "user_1"
        def user_id = "10000"

        def cloudUniqueId = "1:${instance_id}:xxxxx"
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
                "provider" : S3",
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
                    provider : "S3",
                    external_endpoint: "test-external-endpoint"
                ]
        def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", obj_info: s3]
        def instance_body = jsonOutput.toJson(map)

        create_instance_api.call(msHttpPort, instance_body) {
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

        // alter s3 info to instance
        /*
            curl '127.0.0.1:5000/MetaService/http/add_obj_info?token=greedisgood9999' -d '{
                "cloud_unique_id": "cloud_unique_id_compute_node0",
                "obj": {
                    "id": `1`,
                    "ak": "test-ak2",
                    "sk": "test-sk2"
                }
            }'
        */

        def alter_obj_info_api_body = [cloud_unique_id:"${cloudUniqueId}",
                                    obj:[id:"1", ak:"new-ak2", sk:"new-sk2"]]
        jsonOutput = new JsonOutput()
        def alterObjInfoBody = jsonOutput.toJson(alter_obj_info_api_body)
        logger.info("alter obj info body: ${alterObjInfoBody}")

        alter_obj_info_api.call(msHttpPort, alterObjInfoBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        assertTrue(json.result.obj_info[0]["ak"].equalsIgnoreCase("new-ak2"))
        assertTrue(json.result.obj_info[0]["sk"].equalsIgnoreCase("new-sk2"))


        alter_obj_info_api_body = [cloud_unique_id:"${cloudUniqueId}",
                                    obj:[id:"1", role_arn:"new-role-arn", external_id:"new-external-id"]]
        jsonOutput = new JsonOutput()
        alterObjInfoBody = jsonOutput.toJson(alter_obj_info_api_body)
        logger.info("alter obj info body: ${alterObjInfoBody}")

        alter_obj_info_api.call(msHttpPort, alterObjInfoBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        assertTrue(json.result.obj_info[0]["role_arn"].equalsIgnoreCase("new-role-arn"))
        assertTrue(json.result.obj_info[0]["external_id"].equalsIgnoreCase("new-external-id"))
        assertTrue(json.result.obj_info[0]["cred_provider_type"].equalsIgnoreCase("INSTANCE_PROFILE"))

        alter_obj_info_api_body = [cloud_unique_id:"${cloudUniqueId}",
                                    obj:[id:"1", ak:"new-ak3", sk:"new-sk3"]]
        jsonOutput = new JsonOutput()
        alterObjInfoBody = jsonOutput.toJson(alter_obj_info_api_body)
        logger.info("alter obj info body: ${alterObjInfoBody}")

        alter_obj_info_api.call(msHttpPort, alterObjInfoBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        get_instance_api.call(msHttpPort, instance_id) {
            respCode, body ->
                log.info("get instance resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        assertTrue(json.result.obj_info[0]["ak"].equalsIgnoreCase("new-ak3"))
        assertTrue(json.result.obj_info[0]["sk"].equalsIgnoreCase("new-sk3"))
    }

}