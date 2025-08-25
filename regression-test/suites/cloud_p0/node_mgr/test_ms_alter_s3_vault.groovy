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

suite('test_ms_alter_s3_vault', 'p0, docker') {
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

    def show_storage_vaults_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/show_storage_vaults?token=${token}"
            body request_body
            check check_func
        }
    }

    def alter_s3_vault_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/alter_s3_vault?token=$token"
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
            "vault": {
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
        def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", vault: [obj_info: s3]]
        def instance_body = jsonOutput.toJson(map)
        logger.info("instance_body: ${instance_body}")

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

        // show stoarge vaults
        /*

            curl http://127.0.0.1:5000/MetaService/http/show_storage_vaults?token=greedisgood9999 -d '{
                "cloud_unique_id":"cloud_unique_id_compute_node0"
            }'
        */

        def show_storage_vaults_api_body = [cloud_unique_id:"${cloudUniqueId}"]
        show_storage_vaults_api.call(msHttpPort, jsonOutput.toJson(show_storage_vaults_api_body)) {
            respCode, body ->
                log.info("show_storage_vaults_api resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        def mtime = json.result.storage_vault[0]["obj_info"]["mtime"]
        def ctime = json.result.storage_vault[0]["obj_info"]["ctime"]

        // alter s3 vaults
        /*
            curl http://127.0.0.1:5000/MetaService/http/v1/alter_s3_vault?token=greedisgood9999 -d '{
                "cloud_unique_id":"cloud_unique_id_compute_node0",
                "vault": {
                    "name": "built_in_storage_vault",
                    "obj_info": {
                        "ak": "test-ak2",
                        "sk": "test-sk2"
                    }
                }
            }'

            curl http://127.0.0.1:5000/MetaService/http/v1/alter_s3_vault?token=greedisgood9999 -d '{
                "cloud_unique_id":"cloud_unique_id_compute_node0",
                "vault": {
                    "name": "built_in_storage_vault",
                    "obj_info": {
                        "role_arn": "test-role-arn",
                        "external_id": "test-external-id"
                    }
                }
            }'
        */
        sleep(2000)
        def alter_s3_vault_body = [cloud_unique_id:"${cloudUniqueId}", 
                                    vault:[
                                        name:"built_in_storage_vault",
                                        obj_info:[ak:"test-ak2", sk:"test-sk2"]
                                    ]]

        alter_s3_vault_api.call(msHttpPort, jsonOutput.toJson(alter_s3_vault_body)) {
            respCode, body ->
                log.info("alter_s3_vault_api resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        show_storage_vaults_api.call(msHttpPort, jsonOutput.toJson(show_storage_vaults_api_body)) {
            respCode, body ->
                log.info("show_storage_vaults_api resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        assertTrue(json.result.storage_vault[0]["obj_info"]["ak"].equalsIgnoreCase("test-ak2"))
        assertTrue(json.result.storage_vault[0]["obj_info"]["sk"].equalsIgnoreCase("test-sk2"))

        def mtime2 = json.result.storage_vault[0]["obj_info"]["mtime"]
        def ctime2 = json.result.storage_vault[0]["obj_info"]["ctime"]
        assertTrue(mtime2 > mtime)
        assertTrue(ctime2 == ctime)


        sleep(2000)
        alter_s3_vault_body = [cloud_unique_id:"${cloudUniqueId}", 
                                    vault:[
                                        name:"built_in_storage_vault",
                                        obj_info:[role_arn:"test-role-arn", external_id:"test-external-id"]
                                    ]]

        alter_s3_vault_api.call(msHttpPort, jsonOutput.toJson(alter_s3_vault_body)) {
            respCode, body ->
                log.info("alter_s3_vault_api resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        show_storage_vaults_api.call(msHttpPort, jsonOutput.toJson(show_storage_vaults_api_body)) {
            respCode, body ->
                log.info("show_storage_vaults_api resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        assertTrue(json.result.storage_vault[0]["obj_info"]["role_arn"].equalsIgnoreCase("test-role-arn"))
        assertTrue(json.result.storage_vault[0]["obj_info"]["external_id"].equalsIgnoreCase("test-external-id"))
        assertTrue(json.result.storage_vault[0]["obj_info"]["cred_provider_type"].equalsIgnoreCase("INSTANCE_PROFILE"))

        def mtime3 = json.result.storage_vault[0]["obj_info"]["mtime"]
        def ctime3 = json.result.storage_vault[0]["obj_info"]["ctime"]
        assertTrue(mtime3 > mtime)
        assertTrue(ctime3 == ctime)
    }
}