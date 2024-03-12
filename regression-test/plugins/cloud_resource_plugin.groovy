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

/*
    curl "127.0.0.1:5001/MetaService/http/get_obj_store_info?token=greedisgood9999" -d '{"cloud_unique_id": "pipeline_tiny_be"}'

    {
        "code": "OK",
        "msg": "",
        "result": {
            "obj_info": [
                {
                    "ctime": "1666080836",
                    "mtime": "1666080836",
                    "id": "1",
                    "ak": "xxxxxx",
                    "sk": "xxxxxxxxx",
                    "bucket": "xxxxxxxxx",
                    "prefix": "pipeline-pipeline-prefix",
                    "endpoint": "cos.ap-beijing.myqcloud.com",
                    "region": "ap-beijing",
                    "provider": "COS"
                }
            ]
        }
    }
*/
Suite.metaClass.getObjStoreInfo = { String token, String cloudUniqueId ->

    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("Test plugin: suiteName: ${suite.name}, token:${token}, cloudUniqueId: ${cloudUniqueId}".toString())

    def getObjStoreInfoApiBody = [cloud_unique_id:"${cloudUniqueId}"]
    def jsonOutput = new JsonOutput()
    def getObjStoreInfoApiBodyJson = jsonOutput.toJson(getObjStoreInfoApiBody)
    def getObjStoreInfoApi = { requestBody, checkFunc ->
        httpTest {
            endpoint suite.context.config.metaServiceHttpAddress
            uri "/MetaService/http/get_obj_store_info?token=$token"
            body requestBody
            check checkFunc
        }
    }

    def getObjStoreInfoApiResult = null
    getObjStoreInfoApi.call(getObjStoreInfoApiBodyJson) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            getObjStoreInfoApiResult = parseJson(body)
            assertTrue(getObjStoreInfoApiResult.code.equalsIgnoreCase("OK"))
    }
    suite.getLogger().info("getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())
    return getObjStoreInfoApiResult
}

logger.info("Added 'getObjStoreInfo' function to Suite")

