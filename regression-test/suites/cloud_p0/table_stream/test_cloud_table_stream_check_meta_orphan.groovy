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

suite("test_cloud_table_stream_check_meta_orphan", "docker,check_meta") {
    def token = "greedisgood9999"
    def instanceId = "table_stream_check_meta_orphan_${System.currentTimeMillis()}"
    def baseDbId = 9000000000000000000L
    def baseTableId = 9000000000000000001L
    def partitionId = 9000000000000000002L
    def orphanStreamId = 9223372036854770000L

    def options = new ClusterOptions()
    options.cloudMode = true
    options.instanceId = instanceId
    options.setFeNum(1)
    options.setBeNum(1)
    options.msNum = 1
    options.recyclerNum = 1
    options.feConfigs += [
        "enable_table_stream=true",
        "enable_feature_binlog=true",
    ]
    options.beConfigs += [
        "enable_feature_binlog=true",
    ]
    options.msConfigs += [
        "http_token=${token}",
    ]
    options.recycleConfigs += [
        "http_token=${token}",
        "enable_meta_key_check=true",
        "recycler_sleep_before_scheduling_seconds=0",
        "recycle_interval_seconds=1",
        "retention_seconds=0",
    ]

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def recycler = cluster.getAllRecyclers().get(0)
        def fe = cluster.getMasterFe()
        def msEndpoint = "${ms.host}:${ms.httpPort}"
        def recyclerEndpoint = "${recycler.host}:${recycler.httpPort}"
        def jdbcUser = context.config.jdbcUser
        def jdbcPassword = context.config.jdbcPassword

        def httpCall = { String endpointAddress, String uriPath, String requestBody = null ->
            def result = [code: -1, body: ""]
            httpTest {
                endpoint endpointAddress
                uri uriPath
                if (requestBody == null) {
                    op "get"
                } else {
                    body requestBody
                }
                check { responseCode, responseBody ->
                    result.code = responseCode
                    result.body = responseBody
                }
            }
            return result
        }

        def checkMeta = {
            def encodedUser = java.net.URLEncoder.encode(jdbcUser, "UTF-8")
            def encodedPassword = java.net.URLEncoder.encode(jdbcPassword, "UTF-8")
            return httpCall(
                recyclerEndpoint,
                "/RecyclerService/http/check_meta?token=${token}&instance_id=${instanceId}" +
                    "&host=${fe.host}&port=${fe.queryPort}&user=${encodedUser}&password=${encodedPassword}")
        }

        def healthyResult = checkMeta()
        assertEquals(200, healthyResult.code)
        assertEquals("OK", healthyResult.body.trim())

        def commonKeyParams = "instance_id=${instanceId}" +
            "&base_db_id=${baseDbId}&base_table_id=${baseTableId}" +
            "&stream_db_id=${baseDbId}&stream_id=${orphanStreamId}&partition_id=${partitionId}"
        def latestGetUri = "/MetaService/http/get_value?token=${token}" +
            "&key_type=TableStreamOffsetKey&${commonKeyParams}"
        def latestSetUri = "/MetaService/http/set_value?token=${token}" +
            "&key_type=TableStreamOffsetKey&${commonKeyParams}"
        def versionedGetUri = "/MetaService/http/get_value?token=${token}" +
            "&key_type=VersionedTableStreamOffsetKey&${commonKeyParams}"
        def versionedSetUri = "/MetaService/http/set_value?token=${token}" +
            "&key_type=VersionedTableStreamOffsetKey&${commonKeyParams}" +
            "&versionstamp=00000000000000010001"
        def recycleIndexGetUri = "/MetaService/http/get_value?token=${token}" +
            "&key_type=RecycleIndexKey&instance_id=${instanceId}&index_id=${orphanStreamId}"
        def offsetBody = JsonOutput.toJson([
            partition_id: partitionId.toString(),
            state: "TABLE_STREAM_OFFSET_CONSUMED",
            offset_tso: "123",
            last_consumption_time_ms: "456",
        ])
        def missingRecycleIndex = httpCall(msEndpoint, recycleIndexGetUri)
        assertEquals(500, missingRecycleIndex.code)
        assertTrue(missingRecycleIndex.body.contains("kv not found"))

        def latestSetResult = httpCall(msEndpoint, latestSetUri, offsetBody)
        assertEquals(200, latestSetResult.code)
        def versionedSetResult = httpCall(msEndpoint, versionedSetUri, offsetBody)
        assertEquals(200, versionedSetResult.code)

        def latestGetResult = httpCall(msEndpoint, latestGetUri)
        assertEquals(200, latestGetResult.code)
        assertTrue(latestGetResult.body.replaceAll(/\s+/, "").contains("\"offset_tso\":\"123\""))
        def versionedGetResult = httpCall(msEndpoint, versionedGetUri)
        assertEquals(200, versionedGetResult.code)
        assertTrue(versionedGetResult.body.replaceAll(/\s+/, "").contains("\"offset_tso\":\"123\""))
        assertTrue(versionedGetResult.body.contains("versionstamp=00000000000000010001"))

        def orphanResult = checkMeta()
        assertEquals(200, orphanResult.code)
        assertEquals("table stream meta mismatch", orphanResult.body.trim())
    }
}
