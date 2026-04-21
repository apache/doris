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
import groovy.json.JsonSlurper

suite('test_update_and_show_cloud_config') {
    if (!isCloudMode()) {
        return
    }

    def token = context.config.metaServiceToken ?: "greedisgood9999"
    def configKey = "recycle_interval_seconds"
    def newValue = "999"

    // Helper: call show_config on a service and return the value for configKey, or null if not found.
    def showConfig = { serviceAddr, servicePath ->
        def result = null
        httpTest {
            endpoint serviceAddr
            uri "${servicePath}/show_config?token=${token}&conf_key=${configKey}"
            op "get"
            check { respCode, body ->
                assertEquals(200, respCode)
                // Response is {"code":"OK","msg":"","result":[[name, type, value, is_mutable], ...]}
                def parsed = new JsonSlurper().parseText(body.trim())
                assertEquals("OK", parsed.code, "show_config should return code=OK, got: ${body}")
                assertTrue(parsed.result instanceof List, "show_config result should be a JSON array")
                def entry = parsed.result.find { it[0] == configKey }
                assertNotNull(entry, "Config key '${configKey}' not found in show_config response")
                result = entry[2]  // value is the 3rd element
            }
        }
        return result
    }

    // Helper: call update_config on a service and assert success.
    def updateConfig = { serviceAddr, servicePath, value ->
        httpTest {
            endpoint serviceAddr
            uri "${servicePath}/update_config?token=${token}&configs=${configKey}=${value}"
            op "get"
            check { respCode, body ->
                logger.info("update_config response: respCode=${respCode}, body=${body}")
                assertEquals(200, respCode)
                def parsed = new JsonSlurper().parseText(body.trim())
                assertEquals("OK", parsed.code, "update_config should return code=OK, got: ${body}")
            }
        }
    }

    // ── Meta Service ──────────────────────────────────────────────────────────
    def msAddr = context.config.metaServiceHttpAddress
    def msPath = "/MetaService/http"

    // 1. Read the original value so we can restore it afterwards.
    def originalMsValue = showConfig(msAddr, msPath)
    logger.info("meta-service original ${configKey}=${originalMsValue}")

    try {
        // 2. Update to newValue.
        updateConfig(msAddr, msPath, newValue)

        // 3. Verify show_config reflects the new value.
        def updatedMsValue = showConfig(msAddr, msPath)
        logger.info("meta-service updated ${configKey}=${updatedMsValue}")
        assertEquals(newValue, updatedMsValue,
                "meta-service: show_config should return updated value ${newValue}, got ${updatedMsValue}")
    } finally {
        // 4. Restore original value.
        if (originalMsValue != null) {
            updateConfig(msAddr, msPath, originalMsValue)
            logger.info("meta-service restored ${configKey}=${originalMsValue}")
        }
    }

    // ── Recycler Service ──────────────────────────────────────────────────────
    def recyclerAddr = context.config.recycleServiceHttpAddress
    def recyclerPath = "/RecyclerService/http"

    def originalRecyclerValue = showConfig(recyclerAddr, recyclerPath)
    logger.info("recycler original ${configKey}=${originalRecyclerValue}")

    try {
        updateConfig(recyclerAddr, recyclerPath, newValue)

        def updatedRecyclerValue = showConfig(recyclerAddr, recyclerPath)
        logger.info("recycler updated ${configKey}=${updatedRecyclerValue}")
        assertEquals(newValue, updatedRecyclerValue,
                "recycler: show_config should return updated value ${newValue}, got ${updatedRecyclerValue}")
    } finally {
        if (originalRecyclerValue != null) {
            updateConfig(recyclerAddr, recyclerPath, originalRecyclerValue)
            logger.info("recycler restored ${configKey}=${originalRecyclerValue}")
        }
    }
}
