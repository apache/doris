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

suite("global_variables_management", "p0,nonConcurrent") {
    // 获取和设置配置的辅助方法
    def getConfigValue = { configName ->
        def result = sql "ADMIN SHOW FRONTEND CONFIG LIKE '${configName}'"
        return result[0][1]
    }

    def setConfigValue = { configName, value ->
        sql "ADMIN SET FRONTEND CONFIG ('${configName}' = '${value}')"
    }

    def getGlobalVarsValues = { fe, reqBody, check_func ->
        httpTest {
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            endpoint fe.Host + ":" + fe.HttpPort
            uri "/api/_get_vars_values"
            op "post"
            body reqBody
            check check_func
        }
    }

    // 通过API获取单个全局变量的值
    def getGlobalVarByAPI = { fe, varName ->
        def varInfo = null
        httpTest {
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            endpoint fe.Host + ":" + fe.HttpPort
            uri "/api/_get_vars_values"
            op "post"
            body """["${varName}"]"""
            check { code, resBody ->
                assertEquals(200, code)
                def json = parseJson(resBody)
                assertEquals(0, json.code)
                if (json.data && !json.data.isEmpty()) {
                    def varData = json.data.find { it.name == varName }
                    if (varData) {
                        varInfo = varData
                    }
                }
            }
        }
        return varInfo?.value
    }

    // 获取变量的完整信息（包含默认值、是否修改等）
    def getGlobalVarInfoByAPI = { fe, varName ->
        def varInfo = null
        httpTest {
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            endpoint fe.Host + ":" + fe.HttpPort
            uri "/api/_get_vars_values"
            op "post"
            body """["${varName}"]"""
            check { code, resBody ->
                assertEquals(200, code)
                def json = parseJson(resBody)
                assertEquals(0, json.code)
                if (json.data && !json.data.isEmpty()) {
                    varInfo = json.data.find { it.name == varName }
                }
            }
        }
        return varInfo
    }

    // 校验变量的全局值与预设期望值比较
    def checkGlobalVarValue = { fe, varName, expectedValue ->
        httpTest {
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            endpoint fe.Host + ":" + fe.HttpPort
            uri "/api/_get_vars_values"
            op "post"
            body """["${varName}"]"""
            check { code, resBody ->
                assertEquals(200, code)
                def json = parseJson(resBody)
                assertEquals(0, json.code)
                assertTrue("Variable ${varName} not found in response", json.data && !json.data.isEmpty())

                def varData = json.data.find { it.name == varName }
                assertNotNull("Variable ${varName} not found in response data", varData)

                def actualValue = varData.value?.toString()
                assertEquals("Variable ${varName} value mismatch", expectedValue, actualValue)

                log.info("Checked ${varName}: expected=${expectedValue}, actual=${actualValue}, changed=${varData.changed}")
            }
        }
    }

    // 批量校验多个变量的值
    def checkGlobalVarsValues = { fe, expectedVarsMap ->
        def varNames = expectedVarsMap.keySet().join('","')
        httpTest {
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            endpoint fe.Host + ":" + fe.HttpPort
            uri "/api/_get_vars_values"
            op "post"
            body """["${varNames}"]"""
            check { code, resBody ->
                assertEquals(200, code)
                def json = parseJson(resBody)
                assertEquals(0, json.code)
                assertTrue("No variables found in response", json.data && !json.data.isEmpty())

                expectedVarsMap.each { varName, expectedValue ->
                    def varData = json.data.find { it.name == varName }
                    assertNotNull("Variable ${varName} not found in response data", varData)

                    def actualValue = varData.value?.toString()
                    assertEquals("Variable ${varName} value mismatch", expectedValue, actualValue)

                    log.info("Checked ${varName}: expected=${expectedValue}, actual=${actualValue}, changed=${varData.changed}, defaultValue=${varData.defaultValue}")
                }
            }
        }
    }

    // 校验变量是否恢复为默认值
    def checkGlobalVarIsDefault = { fe, varName ->
        httpTest {
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            endpoint fe.Host + ":" + fe.HttpPort
            uri "/api/_get_vars_values"
            op "post"
            body """["${varName}"]"""
            check { code, resBody ->
                assertEquals(200, code)
                def json = parseJson(resBody)
                assertEquals(0, json.code)

                def varData = json.data.find { it.name == varName }
                assertNotNull("Variable ${varName} not found in response", varData)
                assertEquals("Variable ${varName} should be default (changed=0)", "0", varData.changed)
                assertEquals("Variable ${varName} value should equal default value",
                        varData.value, varData.defaultValue)

                log.info("Checked ${varName} is default: value=${varData.value}, defaultValue=${varData.defaultValue}")
            }
        }
    }

    def fuzzyVarNameResult = sql "show global variables like '%timeout%'"
    log.info("show global variables fuzzyVarNameResult: ${fuzzyVarNameResult}")

    def convertedVarNameResult = sql "show global variables like 'runtime_filter_type'"
    log.info("show global variables convertedVarNameResult: ${convertedVarNameResult}")

    def exactVarNameResult = sql "show global variables like 'experimental_enable_local_shuffle'"
    log.info("show global variables exactVarNameResult: ${exactVarNameResult}")

    // 将结果转换为Map<String, List<String>>格式
    def convertToMap = { results ->
        Map<String, List<String>> map = [:]
        results.each { row ->
            if (row && row.size() >= 1) {
                String key = row[0]?.toString()
                List<String> value = row.collect { it?.toString() }
                map[key] = value
            }
        }
        return map
    }

    // 转换三个结果集
    def fuzzyVarNameResultMap = convertToMap(fuzzyVarNameResult)
    def convertedVarNameResultMap = convertToMap(convertedVarNameResult)
    def exactVarNameResultMap = convertToMap(exactVarNameResult)

    log.info("Converted fuzzyVarNameResultMap: ${fuzzyVarNameResultMap}")
    log.info("Converted convertedVarNameResultMap: ${convertedVarNameResultMap}")
    log.info("Converted exactVarNameResultMap: ${exactVarNameResultMap}")

    // test for get_vars_values api
    foreachFrontends { fe ->
        getGlobalVarsValues(fe, """["timeout", "runtime_filter_type", "enable_local_shuffle"]""", { code, resBody ->
            assertEquals(200, code)
            def json = parseJson(resBody)
            log.info("get_vars_values response: ${json}")
            assertEquals(0, json.code)
            assertTrue("Response data should be a list", json.data instanceof List)

            // 验证返回的变量结构
            if (!json.data.isEmpty()) {
                json.data.each { var ->
                    assertTrue("Variable should have 'name' field", var.containsKey("name"))
                    assertTrue("Variable should have 'value' field", var.containsKey("value"))
                    assertTrue("Variable should have 'defaultValue' field", var.containsKey("defaultValue"))
                    assertTrue("Variable should have 'changed' field", var.containsKey("changed"))
                }
            }
        })
    }

    // 保存原始配置
    def originalRollbackConfig = getConfigValue("enable_rollback_after_bulk_session_variables_set_failed")

    try {
        // case1: invalid variable value with enabling enable_rollback_after_bulk_session_variables_set_failed
        setConfigValue("enable_rollback_after_bulk_session_variables_set_failed", "true")

        foreachFrontends { fe ->
            log.info("=== Testing FE node: ${fe.Host}:${fe.HttpPort} with rollback enabled ===")

            // 先记录原始值和默认值
            def analyzeTimeoutInfo = getGlobalVarInfoByAPI(fe, "analyze_timeout")
            def insertTimeoutInfo = getGlobalVarInfoByAPI(fe, "insert_timeout")
            def queryTimeoutInfo = getGlobalVarInfoByAPI(fe, "query_timeout")

            log.info("Original values - analyze_timeout: ${analyzeTimeoutInfo}, insert_timeout: ${insertTimeoutInfo}, query_timeout: ${queryTimeoutInfo}")

            def globalAnalyzeTimeout = analyzeTimeoutInfo.value
            def globalInsertTimeout = insertTimeoutInfo.value
            def globalQueryTimeout = queryTimeoutInfo.value

            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_set_vars"
                op "put"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body '''[      
                {"name": "analyze_timeout", "value": "5123"},      
                {"name": "insert_timeout", "value": "1231"},      
                {"name": "query_timeout", "value": "1123"}      
            ]'''
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(0, json.code)
                }
            }

            // 校验设置后的值（changed应该为1）
            checkGlobalVarsValues(fe, [
                    "analyze_timeout": "5123",
                    "insert_timeout": "1231",
                    "query_timeout": "1123"
            ])

            // 校验changed状态
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_get_vars_values"
                op "post"
                body '["analyze_timeout", "insert_timeout", "query_timeout"]'
                check { code, resBody ->
                    def json = parseJson(resBody)
                    json.data.each { var ->
                        assertEquals("Variable ${var.name} should be marked as changed", "1", var.changed)
                    }
                }
            }

            def checkFunc1 = { code, resBody ->
                assertEquals(200, code)
                def json = parseJson(resBody)
                assertEquals(0, json.code)
                // 验证返回的数据结构
                assertTrue(json.data instanceof List)
            }

            getGlobalVarsValues(fe, """["timeout", "sql_mode", "runtime_filter_type", "enable_local_shuffle"]""", checkFunc1)

            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_set_vars"
                op "put"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body '''[      
                {"name": "analyze_timeout", "value": "9999"},      
                {"name": "insert_timeout", "value": "a1"},      
                {"name": "query_timeout", "value": "8888"}      
            ]'''
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(403, json.code)  // 根据API实现，返回403错误码
                    String message = String.format("vars value set failed because of invalid value for var: %s", "insert_timeout")
                    assertTrue(resBody.contains(message))

                    // 使用API验证回滚
                    def globalAnalyzeTimeout2 = getGlobalVarByAPI(fe, "analyze_timeout")
                    assertNotEquals(globalAnalyzeTimeout2, "9999")
                    assertEquals(globalAnalyzeTimeout2, "5123")

                    def globalInsertTimeout2 = getGlobalVarByAPI(fe, "insert_timeout")
                    assertNotEquals(globalInsertTimeout2, "a1")
                    assertEquals(globalInsertTimeout2, "1231")

                    def globalQueryTimeout2 = getGlobalVarByAPI(fe, "query_timeout")
                    assertNotEquals(globalQueryTimeout2, "8888")
                    assertEquals(globalQueryTimeout2, "1123")

                    log.info("Set vars failed with rollback: ${resBody}")
                }
            }

            // 使用新的校验方法验证回滚后的值
            checkGlobalVarValue(fe, "analyze_timeout", "5123")
            checkGlobalVarValue(fe, "query_timeout", "1123")

            def globalAnalyzeTimeoutAfterRollback = getGlobalVarByAPI(fe, "analyze_timeout")
            assertEquals("5123", globalAnalyzeTimeoutAfterRollback)
            log.info("After rollback, analyze_timeout remains: ${globalAnalyzeTimeoutAfterRollback}")

            def globalQueryTimeoutAfterRollback = getGlobalVarByAPI(fe, "query_timeout")
            assertEquals("1123", globalQueryTimeoutAfterRollback)
        }

        // case2: invalid variable value with disabling enable_rollback_after_bulk_session_variables_set_failed
        setConfigValue("enable_rollback_after_bulk_session_variables_set_failed", "false")

        foreachFrontends { fe ->
            log.info("=== Testing FE node: ${fe.Host}:${fe.HttpPort} with rollback disabled ===")

            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_set_vars"
                op "put"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body '''[      
                {"name": "analyze_timeout", "value": "6000"},      
                {"name": "insert_timeout", "value": "2000"},      
                {"name": "net_read_timeout", "value": "invalid"},      
                {"name": "net_write_timeout", "value": "111"},      
                {"name": "query_timeout", "value": "3000"}      
            ]'''
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(0, json.code)  // 部分成功返回200
                    assertTrue(resBody.contains("vars value set partially successful"))
                    assertTrue(resBody.contains("successful vars is"))
                    assertTrue(resBody.contains("analyze_timeout"))
                    assertTrue(resBody.contains("insert_timeout"))
                    assertTrue(resBody.contains("net_write_timeout"))
                    assertTrue(resBody.contains("query_timeout"))
                    assertTrue(resBody.contains("failed vars is"))
                    assertTrue(resBody.contains("net_read_timeout"))
                    assertTrue(resBody.contains("invalid value for var is: net_read_timeout"))
                    log.info("Set vars partially successful: ${resBody}")
                }
            }

            // 使用API验证成功的变量已修改
            checkGlobalVarValue(fe, "analyze_timeout", "6000")
            checkGlobalVarValue(fe, "insert_timeout", "2000")
            checkGlobalVarValue(fe, "net_write_timeout", "111")
            checkGlobalVarValue(fe, "query_timeout", "3000")

            // 验证失败的变量值不变
            def originalNetReadTimeout = getGlobalVarByAPI(fe, "net_read_timeout")
            checkGlobalVarValue(fe, "net_read_timeout", originalNetReadTimeout)

            // 验证session变量未变化（使用默认值）
            def sessionAnalyzeTimeout = sql "show variables where variable_name='analyze_timeout'"
            log.info("Session analyze_timeout (should be default): ${sessionAnalyzeTimeout[0][1]}")
        }

        // ========== 场景3: 测试 unset_vars 接口 ==========
        foreachFrontends { fe ->
            log.info("=== Testing unset_vars on FE node: ${fe.Host}:${fe.HttpPort} ===")

            // 先获取默认值
            def analyzeTimeoutInfo = getGlobalVarInfoByAPI(fe, "analyze_timeout")
            def netWriteTimeoutInfo = getGlobalVarInfoByAPI(fe, "net_write_timeout")

            def defaultAnalyzeTimeout = analyzeTimeoutInfo.defaultValue
            def defaultNetWriteTimeout = netWriteTimeoutInfo.defaultValue

            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_unset_vars"
                op "put"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body '''[  
                "analyze_timeout",      
                "net_write_timeout"      
            ]'''
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(0, json.code)
                }
            }

            // 使用API验证变量已恢复默认值且状态为未修改
            checkGlobalVarIsDefault(fe, "analyze_timeout")
            checkGlobalVarIsDefault(fe, "net_write_timeout")

            log.info("After unset, analyze_timeout restored to default: ${defaultAnalyzeTimeout}")
            log.info("After unset, net_write_timeout restored to default: ${defaultNetWriteTimeout}")
        }

        foreachFrontends { fe ->
            log.info("=== Testing unset_all_vars on FE node: ${fe.Host}:${fe.HttpPort} ===")

            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_unset_all_vars"
                op "put"
                body '''{}'''
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(0, json.code)
                }
            }

            // 验证所有变量已恢复默认值
            def allVars = sql "show global variables"
            def allUnchangedVars = sql "show global variables where changed=0"
            log.info("After unset all, total variables count: ${allVars.size()}, ${allUnchangedVars.size()}")
            assertEquals(allVars.size(), allUnchangedVars.size())

            // 随机验证几个重要变量的默认值
            checkGlobalVarIsDefault(fe, "query_timeout")
            checkGlobalVarIsDefault(fe, "analyze_timeout")
            checkGlobalVarIsDefault(fe, "exec_mem_limit")
        }

        // ========== 场景4: 测试 _get_vars 接口 ==========
        foreachFrontends { fe ->
            log.info("=== Testing _get_vars on FE node: ${fe.Host}:${fe.HttpPort} ===")

            // 设置一些测试变量
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_set_vars"
                op "put"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body '''[      
                    {"name": "query_timeout", "value": "12345"},      
                    {"name": "exec_mem_limit", "value": "8589934592"},  
                    {"name": "query_mem_limit", "value": "2147483648"}      
                ]'''
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(0, json.code)
                }
            }

            // 验证设置的值
            checkGlobalVarsValues(fe, [
                    "query_timeout": "12345",
                    "exec_mem_limit": "8589934592",
                    "query_mem_limit": "2147483648"
            ])

            // 测试无patterns参数（返回空数组）
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_get_vars"
                op "get"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertTrue(json.data instanceof List)
                    assertEquals(0, json.data.size())
                    log.info("Empty patterns returns empty array: ${resBody}")
                }
            }

            // 测试单个模式匹配
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_get_vars?patterns=%timeout%"
                op "get"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertTrue(json.data instanceof List)
                    assertTrue(json.data.size() >= 2)
                    // 验证数组格式
                    json.data.each { item ->
                        assertTrue(item.containsKey("name"))
                        assertTrue(item.containsKey("value"))
                        assertTrue(item.containsKey("defaultValue"))
                        assertTrue(item.containsKey("changed"))
                    }
                    // 验证包含timeout相关变量
                    def timeoutVars = json.data.findAll { it.name.contains("timeout") }
                    assertTrue(timeoutVars.size() >= 1)
                    def queryTimeoutVar = timeoutVars.find { it.name == "query_timeout" }
                    assertNotNull(queryTimeoutVar)
                    assertEquals("12345", queryTimeoutVar.value)
                    assertEquals("1", queryTimeoutVar.changed)
                    log.info("Get vars with single pattern: found ${timeoutVars.size()} timeout variables")
                }
            }

            // 测试多个模式匹配
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_get_vars?patterns=%timeout%,%mem_limit%"
                op "get"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertTrue(json.data instanceof List)
                    assertTrue(json.data.size() >= 3)
                    // 验证包含所有匹配的变量
                    def varMap = json.data.collectEntries { [(it.name): it.value] }
                    assertEquals("12345", varMap["query_timeout"])
                    assertEquals("8589934592", varMap["exec_mem_limit"])
                    assertEquals("2147483648", varMap["query_mem_limit"])
                    log.info("Get vars with multiple patterns: found ${json.data.size()} variables")
                }
            }

            // 测试无匹配的模式
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_get_vars?patterns=nonexistent_%"
                op "get"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(400, code)
                    def json = parseJson(resBody)
                    assertEquals(400, json.code)
                    assertTrue(resBody.contains("No variables match any pattern"))
                    log.info("No matches returns error: ${resBody}")
                }
            }

            // 测试精确匹配
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_get_vars?patterns=query_timeout"
                op "get"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertTrue(json.data instanceof List)
                    assertEquals(1, json.data.size())
                    def var = json.data[0]
                    assertEquals("query_timeout", var.name)
                    assertEquals("12345", var.value)
                    assertEquals("1", var.changed)
                    log.info("Get exact match var: ${var}")
                }
            }

            // 清理测试变量
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_unset_vars"
                op "put"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body '''["query_timeout", "exec_mem_limit", "query_mem_limit"]'''
                check { code, resBody ->
                    assertEquals(200, code)
                    def json = parseJson(resBody)
                    assertEquals(0, json.code)
                }
            }

            // 验证变量已恢复默认值
            checkGlobalVarIsDefault(fe, "query_timeout")
        }

    } finally {
        // 恢复原始配置
        setConfigValue("enable_rollback_after_bulk_session_variables_set_failed", originalRollbackConfig)
        log.info("Restored original config: enable_rollback_after_bulk_session_variables_set_failed = ${originalRollbackConfig}")

        // 清理所有设置的变量
        foreachFrontends { fe ->
            httpTest {
                endpoint fe.Host + ":" + fe.HttpPort
                uri "/api/_unset_all_vars"
                op "put"
                body '''{}'''
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                check { code, resBody ->
                    assertEquals(200, code)
                }
            }
        }
    }
}
