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

import com.sun.net.httpserver.HttpServer
import com.sun.net.httpserver.HttpHandler
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

suite("test_ai_structured_output", "p0") {
    def expectStructured = new AtomicBoolean(true)
    def expectedFormat = new AtomicReference<String>("json_schema")
    def server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)
    server.createContext("/v1/chat/completions", { exchange ->
        try {
            def request = new JsonSlurper().parseText(exchange.requestBody.getText("UTF-8"))
            def inputs = new JsonSlurper().parseText(request.messages[1].content)
            def structured = request.containsKey("response_format")
            def valid = structured == expectStructured.get() &&
                    (!structured || request.response_format.type == expectedFormat.get()) &&
                    request.messages[0].content.contains("results")
            def content = JsonOutput.toJson([results: inputs.collect { it.input }])
            byte[] response = JsonOutput.toJson([choices: [[message: [content: content]]]]).getBytes("UTF-8")
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(valid ? 200 : 400, response.length)
            exchange.responseBody.write(response)
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    server.start()
    try {
        def host = InetAddress.getLocalHost().getHostAddress()
        sql "DROP RESOURCE IF EXISTS test_ai_structured_output_resource"
        sql """CREATE RESOURCE test_ai_structured_output_resource PROPERTIES (
            'type' = 'ai',
            'ai.provider_type' = 'openai',
            'ai.endpoint' = 'http://${host}:${server.address.port}/v1/chat/completions',
            'ai.model_name' = 'test-model',
            'ai.api_key' = 'test-key',
            'ai.max_retries' = '0',
            'ai.validity_check' = 'false'
        )"""
        def query = """SELECT id, AI_SUMMARIZE('test_ai_structured_output_resource', text)
            FROM (SELECT 1 AS id, 'first row' AS text
                  UNION ALL SELECT 2, 'second row'
                  UNION ALL SELECT 3, NULL) t ORDER BY id"""
        qt_default_batch query
        sql """ALTER RESOURCE test_ai_structured_output_resource PROPERTIES ('ai.enable_structured_output' = 'false')"""
        expectStructured.set(false)
        qt_prompt_only_rows query
        qt_prompt_only_nul """SELECT HEX(AI_SUMMARIZE('test_ai_structured_output_resource',
                CONCAT('A', CHAR(0), 'B')))"""
        test {
            sql """ALTER RESOURCE test_ai_structured_output_resource PROPERTIES ('ai.enable_structured_output' = 'invalid')"""
            exception "[ai.enable_structured_output] must be true or false"
        }
        qt_invalid_alter_preserves_setting query
        sql """ALTER RESOURCE test_ai_structured_output_resource PROPERTIES ('ai.enable_structured_output' = 'true')"""
        expectStructured.set(true)
        qt_batch_restored query
        qt_batch_nul """SELECT HEX(AI_SUMMARIZE('test_ai_structured_output_resource',
                CONCAT('A', CHAR(0), 'B')))"""
        sql """ALTER RESOURCE test_ai_structured_output_resource PROPERTIES (
            'ai.provider_type' = 'baichuan', 'ai.model_name' = 'Baichuan4-Turbo')"""
        expectedFormat.set("json_object")
        qt_baichuan_batch query
        sql """ALTER RESOURCE test_ai_structured_output_resource PROPERTIES ('ai.enable_structured_output' = 'false')"""
        expectStructured.set(false)
        qt_baichuan_prompt_only query
    } finally {
        server.stop(0)
    }
}
