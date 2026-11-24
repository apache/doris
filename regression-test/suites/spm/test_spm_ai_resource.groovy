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

import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.net.InetAddress
import java.net.InetSocketAddress

suite("test_spm_ai_resource", "spm") {
    def responses = ["spm-model-a": "positive", "spm-model-b": "negative"]
    def server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)
    server.createContext("/v1/chat/completions", { exchange ->
        try {
            def request = new JsonSlurper().parseText(exchange.requestBody.getText("UTF-8"))
            def inputs = new JsonSlurper().parseText(request.messages[1].content)
            def sentiment = responses[request.model]
            def results = inputs.collect { sentiment }
            def content = JsonOutput.toJson(request.messages[0].content.contains('"results"')
                    ? [results: results] : results)
            byte[] response = JsonOutput.toJson([choices: [[message: [content: content]]]]).getBytes("UTF-8")
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(sentiment == null ? 400 : 200, response.length)
            exchange.responseBody.write(response)
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    server.start()

    try {
        def host = InetAddress.getLocalHost().getHostAddress()
        ["spm_ai_resource_a": "spm-model-a", "spm_ai_resource_b": "spm-model-b"].each { resource, model ->
            sql "DROP RESOURCE IF EXISTS ${resource}"
            sql """CREATE RESOURCE ${resource} PROPERTIES (
                'type' = 'ai',
                'ai.provider_type' = 'openai',
                'ai.endpoint' = 'http://${host}:${server.address.port}/v1/chat/completions',
                'ai.model_name' = '${model}',
                'ai.api_key' = 'test-key',
                'ai.max_retries' = '0',
                'ai.validity_check' = 'false'
            )"""
        }

        sql "DROP TABLE IF EXISTS spm_ai_resource_input"
        sql """CREATE TABLE spm_ai_resource_input (k INT, s STRING)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")"""
        sql "INSERT INTO spm_ai_resource_input VALUES (1, 'first row'), (2, 'second row')"

        String query = "SELECT k, ai_sentiment(s) AS c FROM spm_ai_resource_input WHERE k >= 1"
        String variant = query.replace("k >= 1", "k >= 2")
        sql "SET enable_sql_cache = false"
        sql "SET enable_spm_rewrite = true"
        sql "SET enable_spm_fallback = false"
        sql "SET default_ai_resource = 'spm_ai_resource_a'"
        long id = sql("CREATE GLOBAL BASELINE PLAN '${query}'")[0][0] as Long
        try {
            qt_default_resource_frozen """SELECT plan_frozen FROM __internal_schema.spm_baselines
                WHERE id = ${id}"""
            order_qt_capture_mode query
            sql "SET default_ai_resource = 'spm_ai_resource_b'"
            order_qt_caller_mode query
            order_qt_caller_variant variant
            boolean originalHit = sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${id},")
            boolean variantHit = sql("EXPLAIN ${variant}").toString().contains("SPM baseline hit: id=${id},")
            qt_default_resource_hits "SELECT ${originalHit}, ${variantHit}"
        } finally {
            sql "DROP BASELINE PLAN IF EXISTS ${id}"
        }
    } finally {
        server.stop(0)
        sql "UNSET VARIABLE default_ai_resource"
        sql "DROP RESOURCE IF EXISTS spm_ai_resource_a"
        sql "DROP RESOURCE IF EXISTS spm_ai_resource_b"
    }
}
