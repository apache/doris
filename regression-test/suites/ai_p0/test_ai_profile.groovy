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

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.doris.regression.action.ProfileAction

import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

suite("test_ai_profile", "p0") {
    if (isCloudMode()) {
        return
    }

    def requests = new ConcurrentHashMap<String, AtomicInteger>()
    ["/scalar", "/embed", "/multimodal", "/aggregate", "/missing", "/recursive"].each {
        requests.put(it, new AtomicInteger())
    }
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)
    server.createContext("/", { HttpExchange exchange ->
        try {
            String path = exchange.requestURI.path
            int attempt = requests.get(path).incrementAndGet()
            def request = new JsonSlurper().parseText(exchange.requestBody.getText("UTF-8"))
            int status = 200
            def response
            if (path == "/embed") {
                response = [data: request.input.collect { [embedding: [0.25, 0.75]] },
                            usage: [prompt_tokens: 5, total_tokens: 5]]
            } else if (path == "/multimodal") {
                response = [output: [embeddings: request.input.contents.collect {
                                [embedding: [0.25, 0.75]]
                            }], usage: [input_tokens: 4, output_tokens: 0, total_tokens: 4]]
            } else {
                String content = "summary"
                if (path != "/aggregate") {
                    // String functions send a JSON array of inputs in the user message.
                    // Return one result per input even when several rows share a request.
                    def inputs = new JsonSlurper().parseText(request.messages.find { it.role == "user" }.content)
                    content = JsonOutput.toJson([results: inputs.collect { "ok" }])
                }
                response = [choices: [[message: [content: content]]]]
                if (path != "/missing") {
                    response.usage = [prompt_tokens: 7, completion_tokens: 3, total_tokens: 10]
                }
                if (path == "/scalar" && attempt == 1) {
                    status = 503
                    response = [error: [message: "temporary failure"], usage: response.usage]
                }
            }
            byte[] body = JsonOutput.toJson(response).getBytes("UTF-8")
            exchange.responseHeaders.set("Content-Type", "application/json")
            exchange.sendResponseHeaders(status, body.length)
            exchange.responseBody.withCloseable { it.write(body) }
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    server.start()

    // Match the local HTTP TVF fixture: the configured address must be reachable from BE.
    String serverHost = context.config.otherConfigs.get("httpTvfHost")
    if (serverHost == null || serverHost.trim().isEmpty()) {
        serverHost = context.config.otherConfigs.get("externalEnvIp")
    }
    if (serverHost == null || serverHost.trim().isEmpty()) {
        serverHost = InetAddress.getLocalHost().getHostAddress()
    }
    String baseUrl = "http://${serverHost}:${server.address.port}"
    String suffix = UUID.randomUUID().toString().replace("-", "")
    String resource = "ai_profile_${suffix}"
    String aggregateResource = "ai_profile_agg_${suffix}"
    String missingResource = "ai_profile_missing_${suffix}"
    String recursiveResource = "ai_profile_recursive_${suffix}"
    def resources = [resource, aggregateResource, missingResource, recursiveResource]
    def profileAction = new ProfileAction(context)

    // Verify the query summary against both the mock provider and the sum of the
    // fragment details. Never add the summary to the detailed counters a second time.
    def readProfile = { String token ->
        String profile = profileAction.getProfileBySql(token, [], 60000, 500)
        int summaryStart = profile.indexOf("AIFunctionSummary")
        int mergedStart = profile.indexOf("MergedProfile:", summaryStart)
        assertTrue(summaryStart >= 0 && mergedStart > summaryStart, "Missing AI summary: ${profile}")
        int detailStart = profile.indexOf("DetailProfile(")
        assertTrue(detailStart >= 0, "Missing detail profile: ${profile}")
        int appendixStart = profile.indexOf("Appendix:", detailStart)
        String detail = appendixStart >= 0 ? profile.substring(detailStart, appendixStart)
                                          : profile.substring(detailStart)
        return [summary: profile.substring(summaryStart, mergedStart), detail: detail]
    }
    def runProfile = { String expression, String predicate ->
        String token = UUID.randomUUID().toString()
        sql "SELECT '${token}', ${expression} FROM test_ai_profile_input ${predicate}"
        return readProfile(token)
    }
    def count = { String profile, String name ->
        def matcher = Pattern.compile("(?m)^\\s*-\\s*" + Pattern.quote(name)
                + ':\\s*(\\d+)\\s*$').matcher(profile)
        long total = 0
        boolean found = false
        while (matcher.find()) {
            found = true
            total += Long.parseLong(matcher.group(1))
        }
        assertTrue(found, "Missing ${name} in ${profile}")
        return total
    }
    def expectCounts = { Map profiles, Map expected ->
        profiles.each { section, profile ->
            expected.each { name, value ->
                assertEquals(value as long, count(profile, name as String), "${section} ${name}: ${profile}")
            }
        }
    }

    try {
        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET ai_context_window_size = 1"
        sql "SET embed_max_batch_size = 1"
        sql "SET query_timeout = 60"
        sql """CREATE RESOURCE '${resource}' PROPERTIES (
            'type' = 'ai',
            'ai.provider_type' = 'OPENAI', 'ai.endpoint' = '${baseUrl}/scalar',
            'ai.model_name' = 'profile-test', 'ai.api_key' = 'local-test',
            'ai.embed.provider_type' = 'OPENAI', 'ai.embed.endpoint' = '${baseUrl}/embed',
            'ai.embed.model_name' = 'profile-test', 'ai.embed.api_key' = 'local-test',
            'ai.embed.mm.provider_type' = 'QWEN', 'ai.embed.mm.endpoint' = '${baseUrl}/multimodal',
            'ai.embed.mm.model_name' = 'profile-test', 'ai.embed.mm.api_key' = 'local-test',
            'ai.max_retries' = '1', 'ai.retry_delay_second' = '0',
            'ai.validity_check' = 'false')"""
        [[name: aggregateResource, path: "/aggregate"], [name: missingResource, path: "/missing"],
         [name: recursiveResource, path: "/recursive"]].each {
            sql """CREATE RESOURCE '${it.name}' PROPERTIES (
                'type' = 'ai', 'ai.provider_type' = 'OPENAI', 'ai.endpoint' = '${baseUrl}${it.path}',
                'ai.model_name' = 'profile-test', 'ai.api_key' = 'local-test',
                'ai.max_retries' = '0', 'ai.validity_check' = 'false')"""
        }
        sql "DROP TABLE IF EXISTS test_ai_profile_input"
        sql """CREATE TABLE test_ai_profile_input (
            id INT, prompt VARCHAR(32) NULL, media JSON NULL
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')"""
        String media = JsonOutput.toJson([uri: "${baseUrl}/image.png", content_type: "image/png"])
        sql """INSERT INTO test_ai_profile_input VALUES
            (1, 'first', '${media}'), (2, 'second', '${media}'), (3, NULL, NULL)"""

        def scalar = runProfile("AI_GENERATE('${resource}', prompt)", "")
        expectCounts(scalar, [AIFunctionInputRows: 3, AIFunctionSkippedNullRows: 1,
                AIFunctionLogicalRequestCount: 2, AIFunctionFirstAttemptCount: 2, AIFunctionRequestCount: 3, AIFunctionRetryCount: 1,
                AIFunctionPromptTokens: 21, AIFunctionCompletionTokens: 9, AIFunctionTotalTokens: 30,
                AIFunctionPromptUsageCount: 3, AIFunctionCompletionUsageCount: 3, AIFunctionTotalUsageCount: 3])
        assertEquals(3, requests.get("/scalar").get())
        ["AIFunctionHttpTime", "AIFunctionDnsTime", "AIFunctionTcpConnectTime", "AIFunctionTlsHandshakeTime", "AIFunctionFirstByteTime",
         "AIFunctionParseTime", "AIFunctionUsageParseTime", "AIFunctionRetryWaitTime"].each {
            assertTrue(scalar.summary.contains("${it}:"), "Missing summary ${it}: ${scalar.summary}")
            assertTrue(scalar.detail.contains("${it}:"), "Missing detail ${it}: ${scalar.detail}")
        }

        def embedding = runProfile("EMBED('${resource}', prompt)", "")
        expectCounts(embedding, [AIFunctionInputRows: 3, AIFunctionSkippedNullRows: 1, AIFunctionRequestCount: 2,
                AIFunctionFirstAttemptCount: 2, AIFunctionRetryCount: 0, AIFunctionPromptTokens: 10,
                AIFunctionCompletionTokens: 0, AIFunctionTotalTokens: 10,
                AIFunctionPromptUsageCount: 2, AIFunctionCompletionUsageCount: 0, AIFunctionTotalUsageCount: 2])
        assertEquals(2, requests.get("/embed").get())

        def multimodal = runProfile("EMBED('${resource}', media)", "WHERE id = 1")
        expectCounts(multimodal, [AIFunctionInputRows: 1, AIFunctionRequestCount: 1, AIFunctionPromptTokens: 4,
                AIFunctionCompletionTokens: 0, AIFunctionTotalTokens: 4,
                AIFunctionPromptUsageCount: 1, AIFunctionCompletionUsageCount: 1, AIFunctionTotalUsageCount: 1])
        assertEquals(1, requests.get("/multimodal").get())

        def missing = runProfile("AI_GENERATE('${missingResource}', prompt)", "WHERE id = 1")
        expectCounts(missing, [AIFunctionRequestCount: 1, AIFunctionPromptTokens: 0, AIFunctionCompletionTokens: 0,
                AIFunctionTotalTokens: 0, AIFunctionPromptUsageCount: 0, AIFunctionCompletionUsageCount: 0, AIFunctionTotalUsageCount: 0])

        ["AI_GENERATE('${resource}', prompt)", "EMBED('${resource}', prompt)"].each { expression ->
            def nulls = runProfile(expression, "WHERE id = 3")
            expectCounts(nulls, [AIFunctionInputRows: 1, AIFunctionSkippedNullRows: 1,
                    AIFunctionLogicalRequestCount: 0, AIFunctionRequestCount: 0, AIFunctionFirstAttemptCount: 0, AIFunctionRetryCount: 0,
                    AIFunctionPromptUsageCount: 0, AIFunctionCompletionUsageCount: 0, AIFunctionTotalUsageCount: 0])
        }
        assertEquals(3, requests.get("/scalar").get())
        assertEquals(2, requests.get("/embed").get())

        // Level 1 must retain the query totals too.
        sql "SET profile_level = 1"
        def levelOne = runProfile("AI_GENERATE('${missingResource}', prompt)", "WHERE id = 1")
        expectCounts(levelOne, [AIFunctionInputRows: 1, AIFunctionRequestCount: 1, AIFunctionTotalTokens: 0, AIFunctionTotalUsageCount: 0])

        // Each recursive stage rebuilds the same fragment ID. Check the completed profile,
        // not a live snapshot, against all three actual provider requests.
        String recursiveToken = UUID.randomUUID().toString()
        sql """WITH RECURSIVE ai_steps(n, value) AS (
            SELECT CAST(0 AS INT), CAST('seed' AS STRING)
            UNION ALL
            SELECT CAST(n + 1 AS INT),
                   AI_GENERATE('${recursiveResource}', CONCAT(value, CAST(n AS STRING)))
            FROM ai_steps WHERE n < 3
        ) SELECT '${recursiveToken}', n, value FROM ai_steps ORDER BY n"""
        assertEquals(3, requests.get("/recursive").get())
        def recursive = readProfile(recursiveToken)
        expectCounts(recursive, [AIFunctionRequestCount: 3, AIFunctionFirstAttemptCount: 3, AIFunctionRetryCount: 0,
                AIFunctionPromptTokens: 21, AIFunctionCompletionTokens: 9, AIFunctionTotalTokens: 30,
                AIFunctionPromptUsageCount: 3, AIFunctionCompletionUsageCount: 3, AIFunctionTotalUsageCount: 3])

        // Aggregation can issue requests in several fragments; compare the sum with
        // the actual local requests rather than assuming a particular aggregate plan.
        sql "SET ai_context_window_size = 4096"
        def aggregate = runProfile("AI_AGG('${aggregateResource}', prompt, 'summarize')", "WHERE id < 3")
        long aggregateRequests = requests.get("/aggregate").get()
        assertTrue(aggregateRequests > 0)
        expectCounts(aggregate, [AIFunctionRequestCount: aggregateRequests,
                AIFunctionFirstAttemptCount: aggregateRequests, AIFunctionRetryCount: 0,
                AIFunctionPromptTokens: 7 * aggregateRequests, AIFunctionCompletionTokens: 3 * aggregateRequests,
                AIFunctionTotalTokens: 10 * aggregateRequests, AIFunctionTotalUsageCount: aggregateRequests])

        // Exercise scalar and aggregate initialization without a statistics owner.
        sql "SET enable_profile = false"
        sql "SELECT AI_GENERATE('${resource}', prompt) FROM test_ai_profile_input ORDER BY id"
        sql "SELECT EMBED('${resource}', prompt) FROM test_ai_profile_input ORDER BY id"
        sql "SELECT AI_AGG('${aggregateResource}', prompt, 'summarize') FROM test_ai_profile_input"
    } finally {
        server.stop(0)
        resources.each { try_sql("DROP RESOURCE IF EXISTS '${it}'") }
        ["enable_profile", "profile_level", "parallel_pipeline_task_num", "ai_context_window_size",
         "embed_max_batch_size", "query_timeout"].each { try_sql("UNSET VARIABLE ${it}") }
    }
}
