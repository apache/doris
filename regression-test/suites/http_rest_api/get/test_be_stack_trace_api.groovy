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

import org.apache.doris.regression.util.Http

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

suite("test_be_stack_trace_api", "nonConcurrent") {
    def marker = "test_be_stack_trace_api_load_marker"
    def queryThreadNum = 3
    def loadQuery = """
        select /* ${marker} */
               sum(length(md5(concat(cast(number as string), '${marker}'))))
        from numbers("number" = "1000000000")
        """

    def aliveBackends = sql_return_maparray("show backends").findAll {
        "${it.Alive}".equalsIgnoreCase("true") && "${it.HttpPort}" != "-1"
    }
    assertTrue(aliveBackends.size() > 0, "No alive backend found for BE stack trace API test.")

    def beEndpoints = aliveBackends.collect { "${it.Host}:${it.HttpPort}" }
    logger.info("Alive BE HTTP endpoints for stack trace API test: ${beEndpoints}".toString())

    def loadQueryIds = new LinkedHashSet<String>()
    def queryFailure = new AtomicReference<Throwable>(null)
    def stopLoad = new AtomicBoolean(false)
    def futures = []

    Boolean enableTLS = (context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false
    if (enableTLS) {
        // Keep direct BE REST calls consistent with the shared HTTP helper setup used by FE REST tests.
        Http.configure(enableTLS,
            context.config.otherConfigs.get("tlsVerifyMode"),
            context.config.otherConfigs.get("trustStorePath"),
            context.config.otherConfigs.get("trustStorePassword"),
            context.config.otherConfigs.get("trustStoreType"),
            context.config.otherConfigs.get("keyStorePath"),
            context.config.otherConfigs.get("keyStorePassword"),
            context.config.otherConfigs.get("keyStoreType")
        )
    }

    def findLoadQueryIds = {
        def processList = sql_return_maparray("show processlist")
        processList.each { row ->
            if ("${row.Info}".contains(marker)) {
                loadQueryIds.add("${row.QueryId}")
            }
        }
        return loadQueryIds.size()
    }

    def killLoadQueries = {
        findLoadQueryIds.call()
        loadQueryIds.each { queryId ->
            try {
                logger.info("Kill BE stack trace regression load query ${queryId}".toString())
                sql """kill query "${queryId}" """
            } catch (Throwable t) {
                logger.info("Ignore load query kill failure for ${queryId}: ${t.getMessage()}".toString())
            }
        }
    }

    // The API should expose both the response structure and a real running pipeline/operator stack.
    // Checking only scheduler frames would be weak because idle workers also contain scheduler calls.
    def hasExecutingOperatorStack = { String stackText ->
        def operatorFrames = [
            "VectorizedFnCall",
            "AggSinkOperatorX",
            "AggSourceOperatorX",
            "FunctionStringDigest",
            "CastToStringFunction"
        ]
        return stackText.split(/(?m)^----- thread /).any { section ->
            section.contains("status=ok") &&
                    section.contains("(p_") &&
                    section.contains("PipelineTask::execute") &&
                    operatorFrames.any { frame -> section.contains(frame) }
        }
    }

    def fetchStack = { String endpoint ->
        def url = "http://${endpoint}/api/stack_trace?mode=FAST&timeout_ms=3000&skip_blocking_syscalls=false"
        return Http.GET(url, false, false) as String
    }

    try {
        (1..queryThreadNum).each { index ->
            futures.add(thread("be-stack-trace-load-${index}") {
                try {
                    sql "set enable_sql_cache=false"
                    sql "set parallel_pipeline_task_num=4"
                    while (!stopLoad.get()) {
                        sql loadQuery
                    }
                } catch (Throwable t) {
                    if (!stopLoad.get()) {
                        queryFailure.compareAndSet(null, t)
                    }
                }
            })
        }

        long queryDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30)
        while (System.currentTimeMillis() < queryDeadline && findLoadQueryIds.call() == 0) {
            if (queryFailure.get() != null) {
                throw queryFailure.get()
            }
            sleep(200)
        }
        assertTrue(loadQueryIds.size() > 0, "Did not observe the load query in processlist.")

        String matchedStack = null
        String lastStack = ""
        long stackDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30)
        while (System.currentTimeMillis() < stackDeadline && matchedStack == null) {
            for (String endpoint : beEndpoints) {
                def stack = fetchStack.call(endpoint)
                lastStack = stack
                assertTrue(stack.contains("BE thread stack traces"))
                assertTrue(stack.contains("service_signal:"))
                assertTrue(stack.contains("thread_count:"))
                assertTrue(stack.contains("dwarf_location_info_mode: fast"))
                assertTrue(stack.contains("skip_blocking_syscalls: false"))
                assertTrue(stack.contains("summary:"))

                if (hasExecutingOperatorStack.call(stack)) {
                    matchedStack = stack
                    logger.info("Matched BE stack trace operator evidence from ${endpoint}".toString())
                    break
                }
            }
            if (queryFailure.get() != null) {
                throw queryFailure.get()
            }
            if (matchedStack == null) {
                sleep(500)
            }
        }

        assertTrue(matchedStack != null,
                "BE stack trace did not contain an executing pipeline/operator stack. Last stack head:\n" +
                        lastStack.readLines().take(80).join("\n"))
    } finally {
        stopLoad.set(true)
        killLoadQueries.call()
        futures.each { future ->
            future.get(60, TimeUnit.SECONDS)
        }
    }
}
