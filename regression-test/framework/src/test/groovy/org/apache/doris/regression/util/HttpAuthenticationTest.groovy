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

package org.apache.doris.regression.util

import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import org.junit.jupiter.api.Test

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import static org.junit.jupiter.api.Assertions.*

class HttpAuthenticationTest {
    @Test
    void credentialsAreRequestScopedAndLegacyCallsStillUseEmptyRootPassword() {
        def server = HttpServer.create(new InetSocketAddress('127.0.0.1', 0), 0)
        def serverThreads = Executors.newCachedThreadPool()
        def callers = Executors.newFixedThreadPool(6)
        boolean originalTls = Http.enableTls
        server.executor = serverThreads
        server.createContext('/') { exchange ->
            def response = [authorization: exchange.requestHeaders.getFirst('Authorization'),
                            body: exchange.requestBody.getText('UTF-8')]
            byte[] bytes = JsonOutput.toJson(response).getBytes('UTF-8')
            exchange.responseHeaders.set('Content-Type', 'application/json; charset=UTF-8')
            exchange.sendResponseHeaders(200, bytes.length)
            exchange.responseBody.withCloseable { it.write(bytes) }
        }
        try {
            Http.enableTls = false
            server.start()
            String url = "http://127.0.0.1:${server.address.port}/"
            def passwords = ['', null, 'test-password', '密碼 \\" :#; $()`\\\\\n']
            def futures = (0..<24).collect { index ->
                callers.submit({ ->
                    String user = "user${index}"
                    String password = passwords[index % passwords.size()]
                    def response = index % 2 == 0
                            ? Http.GET(url, true, false, user, password)
                            : Http.POST(url, [index: index], true, user, password)
                    String decoded = new String(Base64.decoder.decode(response.authorization.substring(6)), 'UTF-8')
                    assertEquals(user + ':' + (password ?: ''), decoded)
                    if (index % 2 != 0) assertEquals(JsonOutput.toJson([index: index]), response.body)
                } as Runnable)
            }
            futures.each { it.get(10, TimeUnit.SECONDS) }
            assertEquals('Basic cm9vdDo=', Http.GET(url, true, false).authorization)
            assertEquals('Basic cm9vdDo=', Http.POST(url, null, true).authorization)
        } finally {
            Http.enableTls = originalTls
            server.stop(0)
            callers.shutdownNow()
            serverThreads.shutdownNow()
        }
    }
}
