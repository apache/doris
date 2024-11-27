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

package org.apache.doris.regression.action

import com.google.common.collect.Maps
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import groovy.util.logging.Slf4j
import org.apache.doris.regression.suite.SuiteContext
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpPost
import org.apache.tools.ant.taskdefs.condition.Http;
import org.junit.Assert

@Slf4j
class HttpCliAction implements SuiteAction {
    private String endpoint
    private String uri
    private String body
    private String result
    private String op
    private Map<String, String> headers = Maps.newLinkedHashMap()
    private Closure check
    private boolean printResponse = true
    SuiteContext context

    HttpCliAction(SuiteContext context) {
        this.context = context
    }

    void endpoint(Closure<String> endpointSupplier) {
        this.endpoint = endpointSupplier.call()
    }

    void endpoint(String endpoint) {
        this.endpoint = endpoint
    }

    void uri(Closure<String> uriSupplier) {
        this.uri = uriSupplier.call()
    }

    void uri(String uri) {
        this.uri = uri
    }

    void header(String key, String value) {
        this.headers.put(key, value)
    }

    void basicAuthorization(String user, String password) {
        String credentials = user + ":" + (password.is(null) ? "" : password)
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes())
        String headerValue = "Basic " + encodedCredentials;
        headers.put("Authorization", headerValue)
    }

    void body(Closure<String> bodySupplier) {
        this.body = bodySupplier.call()
    }

    void body(String body) {
        this.body = body
    }

    void op(Closure<String> opSupplier) {
        this.op = bodySupplier.call()
    }

    void op(String op) {
        this.op = op
    }

    void result(Object result) {
        this.result = result
    }

    void printResponse(boolean printResponse) {
        this.printResponse = printResponse
    }

    @Override
    void run() {
        try {
            def result = HttpClients.createDefault().withCloseable { client ->
                uri = "http://$endpoint" + uri
                log.info("url : " + uri)
                log.info("body: " + body)
                log.info("op: " + op)

                if (op == "get") {
                    HttpGet httpGet = new HttpGet(uri)
                    for (final def header in headers.entrySet()) {
                        httpGet.setHeader(header.getKey(), header.getValue())
                    }

                    client.execute(httpGet).withCloseable { resp ->
                        resp.withCloseable {
                            String respJson = EntityUtils.toString(resp.getEntity())
                            def respCode = resp.getStatusLine().getStatusCode()
                            if (printResponse) {
                                log.info("respCode: ${respCode}, respJson: ${respJson}")
                            }
                            return new ActionResult(respCode, respJson)
                        }
                    }
                } else {
                    HttpPost httpPost = new HttpPost(uri)
                    for (final def header in headers.entrySet()) {
                        httpPost.setHeader(header.getKey(), header.getValue())
                    }
                    StringEntity requestEntity = new StringEntity(
                            body,
                            ContentType.APPLICATION_JSON);
                    httpPost.setEntity(requestEntity)

                    client.execute(httpPost).withCloseable { resp ->
                        resp.withCloseable {
                            String respJson = EntityUtils.toString(resp.getEntity())
                            def respCode = resp.getStatusLine().getStatusCode()
                            if (printResponse) {
                                log.info("respCode: ${respCode}, respJson: ${respJson}")
                            }
                            return new ActionResult(respCode, respJson)
                        }
                    }
                }
            }
            if (printResponse) {
                log.info("result:${result}".toString())
                log.info("this.result:${this.result}".toString())
            }
            if (check != null) {
                check.call(result.respCode, result.body)
            } else {
                if (this.result != null) {
                    Assert.assertEquals(this.result, result)
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("HttpCliAction failed, uri:${uri}", t)
        }
    }

    class ActionResult {
        String body
        int respCode

        ActionResult(int respCode, String body) {
            this.body = body
            this.respCode = respCode
        }
    }

    void check(@ClosureParams(value = FromString, options = ["int, String"]) Closure check) {
        this.check = check
    }
}
