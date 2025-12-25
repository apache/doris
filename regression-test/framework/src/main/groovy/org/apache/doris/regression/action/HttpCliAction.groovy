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
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.ssl.SSLContexts
import org.apache.http.util.EntityUtils
import org.junit.Assert

import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLContext
import java.security.KeyStore

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

    static Boolean enableTls = false
    static String tlsVerifyMode = "none" // none/ca/strict
    static String trustStorePath = null
    static String trustStorePassword = null
    static String keyStorePath = null
    static String keyStorePassword = null
    static String keyStoreType = null
    static String trustStoreType = null


    private SSLConnectionSocketFactory sslSocketFactory
    private CloseableHttpClient httpClient

    HttpCliAction(SuiteContext context) {
        this.context = context
        this.enableTls = (context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false
        this.tlsVerifyMode = context.config.otherConfigs.get("tlsVerifyMode")?.toString()?.toLowerCase() ?: "none"
        this.trustStorePath = context.config.otherConfigs.get("trustStorePath")?.toString() ?: null
        this.trustStorePassword = context.config.otherConfigs.get("trustStorePassword")?.toString() ?: null
        this.trustStoreType = context.config.otherConfigs.get("trustStoreType") ?: 'JKS'
        this.keyStorePath = context.config.otherConfigs.get("keyStorePath")?.toString() ?: null
        this.keyStorePassword = context.config.otherConfigs.get("keyStorePassword")?.toString() ?: null
        this.keyStoreType = context.config.otherConfigs.get("keyStoreType")?.toString() ?: 'PKCS12'

        if (this.enableTls) {
            initSSLContext()
        } else {
            this.httpClient = HttpClients.custom()
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build()
        }
    }

    void endpoint(Closure<String> endpointSupplier) { this.endpoint = endpointSupplier.call() }
    void endpoint(String endpoint) { this.endpoint = endpoint }
    void uri(Closure<String> uriSupplier) { this.uri = uriSupplier.call() }
    void uri(String uri) { this.uri = uri }
    void header(String key, String value) { this.headers.put(key, value) }

    void basicAuthorization(String user, String password) {
        String credentials = user + ":" + (password.is(null) ? "" : password)
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes())
        headers.put("Authorization", "Basic " + encodedCredentials)
    }

    void body(Closure<String> bodySupplier) { this.body = bodySupplier.call() }
    void body(String body) { this.body = body }
    void op(Closure<String> opSupplier) {this.op = opSupplier.call()}
    void op(String op) { this.op = op }
    void result(Object result) { this.result = result }
    void printResponse(boolean printResponse) { this.printResponse = printResponse }

    void initSSLContext() {
        def sslContextBuilder = SSLContexts.custom()

        if (this.tlsVerifyMode == "none") {
            sslContextBuilder.loadTrustMaterial({ chain, authType -> true })
        } else {
            if (this.trustStorePath) {
                KeyStore trustStore = KeyStore.getInstance(this.trustStoreType)
                trustStore.load(new FileInputStream(this.trustStorePath), this.trustStorePassword.toCharArray())
                sslContextBuilder.loadTrustMaterial(trustStore, null)
            }
            if (this.tlsVerifyMode == "strict" && this.keyStorePath) {
                KeyStore keyStore = KeyStore.getInstance(this.keyStoreType)
                keyStore.load(new FileInputStream(this.keyStorePath), this.keyStorePassword.toCharArray())
                sslContextBuilder.loadKeyMaterial(keyStore, this.keyStorePassword.toCharArray())
            }
        }

        SSLContext sslContext = sslContextBuilder.build()
        HostnameVerifier hostnameVerifier = this.tlsVerifyMode == "none" ? NoopHostnameVerifier.INSTANCE : SSLConnectionSocketFactory.getDefaultHostnameVerifier()

        this.sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier)
        this.httpClient = HttpClients.custom()
            .setSSLSocketFactory(this.sslSocketFactory)
            .setRedirectStrategy(new LaxRedirectStrategy())
            .build()
    }

    @Override
    void run() {
        try {
            def protocol = this.enableTls ? "https" : "http"
            uri = "${protocol}://${endpoint}${uri}"
            log.info("url : $uri")
            log.info("body: $body")
            log.info("op: $op")

            def result
            if (op == "get") {
                HttpGet httpGet = new HttpGet(uri)
                headers.each { k, v -> httpGet.setHeader(k, v) }
                result = executeRequest(httpGet)
            } else if (op == "delete") {
                HttpDelete httpDelete = new HttpDelete(uri)
                headers.each { k, v -> httpDelete.setHeader(k, v) }
                result = executeRequest(httpDelete)
            } else {
                HttpPost httpPost = new HttpPost(uri)
                headers.each { k, v -> httpPost.setHeader(k, v) }
                httpPost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON))
                result = executeRequest(httpPost)
            }

            if (printResponse) {
                log.info("result:${result}".toString())
            }
            if (check != null) {
                check.call(result.respCode, result.body)
            } else if (this.result != null) {
                Assert.assertEquals(this.result, result)
            }
        } catch (Throwable t) {
            throw new IllegalStateException("HttpCliAction failed, uri:${uri}", t)
        }
    }

    private ActionResult executeRequest(request) {
        this.httpClient.execute(request).withCloseable { resp ->
            String respJson = EntityUtils.toString(resp.getEntity())
            def respCode = resp.getStatusLine().getStatusCode()
            if (printResponse) {
                log.info("respCode: ${respCode}, respJson: ${respJson}")
            }
            return new ActionResult(respCode, respJson)
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
