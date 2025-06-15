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

import org.codehaus.groovy.runtime.IOGroovyMethods
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.impl.client.LaxRedirectStrategy;

suite("test_manager_fe_http_api") {
    String[][] frontends = sql """ show frontends """
    String frontendId;
    def frontendIdTofrontendIP = [:]
    def frontendIdTofrontendHttpPort = [:]
    for (String[] frontend in frontends) {
        if (frontend[11].equals("true")) {
            frontendIdTofrontendIP.put(frontend[0], frontend[1])
            frontendIdTofrontendHttpPort.put(frontend[0], frontend[3])
        }
    }

    // base64 encode auth for `root:`
    def auth = "cm9vdDo="
    def curl = { String method, String uri /* param */->
        if (method != "GET" && method != "POST")
        {
            throw new Exception(String.format("invalid curl method: %s", method))
        }
        if (uri.isBlank())
        {
            throw new Exception("invalid curl url, blank")
        }

        Integer timeout = 10; // 10 seconds;
        Integer maxRetries = 10; // Maximum number of retries
        Integer retryCount = 0; // Current retry count
        Integer sleepTime = 5000; // Sleep time in milliseconds

        String cmd = String.format("curl --max-time %d -H 'Authorization: Basic %s' -X %s %s", timeout, auth, method, uri).toString()
        logger.info("curl cmd: " + cmd)
        def process
        int code
        String out

        while (retryCount < maxRetries) {
            out = [ 'bash', '-c', "${cmd}" ].execute().text

            // If the command was successful, break the loop
            if (out.isBlank()) {
                retryCount++
                sleep(sleepTime)
            } else {
                break
            }
        }

        // If the command was not successful after maxRetries attempts, log the failure and return the result
        if (code != 0) {
            logger.error("Command curl failed after " + maxRetries + " attempts. code: "  + code)
        }

        logger.info(out)
        return [code, out]
    }

    def get_bootstrap_status = {
        for (String id in frontendIdTofrontendIP.keySet()) {
            def feIp = frontendIdTofrontendIP.get(id)
            def fePort = frontendIdTofrontendHttpPort.get(id)
            def (code, out) = curl("GET", String.format("http://%s:%s/api/bootstrap", feIp, fePort))
            assertTrue(out.contains("success"))
        }
    }

    def login_cluster = {
        for (String id in frontendIdTofrontendIP.keySet()) {
            def feIp = frontendIdTofrontendIP.get(id)
            def fePort = frontendIdTofrontendHttpPort.get(id)
            def (code, out) = curl("POST", String.format("http://%s:%s/rest/v1/login", feIp, fePort))
            assertTrue(out.contains("Login success!"))
        }
    }

    def set_fe_runtime_config = {
        for (String id in frontendIdTofrontendIP.keySet()) {
            def feIp = frontendIdTofrontendIP.get(id)
            def fePort = frontendIdTofrontendHttpPort.get(id)
            def (code, out) = curl("GET", String.format("http://%s:%s/api/_set_config", feIp, fePort))
            assertTrue(out.contains("success"))
        }
    }

    def get_fe_config = {
        for (String id in frontendIdTofrontendIP.keySet()) {
            def feIp = frontendIdTofrontendIP.get(id)
            def fePort = frontendIdTofrontendHttpPort.get(id)
            def (code, out) = curl("POST", String.format("http://%s:%s/rest/v2/manager/node/configuration_info?type=fe", feIp, fePort))
            assertTrue(out.contains("success"))
        }
    }


    def get_be_config = {
        for (String id in frontendIdTofrontendIP.keySet()) {
           def feIp = frontendIdTofrontendIP.get(id)
            def fePort = frontendIdTofrontendHttpPort.get(id)
            def (code, out) = curl("POST", String.format("http://%s:%s/rest/v2/manager/node/configuration_info?type=be", feIp, fePort))
            assertTrue(out.contains("success"))
        }
    }

    get_bootstrap_status()
    login_cluster()
    set_fe_runtime_config()
    get_fe_config()
    get_be_config()
}