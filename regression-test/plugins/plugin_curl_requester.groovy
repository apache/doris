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

import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.NodeType
@Grab(group='org.apache.httpcomponents', module='httpclient', version='4.5.13')
import org.apache.http.client.methods.*
import org.apache.http.impl.client.*
import org.apache.http.util.EntityUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ConnectTimeoutException
import org.apache.http.conn.HttpHostConnectException
import org.codehaus.groovy.runtime.IOGroovyMethods

Suite.metaClass.http_client = { String method, String url /* param */ ->
    Suite suite = delegate as Suite
    if (method != "GET" && method != "POST") {
        throw new Exception("Invalid method: ${method}")
    }
    if (!url || !(url =~ /^https?:\/\/.+/)) {
        throw new Exception("Invalid url: ${url}")
    }
    
    Integer timeout = 60 // seconds
    Integer maxRetries = 10
    Integer retryCount = 0
    Integer sleepTime = 1000 // milliseconds

    logger.info("HTTP request: ${method} ${url}")

    CloseableHttpClient httpClient = HttpClients.custom()
        .setRetryHandler(new DefaultHttpRequestRetryHandler(3, true))
        .build()

    int code
    String err
    String out

    try {
        while (retryCount < maxRetries) {
            HttpRequestBase request
            if (method == "GET") {
                request = new HttpGet(url)
            } else if (method == "POST") {
                request = new HttpPost(url)
            }

            RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000)
                .build()
            request.setConfig(requestConfig)

            try {
                CloseableHttpResponse response = httpClient.execute(request)
                try {
                    int statusCode = response.getStatusLine().getStatusCode()
                    String responseBody = EntityUtils.toString(response.getEntity())
                    
                    if (statusCode >= 200 && statusCode < 300) {
                        code = 0 // to be compatible with the old curl function
                        out = responseBody
                        err = ""
                        return [code, out, err]
                    } else {
                        logger.warn("HTTP request failed with status code ${statusCode}, retrying (${++retryCount}/${maxRetries})")
                    }
                } finally {
                    response.close()
                }
            } catch (ConnectTimeoutException | HttpHostConnectException e) {
                logger.warn("Connection failed, retrying (${++retryCount}/${maxRetries}): ${e.message}")
            } catch (SocketTimeoutException e) {
                timeout = timeout + 10
                logger.warn("Read timed out, retrying (${++retryCount}/${maxRetries}): ${e.message}")
            } catch (Exception e) {
                logger.error("Error executing HTTP request: ${e.message}")
                code = -1
                out = ""
                err = e.message
                return [code, out, err]
            }

            sleep(sleepTime)
            sleepTime = Math.min(sleepTime * 2, 60000) 
        }

        logger.error("HTTP request failed after ${maxRetries} attempts")
        code = -1
        out = ""
        err = "Failed after ${maxRetries} attempts"
        return [code, out, err]
    } finally {
        httpClient.close()
    }
}

logger.info("Added 'http_client' function to Suite")

Suite.metaClass.curl = { String method, String url /* param */-> 
    Suite suite = delegate as Suite
    if (method != "GET" && method != "POST")
    {
        throw new Exception(String.format("invalid curl method: %s", method))
    }
    if (url.isBlank())
    {
        throw new Exception("invalid curl url, blank")
    }
    
    Integer timeout = 10; // 10 seconds;
    Integer maxRetries = 10; // Maximum number of retries
    Integer retryCount = 0; // Current retry count
    Integer sleepTime = 5000; // Sleep time in milliseconds

    String cmd = String.format("curl --max-time %d -X %s %s", timeout, method, url).toString()
    logger.info("curl cmd: " + cmd)
    def process
    int code
    String err
    String out

    while (retryCount < maxRetries) {
        process = cmd.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
        out = process.getText()

        // If the command was successful, break the loop
        if (code == 0) {
            break
        }

        // If the command was not successful, increment the retry count, sleep for a while and try again
        retryCount++
        sleep(sleepTime)
    }

    // If the command was not successful after maxRetries attempts, log the failure and return the result
    if (code != 0) {
        logger.error("Command curl failed after " + maxRetries + " attempts. code: "  + code + ", err: " + err)
    }

    return [code, out, err]
}

logger.info("Added 'curl' function to Suite")


Suite.metaClass.show_be_config = { String ip, String port /*param */ ->
    return curl("GET", String.format("http://%s:%s/api/show_config", ip, port))
}

logger.info("Added 'show_be_config' function to Suite")

Suite.metaClass.be_get_compaction_status{ String ip, String port, String tablet_id  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status?tablet_id=%s", ip, port, tablet_id))
}

Suite.metaClass.be_get_overall_compaction_status{ String ip, String port  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status", ip, port))
}

logger.info("Added 'be_get_compaction_status' function to Suite")

Suite.metaClass._be_run_compaction = { String ip, String port, String tablet_id, String compact_type ->
    return curl("POST", String.format("http://%s:%s/api/compaction/run?tablet_id=%s&compact_type=%s",
            ip, port, tablet_id, compact_type))
}

Suite.metaClass.be_run_base_compaction = { String ip, String port, String tablet_id  /* param */->
    return _be_run_compaction(ip, port, tablet_id, "base")
}

logger.info("Added 'be_run_base_compaction' function to Suite")

Suite.metaClass.be_run_cumulative_compaction = { String ip, String port, String tablet_id  /* param */-> 
    return _be_run_compaction(ip, port, tablet_id, "cumulative")
}

logger.info("Added 'be_run_cumulative_compaction' function to Suite")

Suite.metaClass.be_run_full_compaction = { String ip, String port, String tablet_id  /* param */-> 
    return _be_run_compaction(ip, port, tablet_id, "full")
}

Suite.metaClass.be_run_full_compaction_by_table_id = { String ip, String port, String table_id  /* param */-> 
    return curl("POST", String.format("http://%s:%s/api/compaction/run?table_id=%s&compact_type=full", ip, port, table_id))
}

logger.info("Added 'be_run_full_compaction' function to Suite")

Suite.metaClass.update_be_config = { String ip, String port, String key, String value /*param */ ->
    return curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", ip, port, key, value))
}

logger.info("Added 'update_be_config' function to Suite")

Suite.metaClass.update_all_be_config = { String key, Object value ->
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    backendId_to_backendIP.each { beId, beIp ->
        def port = backendId_to_backendHttpPort.get(beId)
        def url = "http://${beIp}:${port}/api/update_config?${key}=${value}"
        def result = Http.POST(url, null, true)
        assert result.size() == 1, result.toString()
        assert result[0].status == "OK", result.toString()
    }
}

logger.info("Added 'update_all_be_config' function to Suite")


Suite.metaClass._be_report = { String ip, int port, String reportName ->
    def url = "http://${ip}:${port}/api/report/${reportName}"
    def result = Http.GET(url, true)
    Http.checkHttpResult(result, NodeType.BE)
}

// before report, be need random sleep 5s
Suite.metaClass.be_report_disk = { String ip, int port ->
    _be_report(ip, port, "disk")
}

logger.info("Added 'be_report_disk' function to Suite")

// before report, be need random sleep 5s
Suite.metaClass.be_report_tablet = { String ip, int port ->
    _be_report(ip, port, "tablet")
}

logger.info("Added 'be_report_tablet' function to Suite")

// before report, be need random sleep 5s
Suite.metaClass.be_report_task = { String ip, int port ->
    _be_report(ip, port, "task")
}

logger.info("Added 'be_report_task' function to Suite")
