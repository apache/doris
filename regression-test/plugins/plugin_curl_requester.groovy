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
    
    Integer timeout = 300 // seconds
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
                    code = response.getStatusLine().getStatusCode()
                    out = EntityUtils.toString(response.getEntity())
                    
                    if (code >= 200 && code < 300) {
                        code = 0 // to be compatible with the old curl function
                        err = ""
                        return [code, out, err]
                    } else if (code == 500) {
                        return [code, out, "Internal Server Error"]
                    } else {
                        logger.warn("HTTP request failed with status code ${code}, response ${out}, retrying (${++retryCount}/${maxRetries})")
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
                code = 500 // Internal Server Error
                logger.error("Error executing HTTP request: ${e.message}")
                err = e.message
                return [code, out, err]
            }

            sleep(sleepTime)
            sleepTime = Math.min(sleepTime * 2, 60000) 
        }

        logger.error("HTTP request failed after ${maxRetries} attempts")
        err = "Failed after ${maxRetries} attempts"
        code = 500 // Internal Server Error
        return [code, out, err]
    } finally {
        httpClient.close()
    }
}

logger.info("Added 'http_client' function to Suite")

Suite.metaClass.curl = { String method, String url, String body = null /* param */-> 
    Suite suite = delegate as Suite
    if (method != "GET" && method != "POST") {
        throw new Exception(String.format("invalid curl method: %s", method))
    }
    if (url.isBlank()) {
        throw new Exception("invalid curl url, blank")
    }
    
    Integer timeout = 10; // 10 seconds;
    Integer maxRetries = 10; // Maximum number of retries
    Integer retryCount = 0; // Current retry count
    Integer sleepTime = 5000; // Sleep time in milliseconds

    String cmd
    if (method == "POST" && body != null) {
        cmd = String.format("curl --max-time %d -X %s -H Content-Type:application/json -d %s %s", timeout, method, body, url).toString()
    } else {
        cmd = String.format("curl --max-time %d -X %s %s", timeout, method, url).toString()
    }
    
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

Suite.metaClass.be_show_tablet_status{ String ip, String port, String tablet_id  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/show?tablet_id=%s", ip, port, tablet_id))
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

// check nested index file api
Suite.metaClass.check_nested_index_file = { ip, port, tablet_id, expected_rowsets_count, expected_indices_count, format -> 
    def (code, out, err) = http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet_id))
    logger.info("Run show_nested_index_file_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
    // only when the expected_indices_count is 0, the tablet may not have the index file.
    if (code == 500 && expected_indices_count == 0) {
        assertEquals("E-6003", parseJson(out.trim()).status)
        assertTrue(parseJson(out.trim()).msg.contains("not found"))
        return
    }
    assertTrue(code == 0)
    assertEquals(tablet_id, parseJson(out.trim()).tablet_id.toString())
    def rowsets_count = parseJson(out.trim()).rowsets.size();
    assertEquals(expected_rowsets_count, rowsets_count)
    def index_files_count = 0
    def segment_files_count = 0
    for (def rowset in parseJson(out.trim()).rowsets) {
        assertEquals(format, rowset.index_storage_format)
        for (int i = 0; i < rowset.segments.size(); i++) {
            def segment = rowset.segments[i]
            assertEquals(i, segment.segment_id)
            def indices_count = segment.indices.size()
            assertEquals(expected_indices_count, indices_count)
            if (format == "V1") {
                index_files_count += indices_count
            } else {
                index_files_count++
            }
        }
        segment_files_count += rowset.segments.size()
    }
    if (format == "V1") {
        assertEquals(index_files_count, segment_files_count * expected_indices_count)
    } else {
        assertEquals(index_files_count, segment_files_count)
    }
}

logger.info("Added 'check_nested_index_file' function to Suite")