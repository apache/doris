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


// Note: in order to trigger TTL full (thus LRU eviction),
// we need smaller file cache for TTL (<20G). To achive this goal:
//  - set smaller total_size in be conf and/or
//  - set smaller max_ttl_cache_ratio in this test

suite("test_ttl_lru_evict") {
    sql """ use @regression_cluster_name1 """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="3600") """
    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true") && backend[19].contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    assertEquals(backendIdToBackendIP.size(), 1)

    backendId = backendIdToBackendIP.keySet()[0]
    def url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/file_cache?op=clear&sync=true"""
    logger.info(url)
    def clearFileCache = { check_func ->
        httpTest {
            endpoint ""
            uri url
            op "get"
            body ""
            check check_func
        }
    }

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

        String cmd = String.format("curl --max-time %d -X %s %s", timeout, method, uri).toString()
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

    def show_be_config = { String ip, String port /*param */ ->
        return curl("GET", String.format("http://%s:%s/api/show_config", ip, port))
    }

    def get_be_param = { paramName ->
        // assuming paramName on all BEs have save value
        def (code, out, err) = show_be_config(backendIdToBackendIP.get(backendId), backendIdToBackendHttpPort.get(backendId))
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == paramName) {
                return ((List<String>) ele)[2]
            }
        }
    }

    def set_be_param = { paramName, paramValue ->
        // for eache BE node, set paramName=paramValue
        for (String id in backendIdToBackendIP.keySet()) {
            def beIp = backendIdToBackendIP.get(id)
            def bePort = backendIdToBackendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    long org_max_ttl_cache_ratio = Long.parseLong(get_be_param.call("max_ttl_cache_ratio"))

    try {
        set_be_param.call("max_ttl_cache_ratio", "2")

        def tables = [customer_ttl: 15000000]
        def s3BucketName = getS3BucketName()
        def s3WithProperties = """WITH S3 (
            |"AWS_ACCESS_KEY" = "${getS3AK()}",
            |"AWS_SECRET_KEY" = "${getS3SK()}",
            |"AWS_ENDPOINT" = "${getS3Endpoint()}",
            |"AWS_REGION" = "${getS3Region()}",
            |"provider" = "${getS3Provider()}")
            |PROPERTIES(
            |"exec_mem_limit" = "8589934592",
            |"load_parallelism" = "3")""".stripMargin()


        sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
        def load_customer_once =  { String table ->
            def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
            sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
            def loadLabel = table + "_" + uniqueID
            // load data from cos
            def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
            loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
            sql loadSql

            // check load state
            while (true) {
                def stateResult = sql "show load where Label = '${loadLabel}'"
                def loadState = stateResult[stateResult.size() - 1][2].toString()
                if ("CANCELLED".equalsIgnoreCase(loadState)) {
                    throw new IllegalStateException("load ${loadLabel} failed.")
                } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                    break
                }
                sleep(5000)
            }
        }

        def getMetricsMethod = { check_func ->
            httpTest {
                endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
                uri "/brpc_metrics"
                op "get"
                check check_func
            }
        }

        clearFileCache.call() {
            respCode, body -> {}
        }

        long ttl_cache_evict_size_begin = 0;
        getMetricsMethod.call() {
            respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                String out = "${body}".toString()
                def strs = out.split('\n')
                Boolean flag1 = false;
                for (String line in strs) {
                    if (flag1) break;
                    if (line.contains("ttl_cache_evict_size")) {
                        if (line.startsWith("#")) {
                            continue
                        }
                        def i = line.indexOf(' ')
                        ttl_cache_evict_size_begin = line.substring(i).toLong()
                        flag1 = true
                    }
                }
                assertTrue(flag1)
        }

        // load for MANY times to this dup table to make cache full enough to evict
        // It will takes huge amount of time, so it should be p1/p2 level case.
        // But someone told me it is the right place. Never mind just do it!
        for (int i = 0; i < 10; i++) {
            load_customer_once("customer_ttl")
        }
        sleep(10000) // 10s
        // first, we test whether ttl evict actively
        long ttl_cache_evict_size_end = 0;
        getMetricsMethod.call() {
            respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                String out = "${body}".toString()
                def strs = out.split('\n')
                Boolean flag1 = false;
                for (String line in strs) {
                    if (flag1) break;
                    if (line.contains("ttl_cache_evict_size")) {
                        if (line.startsWith("#")) {
                            continue
                        }
                        def i = line.indexOf(' ')
                        ttl_cache_evict_size_end = line.substring(i).toLong()
                        flag1 = true
                    }
                }
                assertTrue(flag1)
        }
        // Note: this only applies when the case is run
        // sequentially, coz we don't know what other cases are
        // doing with TTL cache
        logger.info("ttl evict diff:" + (ttl_cache_evict_size_end - ttl_cache_evict_size_begin).toString())
        assertTrue((ttl_cache_evict_size_end - ttl_cache_evict_size_begin) > 1073741824)

        // then we test skip_cache count when doing query when ttl cache is full
        // we expect it to be rare
        long skip_cache_count_begin = 0
        getMetricsMethod.call() {
            respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                String out = "${body}".toString()
                def strs = out.split('\n')
                Boolean flag1 = false;
                for (String line in strs) {
                    if (flag1) break;
                    if (line.contains("cached_remote_reader_skip_cache_sum")) {
                        if (line.startsWith("#")) {
                            continue
                        }
                        def i = line.indexOf(' ')
                        skip_cache_count_begin = line.substring(i).toLong()
                        flag1 = true
                    }
                }
                assertTrue(flag1)
        }

        // another wave of load to flush the current TTL away
        for (int i = 0; i < 10; i++) {
            load_customer_once("customer_ttl")
        }
        // then we do query and hopefully, we should not run into too many SKIP_CACHE
        sql """ select count(*) from customer_ttl where C_ADDRESS like '%ea%' and C_NAME like '%a%' and C_COMMENT like '%b%' """

        long skip_cache_count_end = 0
        getMetricsMethod.call() {
            respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                String out = "${body}".toString()
                def strs = out.split('\n')
                Boolean flag1 = false;
                for (String line in strs) {
                    if (flag1) break;
                    if (line.contains("cached_remote_reader_skip_cache_sum")) {
                        if (line.startsWith("#")) {
                            continue
                        }
                        def i = line.indexOf(' ')
                        skip_cache_count_end = line.substring(i).toLong()
                        flag1 = true
                    }
                }
                assertTrue(flag1)
        }
        // Note: this only applies when the case is run
        // sequentially, coz we don't know what other cases are
        // doing with TTL cache
        logger.info("skip cache diff:" + (skip_cache_count_end - skip_cache_count_begin).toString())
        assertTrue((skip_cache_count_end - skip_cache_count_begin) < 1000000)

        // finally, we test whether LRU queue clean itself up when all ttl
        // cache evict after expiration
        sleep(200000)
        getMetricsMethod.call() {
            respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                String out = "${body}".toString()
                def strs = out.split('\n')
                Boolean flag1 = false;
                for (String line in strs) {
                    if (flag1) break;
                    if (line.contains("ttl_cache_lru_queue_size")) {
                        if (line.startsWith("#")) {
                            continue
                        }
                        def i = line.indexOf(' ')
                        // all ttl will expire, so the ttl LRU queue set to 0
                        // Note: this only applies when the case is run
                        // sequentially, coz we don't know what other cases are
                        // doing with TTL cache
                        assertEquals(line.substring(i).toLong(), 0)
                        flag1 = true
                    }
                }
                assertTrue(flag1)
        }
    } finally {
        set_be_param.call("max_ttl_cache_ratio", org_max_ttl_cache_ratio.toString())
    }
}
