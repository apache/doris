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

import com.google.common.collect.Iterators
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.BytesInputStream
import org.apache.doris.regression.util.JdbcUtils
import org.apache.doris.regression.util.OutputUtils
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.HttpEntity
import org.apache.http.HttpStatus
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.FileEntity
import org.apache.http.entity.InputStreamEntity
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.junit.Assert

@Slf4j
class StreamLoadAction implements SuiteAction {
    public InetSocketAddress address
    public final String user
    public final String password
    String db
    String table
    String file
    InputStream inputStream
    String inputText
    Iterator<List<Object>> inputIterator
    long time
    Closure check
    Map<String, String> headers
    SuiteContext context
    boolean directToBe = false
    boolean twoPhaseCommit = false

    StreamLoadAction(SuiteContext context) {
        this.address = context.getFeHttpAddress()
        this.user = context.config.feHttpUser
        this.password = context.config.feHttpPassword

        def groupList = context.group.split(',')
        this.db = context.config.getDbNameByFile(context.file)

        this.context = context
        this.headers = new LinkedHashMap<>()
        this.headers.put('label', UUID.randomUUID().toString())
    }

    void db(String db) {
        this.db = db
    }

    void db(Closure<String> db) {
        this.db = db.call()
    }

    void table(String table) {
        this.table = table
    }

    void table(Closure<String> table) {
        this.table = table.call()
    }

    void directToBe(String beHost, int beHttpPort) {
        this.address = new InetSocketAddress(beHost, beHttpPort)
        this.directToBe = true
    }

    void inputStream(InputStream inputStream) {
        this.inputStream = inputStream
    }

    void inputStream(Closure<InputStream> inputStream) {
        this.inputStream = inputStream.call()
    }

    void inputIterator(Iterator<List<Object>> inputIterator) {
        this.inputIterator = inputIterator
    }

    void inputIterator(Closure<Iterator<List<Object>>> inputIterator) {
        this.inputIterator = inputIterator.call()
    }

    void inputText(String inputText) {
        this.inputText = inputText
    }

    void inputText(Closure<String> inputText) {
        this.inputText = inputText.call()
    }

    void sql(String sql) {
        headers.put('sql', sql)
    }

    void sql(Closure<String> sql) {
        headers.put('sql', sql.call())
    }

    void file(String file) {
        this.file = file
    }

    void file(Closure<String> file) {
        this.file = file.call()
    }

    void time(long time) {
        this.time = time
    }

    void time(Closure<Long> time) {
        this.time = time.call()
    }

    void twoPhaseCommit(boolean twoPhaseCommit) {
        this.twoPhaseCommit = twoPhaseCommit;
    }

    void twoPhaseCommit(Closure<Boolean> twoPhaseCommit) {
        this.twoPhaseCommit = twoPhaseCommit.call();
    }

    // compatible with selectdb case
    void isCloud(boolean isCloud) {
    }

    // compatible with selectdb case
    void isCloud(Closure<Boolean> isCloud) {
    }

    void check(@ClosureParams(value = FromString, options = ["String,Throwable,Long,Long"]) Closure check) {
        this.check = check
    }

    void set(String key, String value) {
        headers.put(key, value)
    }

    void unset(String key) {
        headers.remove(key)
    }

    @Override
    void run() {
        String responseText = null
        Throwable ex = null
        long startTime = System.currentTimeMillis()
        def isHttpStream = headers.containsKey("version")
        try {
            def uri = ""
            if (isHttpStream) {
                uri = "http://${address.hostString}:${address.port}/api/_http_stream"
            } else if (twoPhaseCommit) {
                uri = "http://${address.hostString}:${address.port}/api/${db}/_stream_load_2pc"
            } else {
                uri = "http://${address.hostString}:${address.port}/api/${db}/${table}/_stream_load"
            }
            HttpClients.createDefault().withCloseable { client ->
                RequestBuilder requestBuilder = prepareRequestHeader(RequestBuilder.put(uri))
                HttpEntity httpEntity = prepareHttpEntity(client)
                if (!directToBe) {
                    String beLocation = streamLoadToFe(client, requestBuilder)
                    log.info("Redirect stream load to ${beLocation}".toString())
                    requestBuilder.setUri(beLocation)
                }
                requestBuilder.setEntity(httpEntity)
                responseText = streamLoadToBe(client, requestBuilder)
            }
        } catch (Throwable t) {
            ex = t
        }
        long endTime = System.currentTimeMillis()

        log.info("Stream load elapsed ${endTime - startTime} ms, is http stream: ${isHttpStream}, " +
                " response: ${responseText}" + ex.toString())
        checkResult(responseText, ex, startTime, endTime)
    }

    private String httpGetString(CloseableHttpClient client, String url) {
        return client.execute(RequestBuilder.get(url).build()).withCloseable { resp ->
            EntityUtils.toString(resp.getEntity())
        }
    }

    private InputStream httpGetStream(CloseableHttpClient client, String url) {
        CloseableHttpResponse resp = client.execute(RequestBuilder.get(url).build())
        int code = resp.getStatusLine().getStatusCode()
        if (code != HttpStatus.SC_OK) {
            String streamBody = EntityUtils.toString(resp.getEntity())
            throw new IllegalStateException("Get http stream failed, status code is ${code}, body:\n${streamBody}")
        }

        return resp.getEntity().getContent()
    }

    private RequestBuilder prepareRequestHeader(RequestBuilder requestBuilder) {
        String encoding = Base64.getEncoder()
                .encodeToString((user + ":" + (password == null ? "" : password)).getBytes("UTF-8"))
        requestBuilder.setHeader("Authorization", "Basic ${encoding}")

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            requestBuilder.setHeader(entry.key, entry.value)
        }
        requestBuilder.setHeader("Expect", "100-Continue")
        return requestBuilder
    }

    private String cacheHttpFile(CloseableHttpClient client, String url) {
        def relativePath = url.substring(url.indexOf('/', 9))
        def file = new File("${context.config.cacheDataPath}/${relativePath}")
        if (file.exists()) {
            log.info("Found ${url} in ${file.getAbsolutePath()}");
            return file.getAbsolutePath()
        }
        log.info("Start to cache data from ${url} to ${file.getAbsolutePath()}");
        CloseableHttpResponse resp = client.execute(RequestBuilder.get(url).build())
        int code = resp.getStatusLine().getStatusCode()
        if (code != HttpStatus.SC_OK) {
            String streamBody = EntityUtils.toString(resp.getEntity())
            log.info("Fail to download data ${url}, code: ${code}, body:\n${streamBody}")
            throw new IllegalStateException("Get http stream failed, status code is ${code}, body:\n${streamBody}")
        }

        file.getParentFile().mkdirs();
        new File("${context.config.cacheDataPath}/tmp/").mkdir()
        InputStream httpFileStream = resp.getEntity().getContent()
        File tmpFile = File.createTempFile("cache", null, new File("${context.config.cacheDataPath}/tmp/"))

        java.nio.file.Files.copy(httpFileStream, tmpFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        java.nio.file.Files.move(tmpFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        log.info("Cached data from ${url} to ${file.getAbsolutePath()}");
        return file.getAbsolutePath()
    }

    private HttpEntity prepareHttpEntity(CloseableHttpClient client) {
        HttpEntity entity = null
        if (inputStream != null) {
            entity = new InputStreamEntity(inputStream)
        } else if (inputText != null) {
            entity = new StringEntity(inputText)
        } else if (inputIterator != null) {
            def bytesIt = Iterators.transform(inputIterator,
                    {row -> (OutputUtils.toCsvString(row) + "\n").getBytes()})
            entity = new InputStreamEntity(new BytesInputStream(bytesIt))
        } else {
            String fileName = this.file
            if (fileName.startsWith("http://") || fileName.startsWith("https://")) {
                log.info("Set stream load input: ${fileName}".toString())
                def file = new File(context.config.cacheDataPath)
                file.mkdirs();

                if (file.exists() && file.isDirectory() && context.config.enableCacheData) {
                    fileName = cacheHttpFile(client, fileName)
                } else {
                    entity = new InputStreamEntity(httpGetStream(client, fileName))
                    return entity;
                }
            }
            if (!new File(fileName).isAbsolute()) {
                fileName = new File(context.dataPath, fileName).getAbsolutePath()
            }
            def file = new File(fileName)
            if (!file.exists()) {
                log.warn("Stream load input file not exists: ${file}".toString())
                throw new IllegalStateException("Stream load input file not exists: ${file}");
            }
            log.info("Set stream load input: ${file.canonicalPath}".toString())
            entity = new FileEntity(file)
        }
        return entity
    }

    private String streamLoadToFe(CloseableHttpClient client, RequestBuilder requestBuilder) {
        log.info("Stream load to ${requestBuilder.uri}".toString())
        String backendStreamLoadUri = null
        client.execute(requestBuilder.build()).withCloseable { resp ->
            resp.withCloseable {
                String body = EntityUtils.toString(resp.getEntity())
                def respCode = resp.getStatusLine().getStatusCode()
                // should redirect to backend
                if (respCode != 307) {
                    throw new IllegalStateException("Expect frontend stream load response code is 307, " +
                            "but meet ${respCode}\nbody: ${body}")
                }
                backendStreamLoadUri = resp.getFirstHeader("location").getValue()
            }
        }
        return backendStreamLoadUri
    }

    private String streamLoadToBe(CloseableHttpClient client, RequestBuilder requestBuilder) {
        String responseText
        try{
            client.execute(requestBuilder.build()).withCloseable { resp ->
                resp.withCloseable {
                    String body = EntityUtils.toString(resp.getEntity())
                    def respCode = resp.getStatusLine().getStatusCode()
                    if (respCode != 200) {
                        throw new IllegalStateException("Expect backend stream load response code is 200, " +
                                "but meet ${respCode}\nbody: ${body}")
                    }
                    responseText = body
                }
            }
        } catch (Throwable t) {
            log.info("StreamLoadAction Exception: ", t)
        }
        return responseText
    }

    private void checkResult(String responseText, Throwable ex, long startTime, long endTime) {
        String finalStatus = waitForPublishOrFailure(responseText)
        log.info("The origin stream load result: ${responseText}, final status: ${finalStatus}")
        responseText = responseText.replace("Publish Timeout", finalStatus)
        if (check != null) {
            check.call(responseText, ex, startTime, endTime)
        } else {
            if (ex != null) {
                throw ex
            }

            def jsonSlurper = new JsonSlurper()
            def result = jsonSlurper.parseText(responseText)
            String status = result.Status
            if (!"Success".equalsIgnoreCase(status)) {
                String errorUrl = result.ErrorURL
                if (errorUrl != null) {
                    String errorDetails = HttpClients.createDefault().withCloseable { client ->
                        httpGetString(client, errorUrl)
                    }
                    throw new IllegalStateException("Stream load failed:\n${responseText}\n${errorDetails}")
                }
                throw new IllegalStateException("Stream load failed:\n${responseText}")
            }
            long numberTotalRows = result.NumberTotalRows.toLong()
            long numberLoadedRows = result.NumberLoadedRows.toLong()
            if (numberTotalRows != numberLoadedRows) {
                throw new IllegalStateException("Stream load rows mismatch:\n${responseText}")
            }

            if (time > 0) {
                long elapsed = endTime - startTime
                try{
                    Assert.assertTrue("Expect elapsed <= ${time}, but meet ${elapsed}", elapsed <= time)
                } catch (Throwable t) {
                    throw new IllegalStateException("Expect elapsed <= ${time}, but meet ${elapsed}")
                }
            }
        }
    }

    // Sometime the stream load may return "PUBLISH TIMEOUT"
    // This is not a fatal error but may cause test fail.
    // So here we wait for at most 60s, using "show transaction" to check the
    // status of txn, and return once it become ABORTED or VISIBLE.
    private String waitForPublishOrFailure(String responseText) {
        try {
            long maxWaitSecond = 60;
            def jsonSlurper = new JsonSlurper()
            def parsed = jsonSlurper.parseText(responseText)
            String status = parsed.Status
            if (twoPhaseCommit) {
                status = parsed.status
                return status;
            }
            long txnId = parsed.TxnId
            if (!status.equalsIgnoreCase("Publish Timeout")) {
                return status;
            }

            log.info("Stream load with txn ${txnId} is publish timeout")
            String sql = "show transaction from ${db} where id = ${txnId}"
            String st = "PREPARE"
            while (!st.equalsIgnoreCase("VISIBLE") && !st.equalsIgnoreCase("ABORTED") && maxWaitSecond > 0) {
                Thread.sleep(2000)
                maxWaitSecond -= 2
                def (result, meta) = JdbcUtils.executeToStringList(context.getConnection(), sql)
                if (result.size() != 1) {
                    throw new IllegalStateException("Failed to get txn's ${txnId}")
                }
                st = String.valueOf(result[0][3])
            }
            log.info("Stream load with txn ${txnId} is ${st}")
            if (st.equalsIgnoreCase("VISIBLE")) {
                return "Success";
            } else {
                return "Fail";
            }
        } catch (Throwable t) {
            log.info("failed to waitForPublishOrFailure. response: ${responseText}", t);
            throw t;
        }
    }
}
