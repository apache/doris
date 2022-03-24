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
import org.apache.doris.regression.util.OutputUtils
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.HttpEntity
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
    public final InetSocketAddress address
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

    StreamLoadAction(SuiteContext context) {
        this.address = context.config.feHttpInetSocketAddress
        this.user = context.config.feHttpUser
        this.password = context.config.feHttpPassword
        this.db = context.config.defaultDb
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

    void check(@ClosureParams(value = FromString, options = ["String,Throwable,Long,Long"]) Closure check) {
        this.check = check
    }

    void set(String key, String value) {
        headers.put(key, value)
    }

    @Override
    void run() {
        String responseText = null
        Throwable ex = null
        long startTime = System.currentTimeMillis()
        try {
            def uri = "http://${address.hostString}:${address.port}/api/${db}/${table}/_stream_load"
            HttpClients.createDefault().withCloseable { client ->
                RequestBuilder requestBuilder = prepareRequestHeader(RequestBuilder.put(uri))
                HttpEntity httpEntity = prepareHttpEntity(client)
                String beLocation = streamLoadToFe(client, requestBuilder)
                responseText = streamLoadToBe(client, requestBuilder, beLocation, httpEntity)
            }
        } catch (Throwable t) {
            ex = t
        }
        long endTime = System.currentTimeMillis()
        log.info("Stream load elapsed ${endTime - startTime} ms".toString())
        checkResult(responseText, ex, startTime, endTime)
    }

    private String httpGetString(CloseableHttpClient client, String url) {
        return client.execute(RequestBuilder.get(url).build()).withCloseable { resp ->
            EntityUtils.toString(resp.getEntity())
        }
    }

    private InputStream httpGetStream(CloseableHttpClient client, String url) {
        return client.execute(RequestBuilder.get(url).build()).getEntity().getContent()
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
                entity = new InputStreamEntity(httpGetStream(client, fileName))
            } else { // local file
                if (!new File(fileName).isAbsolute()) {
                    fileName = new File(context.dataPath, fileName).getAbsolutePath()
                }
                def file = new File(fileName)
                if (!file.exists()) {
                    log.warn("Stream load input file not exists: ${file}".toString())
                }
                log.info("Set stream load input: ${file.canonicalPath}".toString())
                entity = new FileEntity(file)
            }
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
                    List resList = [context.file.getName(), 'streamLoad', '', "Expect frontend stream load response code is 307, " +
                            "but meet ${respCode}\nbody: ${body}"]
                    context.recorder.reportDiffResult(resList)
                    throw new IllegalStateException("Expect frontend stream load response code is 307, " +
                            "but meet ${respCode}\nbody: ${body}")
                }
                backendStreamLoadUri = resp.getFirstHeader("location").getValue()
            }
        }
        return backendStreamLoadUri
    }

    private String streamLoadToBe(CloseableHttpClient client, RequestBuilder requestBuilder, String beLocation, HttpEntity httpEntity) {
        log.info("Redirect stream load to ${beLocation}".toString())
        requestBuilder.setUri(beLocation)
        requestBuilder.setEntity(httpEntity)
        String responseText
        try{
            client.execute(requestBuilder.build()).withCloseable { resp ->
                resp.withCloseable {
                    String body = EntityUtils.toString(resp.getEntity())
                    def respCode = resp.getStatusLine().getStatusCode()
                    if (respCode != 200) {
                        List resList = [context.file.getName(), 'streamLoad', '', "Expect backend stream load response code is 200, " +
                                "but meet ${respCode}\nbody: ${body}"]
                        context.recorder.reportDiffResult(resList)

                        throw new IllegalStateException("Expect backend stream load response code is 200, " +
                                "but meet ${respCode}\nbody: ${body}")
                    }
                    responseText = body
                }
            }
        } catch (Throwable t) {
            log.info("StreamLoadAction Exception: ", t)
            List resList = [context.file.getName(), 'streamLoad', '', t]
            context.recorder.reportDiffResult(resList)
        }
        return responseText
    }

    private void checkResult(String responseText, Throwable ex, long startTime, long endTime) {
        if (check != null) {
            check.call(responseText, ex, startTime, endTime)
        } else {
            if (ex != null) {
                List resList = [context.file.getName(), 'streamLoad', '', ex]
                context.recorder.reportDiffResult(resList)
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
                    List resList = [context.file.getName(), 'streamLoad', '', "Stream load failed:\n${responseText}\n${errorDetails}"]
                    context.recorder.reportDiffResult(resList)
                    throw new IllegalStateException("Stream load failed:\n${responseText}\n${errorDetails}")
                }
                List resList = [context.file.getName(), 'streamLoad', '', "Stream load failed:\n${responseText}"]
                context.recorder.reportDiffResult(resList)
                throw new IllegalStateException("Stream load failed:\n${responseText}")
            }
            long numberTotalRows = result.NumberTotalRows.toLong()
            long numberLoadedRows = result.NumberLoadedRows.toLong()
            if (numberTotalRows != numberLoadedRows) {
                List resList = [context.file.getName(), 'streamLoad', '', "Stream load rows mismatch:\n${responseText}"]
                context.recorder.reportDiffResult(resList)
                throw new IllegalStateException("Stream load rows mismatch:\n${responseText}")

            }

            if (time > 0) {
                long elapsed = endTime - startTime
                try{
                    Assert.assertTrue("Expect elapsed <= ${time}, but meet ${elapsed}", elapsed <= time)
                } catch (Throwable t) {
                    List resList = [context.file.getName(), 'streamLoad', '', "Expect elapsed <= ${time}, but meet ${elapsed}"]
                    context.recorder.reportDiffResult(resList)
                    throw new IllegalStateException("Expect elapsed <= ${time}, but meet ${elapsed}")
                }
            }
        }
    }
}