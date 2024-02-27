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

import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import groovy.util.logging.Slf4j
import org.apache.commons.io.LineIterator
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.OutputUtils
import org.apache.http.HttpStatus
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.ResultSetMetaData
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils
import org.junit.Assert

import java.util.function.Consumer
import java.util.Random

@Slf4j
@CompileStatic
class TestAction implements SuiteAction {
    private String sql
    private boolean isOrder
    private String resultFileUri
    private String resultTag
    private Iterator<Object> resultIterator
    private Object result
    private long time
    private long rowNum = -1
    private String exception
    private Closure check
    SuiteContext context

    TestAction(SuiteContext context) {
        this.context = context
    }

    @Override
    void run() {
        try{
            def result = doRun(context.getConnection())
            if (check != null) {
                check.call(result.result, result.exception, result.startTime, result.endTime)
            } else {
                if (exception != null || result.exception != null) {
                    def msg = result.exception?.toString()
                    log.info("Exception: ${msg}")
                    Assert.assertTrue("Expect exception msg contains '${exception}', but meet '${msg}'",
                            msg != null && exception != null && msg.contains(exception))
                }
                if (time > 0) {
                    long elapsed = result.endTime - result.startTime
                    Assert.assertTrue("Expect elapsed <= ${time}, but meet ${elapsed}", elapsed <= time)
                }
                if (rowNum >= 0) {
                    if (result.result instanceof Integer || result.result instanceof Long) {
                        def realRowNum = ((Number) result.result).longValue()
                        Assert.assertEquals("RowNum", rowNum, realRowNum)
                    } else if (result.result instanceof List) {
                        def realRowNum = ((List) result.result).size().longValue()
                        Assert.assertEquals("RowNum", rowNum, realRowNum)
                    } else {
                        log.warn("Unknown result: ${result.result}, can not check row num".toString())
                    }
                }
                if (this.result != null) {
                    Assert.assertEquals(this.result, result.result)
                }
                if (this.resultIterator != null) {
                    String errorMsg = OutputUtils.checkOutput(this.resultIterator, result.result.iterator(),
                        { Object row ->
                            if (row instanceof List) {
                                return OutputUtils.toCsvString(row as List)
                            } else {
                                return OutputUtils.toCsvString(row as Object)
                            }
                        },
                            { List<Object> row -> OutputUtils.toCsvString(row) }, "Check failed", result.meta)
                    if (errorMsg != null) {
                        throw new IllegalStateException(errorMsg)
                    }
                }
                if (this.resultFileUri != null) {
                    Consumer<InputStream> checkFunc = { InputStream inputStream ->
                        String errorMsg = null
                        def lineIt = new LineIterator(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                        if (resultTag.is(null)) {
                            def csvIt = new OutputUtils.CsvParserIterator(lineIt)
                            errorMsg = OutputUtils.checkOutput(csvIt, result.result.iterator(),
                                    { List<String> row -> OutputUtils.toCsvString(row as List<Object>) },
                                    { List<Object> row -> OutputUtils.toCsvString(row) },
                                    "Check failed compare to", result.meta)
                            if (errorMsg != null) {
                                throw new IllegalStateException(errorMsg)
                            }
                        } else {
                            def outputIt = OutputUtils.iterator(lineIt)
                            if (!outputIt.hasNextTagBlock(resultTag)) {
                                throw new IllegalStateException("Missing output block for tag '${resultTag}': ${context.outputFile.getAbsolutePath()}")
                            }
                            def expectCsvResults = outputIt.next()
                            try {
                                errorMsg = OutputUtils.checkOutput(expectCsvResults, result.result.iterator(),
                                        { row -> OutputUtils.toCsvString(row as List<Object>) },
                                        { row ->  OutputUtils.toCsvString(row) },
                                        "Check tag '${resultTag}' failed", result.meta)
                            } catch (Throwable t) {
                                throw new IllegalStateException("Check tag '${resultTag}' failed, sql:\n${sql}", t)
                            }
                            if (errorMsg != null) {
                                throw new IllegalStateException("Check tag '${resultTag}' failed:\n${errorMsg}\n\nsql:\n${sql}")
                            }
                        }
                    }

                    if (this.resultFileUri.startsWith("http://") || this.resultFileUri.startsWith("https://")) {
                        log.info("Compare to http stream: ${this.resultFileUri}")
                        HttpClients.createDefault().withCloseable { client ->
                            client.execute(RequestBuilder.get(this.resultFileUri).build()).withCloseable { CloseableHttpResponse resp ->
                                int code = resp.getStatusLine().getStatusCode()
                                if (code != HttpStatus.SC_OK) {
                                    String streamBody = EntityUtils.toString(resp.getEntity())
                                    throw new IllegalStateException("Get http stream failed, status code is ${code}, body:\n${streamBody}")
                                }
                                checkFunc(resp.entity.content)
                            }
                        }
                    } else {
                        String fileName = resultFileUri
                        if (!new File(fileName).isAbsolute()) {
                            fileName = new File(context.dataPath, fileName).getAbsolutePath()
                        }
                        def file = new File(fileName).getCanonicalFile()
                        if (!file.exists()) {
                            log.warn("Result file not exists: ${file}".toString())
                        }

                        log.info("Compare to local file: ${file}".toString())
                        file.newInputStream().withCloseable { inputStream ->
                            checkFunc(inputStream)
                        }
                    }
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("TestAction failed, sql:\n${sql}", t)
        }
    }

    ActionResult doRun(Connection conn) {
        List<List<Object>> result = null
        ResultSetMetaData meta = null
        Throwable ex = null

        long startTime = System.currentTimeMillis()
        try {
            log.info("Execute ${isOrder ? "order_" : ""}sql:\n${sql}".toString())
            (result, meta) = JdbcUtils.executeToList(conn, sql)
            if (isOrder) {
                result = DataUtils.sortByToString(result)
            }
        } catch (Throwable t) {
            ex = t
        }
        long endTime = System.currentTimeMillis()

        return new ActionResult(result, ex, startTime, endTime, meta)
    }

    void sql(String sql, boolean setRandomParallel = true) {
        this.sql = sql
    }

    void sql(Closure<String> sqlSupplier) {
        this.sql = sqlSupplier.call()
    }

    void order(boolean isOrder) {
        this.isOrder = isOrder
    }

    void time(long time) {
        this.time = time
    }

    void time(Closure<Long> timeSupplier) {
        this.time = timeSupplier.call()
    }

    void rowNum(long rowNum) {
        this.rowNum = rowNum
    }

    void rowNum(Closure<Long> rowNum) {
        this.rowNum = rowNum.call()
    }

    void result(Object result) {
        this.result = result
    }

    void result(Closure<Object> resultSupplier) {
        this.result = resultSupplier.call()
    }

    void resultIterator(Iterator<Object> resultIterator) {
        this.resultIterator = resultIterator
    }

    void resultIterator(Closure<Iterator<Object>> resultIteratorSupplier) {
        this.resultIterator = resultIteratorSupplier.call()
    }

    void resultFile(String file, String tag = null) {
        this.resultFileUri = file
        this.resultTag = tag
    }

    void resultFile(Closure<String> resultFileSupplier) {
        this.resultFileUri = resultFileSupplier.call()
    }

    void resultTag(String tag) {
        this.resultTag = tag
    }

    void exception(String exceptionMsg) {
        this.exception = exceptionMsg
    }

    void exception(Closure<String> exceptionMsgSupplier) {
        this.exception = exceptionMsgSupplier.call()
    }

    void check(@ClosureParams(value = FromString, options = ["String,Throwable,Long,Long"]) Closure check) {
        this.check = check
    }

    class ActionResult {
        List<List<Object>> result
        Throwable exception
        long startTime
        long endTime
        ResultSetMetaData meta

        ActionResult(List<List<Object>> result, Throwable exception, long startTime, long endTime, ResultSetMetaData meta) {
            this.result = result
            this.exception = exception
            this.startTime = startTime
            this.endTime = endTime
            this.meta = meta
        }
    }
}
