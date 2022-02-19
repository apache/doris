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

import groovy.util.logging.Slf4j
import java.sql.Connection

import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils
import org.junit.Assert

@Slf4j
class TestAction implements SuiteAction {
    private String sql
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
            def result = doRun(context.conn)
            if (check != null) {
                check.call(result.result, result.exception, result.startTime, result.endTime)
            } else {
                if (exception != null || result.exception != null) {
                    def msg = result.exception?.toString()
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
            }
        } catch (Throwable t) {
            log.info("TestAction Exception: ", t)
            List resList = [context.file.getName(), 'test', sql, t]
            context.recorder.reportDiffResult(resList)
            throw t
        }
    }

    ActionResult doRun(Connection conn) {
        Object result = null
        Throwable ex = null
        long startTime = System.currentTimeMillis()
        try {
            log.info("Execute sql:\n${sql}".toString())
            result = JdbcUtils.executeToList(conn, sql)
        } catch (Throwable t) {
            ex = t
        }
        long endTime = System.currentTimeMillis()
        return new ActionResult(result, ex, startTime, endTime)
    }

    void sql(String sql) {
        this.sql = sql
    }

    void sql(Closure<String> sqlSupplier) {
        this.sql = sqlSupplier.call()
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

    void exception(String exceptionMsg) {
        this.exception = exceptionMsg
    }

    void exception(Closure<String> exceptionMsgSupplier) {
        this.exception = exceptionMsgSupplier.call()
    }

    void check(Closure check) {
        this.check = check
    }

    class ActionResult {
        Object result
        Throwable exception
        long startTime
        long endTime

        ActionResult(Object result, Throwable exception, long startTime, long endTime) {
            this.result = result
            this.exception = exception
            this.startTime = startTime
            this.endTime = endTime
        }
    }
}
