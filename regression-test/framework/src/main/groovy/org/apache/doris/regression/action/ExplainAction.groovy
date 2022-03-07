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

import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils
import groovy.util.logging.Slf4j

import java.util.stream.Collectors

@Slf4j
class ExplainAction implements SuiteAction {
    private String sql
    private SuiteContext context
    private Set<String> containsStrings = new LinkedHashSet<>()
    private Set<String> notContainsStrings = new LinkedHashSet<>()
    private Closure checkFunction

    ExplainAction(SuiteContext context) {
        this.context = context
    }

    void sql(String sql) {
        this.sql = sql
    }

    void sql(Closure<String> sqlSupplier) {
        this.sql = sqlSupplier.call()
    }

    void contains(String subString) {
        containsStrings.add(subString)
    }

    void notContains(String subString) {
        notContainsStrings.add(subString)
    }

    void check(Closure<Boolean> checkFunction) {
        this.checkFunction = checkFunction
    }

    @Override
    void run() {
        String explainSql = "explain\n" + sql
        def result = doTest(explainSql)
        String explainString = result.result
        if (checkFunction != null) {
            try {
                Boolean checkResult = null
                if (checkFunction.parameterTypes.size() == 1) {
                    if (result.exception == null) {
                        checkResult = checkFunction(explainString)
                    } else {
                        throw result.exception
                    }
                } else {
                    checkResult = checkFunction(explainString, result.exception, result.startTime, result.endTime)
                }
                if (checkResult != null && checkResult.booleanValue() == false) {
                    String msg = "Explain and custom check failed, actual explain string is:\n${explainString}".toString()
                    throw new IllegalStateException(msg)
                }
            } catch (Throwable t) {
                log.error("Explain and custom check failed", t)
                List resList = [context.file.getName(), 'explain', sql, t]
                context.recorder.reportDiffResult(resList)
                throw t
            }
        } else if (result.exception != null) {
            String msg = "Explain failed"
            log.error(msg, result.exception)
            List resList = [context.file.getName(), 'explain', sql, result.exception]
            context.recorder.reportDiffResult(resList)
            throw new IllegalStateException(msg, result.exception)
        } else {
            for (String string : containsStrings) {
                if (!explainString.contains(string)) {
                    String msg = ("Explain and check failed, expect contains '${string}',"
                            + "but actual explain string is:\n${explainString}").toString()
                    log.info(msg)
                    def t = new IllegalStateException(msg)
                    List resList = [context.file.getName(), 'explain', sql, t]
                    context.recorder.reportDiffResult(resList)
                    throw t
                }
            }
            for (String string : notContainsStrings) {
                if (explainString.contains(string)) {
                    String msg = ("Explain and check failed, expect not contains '${string}',"
                            + "but actual explain string is:\n${explainString}").toString()
                    log.info(msg)
                    def t = new IllegalStateException(msg)
                    List resList = [context.file.getName(), 'explain', sql, t]
                    context.recorder.reportDiffResult(resList)
                    throw t
                }
            }
        }
    }

    private ActionResult doTest(String explainSql) {
        log.info("Execute sql:\n${explainSql}".toString())
        long startTime = System.currentTimeMillis()
        String explainString = null
        try {
            explainString = JdbcUtils.executeToList(context.conn, explainSql).stream()
                    .map({row -> row.get(0).toString()})
                    .collect(Collectors.joining("\n"))
            return new ActionResult(explainString, null, startTime, System.currentTimeMillis())
        } catch (Throwable t) {
            return new ActionResult(explainString, t, startTime, System.currentTimeMillis())
        }
    }

    class ActionResult {
        String result
        Throwable exception
        long startTime
        long endTime

        ActionResult(String result, Throwable exception, long startTime, long endTime) {
            this.result = result
            this.exception = exception
            this.startTime = startTime
            this.endTime = endTime
        }
    }
}
