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
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils

import java.sql.Connection
import java.sql.ResultSetMetaData

@Slf4j
class CreateMVAction implements SuiteAction {
    private String sql
    private String exception
    private SuiteContext context

    CreateMVAction(SuiteContext context, String sql) {
        this.context = context
        this.sql = sql
    }

    CreateMVAction(SuiteContext context, String sql, String exception) {
        this.context = context
        this.sql = sql
        this.exception = exception
    }

    @Override
    void run() {
        def result = doRun(sql)
        String resultString = result.result

        if (exception != null) {
            if (result.exception == null) {
                throw new IllegalStateException("Expect exception on create mv, but create success");
            }
            def msg = result.exception?.toString()
            Assert.assertTrue("Expect exception msg contains '${exception}', but meet '${msg}'",
                    msg != null && exception != null && msg.contains(exception))
            return
        }

        def tryTimes = 0
        def sqlResult = "null"
        while (!sqlResult.contains("FINISHED")) {
            def tmp = doRun("SHOW ALTER TABLE MATERIALIZED VIEW ORDER BY CreateTime DESC LIMIT 1;")
            sqlResult = tmp.result[0]
            log.info("result: ${sqlResult}".toString())
            if (tryTimes == 120 || sqlResult.contains("CANCELLED")) {
                throw new IllegalStateException("MV create check times over limit, result='${sqlResult}'");
            }
            Thread.sleep(1200)
            tryTimes++
        }

        Thread.sleep(1000)
    }

    ActionResult doRun(String sql) {
        Connection conn = context.getConnection()
        List<List<Object>> result = null
        ResultSetMetaData meta = null
        Throwable ex = null

        long startTime = System.currentTimeMillis()
        try {
            log.info("sql:\n${sql}".toString())
            (result, meta) = JdbcUtils.executeToList(conn, sql)
        } catch (Throwable t) {
            ex = t
        }
        long endTime = System.currentTimeMillis()

        return new ActionResult(result, ex, startTime, endTime, meta)
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
