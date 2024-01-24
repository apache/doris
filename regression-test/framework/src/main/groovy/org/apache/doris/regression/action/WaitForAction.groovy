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
import org.junit.Assert

@Slf4j
class WaitForAction implements SuiteAction{
    private String sql;
    private long time;
    SuiteContext context

    WaitForAction(SuiteContext context) {
        this.context = context
    }

    void sql(String sql) {
        this.sql = sql
    }

    void sql(Closure<String> sql) {
        this.sql = sql.call()
    }

    void time(long time) {
        this.time = time
    }

    void time(Closure<Long> time) {
        this.time = time.call()
    }

    @Override
    void run() {
        while (time--) {
            log.info("sql is :\n${sql}")
            def (result, meta) = JdbcUtils.executeToList(context.getConnection(), sql)
            String res = result.get(0).get(9)
            if (res == "FINISHED" || res == "CANCELLED") {
                Assert.assertEquals("FINISHED", res)
                sleep(3000)
                break
            } else {
                Thread.sleep(2000)
                if (time < 1) {
                    log.info("test timeout," + "state:" + res)
                    Assert.assertEquals("FINISHED",res)
                }
            }
        }
    }
}
