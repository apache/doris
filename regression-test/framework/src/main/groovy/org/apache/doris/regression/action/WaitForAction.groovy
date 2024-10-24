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
import org.apache.commons.lang3.ObjectUtils
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils
import org.junit.Assert

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

@Slf4j
class WaitForAction implements SuiteAction {
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
        if (ObjectUtils.isEmpty(time) || time <= 0) {
            time = 600
        }
        def forRollUp = sql.toUpperCase().contains("ALTER TABLE ROLLUP")
        def num = 9
        if (forRollUp) {
            num = 8
        }
        Awaitility.await().atMost(time, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).and()
                .pollInterval(100, TimeUnit.MILLISECONDS).await().until(() -> {
            log.info("sql is :\n${sql}")
            def (result, meta) = JdbcUtils.executeToList(context.getConnection(), sql)
            String res = result.get(0).get(num)
            if (res == "FINISHED" || res == "CANCELLED") {
                Assert.assertEquals("FINISHED", res)
                return true;
            }
            return false;
        });
    }
}
