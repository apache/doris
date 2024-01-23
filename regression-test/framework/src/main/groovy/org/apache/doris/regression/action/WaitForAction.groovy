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

@Slf4j
class WaitForAction implements SuiteAction{
    private String sqlStr;
    private long time;
    SuiteContext context

    WaitForAction(SuiteContext context) {
        this.context = context
    }

    void sqlStr(String sqlStr) {
        this.sqlStr = sqlStr
    }

    void sqlStr(Closure<String> sqlStr) {
        this.sqlStr = sqlStr.call()
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
            def jobStateResult = sql sqlStr
            res = jobStateResult[0][9]
            if (res == "FINISHED" || res == "CANCELLED") {
                assertEquals("FINISHED", res)
                sleep(3000)
                break
            } else {
                Thread.sleep(2000)
                if (max_try_secs < 1) {
                    log "test timeout," + "state:" + res
                    assertEquals("FINISHED",res)
                }
            }
        }
    }
}
