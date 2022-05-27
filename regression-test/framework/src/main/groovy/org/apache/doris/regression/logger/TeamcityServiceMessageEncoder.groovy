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

package org.apache.doris.regression.logger

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.TeamcityUtils

class TeamcityServiceMessageEncoder extends PatternLayoutEncoder {
    static final ThreadLocal<SuiteContext> CURRENT_SUITE_CONTEXT = new ThreadLocal<>()

    private boolean enableStdErr

    TeamcityServiceMessageEncoder() {
        enableStdErr = Boolean.valueOf(System.getProperty("teamcity.enableStdErr", "true"))
        System.out.println("TeamcityServiceMessageEncoder: teamcity.enableStdErr=${enableStdErr}")
    }

    @Override
    byte[] encode(ILoggingEvent event) {
        String originLog = layout.doLayout(event)

        SuiteContext suiteContext = CURRENT_SUITE_CONTEXT.get()
        if (suiteContext == null) {
            return convertToBytes(originLog)
        }

        String serviceMessageLog
        if (event.getLevel().levelInt == Level.ERROR.levelInt && enableStdErr) {
            serviceMessageLog = TeamcityUtils.formatStdErr(suiteContext, originLog)
        } else {
            serviceMessageLog = TeamcityUtils.formatStdOut(suiteContext, originLog)
        }
        return convertToBytes(serviceMessageLog + "\n")
    }

    private byte[] convertToBytes(String s) {
        if (getCharset() == null) {
            return s.getBytes()
        } else {
            return s.getBytes(getCharset())
        }
    }
}
