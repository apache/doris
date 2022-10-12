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

package org.apache.doris.regression.suite.event

import com.google.common.base.Throwables
import groovy.transform.CompileStatic
import org.apache.doris.regression.logger.TeamcityServiceMessageEncoder
import org.apache.doris.regression.suite.ScriptContext
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.LoggerUtils
import org.apache.doris.regression.util.TeamcityUtils

@CompileStatic
class TeamcityEventListener implements EventListener {
    @Override
    void onScriptStarted(ScriptContext scriptContext) {
        // do nothing
    }

    @Override
    void onScriptFailed(ScriptContext scriptContext, Throwable t) {
        // do nothing
    }

    @Override
    void onScriptFinished(ScriptContext scriptContext, long elapsed) {
        // do nothing
    }

    @Override
    void onSuiteStarted(SuiteContext suiteContext) {
        TeamcityServiceMessageEncoder.CURRENT_SUITE_CONTEXT.set(suiteContext)
        TeamcityUtils.testStarted(suiteContext)
    }

    @Override
    void onSuiteFailed(SuiteContext suiteContext, Throwable t) {
        def (Integer errorLine, String errorInfo) = LoggerUtils.getErrorInfo(t, suiteContext.scriptContext.file)
        String errorMsg = errorInfo == null
                ? "Exception in ${suiteContext.scriptContext.name}:"
                : "Exception in ${suiteContext.scriptContext.name}(line ${errorLine}):\n\n${errorInfo}\n\nException:"
        def stackTrace = Throwables.getStackTraceAsString(t)
        TeamcityUtils.testFailed(suiteContext, errorMsg, stackTrace)
    }

    @Override
    void onSuiteFinished(SuiteContext suiteContext, boolean success, long elapsed) {
        TeamcityServiceMessageEncoder.CURRENT_SUITE_CONTEXT.remove()
        TeamcityUtils.testFinished(suiteContext, elapsed)
    }

    @Override
    void onThreadStarted(SuiteContext suiteContext) {
        TeamcityServiceMessageEncoder.CURRENT_SUITE_CONTEXT.set(suiteContext)
    }

    @Override
    void onThreadFailed(SuiteContext suiteContext, Throwable t) {
        // do nothing
    }

    @Override
    void onThreadFinished(SuiteContext suiteContext, long elapsed) {
        TeamcityServiceMessageEncoder.CURRENT_SUITE_CONTEXT.remove()
    }
}
