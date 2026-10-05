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

import groovy.transform.CompileStatic
import org.apache.doris.regression.suite.ScriptContext
import org.apache.doris.regression.suite.ScriptInfo
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.suite.SuiteInfo
import org.apache.doris.regression.util.Recorder

@CompileStatic
class RecorderEventListener implements EventListener {
    private final Recorder recorder

    RecorderEventListener(Recorder recorder) {
        this.recorder = recorder
    }

    @Override
    void onScriptStarted(ScriptContext scriptContext) {

    }

    @Override
    void onScriptFailed(ScriptContext scriptContext, Throwable t) {
        recorder.onFatal(new ScriptInfo(scriptContext.file))
    }

    @Override
    void onScriptFinished(ScriptContext scriptContext, long elapsed) {

    }

    @Override
    void onSuiteStarted(SuiteContext suiteContext) {

    }

    @Override
    void onSuiteFailed(SuiteContext suiteContext, Throwable t) {
        recorder.onFailure(new SuiteInfo(suiteContext.scriptContext.file, suiteContext.group, suiteContext.suiteName))
    }

    @Override
    void onSuiteFinished(SuiteContext suiteContext, boolean success, long elapsed) {
        if (success) {
            recorder.onSuccess(new SuiteInfo(suiteContext.scriptContext.file, suiteContext.group, suiteContext.suiteName))
        }
    }

    @Override
    void onThreadStarted(SuiteContext suiteContext) {

    }

    @Override
    void onThreadFailed(SuiteContext suiteContext, Throwable t) {

    }

    @Override
    void onThreadFinished(SuiteContext suiteContext, long elapsed) {

    }
}
