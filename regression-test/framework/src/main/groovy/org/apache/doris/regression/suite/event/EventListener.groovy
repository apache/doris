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
import org.apache.doris.regression.suite.SuiteContext

@CompileStatic
interface EventListener {
    void onScriptStarted(ScriptContext scriptContext)
    void onScriptFailed(ScriptContext scriptContext, Throwable t)
    void onScriptFinished(ScriptContext scriptContext, long elapsed)
    void onSuiteStarted(SuiteContext suiteContext)
    void onSuiteFailed(SuiteContext suiteContext, Throwable t)
    void onSuiteFinished(SuiteContext suiteContext, boolean success, long elapsed)
    void onThreadStarted(SuiteContext suiteContext)
    void onThreadFailed(SuiteContext suiteContext, Throwable t)
    void onThreadFinished(SuiteContext suiteContext, long elapsed)
}
