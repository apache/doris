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

import java.util.function.Consumer

@CompileStatic
class StackEventListeners implements EventListener {
    private final Stack<EventListener> listeners = new Stack<>()

    synchronized void addListener(EventListener eventListener) {
        this.listeners.push(eventListener)
    }

    synchronized void foreach(Consumer<EventListener> func) {
        for (int i = listeners.size() - 1; i >= 0; i--) {
            func.accept(listeners.get(i))
        }
    }

    @Override
    synchronized void onScriptStarted(ScriptContext scriptContext) {
        foreach {
            it.onScriptStarted(scriptContext)
        }
    }

    @Override
    synchronized void onScriptFailed(ScriptContext scriptContext, Throwable t) {
        foreach {
            it.onScriptFailed(scriptContext, t)
        }
    }

    @Override
    synchronized void onScriptFinished(ScriptContext scriptContext, long elapsed) {
        foreach {
            it.onScriptFinished(scriptContext, elapsed)
        }
    }

    @Override
    synchronized void onSuiteStarted(SuiteContext suiteContext) {
        foreach {
            it.onSuiteStarted(suiteContext)
        }
    }

    @Override
    synchronized void onSuiteFailed(SuiteContext suiteContext, Throwable t) {
        foreach {
            it.onSuiteFailed(suiteContext, t)
        }
    }

    @Override
    synchronized void onSuiteFinished(SuiteContext suiteContext, boolean success, long elapsed) {
        foreach {
            it.onSuiteFinished(suiteContext, success, elapsed)
        }
    }

    @Override
    synchronized void onThreadStarted(SuiteContext suiteContext) {
        foreach {
            it.onThreadStarted(suiteContext)
        }
    }

    @Override
    synchronized void onThreadFailed(SuiteContext suiteContext, Throwable t) {
        foreach {
            it.onThreadFailed(suiteContext, t)
        }
    }

    @Override
    synchronized void onThreadFinished(SuiteContext suiteContext, long elapsed) {
        foreach {
            it.onThreadFinished(suiteContext, elapsed)
        }
    }
}
