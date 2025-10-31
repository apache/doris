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

package org.apache.doris.qe;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

/**
 * Used to generate new SessionVariables based on the persisted map,
 * and automatically restore to the previous SessionVariables after use.
 */
public class AutoCloseSessionVariable implements AutoCloseable {
    public ConnectContext connectContext;
    public SessionVariable sessionVariable;
    public boolean changed;

    private SessionVariable previousVariable;

    public AutoCloseSessionVariable() {
        this.changed = false;
    }

    public AutoCloseSessionVariable(ConnectContext connectContext, Map<String, String> affectQueryResultVariables) {
        Objects.requireNonNull(connectContext, "require connectContext object");
        this.changed = true;
        this.connectContext = connectContext;
        this.previousVariable = connectContext.getSessionVariable();
        sessionVariable = new SessionVariable();
        sessionVariable.setAffectQueryResultSessionVariables(
                affectQueryResultVariables == null ? Maps.newHashMap() : affectQueryResultVariables);
        connectContext.setSessionVariable(sessionVariable);
    }

    public void call() {
        // try (AutoSessionVariable autoCloseCtx = new AutoSessionVariable(context)) {
        // will report autoCloseCtx is not used, so call an empty method.
    }

    @Override
    public void close() {
        if (changed) {
            connectContext.setSessionVariable(previousVariable);
        }
    }
}
