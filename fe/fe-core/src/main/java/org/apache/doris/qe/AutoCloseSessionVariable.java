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

import java.util.Map;

/**
 * Used to generate new SessionVariables based on the persisted map,
 * and automatically restore to the previous SessionVariables after use.
 */
public class AutoCloseSessionVariable implements AutoCloseable {
    private static final ConnectContext notNullContext = new ConnectContext();
    private ConnectContext currentConnectContext;
    private boolean changed = false;
    private SessionVariable previousVariable;
    private ConnectContext previousConnectContext = notNullContext;

    /** if argument connectContext is not null, must be same with value of ConnectContext.get();
     * if argument connectContext is null, value of ConnectContext.get() must be null*/
    public AutoCloseSessionVariable(ConnectContext connectContext, Map<String, String> affectQueryResultVariables) {
        if (affectQueryResultVariables == null || affectQueryResultVariables.isEmpty()) {
            return;
        }
        if (connectContext == null) {
            previousConnectContext = null;
            currentConnectContext = new ConnectContext();
            currentConnectContext.setThreadLocalInfo();
        } else {
            changed = true;
            currentConnectContext = connectContext;
            previousVariable = connectContext.getSessionVariable();
        }
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setAffectQueryResultInPlanSessionVariables(affectQueryResultVariables);
        currentConnectContext.setSessionVariable(sessionVariable);
    }

    @Override
    public void close() {
        if (previousConnectContext == null) {
            ConnectContext.remove();
            return;
        }
        if (changed) {
            currentConnectContext.setSessionVariable(previousVariable);
        }
    }
}
