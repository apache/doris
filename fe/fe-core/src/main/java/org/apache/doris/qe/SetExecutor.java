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

import org.apache.doris.analysis.SetLdapPassVar;
import org.apache.doris.analysis.SetNamesVar;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetTransaction;
import org.apache.doris.analysis.SetUserDefinedVar;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.common.DdlException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Set executor
public class SetExecutor {
    private static final Logger LOG = LogManager.getLogger(SetExecutor.class);

    private ConnectContext ctx;
    private SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariable(SetVar var) throws DdlException {
        if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            ctx.getEnv().getAuth().setPassword(setPassVar);
        } else if (var instanceof SetLdapPassVar) {
            SetLdapPassVar setLdapPassVar = (SetLdapPassVar) var;
            ctx.getEnv().getAuth().setLdapPassword(setLdapPassVar);
        } else if (var instanceof SetNamesVar) {
            // do nothing
            return;
        } else if (var instanceof SetTransaction) {
            // do nothing
            return;
        } else if (var instanceof SetUserDefinedVar) {
            ConnectContext.get().setUserVar(var);
        } else {
            VariableMgr.setVar(ctx.getSessionVariable(), var);
        }
    }

    public void execute() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            setVariable(var);
        }
    }
}
