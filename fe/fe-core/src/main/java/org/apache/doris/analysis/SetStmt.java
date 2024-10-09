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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import java.util.List;

// Set variables statement. Now only support simple string
public class SetStmt extends StatementBase implements NotFallbackInParser {
    // variables to modify
    private final List<SetVar> setVars;

    public SetStmt(List<SetVar> setVars) {
        this.setVars = setVars;
    }

    public List<SetVar> getSetVars() {
        return setVars;
    }

    // remove setvar of non-set-session-var,
    // change type global to session avoid to write in non-master node.
    public void modifySetVarsForExecute() {
        setVars.removeIf(setVar -> setVar.getVarType() != SetVar.SetVarType.SET_SESSION_VAR);
        for (SetVar var : setVars) {
            if (var.getType() == SetType.GLOBAL) {
                var.setType(SetType.SESSION);
            }
        }
    }

    @Override
    public boolean needAuditEncryption() {
        for (SetVar var : setVars) {
            if (var instanceof SetPassVar) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (setVars == null || setVars.isEmpty()) {
            throw new AnalysisException("Empty set statement.");
        }
        for (SetVar var : setVars) {
            var.analyze(analyzer);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("SET ");
        int idx = 0;
        for (SetVar var : setVars) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(var.toSql());
            idx++;
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SET;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (setVars != null) {
            for (SetVar var : setVars) {
                if (var instanceof SetPassVar || var instanceof SetLdapPassVar) {
                    return RedirectStatus.FORWARD_WITH_SYNC;
                } else if (var.getType() == SetType.GLOBAL) {
                    return RedirectStatus.FORWARD_WITH_SYNC;
                }
            }
        }
        return RedirectStatus.NO_FORWARD;
    }
}
