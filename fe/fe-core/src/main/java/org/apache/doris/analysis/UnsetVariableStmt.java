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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.amazonaws.util.StringUtils;

// Unset variables statement
public class UnsetVariableStmt extends StatementBase {
    private SetType setType;

    // variables to restore
    private String variable = null;

    private boolean applyToAll = false;

    public UnsetVariableStmt(SetType setType, String varName) {
        this.setType = setType;
        this.variable = varName;
    }

    public UnsetVariableStmt(SetType setType, boolean applyToAll) {
        this.setType = setType;
        this.applyToAll = applyToAll;
    }

    public SetType getSetType() {
        return setType;
    }

    public String getVariable() {
        return variable;
    }

    public boolean isApplyToAll() {
        return applyToAll;
    }

    // change type global to session avoid to write in non-master node.
    public void modifySetVarsForExecute() {
        if (setType == SetType.GLOBAL) {
            setType = SetType.SESSION;
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (StringUtils.isNullOrEmpty(variable) && !applyToAll) {
            throw new AnalysisException("You should specific the unset variable.");
        }

        if (setType == SetType.GLOBAL) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("UNSET ");
        sb.append(setType).append(" VARIABLE ");
        if (!StringUtils.isNullOrEmpty(variable)) {
            sb.append(variable).append(" ");
        } else if (applyToAll) {
            sb.append("ALL");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (setType == SetType.GLOBAL) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.UNSET;
    }
}
