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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * UnSetVarOp
 */
public class UnsetVariableCommand extends Command implements Forward {
    private static final Logger LOG = LogManager.getLogger(UnsetVariableCommand.class);

    private SetType setType;

    // variable to restore
    private String variable = null;

    private boolean applyToAll = false;

    public UnsetVariableCommand(SetType setType, String varName) {
        super(PlanType.UNSET_VARIABLE_COMMAND);
        this.setType = setType;
        this.variable = varName;
    }

    public UnsetVariableCommand(SetType setType, boolean applyToAll) {
        super(PlanType.UNSET_VARIABLE_COMMAND);
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

    private void validate() throws UserException {
        if (StringUtils.isEmpty(variable) && !applyToAll) {
            throw new AnalysisException("You should specific the unset variable.");
        }

        if (setType == SetType.GLOBAL) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN");
            }
        }
    }

    /**
     * return sql expression of this command
     * @return string of this command
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("UNSET ");
        sb.append(setType).append(" VARIABLE ");
        if (!StringUtils.isEmpty(variable)) {
            sb.append(variable).append(" ");
        } else if (applyToAll) {
            sb.append("ALL");
        }
        return sb.toString();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        if (setType == SetType.GLOBAL) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public void afterForwardToMaster(ConnectContext context) throws Exception {
        if (isApplyToAll()) {
            VariableMgr.setAllVarsToDefaultValue(context.getSessionVariable(), SetType.SESSION);
        } else {
            String defaultValue = VariableMgr.getDefaultValue(getVariable());
            if (defaultValue == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, getVariable());
            }
            SetVar var = new SetVar(SetType.SESSION, getVariable(),
                    new StringLiteral(defaultValue), SetVar.SetVarType.SET_SESSION_VAR);
            VariableMgr.setVar(context.getSessionVariable(), var);
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        try {
            if (isApplyToAll()) {
                VariableMgr.setAllVarsToDefaultValue(ctx.getSessionVariable(), getSetType());
            } else {
                String defaultValue = VariableMgr.getDefaultValue(getVariable());
                if (defaultValue == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, getVariable());
                }
                SetVar var = new SetVar(getSetType(), getVariable(),
                        new StringLiteral(defaultValue), SetVar.SetVarType.SET_SESSION_VAR);
                VariableMgr.setVar(ctx.getSessionVariable(), var);
            }
        } catch (DdlException e) {
            LOG.warn("", e);
            // Return error message to client.
            ctx.getState().setError(ErrorCode.ERR_LOCAL_VARIABLE, e.getMessage() + toSql());
            return;
        }
        ctx.getState().setOk();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnsetVariableCommand(this, context);
    }
}
