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

package org.apache.doris.nereids.properties;

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.trees.plans.commands.info.SetSessionVarOp;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import java.util.ArrayList;
import java.util.List;

/**
 * select hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class SelectHintSetVar extends Hint {
    // e.g. query_timeout='1800', exec_mem_limit='2147483648'
    private final List<SetSessionVarOp> setSessionVarOpList;

    private List<HintStatus> hintStatusList;

    private List<String> errorMsgList;

    public SelectHintSetVar(String hintName, List<SetSessionVarOp> setSessionVarOpList) {
        super(hintName);
        this.setSessionVarOpList = setSessionVarOpList;
        hintStatusList = new ArrayList<>(setSessionVarOpList.size());
        errorMsgList = new ArrayList<>(setSessionVarOpList.size());
    }

    /**
     * set temporary session value, and then revert value in the 'finally block' of StmtExecutor#execute by
     * revertSessionValue
     * @param context statement context
     */
    public void setVarOnceInSql(StatementContext context) {
        for (int i = 0; i < setSessionVarOpList.size(); i++) {
            SetSessionVarOp op = setSessionVarOpList.get(i);
            try {
                op.validate(context.getConnectContext());
            } catch (UserException e) {
                hintStatusList.add(HintStatus.SYNTAX_ERROR);
                errorMsgList.add(e.getMessage());
                continue;
            }
            SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
            sessionVariable.setIsSingleSetVar(true);
            try {
                VariableMgr.setVar(sessionVariable, op.translateToLegacyVar(context.getConnectContext()));
                context.invalidCache(op.getName());
            } catch (Throwable t) {
                hintStatusList.add(HintStatus.SYNTAX_ERROR);
                errorMsgList.add("Can not set session variable '"
                        + op.getName() + "' = '" + op.getValue().toString() + "'");
                continue;
            }
            hintStatusList.add(HintStatus.SUCCESS);
            errorMsgList.add("");
            setStatus(HintStatus.SUCCESS);
        }
    }

    @Override
    public String getExplainString() {
        StringBuilder explain = new StringBuilder();
        explain.append("SetVar(");
        for (int i = 0; i < setSessionVarOpList.size(); i++) {
            explain.append(setSessionVarOpList.get(i).toSql());
            explain.append("-");
            explain.append(hintStatusList.get(i).toString());
            explain.append(errorMsgList.get(i));
            if (i != setSessionVarOpList.size() - 1) {
                explain.append(", ");
            }
        }
        explain.append(")");
        return explain.toString();
    }
}
