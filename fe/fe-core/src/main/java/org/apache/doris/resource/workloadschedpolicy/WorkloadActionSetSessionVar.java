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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.SetExecutor;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class WorkloadActionSetSessionVar implements WorkloadAction {

    private static final Logger LOG = LogManager.getLogger(WorkloadActionSetSessionVar.class);

    private String varName;
    private String varValue;

    public WorkloadActionSetSessionVar(String varName, String varValue) {
        this.varName = varName;
        this.varValue = varValue;
    }

    @Override
    public void exec(WorkloadQueryInfo queryInfo) {
        try {
            List<SetVar> list = new ArrayList<>();
            SetVar sv = new SetVar(varName, new StringLiteral(varValue));
            list.add(sv);
            SetStmt setStmt = new SetStmt(list);
            SetExecutor executor = new SetExecutor(queryInfo.context, setStmt);
            executor.execute();
        } catch (Throwable t) {
            LOG.error("error happens when exec {}", WorkloadActionType.SET_SESSION_VARIABLE, t);
        }
    }

    @Override
    public WorkloadActionType getWorkloadActionType() {
        return WorkloadActionType.SET_SESSION_VARIABLE;
    }

    public String getVarName() {
        return varName;
    }

    public static WorkloadAction createWorkloadAction(String actionCmdArgs) throws UserException {
        String[] strs = actionCmdArgs.split("=");
        if (strs.length != 2 || StringUtils.isEmpty(strs[0].trim()) || StringUtils.isEmpty(strs[1].trim())) {
            throw new UserException("illegal arguments, it should be like set_session_variable \"xxx=xxx\"");
        }
        return new WorkloadActionSetSessionVar(strs[0].trim(), strs[1].trim());
    }
}
