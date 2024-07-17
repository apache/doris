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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

/**
 * show policy
 */
public class ShowPolicyCommand extends Command implements NoForward {

    private final PolicyTypeEnum policyType;

    private final UserIdentity user;

    private final String roleName;

    public ShowPolicyCommand(PolicyTypeEnum policyType, UserIdentity user, String roleName) {
        super(PlanType.SHOW_POLICY_COMMAND);
        this.policyType = policyType;
        this.user = user;
        this.roleName = roleName;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowPolicyCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (policyType == PolicyTypeEnum.DATA_MASK) {
            if (user != null) {
                user.analyze();
            }
            // check auth
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            ShowResultSet showResultSet = Env.getCurrentEnv().getPolicyMgr().showPolicy(user, roleName, policyType);
            executor.sendResultSet(showResultSet);
            return;
        }
        ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
        throw new AnalysisException("Not support show policy command in Nereids now");
    }
}
