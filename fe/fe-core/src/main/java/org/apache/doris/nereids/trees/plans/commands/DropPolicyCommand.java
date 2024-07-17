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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * drop policy command
 */
public class DropPolicyCommand extends Command implements ForwardWithSync {

    private final PolicyTypeEnum policyType;
    private final String policyName;
    private final boolean ifExists;

    /**
     * constructor for drop command
     */
    public DropPolicyCommand(PolicyTypeEnum policyType, String policyName, boolean ifExists) {
        super(PlanType.DROP_POLICY_COMMAND);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifExists = ifExists;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropPolicyCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (policyType == PolicyTypeEnum.DATA_MASK) {
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                     .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        PrivPredicate.GRANT.getPrivs().toString());
            }
            DropPolicyLog dropPolicyLog = new DropPolicyLog(policyType, policyName);
            Env.getCurrentEnv().getPolicyMgr().dropPolicy(dropPolicyLog, ifExists);
            return;
        }
        ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
        throw new AnalysisException("Not support drop policy command in Nereids now");
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
