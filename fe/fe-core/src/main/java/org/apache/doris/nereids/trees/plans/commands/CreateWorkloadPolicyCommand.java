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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadActionMeta;
import org.apache.doris.resource.workloadschedpolicy.WorkloadConditionMeta;

import java.util.List;
import java.util.Map;

/**
 * Command for CREATE WORKLOAD POLICY in Nereids.
 */
public class CreateWorkloadPolicyCommand extends Command implements ForwardWithSync {

    private final boolean ifNotExists;
    private final String policyName;
    private final List<WorkloadConditionMeta> conditions;
    private final List<WorkloadActionMeta> actions;
    private final Map<String, String> properties;

    /**
     * Constructor
     */
    public CreateWorkloadPolicyCommand(boolean ifNotExists, String policyName,
            List<WorkloadConditionMeta> conditions, List<WorkloadActionMeta> actions,
            Map<String, String> properties) {
        super(PlanType.CREATE_WORKLOAD_POLICY_COMMAND);
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.conditions = conditions;
        this.actions = actions;
        this.properties = properties;
    }

    private void validate(ConnectContext ctx) throws UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkWorkloadSchedPolicyName(policyName);

        if (conditions == null || conditions.size() < 1) {
            throw new DdlException("At least one condition needs to be specified");
        }

        if (actions == null || actions.size() < 1) {
            throw new DdlException("At least one action needs to be specified");
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws UserException {
        validate(ctx);

        Env.getCurrentEnv().getWorkloadSchedPolicyMgr().createWorkloadSchedPolicy(policyName,
                ifNotExists, conditions, actions, properties);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateWorkloadPolicyCommand(this, context);
    }

}
