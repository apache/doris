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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * alter workload group command
 */
public class AlterWorkloadGroupCommand extends AlterCommand {
    private final String workloadGroupName;
    private final Map<String, String> properties;

    /**
     * constructor
     */
    public AlterWorkloadGroupCommand(String workloadGroupName, Map<String, String> properties) {
        super(PlanType.ALTER_WORKLOAD_GROUP_COMMAND);
        this.workloadGroupName = workloadGroupName;
        this.properties = properties;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Workload Group properties can't be empty");
        }

        if (properties.containsKey(WorkloadGroup.INTERNAL_TYPE)) {
            throw new AnalysisException(WorkloadGroup.INTERNAL_TYPE + " can not be create or modified ");
        }
        String tagStr = properties.get(WorkloadGroup.TAG);
        if (!StringUtils.isEmpty(tagStr) && (WorkloadGroupMgr.DEFAULT_GROUP_NAME.equals(workloadGroupName)
                || WorkloadGroupMgr.INTERNAL_GROUP_NAME.equals(workloadGroupName))) {
            throw new AnalysisException(
                    WorkloadGroupMgr.INTERNAL_GROUP_NAME + " and " + WorkloadGroupMgr.DEFAULT_GROUP_NAME
                            + " group can not set tag");
        }

        Env.getCurrentEnv().getWorkloadGroupMgr().alterWorkloadGroup(workloadGroupName, properties);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterWorkloadGroupCommand(this, context);
    }
}
