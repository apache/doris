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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.workloadgroup.WorkloadGroupKey;

import org.apache.commons.lang3.StringUtils;

/**
 * drop workload group command
 */
public class DropWorkloadGroupCommand extends DropCommand {
    private final boolean ifExists;
    private final String workloadGroupName;
    private String computeGroup;

    /**
     * constructor
     */
    public DropWorkloadGroupCommand(String computeGroupName, String workloadGroupName, boolean ifExists) {
        super(PlanType.DROP_WORKLOAD_GROUP_NAME);

        this.workloadGroupName = workloadGroupName;
        this.computeGroup = computeGroupName;
        this.ifExists = ifExists;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (Config.isCloudMode()) {
            String originCgStr = computeGroup;
            if (StringUtils.isEmpty(computeGroup)) {
                computeGroup = Tag.VALUE_DEFAULT_COMPUTE_GROUP_NAME;
            }
            String clusterId = ((CloudSystemInfoService) Env.getCurrentEnv().getClusterInfo()).getCloudClusterIdByName(
                    computeGroup);
            // there are two cases can not find a cluster_id:
            // 1. user specify an error cluster name, this case should throw exception.
            // 2. user specify a valid cluster_id but which has been dropped, this case should drop workload group.
            if (StringUtils.isEmpty(clusterId)) {
                WorkloadGroupKey wgKey = WorkloadGroupKey.get(computeGroup, workloadGroupName);
                if (!Env.getCurrentEnv().getWorkloadGroupMgr().isWorkloadGroupExists(wgKey)) {
                    throw new UserException(
                            "Can not find workload group  " + workloadGroupName + " in compute group " + originCgStr
                                    + ".");
                } // else case 2, input computeGroup is already a valid cluster id which has been dropped, so drop wg.
            } else {
                computeGroup = clusterId;
            }
            Env.getCurrentEnv().getWorkloadGroupMgr().dropWorkloadGroup(computeGroup, workloadGroupName, ifExists);
        } else {
            if (StringUtils.isEmpty(computeGroup)) {
                computeGroup = Tag.VALUE_DEFAULT_TAG;
            }
            Env.getCurrentEnv().getWorkloadGroupMgr().dropWorkloadGroup(computeGroup, workloadGroupName, ifExists);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropWorkloadGroupCommand(this, context);
    }
}
