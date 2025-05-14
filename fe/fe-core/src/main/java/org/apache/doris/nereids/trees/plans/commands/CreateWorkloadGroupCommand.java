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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
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
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Create workload group command
 */
public class CreateWorkloadGroupCommand extends Command implements ForwardWithSync {
    private final boolean ifNotExists;
    private final String workloadGroupName;
    private final Map<String, String> properties;
    private String computeGroup;

    /**
     * Constructor for CreateWorkloadGroupCommand
     */
    public CreateWorkloadGroupCommand(String computeGroup, String workloadGroupName, boolean ifNotExists,
            Map<String, String> properties) {
        super(PlanType.CREATE_WORKLOAD_GROUP_COMMAND);

        this.workloadGroupName = workloadGroupName;
        this.ifNotExists = ifNotExists;
        this.properties = properties;
        this.computeGroup = computeGroup;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkWorkloadGroupName(workloadGroupName);

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Workload Group properties can't be empty");
        }

        String tagStr = properties.get(WorkloadGroup.TAG);
        if (!StringUtils.isEmpty(tagStr)) {
            throw new AnalysisException(
                    "tag is deprecated, you can use create workload group [for compute group] as a replacement.");
        }

        String cgStr = properties.get(WorkloadGroup.COMPUTE_GROUP);
        if (!StringUtils.isEmpty(cgStr)) {
            throw new AnalysisException(WorkloadGroup.COMPUTE_GROUP + " can not be set in property.");
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        if (StringUtils.isEmpty(computeGroup)) {
            computeGroup = Config.isCloudMode() ? Tag.VALUE_DEFAULT_COMPUTE_GROUP_NAME
                    : Tag.DEFAULT_BACKEND_TAG.value;
        } else if (Config.isNotCloudMode()) {
            try {
                FeNameFormat.checkCommonName("Workload group's compute group", computeGroup);
            } catch (AnalysisException e) {
                throw new DdlException("Compute group's format is illegal: " + computeGroup);
            }
        }

        if (Config.isCloudMode()) {
            String originStr = computeGroup;
            computeGroup = ((CloudSystemInfoService) Env.getCurrentEnv().getClusterInfo()).getCloudClusterIdByName(
                    computeGroup);
            if (StringUtils.isEmpty(computeGroup)) {
                throw new UserException("Can not find compute group " + originStr + ".");
            }
        }

        // Create workload group
        properties.put(WorkloadGroup.COMPUTE_GROUP, computeGroup);
        WorkloadGroup workloadGroup = WorkloadGroup.create(workloadGroupName, properties);
        WorkloadGroupMgr workloadGroupMgr = Env.getCurrentEnv().getWorkloadGroupMgr();
        workloadGroupMgr.createWorkloadGroup(computeGroup, workloadGroup, ifNotExists);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateWorkloadGroupCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
