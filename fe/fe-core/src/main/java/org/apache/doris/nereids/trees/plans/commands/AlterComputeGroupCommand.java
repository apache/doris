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
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

//import org.apache.commons.lang3.StringUtils;
import java.util.Map;

/**
 * alter compute group command
 */
public class AlterComputeGroupCommand extends AlterCommand {
    private final String computeGroupName;
    private final Map<String, String> properties;

    /**
     * constructor
     */
    public AlterComputeGroupCommand(String computeGroupName, Map<String, String> properties) {
        super(PlanType.ALTER_COMPUTE_GROUP_COMMAND);
        this.computeGroupName = computeGroupName;
        this.properties = properties;
    }

    /**
     * validate
     */
    public void validate(ConnectContext connectContext) throws UserException {
        if (Config.isNotCloudMode()) {
            throw new AnalysisException("Currently, Alter compute group is only supported in cloud mode");
        }
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Compute Group properties can't be empty");
        }

        CloudSystemInfoService cloudSys = ((CloudSystemInfoService) Env.getCurrentSystemInfo());
        // check compute group exist
        ComputeGroup cg = cloudSys.getComputeGroupByName(computeGroupName);
        if (cg == null) {
            throw new AnalysisException("Compute Group " + computeGroupName + " does not exist");
        }

        if (cg.isVirtual()) {
            throw new AnalysisException("Virtual Compute Group " + computeGroupName + " can not be altered");
        }

        // check compute group's properties can be modified
        cg.checkProperties(properties);

        cg.modifyProperties(properties);
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        CloudSystemInfoService cloudSys = ((CloudSystemInfoService) Env.getCurrentSystemInfo());
        // send rpc to ms
        cloudSys.alterComputeGroupProperties(computeGroupName, properties);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterComputeGroupCommand(this, context);
    }
}
