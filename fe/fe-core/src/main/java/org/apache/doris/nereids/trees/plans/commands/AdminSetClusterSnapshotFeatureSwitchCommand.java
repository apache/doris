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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.snapshot.CloudSnapshotHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ADMIN SET CLUSTER SNAPSHOT FEATURE (ON|OFF);
 */
public class AdminSetClusterSnapshotFeatureSwitchCommand extends Command implements ForwardWithSync {

    private static final Logger LOG = LogManager.getLogger(AdminSetClusterSnapshotFeatureSwitchCommand.class);

    private boolean on;

    /**
     * AdminSetClusterSnapshotFeatureSwitchCommand
     */
    public AdminSetClusterSnapshotFeatureSwitchCommand(boolean on) {
        super(PlanType.ADMIN_SET_CLUSTER_SNAPSHOT_FEATURE_SWITCH_COMMAND);
        this.on = on;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Cloud.AlterInstanceRequest.Builder builder = Cloud.AlterInstanceRequest.newBuilder()
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .setInstanceId(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())
                .setOp(Cloud.AlterInstanceRequest.Operation.SET_SNAPSHOT_PROPERTY)
                .putProperties(Cloud.AlterInstanceRequest.SnapshotProperty.ENABLE_SNAPSHOT.name(), String.valueOf(on));
        CloudSnapshotHandler cloudSnapshotHandler = ((CloudEnv) ctx.getEnv()).getCloudSnapshotHandler();
        cloudSnapshotHandler.alterInstance(builder.build());
        cloudSnapshotHandler.refreshAutoSnapshotJob();
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode ");
        }
        // Check privilege based on configuration
        if ("admin".equalsIgnoreCase(Config.cluster_snapshot_min_privilege)) {
            // When configured as admin, check ADMIN privilege
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        PrivPredicate.ADMIN.getPrivs().toString());
            }
        } else {
            // Default or configured as root, check if user is root
            UserIdentity currentUser = ctx.getCurrentUserIdentity();
            if (currentUser == null || !currentUser.isRootUser()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "root privilege");
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetClusterSnapshotFeatureSwitchCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
