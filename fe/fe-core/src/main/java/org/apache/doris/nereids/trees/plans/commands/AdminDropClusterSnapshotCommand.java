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
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '323741';
 */
public class AdminDropClusterSnapshotCommand extends Command implements ForwardWithSync {

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final Logger LOG = LogManager.getLogger(AdminDropClusterSnapshotCommand.class);

    private String key;
    private String value;

    /**
     * AdminDropClusterSnapshotCommand
     */
    public AdminDropClusterSnapshotCommand(String key, String value) {
        super(PlanType.ADMIN_DROP_CLUSTER_SNAPSHOT_COMMAND);
        this.key = key;
        this.value = value;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        dropSnapshot();
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode ");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (key == null || !key.equalsIgnoreCase(SNAPSHOT_ID)) {
            throw new AnalysisException("Where clause should be " + SNAPSHOT_ID + " = \"xxxx\"");
        }
        if (value == null || value.isEmpty()) {
            throw new AnalysisException(SNAPSHOT_ID + " value can not be empty");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminDropClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }

    private void dropSnapshot() throws DdlException {
        try {
            Cloud.DropSnapshotRequest request = Cloud.DropSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(value).build();
            Cloud.DropSnapshotResponse response = MetaServiceProxy.getInstance().dropSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("dropSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }
}
