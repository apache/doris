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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.snapshot.CloudSnapshotHandler;
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

import java.util.Map;
import java.util.Objects;

/**
 * ADMIN SET CLUSTER SNAPSHOT PROPERTIES('enabled'='true', 'max_reserved_snapshots'='10',
 * 'snapshot_interval_seconds'='3600');
 */
public class AdminSetClusterSnapshotCommand extends Command implements ForwardWithSync {

    public static final String PROP_ENABLED = "enabled";
    public static final String PROP_MAX_RESERVED_SNAPSHOTS = "max_reserved_snapshots";
    public static final String PROP_SNAPSHOT_INTERVAL_SECONDS = "snapshot_interval_seconds";
    private static final Logger LOG = LogManager.getLogger(AdminSetClusterSnapshotCommand.class);

    private Map<String, String> properties;
    private boolean enabled;
    private long maxReservedSnapshots;
    private long snapshotIntervalSeconds;

    /**
     * AdminSetClusterSnapshotCommand
     */
    public AdminSetClusterSnapshotCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_SET_CLUSTER_SNAPSHOT_COMMAND);
        Objects.requireNonNull(properties, "properties is null");
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        Cloud.AlterInstanceRequest.Builder builder = Cloud.AlterInstanceRequest.newBuilder()
                .setInstanceId(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())
                .setOp(Cloud.AlterInstanceRequest.Operation.SET_SNAPSHOT_PROPERTY);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            builder.putProperties(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
        }
        alterInstance(builder.build());

        CloudSnapshotHandler cloudSnapshotHandler = ((CloudEnv) ctx.getEnv()).getCloudSnapshotHandler();
        cloudSnapshotHandler.refreshAutoSnapshotJob();
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

        if (properties.isEmpty()) {
            throw new AnalysisException("No properties to set");
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            try {
                if (entry.getKey().equalsIgnoreCase(PROP_ENABLED)) {
                    enabled = Boolean.valueOf(entry.getValue());
                } else if (entry.getKey().equalsIgnoreCase(PROP_MAX_RESERVED_SNAPSHOTS)) {
                    maxReservedSnapshots = Long.valueOf(entry.getValue());
                    if (maxReservedSnapshots < 0 || maxReservedSnapshots > 35) {
                        throw new AnalysisException("property: " + entry.getKey() + " value should in [0-35]");
                    }
                } else if (entry.getKey().equalsIgnoreCase(PROP_SNAPSHOT_INTERVAL_SECONDS)) {
                    snapshotIntervalSeconds = Long.valueOf(entry.getValue());
                    if (snapshotIntervalSeconds < 3600) {
                        throw new AnalysisException("property: " + entry.getKey() + " value minimum is 3600 seconds");
                    }
                } else {
                    throw new AnalysisException("Unknown property: " + entry.getKey());
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid property: " + entry.getKey());
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }

    private void alterInstance(Cloud.AlterInstanceRequest request) throws DdlException {
        try {
            Cloud.AlterInstanceResponse response = MetaServiceProxy.getInstance().alterInstance(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("alterInstance response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }
}
