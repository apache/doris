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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;

/**
 * ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('max_reserved_snapshots'='10', 'snapshot_interval_seconds'='3600');
 */
public class AdminSetAutoClusterSnapshotCommand extends Command implements ForwardWithSync {

    public static final String PROP_MAX_RESERVED_SNAPSHOTS = "max_reserved_snapshots";
    public static final String PROP_SNAPSHOT_INTERVAL_SECONDS = "snapshot_interval_seconds";
    private static final Logger LOG = LogManager.getLogger(AdminSetAutoClusterSnapshotCommand.class);

    private Map<String, String> properties;
    private long maxReservedSnapshots;
    private long snapshotIntervalSeconds;

    /**
     * AdminSetAutoClusterSnapshotCommand
     */
    public AdminSetAutoClusterSnapshotCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_SET_AUTO_CLUSTER_SNAPSHOT_COMMAND);
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
            String property;
            if (entry.getKey().equalsIgnoreCase(PROP_MAX_RESERVED_SNAPSHOTS)) {
                property = Cloud.AlterInstanceRequest.SnapshotProperty.MAX_RESERVED_SNAPSHOTS.name();
            } else if (entry.getKey().equalsIgnoreCase(PROP_SNAPSHOT_INTERVAL_SECONDS)) {
                property = Cloud.AlterInstanceRequest.SnapshotProperty.SNAPSHOT_INTERVAL_SECONDS.name();
            } else {
                throw new RuntimeException("Unknown property: " + entry.getKey());
            }
            builder.putProperties(property, entry.getValue().toLowerCase());
        }
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
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (properties.isEmpty()) {
            throw new AnalysisException("No properties to set");
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            try {
                if (entry.getKey().equalsIgnoreCase(PROP_MAX_RESERVED_SNAPSHOTS)) {
                    maxReservedSnapshots = Long.valueOf(entry.getValue());
                    if (maxReservedSnapshots < 0
                            || maxReservedSnapshots > Config.cloud_auto_snapshot_max_reversed_num) {
                        throw new AnalysisException("property: " + entry.getKey() + " value should in [0-"
                                + Config.cloud_auto_snapshot_max_reversed_num + "]");
                    }
                } else if (entry.getKey().equalsIgnoreCase(PROP_SNAPSHOT_INTERVAL_SECONDS)) {
                    snapshotIntervalSeconds = Long.valueOf(entry.getValue());
                    if (snapshotIntervalSeconds < Config.cloud_auto_snapshot_min_interval_seconds) {
                        throw new AnalysisException("property: " + entry.getKey() + " value minimum is "
                                + Config.cloud_auto_snapshot_min_interval_seconds + " seconds");
                    }
                } else {
                    throw new AnalysisException("Unknown property: " + entry.getKey());
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid value: " + entry.getValue() + " of property: " + entry.getKey());
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetAutoClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
