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
 * ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_snapshot');
 */
public class AdminCreateClusterSnapshotCommand extends Command implements ForwardWithSync {

    public static final String PROP_TTL = "ttl";
    public static final String PROP_LABEL = "label";
    private static final Logger LOG = LogManager.getLogger(AdminCreateClusterSnapshotCommand.class);

    private Map<String, String> properties;
    private long ttl;
    private String label = null;

    /**
     * AdminCreateClusterSnapshotCommand
     */
    public AdminCreateClusterSnapshotCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_CREATE_CLUSTER_SNAPSHOT_COMMAND);
        Objects.requireNonNull(properties, "properties is null");
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        CloudSnapshotHandler cloudSnapshotHandler = ((CloudEnv) ctx.getEnv()).getCloudSnapshotHandler();
        cloudSnapshotHandler.submitJob(ttl, label);
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

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(PROP_TTL)) {
                try {
                    ttl = Long.valueOf(entry.getValue());
                } catch (NumberFormatException e) {
                    throw new AnalysisException(
                            "Invalid value: " + entry.getValue() + " of property: " + entry.getKey());
                }
                if (ttl <= 0) {
                    throw new AnalysisException("Property 'ttl' must be positive: " + entry.getValue());
                }
            } else if (entry.getKey().equalsIgnoreCase(PROP_LABEL)) {
                label = entry.getValue();
                if (label == null || label.isEmpty()) {
                    throw new AnalysisException("Property 'label' cannot be empty");
                }
            } else {
                throw new AnalysisException("Unknown property: " + entry.getKey());
            }
        }

        if (ttl <= 0) {
            throw new AnalysisException("Property 'ttl' is required");
        }
        if (label == null || label.isEmpty()) {
            throw new AnalysisException("Property 'label' is required");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCreateClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
