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
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
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
 *  admin set replicas status properties ("key" = "val", ..);
 *  Required:
 *      "tablet_id" = "10010",
 *      "backend_id" = "10001"
 *      "status" = "drop"/"bad"/"ok"
 */
public class AdminSetReplicaStatusCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(AdminSetReplicaStatusCommand.class);
    private static final String TABLET_ID = "tablet_id";
    private static final String BACKEND_ID = "backend_id";
    private static final String STATUS = "status";

    private final Map<String, String> properties;
    private long tabletId = -1;
    private long backendId = -1;
    private Replica.ReplicaStatus status;

    public AdminSetReplicaStatusCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_SET_REPLICA_STATUS_COMMAND);
        Objects.requireNonNull(properties, "properties is null");
        this.properties = properties;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public Replica.ReplicaStatus getStatus() {
        return status;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        ctx.getEnv().setReplicaStatus(this);
    }

    public void validate() throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        checkProperties();
    }

    private void checkProperties() throws AnalysisException {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            if (key.equalsIgnoreCase(TABLET_ID)) {
                try {
                    tabletId = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid tablet id format: " + val);
                }
            } else if (key.equalsIgnoreCase(BACKEND_ID)) {
                try {
                    backendId = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid backend id format: " + val);
                }
            } else if (key.equalsIgnoreCase(STATUS)) {
                status = Replica.ReplicaStatus.valueOf(val.toUpperCase());
                if (status != Replica.ReplicaStatus.BAD && status != Replica.ReplicaStatus.OK
                        && status != Replica.ReplicaStatus.DROP) {
                    throw new AnalysisException("Do not support setting replica status as " + val);
                }
            } else {
                throw new AnalysisException("Unknown property: " + key);
            }
        }

        if (tabletId == -1 || backendId == -1 || status == null) {
            throw new AnalysisException("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        }
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminSetReplicaStatusCommand not supported in cloud mode");
        throw new DdlException("denied");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetReplicaStatusCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
