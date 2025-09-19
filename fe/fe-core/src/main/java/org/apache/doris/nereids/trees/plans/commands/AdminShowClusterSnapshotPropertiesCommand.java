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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.SnapshotSwitchStatus;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * ADMIN SHOW CLUSTER SNAPSHOT PROPERTIES;
 */
public class AdminShowClusterSnapshotPropertiesCommand extends ShowCommand {

    public static final String PROP_READY = "ready";
    private static final Logger LOG = LogManager.getLogger(AdminShowClusterSnapshotPropertiesCommand.class);

    /**
     * AdminShowClusterSnapshotPropertiesCommand
     */
    public AdminShowClusterSnapshotPropertiesCommand() {
        super(PlanType.ADMIN_SHOW_CLUSTER_SNAPSHOT_PROPERTIES_COMMAND);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        String[] columnNames = new String[] {PROP_READY,
                AdminSetClusterSnapshotCommand.PROP_ENABLED,
                AdminSetClusterSnapshotCommand.PROP_MAX_RESERVED_SNAPSHOTS,
                AdminSetClusterSnapshotCommand.PROP_SNAPSHOT_INTERVAL_SECONDS};
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String columnName : columnNames) {
            builder.addColumn(new Column(columnName, ScalarType.createVarchar(-1)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<String> row = new ArrayList<>();
        Cloud.GetInstanceResponse response = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudInstance();
        Cloud.SnapshotSwitchStatus switchStatus = response.getInstance().getSnapshotSwitchStatus();
        row.add(String.valueOf(switchStatus != SnapshotSwitchStatus.SNAPSHOT_SWITCH_DISABLED));
        row.add(String.valueOf(switchStatus == SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON));
        row.add(String.valueOf(response.getInstance().getMaxReservedSnapshot()));
        row.add(String.valueOf(response.getInstance().getSnapshotIntervalSeconds()));
        List<List<String>> rows = new ArrayList<>();
        rows.add(row);
        return new ShowResultSet(getMetaData(), rows);
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
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminShowClusterSnapshotPropertiesCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
