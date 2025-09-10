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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * ADMIN SHOW [FULL] CLUSTER SNAPSHOT;
 */
public class AdminShowClusterSnapshotCommand extends ShowCommand {

    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("id").add("ancestor").add("create_at").add("finish_at")
            .add("image_url").add("journal_id").add("state").add("manual")
            .add("ttl").add("label").add("msg").add("count")
            .build();
    private static final Logger LOG = LogManager.getLogger(AdminShowClusterSnapshotCommand.class);

    private boolean full;

    /**
     * AdminShowClusterSnapshotCommand
     */
    public AdminShowClusterSnapshotCommand(boolean full) {
        super(PlanType.ADMIN_SHOW_CLUSTER_SNAPSHOT_COMMAND);
        this.full = full;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(-1)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<List<String>> rows = new ArrayList<>();
        Cloud.ListSnapshotResponse response = ((CloudEnv) Env.getCurrentEnv()).getCloudSnapshotHandler()
                .listSnapshot(full);
        for (Cloud.SnapshotInfoPB snapshot : response.getSnapshotsList()) {
            List<String> row = new ArrayList<>(TITLE_NAMES.size());
            row.add(snapshot.getSnapshotId());
            row.add(snapshot.getAncestorId());
            row.add(TimeUtils.longToTimeStringWithms(snapshot.getCreateAt()));
            row.add(TimeUtils.longToTimeStringWithms(snapshot.getFinishAt()));
            row.add(snapshot.getImageUrl());
            row.add(String.valueOf(snapshot.getJournalId()));
            row.add(snapshot.getStatus().toString());
            row.add(String.valueOf(snapshot.getAutoSnapshot()));
            row.add(String.valueOf(snapshot.getTtlSeconds()));
            row.add(snapshot.getSnapshotLabel());
            row.add(snapshot.getReason());
            // TODO
            row.add(String.valueOf(0));
            rows.add(row);
        }
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
        return visitor.visitAdminShowClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }

}
