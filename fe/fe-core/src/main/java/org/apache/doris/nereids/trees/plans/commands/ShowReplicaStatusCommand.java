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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MetadataViewer;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * ShowReplicaStatusCommand
 */
public class ShowReplicaStatusCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("Version").add("LastFailedVersion")
            .add("LastSuccessVersion").add("CommittedVersion").add("SchemaHash").add("VersionNum")
            .add("IsBad").add("IsUserDrop").add("State").add("Status")
            .build();

    private final TableRefInfo tableRefInfo;
    private Expression where;
    private List<String> partitions = Lists.newArrayList();
    private ReplicaStatus statusFilter;

    public ShowReplicaStatusCommand(TableRefInfo tableRefInfo, Expression where) {
        super(PlanType.SHOW_REPLICA_STATUS_COMMAND);
        this.tableRefInfo = tableRefInfo;
        this.where = where;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowReplicaStatus();
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tableRefInfo.getTableNameInfo().analyze(ctx);
        Util.prohibitExternalCatalog(tableRefInfo.getTableNameInfo().getCtl(), this.getClass().getSimpleName());

        PartitionNamesInfo partitionNamesInfo = tableRefInfo.getPartitionNamesInfo();
        if (partitionNamesInfo != null) {
            if (partitionNamesInfo.isTemp()) {
                throw new AnalysisException("Do not support showing replica status of temporary partitions");
            }
            partitions.addAll(partitionNamesInfo.getPartitionNames());
        }

        if (!analyzeWhereClause()) {
            throw new AnalysisException(
                "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
        }
    }

    private boolean analyzeWhereClause() {
        if (where == null) {
            return true;
        }

        if (!(where instanceof EqualTo || where instanceof Not)) {
            return false;
        }

        if (where instanceof Not) {
            if (!(where.child(0) instanceof EqualTo)) {
                return false;
            }
        }

        Expression leftExpr = null;
        if (where instanceof EqualTo) {
            leftExpr = where.child(0);
        } else if (where instanceof Not) {
            leftExpr = where.child(0).child(0);
        }

        if (leftExpr == null) {
            return false;
        }

        if (!(leftExpr instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) leftExpr).getName();
        if (!leftKey.equalsIgnoreCase("status")) {
            return false;
        }

        Expression rightExpr = null;
        if (where instanceof EqualTo) {
            rightExpr = where.child(1);
        } else if (where instanceof Not) {
            rightExpr = where.child(0).child(1);
        }

        if (rightExpr == null) {
            return false;
        }

        if (!(rightExpr instanceof StringLiteral)) {
            return false;
        }

        try {
            statusFilter = ReplicaStatus.valueOf(((StringLiteral) rightExpr).getStringValue().toUpperCase());
        } catch (Exception e) {
            return false;
        }

        if (statusFilter == null) {
            return false;
        }

        return true;
    }

    public String getDbName() {
        return tableRefInfo.getTableNameInfo().getDb();
    }

    public String getTblName() {
        return tableRefInfo.getTableNameInfo().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    /**
     * where expression is equal expression or not
     */
    public boolean isEqual() {
        if (where == null) {
            return false;
        }

        if (where instanceof EqualTo) {
            return true;
        }

        return false;
    }

    public ReplicaStatus getStatusFilter() {
        return statusFilter;
    }

    private ShowResultSet handleShowReplicaStatus() throws AnalysisException {
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletStatus(this);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        return new ShowResultSet(getMetaData(), results);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowReplicaStatusCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
