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
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * ShowReplicaStatusCommand
 */
public class AdminShowReplicaStatusCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("Version").add("LastFailedVersion")
            .add("LastSuccessVersion").add("CommittedVersion").add("SchemaHash").add("VersionNum")
            .add("IsBad").add("IsUserDrop").add("State").add("Status")
            .build();
    private static final Logger LOG = LogManager.getLogger(AdminShowReplicaStatusCommand.class);

    private TableRefInfo tableRefInfo;
    private Expression where;

    private Replica.ReplicaStatus statusFilter;

    public AdminShowReplicaStatusCommand(TableRefInfo tableRefInfo, Expression where) {
        super(PlanType.SHOW_REPLICA_STATUS_COMMAND);
        this.tableRefInfo = tableRefInfo;
        this.where = where;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletStatus(tableRefInfo.getTableNameInfo().getDb(),
                tableRefInfo.getTableNameInfo().getTbl(),
                tableRefInfo.getPartitionNamesInfo().getPartitionNames(), statusFilter, where);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        return new ShowResultSet(getMetaData(), results);
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    /**
     * validate for show replica
     */
    public void validate(ConnectContext ctx) throws AnalysisException, UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // bind relation
        tableRefInfo.analyze(ctx);
        Util.prohibitExternalCatalog(tableRefInfo.getTableNameInfo().getCtl(), this.getClass().getSimpleName());

        if (!validateWhere()) {
            throw new AnalysisException(
                "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
        }
    }

    private boolean validateWhere() throws AnalysisException {
        // analyze where clause if not null
        if (where == null) {
            return true;
        }

        if (!(where instanceof EqualTo || (where instanceof Not && where.child(0) instanceof EqualTo))) {
            return false;
        }

        EqualTo equalTo = where instanceof EqualTo ? (EqualTo) where : (EqualTo) ((Not) where).child();

        Expression leftChild = equalTo.child(0);
        if (!(leftChild instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) leftChild).getName();
        if (!leftKey.equalsIgnoreCase("status")) {
            return false;
        }

        Expression rightChild = equalTo.child(1);
        if (!(rightChild instanceof StringLikeLiteral)) {
            return false;
        }

        try {
            statusFilter = Replica.ReplicaStatus
                    .valueOf(((StringLikeLiteral) rightChild).getStringValue().toUpperCase());
        } catch (Exception e) {
            return false;
        }

        if (statusFilter == null) {
            return false;
        }

        return true;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminShowReplicaStatusCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        if (!ctx.getCurrentUserIdentity().getUser().equals(Auth.ROOT_USER)) {
            LOG.info("ShowReplicaStatusCommand not supported in cloud mode");
            throw new DdlException("Unsupported operation");
        }
    }
}
