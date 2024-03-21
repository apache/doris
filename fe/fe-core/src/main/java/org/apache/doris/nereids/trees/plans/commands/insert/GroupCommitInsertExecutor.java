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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Handle group commit
 */
public class GroupCommitInsertExecutor extends AbstractInsertExecutor {
    public static final Logger LOG = LogManager.getLogger(GroupCommitInsertExecutor.class);

    protected final NereidsPlanner planner;

    public GroupCommitInsertExecutor(ConnectContext ctx, TableIf table, String labelName, NereidsPlanner planner,
                                     Optional<InsertCommandContext> insertCtx) {
        super(ctx, table, labelName, planner, insertCtx);
        this.planner = planner;
    }

    /**
     * Handle group commit
     */
    public static boolean groupCommit(ConnectContext ctx, DataSink sink, PhysicalSink physicalSink)
                throws TException, RpcException, UserException, ExecutionException, InterruptedException {
        PhysicalOlapTableSink<?> olapSink = (PhysicalOlapTableSink<?>) physicalSink;
        boolean can = canGroupCommit(ctx, sink, olapSink);
        ctx.setGroupCommit(can);
        return can;
    }

    private static boolean canGroupCommit(ConnectContext ctx, DataSink sink,
            PhysicalOlapTableSink<?> physicalOlapTableSink) {
        if (!(sink instanceof OlapTableSink) || !ctx.getSessionVariable().isEnableInsertGroupCommit()
                || ctx.getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
            return false;
        }
        OlapTable targetTable = physicalOlapTableSink.getTargetTable();
        return ctx.getSessionVariable().getSqlMode() != SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES
                && !ctx.isTxnModel() && isGroupCommitAvailablePlan(physicalOlapTableSink)
                && physicalOlapTableSink.getPartitionIds().isEmpty() && targetTable.getTableProperty()
                .getUseSchemaLightChange() && !targetTable.getQualifiedDbName()
                .equalsIgnoreCase(FeConstants.INTERNAL_DB_NAME);
    }

    private static boolean isGroupCommitAvailablePlan(PhysicalOlapTableSink<? extends Plan> sink) {
        Plan child = sink.child();
        if (child instanceof PhysicalDistribute) {
            child = child.child(0);
        }
        return child instanceof OneRowRelation || (child instanceof PhysicalUnion && child.arity() == 0);
    }

    private static void handleGroupCommit(ConnectContext ctx, DataSink sink,
            PhysicalOlapTableSink<?> physicalOlapTableSink)
                throws UserException, TException, RpcException, ExecutionException, InterruptedException {
        // TODO we should refactor this to remove rely on UnionNode
        List<InternalService.PDataRow> rows = new ArrayList<>();
        List<List<Expr>> materializedConstExprLists = ((UnionNode) sink.getFragment()
                .getPlanRoot()).getMaterializedConstExprLists();
        int filterSize = 0;
        for (Slot slot : physicalOlapTableSink.getOutput()) {
            if (slot.getName().contains(Column.DELETE_SIGN)
                    || slot.getName().contains(Column.VERSION_COL)) {
                filterSize += 1;
            }
        }
        for (List<Expr> list : materializedConstExprLists) {
            rows.add(GroupCommitPlanner.getRowStringValue(list, filterSize));
        }
        GroupCommitPlanner groupCommitPlanner = EnvFactory.getInstance().createGroupCommitPlanner(
                physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), null, ctx.queryId(),
                ConnectContext.get().getSessionVariable().getGroupCommit());
        PGroupCommitInsertResponse response = groupCommitPlanner.executeGroupCommitInsert(ctx, rows);
        TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
        if (code == TStatusCode.DATA_QUALITY_ERROR) {
            LOG.info("group commit insert failed. query id: {}, backend id: {}, status: {}, "
                            + "schema version: {}", ctx.queryId(),
                    groupCommitPlanner.getBackend(), response.getStatus(),
                    physicalOlapTableSink.getTargetTable().getBaseSchemaVersion());
        } else if (code != TStatusCode.OK) {
            String errMsg = "group commit insert failed. backend id: "
                    + groupCommitPlanner.getBackend().getId() + ", status: "
                    + response.getStatus();
            ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
        }
        TransactionStatus txnStatus = TransactionStatus.PREPARE;
        String sb = "{'label':'" + response.getLabel() + "', 'status':'" + txnStatus.name()
                + "', 'txnId':'" + response.getTxnId() + "'"
                + "', 'optimizer':'" + "nereids" + "'"
                + "}";
        ctx.getState().setOk(response.getLoadedRows(), (int) response.getFilteredRows(), sb);
        ctx.setOrUpdateInsertResult(response.getTxnId(), response.getLabel(),
                physicalOlapTableSink.getDatabase().getFullName(), physicalOlapTableSink.getTargetTable().getName(),
                txnStatus, response.getLoadedRows(), (int) response.getFilteredRows());
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) response.getLoadedRows());
    }

    @Override
    public void beginTransaction() {

    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {

    }

    @Override
    protected void beforeExec() {

    }

    @Override
    protected void onComplete() throws UserException {
        Optional<PhysicalOlapTableSink<?>> plan = (planner.getPhysicalPlan()
                .<Set<PhysicalOlapTableSink<?>>>collect(PhysicalSink.class::isInstance)).stream()
                .findAny();
        PhysicalOlapTableSink<?> olapSink = plan.get();
        DataSink sink = planner.getFragments().get(0).getSink();
        try {
            handleGroupCommit(ctx, sink, olapSink);
        } catch (TException | RpcException | ExecutionException | InterruptedException e) {
            LOG.warn("errors when group commit insert. {}", e);
        }
    }

    @Override
    protected void onFail(Throwable t) {

    }

    @Override
    protected void afterExec(StmtExecutor executor) {

    }
}
