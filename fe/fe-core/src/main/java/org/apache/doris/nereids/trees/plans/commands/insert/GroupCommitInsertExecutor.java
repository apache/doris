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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ProtocolStringList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Handle group commit
 */
public class GroupCommitInsertExecutor extends AbstractInsertExecutor {
    public static final Logger LOG = LogManager.getLogger(GroupCommitInsertExecutor.class);
    private static final long INVALID_TXN_ID = -1L;
    protected final NereidsPlanner planner;
    private long txnId = INVALID_TXN_ID;
    private TransactionStatus txnStatus = TransactionStatus.ABORTED;

    public GroupCommitInsertExecutor(ConnectContext ctx, TableIf table, String labelName, NereidsPlanner planner,
                                     Optional<InsertCommandContext> insertCtx) {
        super(ctx, table, labelName, planner, insertCtx);
        this.planner = planner;
    }

    /**
     * Handle group commit
     */
    public static boolean canGroupCommit(ConnectContext ctx, DataSink sink,
                                         PhysicalSink physicalSink, NereidsPlanner planner) {
        PhysicalOlapTableSink<?> olapSink = (PhysicalOlapTableSink<?>) physicalSink;
        boolean can = analyzeGroupCommit(ctx, sink, olapSink, planner);
        ctx.setGroupCommit(can);
        return can;
    }

    private static boolean analyzeGroupCommit(ConnectContext ctx, DataSink sink,
                    PhysicalOlapTableSink<?> physicalOlapTableSink, NereidsPlanner planner) {
        if (!(sink instanceof OlapTableSink) || !ctx.getSessionVariable().isEnableInsertGroupCommit()
                || ctx.getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
            return false;
        }
        OlapTable targetTable = physicalOlapTableSink.getTargetTable();
        return ctx.getSessionVariable().getSqlMode() != SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES
                && !ctx.isTxnModel() && isGroupCommitAvailablePlan(physicalOlapTableSink, planner)
                && physicalOlapTableSink.getPartitionIds().isEmpty() && targetTable.getTableProperty()
                .getUseSchemaLightChange() && !targetTable.getQualifiedDbName()
                .equalsIgnoreCase(FeConstants.INTERNAL_DB_NAME);
    }

    private static boolean literalExpr(NereidsPlanner planner) {
        Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                .<Set<PhysicalUnion>>collect(PhysicalUnion.class::isInstance).stream().findAny();
        List<List<NamedExpression>> constantExprsList = null;
        if (union.isPresent()) {
            constantExprsList = union.get().getConstantExprsList();
        }
        Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                .<Set<PhysicalOneRowRelation>>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
        if (oneRowRelation.isPresent()) {
            constantExprsList = ImmutableList.of(oneRowRelation.get().getProjects());
        }
        for (List<NamedExpression> row : constantExprsList) {
            for (Expression expr : row) {
                while (expr instanceof Alias || expr instanceof Cast) {
                    expr = expr.child(0);
                }
                if (!(expr instanceof Literal)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isGroupCommitAvailablePlan(PhysicalOlapTableSink<? extends Plan> sink,
                                                      NereidsPlanner planner) {
        Plan child = sink.child();
        if (child instanceof PhysicalDistribute) {
            child = child.child(0);
        }
        return (child instanceof OneRowRelation || (child instanceof PhysicalUnion && child.arity() == 0))
                && literalExpr(planner);
    }

    private void handleGroupCommit(ConnectContext ctx, DataSink sink,
            PhysicalOlapTableSink<?> physicalOlapTableSink, NereidsPlanner planner)
                throws UserException, TException, RpcException, ExecutionException, InterruptedException {
        // TODO we should refactor this to remove rely on UnionNode
        List<InternalService.PDataRow> rows = new ArrayList<>();

        Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                .<Set<PhysicalUnion>>collect(PhysicalUnion.class::isInstance).stream().findAny();
        List<List<NamedExpression>> constantExprsList = null;
        if (union.isPresent()) {
            constantExprsList = union.get().getConstantExprsList();
        }
        Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                .<Set<PhysicalOneRowRelation>>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
        if (oneRowRelation.isPresent()) {
            constantExprsList = ImmutableList.of(oneRowRelation.get().getProjects());
        }
        List<String> columnNames = physicalOlapTableSink.getTargetTable().getFullSchema().stream()
                .map(Column::getName)
                .map(n -> n.replace("`", "``"))
                .collect(Collectors.toList());
        for (List<NamedExpression> row : constantExprsList) {
            rows.add(InsertUtils.getRowStringValue(row));
        }
        GroupCommitPlanner groupCommitPlanner = EnvFactory.getInstance().createGroupCommitPlanner(
                physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), columnNames, ctx.queryId(),
                ConnectContext.get().getSessionVariable().getGroupCommit());
        PGroupCommitInsertResponse response = groupCommitPlanner.executeGroupCommitInsert(ctx, rows);
        TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
        ProtocolStringList errorMsgsList = response.getStatus().getErrorMsgsList();
        // TODO: in legacy, there is a retry, we need to implement
        if (code == TStatusCode.DATA_QUALITY_ERROR && !errorMsgsList.isEmpty() && errorMsgsList.get(0)
                .contains("schema version not match")) {
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
        txnStatus = TransactionStatus.PREPARE;
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
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.info("start insert [{}] with query id {} and txn id {}", labelName, queryId, txnId);
    }

    @Override
    protected void onComplete() throws UserException {

    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("'status':'").append(ctx.isTxnModel() ? TransactionStatus.PREPARE.name() : txnStatus.name());
        // sb.append("', 'txnId':'").append(txnId).append("'");
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append("', 'err':'").append(errMsg).append("'");
        }
        sb.append("}");
        ctx.getState().setOk(loadedRows, 0, sb.toString());
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(txnId, labelName, database.getFullName(), table.getName(),
                txnStatus, loadedRows, 0);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) loadedRows);
    }

    @Override
    protected void afterExec(StmtExecutor executor) {

    }

    protected final void execImpl() throws UserException {
        Optional<PhysicalOlapTableSink<?>> plan = (planner.getPhysicalPlan()
                .<Set<PhysicalOlapTableSink<?>>>collect(PhysicalSink.class::isInstance)).stream()
                .findAny();
        PhysicalOlapTableSink<?> olapSink = plan.get();
        DataSink sink = planner.getFragments().get(0).getSink();
        try {
            handleGroupCommit(ctx, sink, olapSink, planner);
        } catch (TException | RpcException | ExecutionException | InterruptedException e) {
            LOG.error("errors when group commit insert. {}", e);
            throw new UserException("errors when group commit insert. " + e.getMessage(), e);
        }
    }

    @Override
    public void executeSingleInsert(StmtExecutor executor, long jobId) throws Exception {
        beforeExec();
        try {
            execImpl();
            onComplete();
        } catch (Throwable t) {
            onFail(t);
            // retry group_commit insert when meet
            if (t.getMessage().contains(GroupCommitPlanner.SCHEMA_CHANGE)) {
                throw t;
            }
            return;
        }
        afterExec(executor);
    }
}
