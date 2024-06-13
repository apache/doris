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
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
                                     Optional<InsertCommandContext> insertCtx, boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
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
                && !ctx.isTxnModel()
                && physicalOlapTableSink.getPartitionIds().isEmpty()
                && targetTable.getTableProperty().getUseSchemaLightChange()
                && !targetTable.getQualifiedDbName().equalsIgnoreCase(FeConstants.INTERNAL_DB_NAME)
                && isGroupCommitAvailablePlan(physicalOlapTableSink, planner);
    }

    private static boolean literalExpr(NereidsPlanner planner) {
        Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                .<PhysicalUnion>collect(PhysicalUnion.class::isInstance).stream().findAny();
        List<List<NamedExpression>> constantExprsList = null;
        if (union.isPresent()) {
            constantExprsList = union.get().getConstantExprsList();
        }
        Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                .<PhysicalOneRowRelation>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
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
            PhysicalOlapTableSink<?> physicalOlapTableSink, NereidsPlanner planner) throws Exception {
        // TODO we should refactor this to remove rely on UnionNode
        List<InternalService.PDataRow> rows = new ArrayList<>();

        Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                .<PhysicalUnion>collect(PhysicalUnion.class::isInstance).stream().findAny();
        List<List<NamedExpression>> constantExprsList = null;
        if (union.isPresent()) {
            constantExprsList = union.get().getConstantExprsList();
        }
        Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                .<PhysicalOneRowRelation>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
        if (oneRowRelation.isPresent()) {
            constantExprsList = ImmutableList.of(oneRowRelation.get().getProjects());
        }

        // should set columns of sink since we maybe generate some invisible columns
        List<Column> fullSchema = physicalOlapTableSink.getTargetTable().getFullSchema();
        List<Column> targetSchema;
        if (physicalOlapTableSink.getTargetTable().getFullSchema().size() != physicalOlapTableSink.getCols().size()) {
            targetSchema = fullSchema;
        } else {
            targetSchema = new ArrayList<>(physicalOlapTableSink.getCols());
        }
        List<String> columnNames = targetSchema.stream()
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
        // TODO: in legacy, there is a retry, we need to implement
        if (code != TStatusCode.OK) {
            String errMsg = "group commit insert failed. query_id: " + DebugUtil.printId(ConnectContext.get().queryId())
                    + ", backend id: " + groupCommitPlanner.getBackend().getId() + ", status: " + response.getStatus();
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
    }

    @Override
    protected void onComplete() throws UserException {

    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, t.getMessage());
    }

    @Override
    protected void afterExec(StmtExecutor executor) {

    }

    protected final void execImpl() throws Exception {
        Optional<PhysicalOlapTableSink<?>> plan = (planner.getPhysicalPlan()
                .<PhysicalOlapTableSink<?>>collect(PhysicalSink.class::isInstance)).stream()
                .findAny();
        PhysicalOlapTableSink<?> olapSink = plan.get();
        DataSink sink = planner.getFragments().get(0).getSink();
        handleGroupCommit(ctx, sink, olapSink, planner);
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
