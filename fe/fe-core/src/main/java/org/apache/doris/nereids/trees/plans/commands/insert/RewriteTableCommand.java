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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.DataSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RewriteTableCommand: dedicated command for rewrite operations (currently
 * Iceberg only).
 * This command is used to rewrite the data file of an iceberg table.
 */
public class RewriteTableCommand extends Command implements NeedAuditEncryption, ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(RewriteTableCommand.class);

    private LogicalPlan originLogicalQuery;
    private Optional<LogicalPlan> logicalQuery;
    private Optional<String> labelName;
    private Optional<Plan> parsedPlan;
    private final Optional<InsertCommandContext> insertCtx;
    private final Optional<LogicalPlan> cte;
    private final Optional<String> branchName;
    private long jobId;

    /**
     * constructor for rewrite operation
     */
    public RewriteTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<InsertCommandContext> insertCtx, Optional<LogicalPlan> cte, Optional<String> branchName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.originLogicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        this.logicalQuery = Optional.empty();
        this.insertCtx = insertCtx;
        this.cte = cte;
        this.branchName = branchName;
        if (Env.getCurrentEnv().isMaster()) {
            this.jobId = Env.getCurrentEnv().getNextId();
        } else {
            this.jobId = -1;
        }
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery.orElse(originLogicalQuery);
    }

    public Optional<Plan> getParsedPlan() {
        return parsedPlan;
    }

    protected void setLogicalQuery(LogicalPlan plan) {
        this.logicalQuery = Optional.of(plan);
    }

    protected TableIf getTargetTableIf(ConnectContext ctx, List<String> qualifiedTargetTableName) {
        return RelationUtil.getTable(qualifiedTargetTableName, ctx.getEnv(), Optional.empty());
    }

    /**
     * For rewrite, we never begin transaction here. We always finalize sink in init
     * stage to keep parity with previous rewrite flow, and let external caller
     * inject txnId to coordinator.
     */
    public AbstractInsertExecutor initPlan(ConnectContext ctx, StmtExecutor stmtExecutor) throws Exception {
        List<String> qualifiedTargetTableName = InsertUtils.getTargetTableQualified(originLogicalQuery, ctx);
        ctx.getStatementContext().setIsInsert(true);

        TableIf targetTableIf = getTargetTableIf(ctx, qualifiedTargetTableName);
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), targetTableIf.getDatabase().getCatalog().getName(),
                        targetTableIf.getDatabase().getFullName(), targetTableIf.getName(),
                        org.apache.doris.mysql.privilege.PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    targetTableIf.getDatabase().getFullName() + "."
                            + Util.getTempTableDisplayName(targetTableIf.getName()));
        }

        BuildResult buildResult;
        try {
            buildResult = initPlanOnce(ctx, stmtExecutor, targetTableIf);
        } catch (Throwable e) {
            Throwables.throwIfInstanceOf(e, RuntimeException.class);
            throw new IllegalStateException(e.getMessage(), e);
        }
        AbstractInsertExecutor insertExecutor = buildResult.executor;
        parsedPlan = Optional.ofNullable(buildResult.planner.getParsedPlan());

        // For rewrite: do not begin transaction here, but finalize sink
        insertExecutor.finalizeSink(
                buildResult.planner.getFragments().get(0), buildResult.dataSink, buildResult.physicalSink);
        return insertExecutor;
    }

    private BuildResult initPlanOnce(ConnectContext ctx, StmtExecutor stmtExecutor, TableIf targetTableIf)
            throws Throwable {
        targetTableIf.readLock();
        try {
            Optional<CascadesContext> analyzeContext = Optional.of(
                    CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY));
            // rewrite does not need special logic like normalize insert-into-values, just
            // use normalize directly
            this.logicalQuery = Optional.of((LogicalPlan) InsertUtils.normalizePlan(originLogicalQuery,
                    targetTableIf, analyzeContext, insertCtx));
            if (cte.isPresent()) {
                this.logicalQuery = Optional.of((LogicalPlan) cte.get().withChildren(logicalQuery.get()));
            }
        } finally {
            targetTableIf.readUnlock();
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery.get(), ctx.getStatementContext());
        return planInsertExecutor(ctx, stmtExecutor, logicalPlanAdapter, targetTableIf);
    }

    private ExecutorFactory selectInsertExecutorFactory(NereidsPlanner planner, ConnectContext ctx,
            StmtExecutor stmtExecutor, TableIf targetTableIf) {
        try {
            stmtExecutor.setPlanner(planner);
            stmtExecutor.checkBlockRules();
            if (ctx.getConnectType() == ConnectContext.ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }
            Optional<PhysicalSink<?>> plan = (planner.getPhysicalPlan()
                    .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance)).stream().findAny();
            Preconditions.checkArgument(plan.isPresent(), "rewrite command must contain target table");
            PhysicalSink<?> physicalSink = plan.get();
            DataSink dataSink = planner.getFragments().get(0).getSink();
            String label = this.labelName.orElse(String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo));

            if (physicalSink instanceof PhysicalIcebergTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                IcebergExternalTable icebergExternalTable = (IcebergExternalTable) targetTableIf;
                IcebergInsertCommandContext icebergInsertCtx = insertCtx
                        .map(c -> (IcebergInsertCommandContext) c)
                        .orElseGet(IcebergInsertCommandContext::new);
                branchName.ifPresent(notUsed -> icebergInsertCtx.setBranchName(branchName));
                return ExecutorFactory.from(planner, dataSink, physicalSink,
                        () -> new IcebergRewriteExecutor(ctx, icebergExternalTable, label, planner,
                                Optional.of(icebergInsertCtx), emptyInsert, jobId));
            }
            throw new AnalysisException("Rewrite only supports iceberg table");
        } catch (Throwable t) {
            Throwables.throwIfInstanceOf(t, RuntimeException.class);
            throw new IllegalStateException(t.getMessage(), t);
        }
    }

    private BuildResult planInsertExecutor(ConnectContext ctx, StmtExecutor stmtExecutor,
            LogicalPlanAdapter logicalPlanAdapter, TableIf targetTableIf) throws Throwable {
        LogicalPlan logicalPlan = logicalPlanAdapter.getLogicalPlan();
        boolean supportFastInsertIntoValues = InsertUtils.supportFastInsertIntoValues(logicalPlan, targetTableIf, ctx);
        AtomicReference<ExecutorFactory> executorFactoryRef = new AtomicReference<>();

        FastInsertIntoValuesPlanner planner = new FastInsertIntoValuesPlanner(
                ctx.getStatementContext(), supportFastInsertIntoValues) {
            @Override
            protected void doDistribute(boolean canUseNereidsDistributePlanner, ExplainLevel explainLevel) {
                executorFactoryRef.set(selectInsertExecutorFactory(this, ctx, stmtExecutor, targetTableIf));
                super.doDistribute(canUseNereidsDistributePlanner, explainLevel);
            }
        };

        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        if (LOG.isDebugEnabled()) {
            LOG.debug("rewrite plan for query_id: {} is: {}.", DebugUtil.printId(ctx.queryId()),
                    planner.getPhysicalPlan().treeString());
        }
        return executorFactoryRef.get().build();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        AbstractInsertExecutor insertExecutor = initPlan(ctx, executor);
        if (insertExecutor.isEmptyInsert()) {
            return;
        }
        insertExecutor.executeSingleInsert(executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY));
        Plan plan = InsertUtils.getPlanForExplain(ctx, analyzeContext, getLogicalQuery());
        if (cte.isPresent()) {
            plan = cte.get().withChildren(plan);
        }
        return plan;
    }

    @Override
    public Optional<NereidsPlanner> getExplainPlanner(LogicalPlan logicalPlan, StatementContext ctx) {
        ConnectContext connectContext = ctx.getConnectContext();
        TableIf targetTableIf = InsertUtils.getTargetTable(originLogicalQuery, connectContext);
        boolean supportFastInsertIntoValues = InsertUtils.supportFastInsertIntoValues(logicalPlan, targetTableIf,
                connectContext);
        return Optional.of(new FastInsertIntoValuesPlanner(ctx, supportFastInsertIntoValues));
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public boolean needAuditEncryption() {
        return originLogicalQuery
                .anyMatch(node -> node instanceof TVFRelation);
    }

    private boolean childIsEmptyRelation(PhysicalSink<? extends Plan> sink) {
        if (sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation) {
            return true;
        }
        return false;
    }

    private static class ExecutorFactory {
        public final NereidsPlanner planner;
        public final DataSink dataSink;
        public final PhysicalSink<?> physicalSink;
        public final java.util.function.Supplier<AbstractInsertExecutor> executorSupplier;

        private ExecutorFactory(NereidsPlanner planner, DataSink dataSink, PhysicalSink<?> physicalSink,
                java.util.function.Supplier<AbstractInsertExecutor> executorSupplier) {
            this.planner = planner;
            this.dataSink = dataSink;
            this.physicalSink = physicalSink;
            this.executorSupplier = executorSupplier;
        }

        public static ExecutorFactory from(NereidsPlanner planner, DataSink dataSink, PhysicalSink<?> physicalSink,
                java.util.function.Supplier<AbstractInsertExecutor> executorSupplier) {
            return new ExecutorFactory(planner, dataSink, physicalSink, executorSupplier);
        }

        public BuildResult build() {
            AbstractInsertExecutor executor = executorSupplier.get();
            return new BuildResult(planner, executor, dataSink, physicalSink);
        }
    }

    private static class BuildResult {
        private final NereidsPlanner planner;
        private final AbstractInsertExecutor executor;
        private final DataSink dataSink;
        private final PhysicalSink<?> physicalSink;

        public BuildResult(NereidsPlanner planner, AbstractInsertExecutor executor, DataSink dataSink,
                PhysicalSink<?> physicalSink) {
            this.planner = planner;
            this.executor = executor;
            this.dataSink = dataSink;
            this.physicalSink = physicalSink;
        }
    }
}
