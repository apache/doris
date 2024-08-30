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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.profile.ProfileManager.ProfileType;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.implementation.LogicalOlapTableSinkToPhysicalOlapTableSink;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.DataSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * insert into select command implementation
 * insert into select command support the grammar: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private LogicalPlan logicalQuery;
    private Optional<CascadesContext> analyzeContext;
    private Optional<String> labelName;
    /**
     * When source it's from job scheduler,it will be set.
     */
    private long jobId;
    private Optional<InsertCommandContext> insertCtx;
    private final Optional<LogicalPlan> cte;

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
                                  Optional<InsertCommandContext> insertCtx, Optional<LogicalPlan> cte) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        this.insertCtx = insertCtx;
        this.cte = cte;
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery;
    }

    public Optional<String> getLabelName() {
        return labelName;
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        runInternal(ctx, executor);
    }

    public void runWithUpdateInfo(ConnectContext ctx, StmtExecutor executor,
                                  LoadStatistic loadStatistic) throws Exception {
        // TODO: add coordinator statistic
        runInternal(ctx, executor);
    }

    public AbstractInsertExecutor initPlan(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return initPlan(ctx, executor, true);
    }

    /**
     * This function is used to generate the plan for Nereids.
     * There are some load functions that only need to the plan, such as stream_load.
     * Therefore, this section will be presented separately.
     * @param needBeginTransaction whether to start a transaction.
     *       For external uses such as creating a job, only basic analysis is needed without starting a transaction,
     *       in which case this can be set to false.
     */
    public AbstractInsertExecutor initPlan(ConnectContext ctx, StmtExecutor stmtExecutor,
                                           boolean needBeginTransaction) throws Exception {
        TableIf targetTableIf = InsertUtils.getTargetTable(logicalQuery, ctx);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), targetTableIf.getDatabase().getCatalog().getName(),
                        targetTableIf.getDatabase().getFullName(), targetTableIf.getName(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    targetTableIf.getDatabase().getFullName() + "." + targetTableIf.getName());
        }

        AbstractInsertExecutor insertExecutor = null;
        // should lock target table until we begin transaction.
        targetTableIf.readLock();
        try {
            this.analyzeContext = Optional.of(
                    CascadesContext.initContext(ctx.getStatementContext(), logicalQuery, PhysicalProperties.ANY)
            );

            // 1. process inline table (default values, empty values)
            this.logicalQuery = (LogicalPlan) InsertUtils.normalizePlan(
                logicalQuery, targetTableIf, analyzeContext, insertCtx
            );
            if (cte.isPresent()) {
                this.logicalQuery = ((LogicalPlan) cte.get().withChildren(logicalQuery));
            }
            OlapGroupCommitInsertExecutor.analyzeGroupCommit(ctx, targetTableIf, this.logicalQuery, this.insertCtx);
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());

            BuildInsertExecutorResult buildResult = planInsertExecutor(
                    ctx, stmtExecutor, logicalPlanAdapter, targetTableIf
            );

            insertExecutor = buildResult.executor;

            if (!needBeginTransaction) {
                targetTableIf.readUnlock();
                return insertExecutor;
            }
            if (!insertExecutor.isEmptyInsert()) {
                insertExecutor.beginTransaction();
                insertExecutor.finalizeSink(
                        buildResult.planner.getFragments().get(0), buildResult.dataSink, buildResult.physicalSink
                );
            }
            targetTableIf.readUnlock();
        } catch (Throwable e) {
            targetTableIf.readUnlock();
            // the abortTxn in onFail need to acquire table write lock
            if (insertExecutor != null) {
                insertExecutor.onFail(e);
            }
            Throwables.propagateIfInstanceOf(e, RuntimeException.class);
            throw new IllegalStateException(e.getMessage(), e);
        }

        stmtExecutor.setProfileType(ProfileType.LOAD);
        // We exposed @StmtExecutor#cancel as a unified entry point for statement interruption,
        // so we need to set this here
        insertExecutor.getCoordinator().setTxnId(insertExecutor.getTxnId());
        stmtExecutor.setCoord(insertExecutor.getCoordinator());
        return insertExecutor;
    }

    // we should select the factory type first, but we can not initial InsertExecutor at this time,
    // because Nereids's DistributePlan are not gernerated, so we return factory and after the
    // DistributePlan have been generated, we can create InsertExecutor
    private ExecutorFactory selectInsertExecutorFactory(
            NereidsPlanner planner, ConnectContext ctx, StmtExecutor stmtExecutor, TableIf targetTableIf) {
        try {
            stmtExecutor.setPlanner(planner);
            stmtExecutor.checkBlockRules();
            if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }
            Optional<PhysicalSink<?>> plan = (planner.getPhysicalPlan()
                    .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance)).stream()
                    .findAny();
            Preconditions.checkArgument(plan.isPresent(), "insert into command must contain target table");
            PhysicalSink<?> physicalSink = plan.get();
            DataSink dataSink = planner.getFragments().get(0).getSink();
            // Transaction insert should reuse the label in the transaction.
            String label = this.labelName.orElse(
                    ctx.isTxnModel() ? null : String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo));

            if (physicalSink instanceof PhysicalOlapTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                OlapTable olapTable = (OlapTable) targetTableIf;

                ExecutorFactory executorFactory;
                // the insertCtx contains some variables to adjust SinkNode
                if (ctx.isTxnModel()) {
                    executorFactory = ExecutorFactory.from(
                            planner,
                            dataSink,
                            physicalSink,
                            () -> new OlapTxnInsertExecutor(ctx, olapTable, label, planner, insertCtx, emptyInsert)
                    );
                } else if (ctx.isGroupCommit()) {
                    Backend groupCommitBackend = Env.getCurrentEnv()
                            .getGroupCommitManager()
                            .selectBackendForGroupCommit(targetTableIf.getId(), ctx);
                    // set groupCommitBackend for Nereids's DistributePlanner
                    planner.getCascadesContext().getStatementContext().setGroupCommitMergeBackend(groupCommitBackend);
                    executorFactory = ExecutorFactory.from(
                            planner,
                            dataSink,
                            physicalSink,
                            () -> new OlapGroupCommitInsertExecutor(
                                    ctx, olapTable, label, planner, insertCtx, emptyInsert, groupCommitBackend
                            )
                    );
                } else {
                    executorFactory = ExecutorFactory.from(
                            planner,
                            dataSink,
                            physicalSink,
                            () -> new OlapInsertExecutor(ctx, olapTable, label, planner, insertCtx, emptyInsert)
                    );
                }

                return executorFactory.onCreate(executor -> {
                    Coordinator coordinator = executor.getCoordinator();
                    boolean isEnableMemtableOnSinkNode = olapTable.getTableProperty().getUseSchemaLightChange()
                                    && coordinator.getQueryOptions().isEnableMemtableOnSinkNode();
                    coordinator.getQueryOptions().setEnableMemtableOnSinkNode(isEnableMemtableOnSinkNode);
                });
            } else if (physicalSink instanceof PhysicalHiveTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                HMSExternalTable hiveExternalTable = (HMSExternalTable) targetTableIf;
                return ExecutorFactory.from(
                        planner,
                        dataSink,
                        physicalSink,
                        () -> new HiveInsertExecutor(ctx, hiveExternalTable, label, planner,
                                Optional.of(insertCtx.orElse((new HiveInsertCommandContext()))), emptyInsert)
                );
                // set hive query options
            } else if (physicalSink instanceof PhysicalIcebergTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                IcebergExternalTable icebergExternalTable = (IcebergExternalTable) targetTableIf;
                return ExecutorFactory.from(
                        planner,
                        dataSink,
                        physicalSink,
                        () -> new IcebergInsertExecutor(ctx, icebergExternalTable, label, planner,
                                Optional.of(insertCtx.orElse((new BaseExternalTableInsertCommandContext()))),
                                emptyInsert
                        )
                );
            } else if (physicalSink instanceof PhysicalJdbcTableSink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                List<Column> cols = ((PhysicalJdbcTableSink<?>) physicalSink).getCols();
                List<Slot> slots = physicalSink.getOutput();
                if (physicalSink.children().size() == 1) {
                    if (physicalSink.child(0) instanceof PhysicalOneRowRelation
                            || physicalSink.child(0) instanceof PhysicalUnion) {
                        for (int i = 0; i < cols.size(); i++) {
                            if (!(cols.get(i).isAllowNull()) && slots.get(i).nullable()) {
                                throw new AnalysisException("Column `" + cols.get(i).getName()
                                        + "` is not nullable, but the inserted value is nullable.");
                            }
                        }
                    }
                }
                JdbcExternalTable jdbcExternalTable = (JdbcExternalTable) targetTableIf;
                return ExecutorFactory.from(
                        planner,
                        dataSink,
                        physicalSink,
                        () -> new JdbcInsertExecutor(ctx, jdbcExternalTable, label, planner,
                                Optional.of(insertCtx.orElse((new JdbcInsertCommandContext()))), emptyInsert)
                );
            } else {
                // TODO: support other table types
                throw new AnalysisException("insert into command only support [olap, hive, iceberg, jdbc] table");
            }
        } catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, RuntimeException.class);
            throw new IllegalStateException(t.getMessage(), t);
        }
    }

    private BuildInsertExecutorResult planInsertExecutor(
            ConnectContext ctx, StmtExecutor stmtExecutor,
            LogicalPlanAdapter logicalPlanAdapter, TableIf targetTableIf) throws Throwable {
        LogicalPlan logicalPlan = logicalPlanAdapter.getLogicalPlan();

        boolean isInsertIntoValues
                = logicalPlan instanceof UnboundTableSink && logicalPlan.child(0) instanceof LogicalInlineTable;
        // the key logical when use new coordinator:
        // 1. use NereidsPlanner to generate PhysicalPlan
        // 2. use PhysicalPlan to select InsertExecutorFactory, some InsertExecutors want to control
        //    which backend should be used, for example, OlapGroupCommitInsertExecutor need select
        //    a backend to do group commit.
        //    Note: we can not initialize InsertExecutor at this time, because the DistributePlans
        //    have not been generated, so the NereidsSqlCoordinator can not initial too,
        // 3. NereidsPlanner use PhysicalPlan and the provided backend to generate DistributePlan
        // 4. ExecutorFactory use the DistributePlan to generate the NereidsSqlCoordinator and InsertExecutor

        AtomicReference<ExecutorFactory> executorFactoryRef = new AtomicReference<>();
        InsertByInlineTablePlanner planner = new InsertByInlineTablePlanner(
                ctx.getStatementContext(), isInsertIntoValues) {
            @Override
            protected void doDistribute(boolean canUseNereidsDistributePlanner) {
                // when enter this method, the step 1 already executed

                // step 2
                executorFactoryRef.set(
                        selectInsertExecutorFactory(this, ctx, stmtExecutor, targetTableIf)
                );
                // step 3
                super.doDistribute(canUseNereidsDistributePlanner);
            }
        };

        // step 1, 2, 3
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        // step 4
        return executorFactoryRef.get().build();
    }

    private void runInternal(ConnectContext ctx, StmtExecutor executor) throws Exception {
        AbstractInsertExecutor insertExecutor = initPlan(ctx, executor);
        // if the insert stmt data source is empty, directly return, no need to be executed.
        if (insertExecutor.isEmptyInsert()) {
            return;
        }
        insertExecutor.executeSingleInsert(executor, jobId);
    }

    public boolean isExternalTableSink() {
        return !(logicalQuery instanceof UnboundTableSink);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return InsertUtils.getPlanForExplain(ctx, this.analyzeContext, this.logicalQuery);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoTableCommand(this, context);
    }

    private boolean childIsEmptyRelation(PhysicalSink sink) {
        if (sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation) {
            return true;
        }
        return false;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }

    /**
     * this factory is used to delay create the AbstractInsertExecutor until the DistributePlan is generated
     * by NereidsPlanner
     */
    private static class ExecutorFactory {
        public final NereidsPlanner planner;
        public final DataSink dataSink;
        public final PhysicalSink<?> physicalSink;
        public final Supplier<AbstractInsertExecutor> executorSupplier;
        private List<Consumer<AbstractInsertExecutor>> createCallback;

        private ExecutorFactory(NereidsPlanner planner, DataSink dataSink, PhysicalSink<?> physicalSink,
                Supplier<AbstractInsertExecutor> executorSupplier) {
            this.planner = planner;
            this.dataSink = dataSink;
            this.physicalSink = physicalSink;
            this.executorSupplier = executorSupplier;
            this.createCallback = Lists.newArrayList();
        }

        public static ExecutorFactory from(
                 NereidsPlanner planner, DataSink dataSink, PhysicalSink<?> physicalSink,
                Supplier<AbstractInsertExecutor> executorSupplier) {
            return new ExecutorFactory(planner, dataSink, physicalSink, executorSupplier);
        }

        public ExecutorFactory onCreate(Consumer<AbstractInsertExecutor> onCreate) {
            this.createCallback.add(onCreate);
            return this;
        }

        public BuildInsertExecutorResult build() {
            AbstractInsertExecutor executor = executorSupplier.get();
            for (Consumer<AbstractInsertExecutor> callback : createCallback) {
                callback.accept(executor);
            }
            return new BuildInsertExecutorResult(planner, executor, dataSink, physicalSink);
        }
    }

    private static class BuildInsertExecutorResult {
        private final NereidsPlanner planner;
        private final AbstractInsertExecutor executor;
        private final DataSink dataSink;
        private final PhysicalSink<?> physicalSink;

        public BuildInsertExecutorResult(NereidsPlanner planner, AbstractInsertExecutor executor, DataSink dataSink,
                PhysicalSink<?> physicalSink) {
            this.planner = planner;
            this.executor = executor;
            this.dataSink = dataSink;
            this.physicalSink = physicalSink;
        }
    }

    private static class InsertByInlineTablePlanner extends NereidsPlanner {
        private static final Rule toPhysicalOlapTableSink = new LogicalOlapTableSinkToPhysicalOlapTableSink()
                .build();
        private final AtomicReference<Group> rootGroupRef = new AtomicReference<>();

        private final boolean isInsertIntoValues;

        public InsertByInlineTablePlanner(StatementContext statementContext, boolean isInsertIntoValues) {
            super(statementContext);
            this.isInsertIntoValues = isInsertIntoValues;
        }

        @Override
        protected void analyze(boolean showPlanProcess) {
            if (!isInsertIntoValues) {
                super.analyze(showPlanProcess);
                return;
            }

            DefaultPlanRewriter<Void> analyzer = new DefaultPlanRewriter<Void>() {
                @Override
                public Plan visitUnboundTableSink(
                        UnboundTableSink<? extends Plan> olapTableSink, Void context) {
                    olapTableSink =
                            (UnboundTableSink<? extends Plan>) super.visitUnboundTableSink(olapTableSink, context);

                    return (LogicalOlapTableSink<?>) new BindSink()
                        .buildRules()
                        .stream()
                        .filter(rule -> rule.getRuleType() == RuleType.BINDING_INSERT_TARGET_TABLE)
                        .findFirst()
                        .get()
                        .transform(olapTableSink, getCascadesContext())
                        .get(0);
                }

                @Override
                public Plan visitLogicalInlineTable(LogicalInlineTable logicalInlineTable, Void context) {
                    logicalInlineTable =
                            (LogicalInlineTable) super.visitLogicalInlineTable(logicalInlineTable, context);

                    List<NamedExpression> outputs = Lists.newArrayList();
                    for (Slot slot : logicalInlineTable.getOutput()) {
                        outputs.add(new SlotReference(slot.getName(), slot.getDataType(), slot.nullable()));
                    }

                    LogicalUnion union = new LogicalUnion(
                        Qualifier.ALL, logicalInlineTable.getConstantExprsList(), ImmutableList.of()
                    ).withNewOutputs(outputs);

                    return union;
                }
            };

            Plan boundPlan = getCascadesContext().getRewritePlan().accept(analyzer, null);
            getCascadesContext().setRewritePlan(boundPlan);
        }

        @Override
        protected void rewrite(boolean showPlanProcess) {
            if (!isInsertIntoValues) {
                super.rewrite(showPlanProcess);
                return;
            }

            DefaultPlanRewriter<Void> rewriter = new DefaultPlanRewriter<Void>() {
                @Override
                public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
                    project = (LogicalProject<? extends Plan>) super.visitLogicalProject(project, context);
                    if (project.child() instanceof LogicalProject) {
                        return new MergeProjects()
                                .build()
                                .transform(project, getCascadesContext())
                                .get(0);
                    }
                    return project;
                }
            };
            getCascadesContext().setRewritePlan(
                    getCascadesContext().getRewritePlan().accept(rewriter, null)
            );
        }

        @Override
        protected void optimize() {
            if (!isInsertIntoValues) {
                super.optimize();
                return;
            }

            DefaultPlanRewriter<Void> optimizer = new DefaultPlanRewriter<Void>() {
                @Override
                public Plan visitLogicalUnion(LogicalUnion logicalUnion, Void context) {
                    logicalUnion = (LogicalUnion) super.visitLogicalUnion(logicalUnion, context);

                    return new PhysicalUnion(logicalUnion.getQualifier(),
                            logicalUnion.getOutputs(),
                            logicalUnion.getRegularChildrenOutputs(),
                            logicalUnion.getConstantExprsList(),
                            logicalUnion.getLogicalProperties(),
                            logicalUnion.children()
                    );
                }

                @Override
                public Plan visitLogicalProject(LogicalProject<? extends Plan> logicalProject, Void context) {
                    logicalProject =
                            (LogicalProject<? extends Plan>) super.visitLogicalProject(logicalProject, context);

                    return new PhysicalProject<>(
                            logicalProject.getProjects(),
                            logicalProject.getLogicalProperties(),
                            logicalProject.child()
                    );
                }

                @Override
                public Plan visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> olapTableSink,
                        Void context) {
                    olapTableSink =
                            (LogicalOlapTableSink) super.visitLogicalOlapTableSink(olapTableSink, context);
                    return toPhysicalOlapTableSink
                            .transform(olapTableSink, getCascadesContext())
                            .get(0);
                }
            };

            PhysicalPlan physicalPlan =
                    (PhysicalPlan) getCascadesContext().getRewritePlan().accept(optimizer, null);

            super.physicalPlan = physicalPlan;

            GroupId rootGroupId = GroupId.createGenerator().getNextId();
            Group rootGroup = new Group(rootGroupId, physicalPlan.getLogicalProperties());
            rootGroupRef.set(rootGroup);
        }

        @Override
        public Group getRoot() {
            if (!isInsertIntoValues) {
                return super.getRoot();
            }
            return rootGroupRef.get();
        }

        @Override
        protected PhysicalPlan chooseNthPlan(
                Group rootGroup, PhysicalProperties physicalProperties, int nthPlan) {
            if (!isInsertIntoValues) {
                return super.chooseNthPlan(rootGroup, physicalProperties, nthPlan);
            }
            return super.physicalPlan;
        }

        @Override
        protected PhysicalPlan postProcess(PhysicalPlan physicalPlan) {
            if (!isInsertIntoValues) {
                return super.postProcess(physicalPlan);
            }
            return physicalPlan;
        }
    }
}
