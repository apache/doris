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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ProfileManager.ProfileType;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDictionarySink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.DataSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
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
public class InsertIntoTableCommand extends Command implements NeedAuditEncryption, ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private LogicalPlan originLogicalQuery;
    private Optional<LogicalPlan> logicalQuery;
    private Optional<String> labelName;
    private Optional<String> branchName;
    /**
     * When source it's from job scheduler,it will be set.
     */
    private long jobId;

    // default is empty. only for OlapInsertExecutor#finalizeSink will construct one for check allow auto partition
    private final Optional<InsertCommandContext> insertCtx;
    private final Optional<LogicalPlan> cte;
    private final boolean needNormalizePlan;

    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<InsertCommandContext> insertCtx, Optional<LogicalPlan> cte) {
        this(logicalQuery, labelName, insertCtx, cte, true, Optional.empty());
    }

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
                                  Optional<InsertCommandContext> insertCtx, Optional<LogicalPlan> cte,
                                  boolean needNormalizePlan, Optional<String> branchName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.originLogicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        this.logicalQuery = Optional.empty();
        this.insertCtx = insertCtx;
        this.cte = cte;
        this.needNormalizePlan = needNormalizePlan;
        this.branchName = branchName;
    }

    /**
     * constructor for derived class
     */
    public InsertIntoTableCommand(InsertIntoTableCommand command, PlanType planType) {
        super(planType);
        this.originLogicalQuery = command.originLogicalQuery;
        this.labelName = command.labelName;
        this.logicalQuery = command.logicalQuery;
        this.insertCtx = command.insertCtx;
        this.cte = command.cte;
        this.jobId = command.jobId;
        this.needNormalizePlan = true;
        this.branchName = command.branchName;
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery.orElse(originLogicalQuery);
    }

    protected void setLogicalQuery(LogicalPlan logicalQuery) {
        this.logicalQuery = Optional.of(logicalQuery);
    }

    protected void setOriginLogicalQuery(LogicalPlan logicalQuery) {
        this.originLogicalQuery = logicalQuery;
    }

    public Optional<String> getLabelName() {
        return labelName;
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public long getJobId() {
        return jobId;
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

    // may be overridden
    protected TableIf getTargetTableIf(
            ConnectContext ctx, List<String> qualifiedTargetTableName) {
        return RelationUtil.getTable(qualifiedTargetTableName, ctx.getEnv(), Optional.empty());
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
        List<String> qualifiedTargetTableName = InsertUtils.getTargetTableQualified(originLogicalQuery, ctx);

        AbstractInsertExecutor insertExecutor;
        int retryTimes = 0;
        while (++retryTimes < Math.max(ctx.getSessionVariable().dmlPlanRetryTimes, 3)) {
            TableIf targetTableIf = getTargetTableIf(ctx, qualifiedTargetTableName);
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), targetTableIf.getDatabase().getCatalog().getName(),
                            targetTableIf.getDatabase().getFullName(), targetTableIf.getName(),
                            PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        targetTableIf.getDatabase().getFullName()
                                + "." + Util.getTempTableDisplayName(targetTableIf.getName()));
            }
            BuildInsertExecutorResult buildResult;
            try {
                // use originLogicalQuery to build logicalQuery again.
                buildResult = initPlanOnce(ctx, stmtExecutor, targetTableIf);
            } catch (Throwable e) {
                Throwables.throwIfInstanceOf(e, RuntimeException.class);
                throw new IllegalStateException(e.getMessage(), e);
            }
            insertExecutor = buildResult.executor;
            if (!needBeginTransaction) {
                return insertExecutor;
            }

            // lock after plan and check does table's schema changed to ensure we lock table order by id.
            TableIf newestTargetTableIf = getTargetTableIf(ctx, qualifiedTargetTableName);
            newestTargetTableIf.readLock();
            try {
                if (targetTableIf.getId() != newestTargetTableIf.getId()) {
                    LOG.warn("insert plan failed {} times. query id is {}. table id changed from {} to {}",
                            retryTimes, DebugUtil.printId(ctx.queryId()),
                            targetTableIf.getId(), newestTargetTableIf.getId());
                    newestTargetTableIf.readUnlock();
                    continue;
                }
                // Use the schema saved during planning as the schema of the original target table.
                if (!ctx.getStatementContext().getInsertTargetSchema().equals(newestTargetTableIf.getFullSchema())) {
                    LOG.warn("insert plan failed {} times. query id is {}. table schema changed from {} to {}",
                            retryTimes, DebugUtil.printId(ctx.queryId()),
                            ctx.getStatementContext().getInsertTargetSchema(), newestTargetTableIf.getFullSchema());
                    newestTargetTableIf.readUnlock();
                    continue;
                }
                if (!insertExecutor.isEmptyInsert()) {
                    insertExecutor.beginTransaction();
                    insertExecutor.finalizeSink(
                            buildResult.planner.getFragments().get(0), buildResult.dataSink,
                            buildResult.physicalSink
                    );
                }
                newestTargetTableIf.readUnlock();
            } catch (Throwable e) {
                newestTargetTableIf.readUnlock();
                // the abortTxn in onFail need to acquire table write lock
                if (insertExecutor != null) {
                    insertExecutor.onFail(e);
                }
                Throwables.throwIfInstanceOf(e, RuntimeException.class);
                throw new IllegalStateException(e.getMessage(), e);
            }
            stmtExecutor.setProfileType(ProfileType.LOAD);
            // We exposed @StmtExecutor#cancel as a unified entry point for statement interruption,
            // so we need to set this here
            insertExecutor.getCoordinator().setTxnId(insertExecutor.getTxnId());
            stmtExecutor.setCoord(insertExecutor.getCoordinator());
            return insertExecutor;
        }
        LOG.warn("insert plan failed {} times. query id is {}.", retryTimes, DebugUtil.printId(ctx.queryId()));
        throw new AnalysisException("Insert plan failed. Could not get target table lock.");
    }

    private BuildInsertExecutorResult initPlanOnce(ConnectContext ctx,
            StmtExecutor stmtExecutor, TableIf targetTableIf) throws Throwable {
        targetTableIf.readLock();
        try {
            Optional<CascadesContext> analyzeContext = Optional.of(
                    CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
            );
            if (!(this instanceof InsertIntoDictionaryCommand)) {
                // process inline table (default values, empty values)
                if (needNormalizePlan) {
                    this.logicalQuery = Optional.of((LogicalPlan) InsertUtils.normalizePlan(originLogicalQuery,
                            targetTableIf, analyzeContext, insertCtx));
                } else {
                    this.logicalQuery = Optional.of(originLogicalQuery);
                }
            }
            if (cte.isPresent()) {
                this.logicalQuery = Optional.of((LogicalPlan) cte.get().withChildren(logicalQuery.get()));
            }
            OlapGroupCommitInsertExecutor.analyzeGroupCommit(
                    ctx, targetTableIf, this.logicalQuery.get(), this.insertCtx);
        } finally {
            targetTableIf.readUnlock();
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery.get(), ctx.getStatementContext());
        return planInsertExecutor(ctx, stmtExecutor, logicalPlanAdapter, targetTableIf);
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

            // check branch
            if (branchName.isPresent() && !(physicalSink instanceof PhysicalIcebergTableSink)) {
                throw new AnalysisException("Only support insert data into iceberg table's branch");
            }

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
                if (hiveExternalTable.isHiveTransactionalTable()) {
                    throw new UserException("Not supported insert into hive transactional table.");
                }

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
                IcebergInsertCommandContext icebergInsertCtx = insertCtx
                        .map(insertCommandContext -> (IcebergInsertCommandContext) insertCommandContext)
                        .orElseGet(IcebergInsertCommandContext::new);
                branchName.ifPresent(notUsed -> icebergInsertCtx.setBranchName(branchName));
                return ExecutorFactory.from(
                        planner,
                        dataSink,
                        physicalSink,
                        () -> new IcebergInsertExecutor(ctx, icebergExternalTable, label, planner,
                                Optional.of(icebergInsertCtx),
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
            } else if (physicalSink instanceof PhysicalDictionarySink) {
                boolean emptyInsert = childIsEmptyRelation(physicalSink);
                Dictionary dictionary = (Dictionary) targetTableIf;
                // insertCtx is not useful for dictionary. so keep it empty is ok.
                return ExecutorFactory.from(planner, dataSink, physicalSink,
                        () -> new DictionaryInsertExecutor(ctx, dictionary, label, planner, insertCtx, emptyInsert));
            } else {
                // TODO: support other table types
                throw new AnalysisException(
                        "insert into command only support [olap, dictionary, hive, iceberg, jdbc] table");
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

        boolean supportFastInsertIntoValues = InsertUtils.supportFastInsertIntoValues(logicalPlan, targetTableIf, ctx);
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
        FastInsertIntoValuesPlanner planner = new FastInsertIntoValuesPlanner(
                ctx.getStatementContext(), supportFastInsertIntoValues) {
            @Override
            protected void doDistribute(boolean canUseNereidsDistributePlanner, ExplainLevel explainLevel) {
                // when enter this method, the step 1 already executed

                // step 2
                executorFactoryRef.set(
                        selectInsertExecutorFactory(this, ctx, stmtExecutor, targetTableIf)
                );
                // step 3
                super.doDistribute(canUseNereidsDistributePlanner, explainLevel);
            }
        };

        // step 1, 2, 3
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        if (LOG.isDebugEnabled()) {
            LOG.debug("insert into plan for query_id: {} is: {}.", DebugUtil.printId(ctx.queryId()),
                    planner.getPhysicalPlan().treeString());
        }
        // step 4
        BuildInsertExecutorResult build = executorFactoryRef.get().build();
        return build;
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
        return !(getLogicalQuery() instanceof UnboundTableSink);
    }

    /**
     * get the target table of the insert command
     */
    public TableIf getTable(ConnectContext ctx) throws Exception {
        TableIf targetTableIf = InsertUtils.getTargetTable(originLogicalQuery, ctx);
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), targetTableIf.getDatabase().getCatalog().getName(),
                        targetTableIf.getDatabase().getFullName(), targetTableIf.getName(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    targetTableIf.getDatabase().getFullName() + "."
                            + Util.getTempTableDisplayName(targetTableIf.getName()));
        }
        return targetTableIf;
    }

    /**
     * get the target columns of the insert command
     */
    public List<String> getTargetColumns() {
        if (originLogicalQuery instanceof UnboundTableSink) {
            UnboundLogicalSink<? extends Plan> unboundTableSink = (UnboundTableSink<? extends Plan>) originLogicalQuery;
            return CollectionUtils.isEmpty(unboundTableSink.getColNames()) ? null : unboundTableSink.getColNames();
        } else {
            throw new AnalysisException(
                    "the root of plan should be [UnboundTableSink], but it is " + originLogicalQuery.getType());
        }
    }

    // todo: add ut
    public String getFirstTvfName() {
        return getFirstTvfInPlan(getLogicalQuery());
    }

    private String getFirstTvfInPlan(LogicalPlan plan) {
        if (plan instanceof UnboundTVFRelation) {
            UnboundTVFRelation tvfRelation = (UnboundTVFRelation) plan;
            return tvfRelation.getFunctionName();
        }

        for (Plan child : plan.children()) {
            if (child instanceof LogicalPlan) {
                return getFirstTvfInPlan((LogicalPlan) child);
            }
        }
        return "";
    }

    // todo: add ut
    public void rewriteTvfProperties(String functionName, Map<String, String> props) {
        rewriteTvfInPlan(originLogicalQuery, functionName, props);
        if (logicalQuery.isPresent()) {
            rewriteTvfInPlan(logicalQuery.get(), functionName, props);
        }
    }

    private void rewriteTvfInPlan(LogicalPlan plan, String functionName, Map<String, String> props) {
        if (plan instanceof UnboundTVFRelation) {
            UnboundTVFRelation tvfRelation = (UnboundTVFRelation) plan;
            if (functionName.equalsIgnoreCase(tvfRelation.getFunctionName())) {
                tvfRelation.getProperties().getMap().putAll(props);
            }
        }

        for (Plan child : plan.children()) {
            if (child instanceof LogicalPlan) {
                rewriteTvfInPlan((LogicalPlan) child, functionName, props);
            }
        }
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
        );
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
        boolean supportFastInsertIntoValues
                = InsertUtils.supportFastInsertIntoValues(logicalPlan, targetTableIf, connectContext);
        return Optional.of(new FastInsertIntoValuesPlanner(ctx, supportFastInsertIntoValues));
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

    @Override
    public RedirectStatus toRedirectStatus() {
        if (ConnectContext.get().isGroupCommit()) {
            return RedirectStatus.NO_FORWARD;
        } else {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }
    }

    @Override
    public boolean needAuditEncryption() {
        return originLogicalQuery.anyMatch(node -> node instanceof TVFRelation);
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
}
