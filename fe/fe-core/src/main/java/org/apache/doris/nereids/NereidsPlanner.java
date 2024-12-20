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

package org.apache.doris.nereids;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.pre.PlanPreprocessors;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {
    public static final Logger LOG = LogManager.getLogger(NereidsPlanner.class);
    private CascadesContext cascadesContext;
    private final StatementContext statementContext;
    private final List<ScanNode> scanNodeList = Lists.newArrayList();
    private DescriptorTable descTable;

    private Plan parsedPlan;
    private Plan analyzedPlan;
    private Plan rewrittenPlan;
    private Plan optimizedPlan;
    private PhysicalPlan physicalPlan;
    // The cost of optimized plan
    private double cost = 0;
    private LogicalPlanAdapter logicalPlanAdapter;

    public NereidsPlanner(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    @Override
    public void plan(StatementBase queryStmt, org.apache.doris.thrift.TQueryOptions queryOptions) {
        this.queryOptions = queryOptions;
        if (statementContext.getConnectContext().getSessionVariable().isEnableNereidsTrace()) {
            NereidsTracer.init();
        } else {
            NereidsTracer.disable();
        }
        if (!(queryStmt instanceof LogicalPlanAdapter)) {
            throw new RuntimeException("Wrong type of queryStmt, expected: <? extends LogicalPlanAdapter>");
        }

        logicalPlanAdapter = (LogicalPlanAdapter) queryStmt;

        ExplainLevel explainLevel = getExplainLevel(queryStmt.getExplainOptions());

        LogicalPlan parsedPlan = logicalPlanAdapter.getLogicalPlan();
        NereidsTracer.logImportantTime("EndParsePlan");
        setParsedPlan(parsedPlan);
        PhysicalProperties requireProperties = buildInitRequireProperties();
        statementContext.getStopwatch().reset().start();
        try {
            boolean showPlanProcess = showPlanProcess(queryStmt.getExplainOptions());
            planWithLock(parsedPlan, requireProperties, explainLevel, showPlanProcess, plan -> {
                setOptimizedPlan(plan);
                if (explainLevel.isPlanLevel) {
                    return;
                }
                physicalPlan = (PhysicalPlan) plan;
                translate(physicalPlan);
            });
        } finally {
            statementContext.getStopwatch().stop();
        }
    }

    @VisibleForTesting
    public void plan(StatementBase queryStmt) {
        try {
            plan(queryStmt, statementContext.getConnectContext().getSessionVariable().toThrift());
        } catch (Exception e) {
            throw new NereidsException(e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public PhysicalPlan planWithLock(LogicalPlan plan, PhysicalProperties outputProperties) {
        return (PhysicalPlan) planWithLock(plan, outputProperties, ExplainLevel.NONE, false);
    }

    // TODO check all caller
    public Plan planWithLock(LogicalPlan plan, PhysicalProperties requireProperties, ExplainLevel explainLevel) {
        return planWithLock(plan, requireProperties, explainLevel, false);
    }

    @VisibleForTesting
    public Plan planWithLock(LogicalPlan plan, PhysicalProperties requireProperties,
            ExplainLevel explainLevel, boolean showPlanProcess) {
        Consumer<Plan> noCallback = p -> {};
        return planWithLock(plan, requireProperties, explainLevel, showPlanProcess, noCallback);
    }

    /**
     * Do analyze and optimize for query plan.
     *
     * @param plan wait for plan
     * @param requireProperties request physical properties constraints
     * @param showPlanProcess is record plan process to CascadesContext
     * @param lockCallback this callback function will invoke the table lock
     * @return plan generated by this planner
     * @throws AnalysisException throw exception if failed in ant stage
     */
    private Plan planWithLock(LogicalPlan plan, PhysicalProperties requireProperties,
            ExplainLevel explainLevel, boolean showPlanProcess, Consumer<Plan> lockCallback) {
        try {
            long beforePlanGcTime = getGarbageCollectionTime();
            if (plan instanceof LogicalSqlCache) {
                rewrittenPlan = analyzedPlan = plan;
                LogicalSqlCache logicalSqlCache = (LogicalSqlCache) plan;
                optimizedPlan = physicalPlan = new PhysicalSqlCache(
                        logicalSqlCache.getQueryId(),
                        logicalSqlCache.getColumnLabels(), logicalSqlCache.getFieldInfos(),
                        logicalSqlCache.getResultExprs(), logicalSqlCache.getResultSetInFe(),
                        logicalSqlCache.getCacheValues(), logicalSqlCache.getBackendAddress(),
                        logicalSqlCache.getPlanBody()
                );
                return physicalPlan;
            }
            if (explainLevel == ExplainLevel.PARSED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                parsedPlan = plan;
                if (explainLevel == ExplainLevel.PARSED_PLAN) {
                    return parsedPlan;
                }
            }

            // pre-process logical plan out of memo, e.g. process SET_VAR hint
            plan = preprocess(plan);

            initCascadesContext(plan, requireProperties);
            // collect table and lock them in the order of table id
            collectAndLockTable(showAnalyzeProcess(explainLevel, showPlanProcess));
            // after table collector, we should use a new context.
            statementContext.loadSnapshots();
            Plan resultPlan = planWithoutLock(plan, requireProperties, explainLevel, showPlanProcess);
            lockCallback.accept(resultPlan);
            if (statementContext.getConnectContext().getExecutor() != null) {
                statementContext.getConnectContext().getExecutor().getSummaryProfile()
                        .setNereidsGarbageCollectionTime(getGarbageCollectionTime() - beforePlanGcTime);
            }
            return resultPlan;
        } finally {
            statementContext.releasePlannerResources();
        }
    }

    /**
     * do plan but not lock any table
     */
    private Plan planWithoutLock(
            LogicalPlan plan, PhysicalProperties requireProperties, ExplainLevel explainLevel,
            boolean showPlanProcess) {
        // minidump of input must be serialized first, this process ensure minidump string not null
        try {

            MinidumpUtils.serializeInputsToDumpFile(plan, statementContext);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // analyze this query, resolve column, table and function
        analyze(showAnalyzeProcess(explainLevel, showPlanProcess));
        if (explainLevel == ExplainLevel.ANALYZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
            analyzedPlan = cascadesContext.getRewritePlan();
            if (explainLevel == ExplainLevel.ANALYZED_PLAN) {
                return analyzedPlan;
            }
        }

        // rule-based optimize
        rewrite(showRewriteProcess(explainLevel, showPlanProcess));
        if (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
            rewrittenPlan = cascadesContext.getRewritePlan();
            if (explainLevel == ExplainLevel.REWRITTEN_PLAN) {
                return rewrittenPlan;
            }
        }

        optimize();
        // print memo before choose plan.
        // if chooseNthPlan failed, we could get memo to debug
        if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
            String memo = cascadesContext.getMemo().toString();
            LOG.info("{}\n{}", ConnectContext.get().getQueryIdentifier(), memo);
        }

        int nth = cascadesContext.getConnectContext().getSessionVariable().getNthOptimizedPlan();
        PhysicalPlan physicalPlan = chooseNthPlan(getRoot(), requireProperties, nth);

        physicalPlan = postProcess(physicalPlan);
        if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
            String tree = physicalPlan.treeString();
            LOG.info("{}\n{}", ConnectContext.get().getQueryIdentifier(), tree);
        }
        if (explainLevel == ExplainLevel.OPTIMIZED_PLAN
                || explainLevel == ExplainLevel.ALL_PLAN
                || explainLevel == ExplainLevel.SHAPE_PLAN) {
            optimizedPlan = physicalPlan;
        }
        // serialize optimized plan to dumpfile, dumpfile do not have this part means optimize failed
        MinidumpUtils.serializeOutputToDumpFile(physicalPlan);
        NereidsTracer.output(statementContext.getConnectContext());

        return physicalPlan;
    }

    private LogicalPlan preprocess(LogicalPlan logicalPlan) {
        return new PlanPreprocessors(statementContext).process(logicalPlan);
    }

    /**
     * compute rf wait time according to max table row count, if wait time is not default value
     *     olap:
     *     row < 1G: 1 sec
     *     1G <= row < 10G: 5 sec
     *     10G < row: 20 sec
     *     external:
     *     row < 1G: 5 sec
     *     1G <= row < 10G: 10 sec
     *     10G < row: 50 sec
     */
    private void setRuntimeFilterWaitTimeByTableRowCountAndType() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().getRuntimeFilterWaitTimeMs()
                != VariableMgr.getDefaultSessionVariable().getRuntimeFilterWaitTimeMs()) {
            List<LogicalCatalogRelation> scans = cascadesContext.getRewritePlan()
                    .collectToList(LogicalCatalogRelation.class::isInstance);
            double maxRow = StatsCalculator.getMaxTableRowCount(scans, cascadesContext);
            boolean hasExternalTable = scans.stream().anyMatch(scan -> !(scan instanceof LogicalOlapScan));
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (hasExternalTable) {
                if (maxRow < 1_000_000_000L) {
                    sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "5000");
                } else if (maxRow < 10_000_000_000L) {
                    sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "20000");
                } else {
                    sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "50000");
                }
            } else {
                if (maxRow < 1_000_000_000L) {
                    sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "1000");
                } else if (maxRow < 10_000_000_000L) {
                    sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "5000");
                } else {
                    sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "20000");
                }
            }
        }
    }

    private void initCascadesContext(LogicalPlan plan, PhysicalProperties requireProperties) {
        cascadesContext = CascadesContext.initContext(statementContext, plan, requireProperties);
    }

    protected void collectAndLockTable(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start collect and lock table");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newTableCollector().collect());
        statementContext.lock();
        cascadesContext.setCteContext(new CTEContext());
        NereidsTracer.logImportantTime("EndCollectAndLockTables");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End collect and lock table");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsLockTableFinishTime();
        }
    }

    private void analyze(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start analyze plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newAnalyzer().analyze());
        this.statementContext.getPlannerHooks().forEach(hook -> hook.afterAnalyze(this));
        NereidsTracer.logImportantTime("EndAnalyzePlan");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End analyze plan");
        }

        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setQueryAnalysisFinishTime();
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsAnalysisTime();
        }
    }

    /**
     * Logical plan rewrite based on a series of heuristic rules.
     */
    private void rewrite(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start rewrite plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> Rewriter.getWholeTreeRewriter(cascadesContext).execute());
        NereidsTracer.logImportantTime("EndRewritePlan");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End rewrite plan");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsRewriteTime();
        }
    }

    // DependsRules: EnsureProjectOnTopJoin.class
    private void optimize() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start optimize plan");
        }
        new Optimizer(cascadesContext).execute();
        NereidsTracer.logImportantTime("EndOptimizePlan");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End optimize plan");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsOptimizeTime();
        }
    }

    private void translate(PhysicalPlan resultPlan) {
        if (resultPlan instanceof PhysicalSqlCache) {
            return;
        }

        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator(planTranslatorContext,
                statementContext.getConnectContext().getStatsErrorEstimator());
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsTranslateTime();
        }
        if (cascadesContext.getConnectContext().getSessionVariable().isEnableNereidsTrace()) {
            CounterEvent.clearCounter();
        }
        if (cascadesContext.getConnectContext().getSessionVariable().isPlayNereidsDump()) {
            return;
        }
        PlanFragment root = physicalPlanTranslator.translatePlan(physicalPlan);

        scanNodeList.addAll(planTranslatorContext.getScanNodes());
        descTable = planTranslatorContext.getDescTable();
        fragments = new ArrayList<>(planTranslatorContext.getPlanFragments());
        for (int seq = 0; seq < fragments.size(); seq++) {
            fragments.get(seq).setFragmentSequenceNum(seq);
        }
        // set output exprs
        logicalPlanAdapter.setResultExprs(root.getOutputExprs());
        ArrayList<String> columnLabels = Lists.newArrayListWithExpectedSize(physicalPlan.getOutput().size());
        List<FieldInfo> fieldInfos = Lists.newArrayListWithExpectedSize(physicalPlan.getOutput().size());
        for (NamedExpression output : physicalPlan.getOutput()) {
            Optional<Column> column = Optional.empty();
            Optional<TableIf> table = Optional.empty();
            if (output instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) output;
                column = slotReference.getColumn();
                table = slotReference.getTable();
            }
            columnLabels.add(output.getName());
            FieldInfo fieldInfo = new FieldInfo(
                    table.isPresent() ? (table.get().getDatabase() != null
                            ? table.get().getDatabase().getFullName() : "") : "",
                    !output.getQualifier().isEmpty() ? output.getQualifier().get(output.getQualifier().size() - 1)
                            : (table.isPresent() ? table.get().getName() : ""),
                    table.isPresent() ? table.get().getName() : "",
                    output.getName(),
                    column.isPresent() ? column.get().getName() : ""
            );
            fieldInfos.add(fieldInfo);
        }
        logicalPlanAdapter.setColLabels(columnLabels);
        logicalPlanAdapter.setFieldInfos(fieldInfos);
        logicalPlanAdapter.setViewDdlSqls(statementContext.getViewDdlSqls());
        if (statementContext.getSqlCacheContext().isPresent()) {
            SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().get();
            sqlCacheContext.setColLabels(columnLabels);
            sqlCacheContext.setFieldInfos(fieldInfos);
            sqlCacheContext.setResultExprs(root.getOutputExprs());
            sqlCacheContext.setPhysicalPlan(resultPlan.treeString());
        }

        cascadesContext.releaseMemo();
    }

    private PhysicalPlan postProcess(PhysicalPlan physicalPlan) {
        return new PlanPostProcessors(cascadesContext).process(physicalPlan);
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return scanNodeList;
    }

    public Group getRoot() {
        return cascadesContext.getMemo().getRoot();
    }

    private PhysicalPlan chooseNthPlan(Group rootGroup, PhysicalProperties physicalProperties, int nthPlan) {
        if (nthPlan <= 1) {
            cost = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                    () -> new AnalysisException("lowestCostPlans with physicalProperties("
                            + physicalProperties + ") doesn't exist in root group")).first.getValue();
            return chooseBestPlan(rootGroup, physicalProperties);
        }
        Memo memo = cascadesContext.getMemo();

        Pair<Long, Double> idCost = memo.rank(nthPlan);
        cost = idCost.second;
        return memo.unrank(idCost.first);
    }

    private PhysicalPlan chooseBestPlan(Group rootGroup, PhysicalProperties physicalProperties)
            throws AnalysisException {
        try {
            GroupExpression groupExpression = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                    () -> new AnalysisException("lowestCostPlans with physicalProperties("
                            + physicalProperties + ") doesn't exist in root group")).second;
            List<PhysicalProperties> inputPropertiesList = groupExpression.getInputPropertiesList(physicalProperties);
            List<Plan> planChildren = Lists.newArrayList();
            for (int i = 0; i < groupExpression.arity(); i++) {
                planChildren.add(chooseBestPlan(groupExpression.child(i), inputPropertiesList.get(i)));
            }

            Plan plan = groupExpression.getPlan().withChildren(planChildren);
            if (!(plan instanceof PhysicalPlan)) {
                // TODO need add some log
                throw new AnalysisException("Result plan must be PhysicalPlan");
            }
            // add groupExpression to plan so that we could print group id in plan.treeString()
            plan = plan.withGroupExpression(Optional.of(groupExpression));
            PhysicalPlan physicalPlan = ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                    physicalProperties, groupExpression.getOwnerGroup().getStatistics());
            return physicalPlan;
        } catch (Exception e) {
            if (e instanceof AnalysisException && e.getMessage().contains("Failed to choose best plan")) {
                throw e;
            }
            LOG.warn("Failed to choose best plan, memo structure:{}", cascadesContext.getMemo(), e);
            throw new AnalysisException("Failed to choose best plan: " + e.getMessage(), e);
        }
    }

    private long getGarbageCollectionTime() {
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long initialGCTime = 0;
        for (GarbageCollectorMXBean gcBean : gcMxBeans) {
            initialGCTime += gcBean.getCollectionTime();
        }
        return initialGCTime;
    }

    /**
     * getting hints explain string, which specified by enumerate and show in lists
     * @param hints hint map recorded in statement context
     * @return explain string shows using of hint
     */
    public String getHintExplainString(List<Hint> hints) {
        String used = "";
        String unUsed = "";
        String syntaxError = "";
        int distributeHintIndex = 1;
        for (Hint hint : hints) {
            String distributeIndex = "";
            if (hint instanceof DistributeHint) {
                distributeHintIndex++;
                if (!hint.getExplainString().equals("")) {
                    distributeIndex = "_" + distributeHintIndex;
                }
            }
            switch (hint.getStatus()) {
                case UNUSED:
                    unUsed = unUsed + " " + hint.getExplainString() + distributeIndex;
                    break;
                case SYNTAX_ERROR:
                    syntaxError = syntaxError + " " + hint.getExplainString() + distributeIndex
                        + " Msg:" + hint.getErrorMessage();
                    break;
                case SUCCESS:
                    used = used + " " + hint.getExplainString() + distributeIndex;
                    break;
                default:
                    break;
            }
        }
        return "\nHint log:" + "\nUsed:" + used + "\nUnUsed:" + unUsed + "\nSyntaxError:" + syntaxError;
    }

    @Override
    public String getExplainString(ExplainOptions explainOptions) {
        ExplainLevel explainLevel = getExplainLevel(explainOptions);
        String plan = "";
        String mvSummary = "";
        if ((this.getPhysicalPlan() != null || this.getOptimizedPlan() != null) && cascadesContext != null) {
            mvSummary = cascadesContext.getMaterializationContexts().isEmpty() ? "" :
                    "\n\n========== MATERIALIZATIONS ==========\n"
                            + MaterializationContext.toSummaryString(cascadesContext.getMaterializationContexts(),
                            this.getPhysicalPlan() == null ? this.getOptimizedPlan() : this.getPhysicalPlan());
        }
        switch (explainLevel) {
            case PARSED_PLAN:
                plan = parsedPlan.treeString();
                break;
            case ANALYZED_PLAN:
                plan = analyzedPlan.treeString();
                break;
            case REWRITTEN_PLAN:
                plan = rewrittenPlan.treeString();
                break;
            case OPTIMIZED_PLAN:
                plan = "cost = " + cost + "\n" + optimizedPlan.treeString() + mvSummary;
                break;
            case SHAPE_PLAN:
                plan = optimizedPlan.shape("");
                break;
            case MEMO_PLAN:
                plan = cascadesContext.getMemo().toString()
                        + "\n\n========== OPTIMIZED PLAN ==========\n"
                        + optimizedPlan.treeString()
                        + mvSummary;
                break;
            case ALL_PLAN:
                plan = "========== PARSED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyParseSqlTime) + " ==========\n"
                        + parsedPlan.treeString() + "\n\n"
                        + "========== LOCK TABLE "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsLockTableTime) + " ==========\n"
                        + "========== ANALYZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsAnalysisTime) + " ==========\n"
                        + analyzedPlan.treeString() + "\n\n"
                        + "========== REWRITTEN PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsRewriteTime) + " ==========\n"
                        + rewrittenPlan.treeString() + "\n\n"
                        + "========== OPTIMIZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsOptimizeTime) + " ==========\n"
                        + optimizedPlan.treeString() + "\n\n";
                plan += mvSummary;
                break;
            default:
                plan = super.getExplainString(explainOptions);
                plan += mvSummary;
                if (statementContext != null) {
                    if (statementContext.isHasUnknownColStats()) {
                        plan += "\n\n\n========== STATISTICS ==========\n";
                        plan += "planed with unknown column statistics\n";
                    }
                }
        }

        if (statementContext != null) {
            if (!statementContext.getHints().isEmpty()) {
                String hint = getHintExplainString(statementContext.getHints());
                return plan + hint;
            }
        }
        return plan;
    }

    @Override
    public DescriptorTable getDescTable() {
        return descTable;
    }

    @Override
    public void appendTupleInfo(StringBuilder str) {
        str.append(descTable.getExplainString());
    }

    @Override
    public List<RuntimeFilter> getRuntimeFilters() {
        return cascadesContext.getRuntimeFilterContext().getLegacyFilters();
    }

    @Override
    public Optional<ResultSet> handleQueryInFe(StatementBase parsedStmt) {
        if (!(parsedStmt instanceof LogicalPlanAdapter)) {
            return Optional.empty();
        }

        setFormatOptions();
        if (physicalPlan instanceof ComputeResultSet) {
            Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
            Optional<ResultSet> resultSet = ((ComputeResultSet) physicalPlan)
                    .computeResultInFe(cascadesContext, sqlCacheContext, physicalPlan.getOutput());
            if (resultSet.isPresent()) {
                return resultSet;
            }
        }

        return Optional.empty();
    }

    private void setFormatOptions() {
        ConnectContext ctx = statementContext.getConnectContext();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        switch (sessionVariable.serdeDialect) {
            case "presto":
            case "trino":
                statementContext.setFormatOptions(FormatOptions.getForPresto());
                break;
            case "doris":
                statementContext.setFormatOptions(FormatOptions.getDefault());
                break;
            default:
                throw new AnalysisException("Unsupported serde dialect: " + sessionVariable.serdeDialect);
        }
    }

    @VisibleForTesting
    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public static PhysicalProperties buildInitRequireProperties() {
        return PhysicalProperties.GATHER;
    }

    private ExplainLevel getExplainLevel(ExplainOptions explainOptions) {
        if (explainOptions == null) {
            return ExplainLevel.NONE;
        }
        ExplainLevel explainLevel = explainOptions.getExplainLevel();
        return explainLevel == null ? ExplainLevel.NONE : explainLevel;
    }

    @VisibleForTesting
    public Plan getParsedPlan() {
        return parsedPlan;
    }

    @VisibleForTesting
    public void setParsedPlan(Plan parsedPlan) {
        this.parsedPlan = parsedPlan;
    }

    @VisibleForTesting
    public void setOptimizedPlan(Plan optimizedPlan) {
        this.optimizedPlan = optimizedPlan;
    }

    @VisibleForTesting
    public Plan getAnalyzedPlan() {
        return analyzedPlan;
    }

    @VisibleForTesting
    public Plan getRewrittenPlan() {
        return rewrittenPlan;
    }

    @VisibleForTesting
    public Plan getOptimizedPlan() {
        return optimizedPlan;
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public LogicalPlanAdapter getLogicalPlanAdapter() {
        return logicalPlanAdapter;
    }

    private String getTimeMetricString(Function<SummaryProfile, String> profileSupplier) {
        return getProfile(summaryProfile -> {
            String metricString = profileSupplier.apply(summaryProfile);
            return (metricString == null || "N/A".equals(metricString)) ? "" : "(time: " + metricString + ")";
        }, "");
    }

    private <T> T getProfile(Function<SummaryProfile, T> profileSupplier, T defaultMetric) {
        T metric = null;
        if (statementContext.getConnectContext().getExecutor() != null) {
            SummaryProfile summaryProfile = statementContext.getConnectContext().getExecutor().getSummaryProfile();
            if (summaryProfile != null) {
                metric = profileSupplier.apply(summaryProfile);
            }
        }
        return metric == null ? defaultMetric : metric;
    }

    private boolean showAnalyzeProcess(ExplainLevel explainLevel, boolean showPlanProcess) {
        return showPlanProcess
                && (explainLevel == ExplainLevel.ANALYZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN);
    }

    private boolean showRewriteProcess(ExplainLevel explainLevel, boolean showPlanProcess) {
        return showPlanProcess
                && (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN);
    }

    private boolean showPlanProcess(ExplainOptions explainOptions) {
        return explainOptions != null && explainOptions.showPlanProcess();
    }

    private void keepOrShowPlanProcess(boolean showPlanProcess, Runnable task) {
        if (showPlanProcess) {
            cascadesContext.withPlanProcess(showPlanProcess, task);
        } else {
            task.run();
        }
    }
}
