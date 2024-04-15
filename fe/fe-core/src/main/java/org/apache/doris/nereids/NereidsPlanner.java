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
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.CascadesContext.Lock;
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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private List<PlannerHook> hooks = new ArrayList<>();

    public NereidsPlanner(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    @Override
    public void plan(StatementBase queryStmt, org.apache.doris.thrift.TQueryOptions queryOptions) throws UserException {
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
        statementContext.getStopwatch().start();
        boolean showPlanProcess = showPlanProcess(queryStmt.getExplainOptions());
        Plan resultPlan = plan(parsedPlan, requireProperties, explainLevel, showPlanProcess);
        statementContext.getStopwatch().stop();
        setOptimizedPlan(resultPlan);
        if (explainLevel.isPlanLevel) {
            return;
        }
        physicalPlan = (PhysicalPlan) resultPlan;
        translate(physicalPlan);
    }

    @VisibleForTesting
    public void plan(StatementBase queryStmt) {
        try {
            plan(queryStmt, statementContext.getConnectContext().getSessionVariable().toThrift());
        } catch (Exception e) {
            throw new NereidsException(e.getMessage(), e);
        }
    }

    public PhysicalPlan plan(LogicalPlan plan, PhysicalProperties outputProperties) {
        return (PhysicalPlan) plan(plan, outputProperties, ExplainLevel.NONE, false);
    }

    public Plan plan(LogicalPlan plan, PhysicalProperties requireProperties, ExplainLevel explainLevel) {
        return plan(plan, requireProperties, explainLevel, false);
    }

    /**
     * Do analyze and optimize for query plan.
     *
     * @param plan wait for plan
     * @param requireProperties request physical properties constraints
     * @return plan generated by this planner
     * @throws AnalysisException throw exception if failed in ant stage
     */
    public Plan plan(LogicalPlan plan, PhysicalProperties requireProperties,
                ExplainLevel explainLevel, boolean showPlanProcess) {
        try {
            if (plan instanceof LogicalSqlCache) {
                rewrittenPlan = analyzedPlan = plan;
                LogicalSqlCache logicalSqlCache = (LogicalSqlCache) plan;
                physicalPlan = new PhysicalSqlCache(
                        logicalSqlCache.getQueryId(), logicalSqlCache.getColumnLabels(),
                        logicalSqlCache.getResultExprs(), logicalSqlCache.getCacheValues(),
                        logicalSqlCache.getBackendAddress(), logicalSqlCache.getPlanBody()
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

            try (Lock lock = new Lock(plan, cascadesContext)) {
                // resolve column, table and function
                // analyze this query
                analyze(showAnalyzeProcess(explainLevel, showPlanProcess));
                // minidump of input must be serialized first, this process ensure minidump string not null
                try {
                    MinidumpUtils.serializeInputsToDumpFile(plan, cascadesContext.getTables());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (statementContext.getConnectContext().getExecutor() != null) {
                    statementContext.getConnectContext().getExecutor().getSummaryProfile().setQueryAnalysisFinishTime();
                    statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsAnalysisTime();
                }

                if (explainLevel == ExplainLevel.ANALYZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                    analyzedPlan = cascadesContext.getRewritePlan();
                    if (explainLevel == ExplainLevel.ANALYZED_PLAN) {
                        return analyzedPlan;
                    }
                }

                // rule-based optimize
                rewrite(showRewriteProcess(explainLevel, showPlanProcess));
                if (statementContext.getConnectContext().getExecutor() != null) {
                    statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsRewriteTime();
                }

                if (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                    rewrittenPlan = cascadesContext.getRewritePlan();
                    if (explainLevel == ExplainLevel.REWRITTEN_PLAN) {
                        return rewrittenPlan;
                    }
                }

                optimize();
                if (statementContext.getConnectContext().getExecutor() != null) {
                    statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsOptimizeTime();
                }

                // print memo before choose plan.
                // if chooseNthPlan failed, we could get memo to debug
                if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
                    String memo = cascadesContext.getMemo().toString();
                    LOG.info(ConnectContext.get().getQueryIdentifier() + "\n" + memo);
                }

                int nth = cascadesContext.getConnectContext().getSessionVariable().getNthOptimizedPlan();
                PhysicalPlan physicalPlan = chooseNthPlan(getRoot(), requireProperties, nth);

                physicalPlan = postProcess(physicalPlan);
                if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
                    String tree = physicalPlan.treeString();
                    LOG.info(ConnectContext.get().getQueryIdentifier() + "\n" + tree);
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
        } finally {
            statementContext.releasePlannerResources();
        }
    }

    private LogicalPlan preprocess(LogicalPlan logicalPlan) {
        return new PlanPreprocessors(statementContext).process(logicalPlan);
    }

    private void initCascadesContext(LogicalPlan plan, PhysicalProperties requireProperties) {
        cascadesContext = CascadesContext.initContext(statementContext, plan, requireProperties);
        if (statementContext.getConnectContext().getTables() != null) {
            cascadesContext.setTables(statementContext.getConnectContext().getTables());
        }
    }

    private void analyze(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start analyze plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newAnalyzer().analyze());
        getHooks().forEach(hook -> hook.afterAnalyze(this));
        NereidsTracer.logImportantTime("EndAnalyzePlan");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End analyze plan");
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
    }

    private void translate(PhysicalPlan resultPlan) throws UserException {
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
        ArrayList<String> columnLabelList = physicalPlan.getOutput().stream().map(NamedExpression::getName)
                .collect(Collectors.toCollection(ArrayList::new));
        logicalPlanAdapter.setColLabels(columnLabelList);
        logicalPlanAdapter.setViewDdlSqls(statementContext.getViewDdlSqls());
        if (statementContext.getSqlCacheContext().isPresent()) {
            SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().get();
            sqlCacheContext.setColLabels(columnLabelList);
            sqlCacheContext.setResultExprs(root.getOutputExprs());
            sqlCacheContext.setPhysicalPlan(resultPlan.treeString());
        }

        cascadesContext.releaseMemo();

        // update scan nodes visible version at the end of plan phase.
        ScanNode.setVisibleVersionForOlapScanNodes(getScanNodes());
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
                    groupExpression.getOutputProperties(physicalProperties),
                    groupExpression.getOwnerGroup().getStatistics());
            return physicalPlan;
        } catch (Exception e) {
            if (e instanceof AnalysisException && e.getMessage().contains("Failed to choose best plan")) {
                throw e;
            }
            LOG.warn("Failed to choose best plan, memo structure:{}", cascadesContext.getMemo(), e);
            throw new AnalysisException("Failed to choose best plan: " + e.getMessage(), e);
        }
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
                plan = "cost = " + cost + "\n" + optimizedPlan.treeString();
                break;
            case SHAPE_PLAN:
                plan = optimizedPlan.shape("");
                break;
            case MEMO_PLAN:
                plan = cascadesContext.getMemo().toString()
                        + "\n\n========== OPTIMIZED PLAN ==========\n"
                        + optimizedPlan.treeString()
                        + "\n\n========== MATERIALIZATIONS ==========\n"
                        + MaterializationContext.toDetailString(cascadesContext.getMaterializationContexts());
                break;
            case ALL_PLAN:
                plan = "========== PARSED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyParseSqlTime) + " ==========\n"
                        + parsedPlan.treeString() + "\n\n"
                        + "========== ANALYZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsAnalysisTime) + " ==========\n"
                        + analyzedPlan.treeString() + "\n\n"
                        + "========== REWRITTEN PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsRewriteTime) + " ==========\n"
                        + rewrittenPlan.treeString() + "\n\n"
                        + "========== OPTIMIZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsOptimizeTime) + " ==========\n"
                        + optimizedPlan.treeString();
                break;
            default:
                List<MTMV> materializationListChosenByCbo = this.getPhysicalPlan()
                        .collectToList(node -> node instanceof PhysicalCatalogRelation
                                && ((PhysicalCatalogRelation) node).getTable() instanceof MTMV).stream()
                        .map(node -> (MTMV) ((PhysicalCatalogRelation) node).getTable())
                        .collect(Collectors.toList());
                plan = super.getExplainString(explainOptions)
                        + MaterializationContext.toSummaryString(cascadesContext.getMaterializationContexts(),
                        materializationListChosenByCbo);
        }
        if (statementContext != null && !statementContext.getHints().isEmpty()) {
            String hint = getHintExplainString(statementContext.getHints());
            return plan + hint;
        }
        return plan;
    }

    @Override
    public boolean isBlockQuery() {
        return true;
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
        if (!(physicalPlan instanceof PhysicalResultSink)) {
            return Optional.empty();
        }
        if (!(((PhysicalResultSink<?>) physicalPlan).child() instanceof PhysicalOneRowRelation)) {
            return Optional.empty();
        }
        PhysicalOneRowRelation physicalOneRowRelation
                = (PhysicalOneRowRelation) ((PhysicalResultSink<?>) physicalPlan).child();
        List<Column> columns = Lists.newArrayList();
        List<String> data = Lists.newArrayList();
        for (int i = 0; i < physicalOneRowRelation.getProjects().size(); i++) {
            NamedExpression item = physicalOneRowRelation.getProjects().get(i);
            NamedExpression output = physicalPlan.getOutput().get(i);
            Expression expr = item.child(0);
            if (expr instanceof Literal) {
                LiteralExpr legacyExpr = ((Literal) expr).toLegacyLiteral();
                columns.add(new Column(output.getName(), output.getDataType().toCatalogDataType()));
                data.add(legacyExpr.getStringValueInFe());
            } else {
                return Optional.empty();
            }
        }
        ResultSetMetaData metadata = new CommonResultSet.CommonResultSetMetaData(columns);
        ResultSet resultSet = new CommonResultSet(metadata, Collections.singletonList(data));
        return Optional.of(resultSet);
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

    public List<PlannerHook> getHooks() {
        return hooks;
    }

    public void addHook(PlannerHook hook) {
        this.hooks.add(hook);
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
        return explainOptions == null ? false : explainOptions.showPlanProcess();
    }

    private void keepOrShowPlanProcess(boolean showPlanProcess, Runnable task) {
        if (showPlanProcess) {
            cascadesContext.withPlanProcess(showPlanProcess, task);
        } else {
            task.run();
        }
    }
}
