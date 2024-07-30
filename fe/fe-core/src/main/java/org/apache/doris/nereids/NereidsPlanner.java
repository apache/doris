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
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.mysql.FieldInfo;
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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.distribute.DistributePlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.SessionVariable;

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

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {
    public static final Logger LOG = LogManager.getLogger(NereidsPlanner.class);
    private CascadesContext cascadesContext;
    private final StatementContext statementContext;
    private final List<ScanNode> scanNodeList = Lists.newArrayList();
    private final List<PhysicalRelation> physicalRelations = Lists.newArrayList();
    private DescriptorTable descTable;

    private Plan parsedPlan;
    private Plan analyzedPlan;
    private Plan rewrittenPlan;
    private Plan optimizedPlan;
    private PhysicalPlan physicalPlan;
    private FragmentIdMapping<DistributedPlan> distributedPlans;
    // The cost of optimized plan
    private double cost = 0;
    private LogicalPlanAdapter logicalPlanAdapter;

    public NereidsPlanner(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    @Override
    public void plan(StatementBase queryStmt, org.apache.doris.thrift.TQueryOptions queryOptions) throws UserException {
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
        Plan resultPlan = null;
        try {
            boolean showPlanProcess = showPlanProcess(queryStmt.getExplainOptions());
            resultPlan = plan(parsedPlan, requireProperties, explainLevel, showPlanProcess);
        } finally {
            statementContext.getStopwatch().stop();
        }
        setOptimizedPlan(resultPlan);

        if (resultPlan instanceof PhysicalPlan) {
            physicalPlan = (PhysicalPlan) resultPlan;
            distribute(physicalPlan, explainLevel);
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
        this.statementContext.getPlannerHooks().forEach(hook -> hook.afterAnalyze(this));
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

    private void splitFragments(PhysicalPlan resultPlan) throws UserException {
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
        physicalRelations.addAll(planTranslatorContext.getPhysicalRelations());
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

        // update scan nodes visible version at the end of plan phase.
        ScanNode.setVisibleVersionForOlapScanNodes(getScanNodes());
    }

    private void distribute(PhysicalPlan physicalPlan, ExplainLevel explainLevel) throws UserException {
        boolean canUseNereidsDistributePlanner = SessionVariable.canUseNereidsDistributePlanner();
        if ((!canUseNereidsDistributePlanner && explainLevel.isPlanLevel)) {
            return;
        } else if ((canUseNereidsDistributePlanner && explainLevel.isPlanLevel
                && (explainLevel != ExplainLevel.ALL_PLAN && explainLevel != ExplainLevel.DISTRIBUTED_PLAN))) {
            return;
        }

        splitFragments(physicalPlan);

        if (!canUseNereidsDistributePlanner) {
            return;
        }

        distributedPlans = new DistributePlanner(fragments).plan();
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile().setNereidsDistributeTime();
        }
    }

    private PhysicalPlan postProcess(PhysicalPlan physicalPlan) {
        return new PlanPostProcessors(cascadesContext).process(physicalPlan);
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return scanNodeList;
    }

    public List<PhysicalRelation> getPhysicalRelations() {
        return physicalRelations;
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
            if (rootGroup.getEnforcers().containsKey(groupExpression)) {
                rootGroup.addChosenEnforcerId(groupExpression.getId().asInt());
                rootGroup.addChosenEnforcerProperties(physicalProperties);
            } else {
                rootGroup.setChosenProperties(physicalProperties);
                rootGroup.setChosenGroupExpressionId(groupExpression.getId().asInt());
            }
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
                StringBuilder materializationStringBuilder = new StringBuilder();
                materializationStringBuilder.append("materializationContexts:").append("\n");
                for (MaterializationContext ctx : cascadesContext.getMaterializationContexts()) {
                    materializationStringBuilder.append("\n").append(ctx).append("\n");
                }
                plan = cascadesContext.getMemo().toString()
                        + "\n\n========== OPTIMIZED PLAN ==========\n"
                        + optimizedPlan.treeString()
                        + "\n\n========== MATERIALIZATIONS ==========\n"
                        + materializationStringBuilder;
                break;
            case DISTRIBUTED_PLAN:
                StringBuilder distributedPlanStringBuilder = new StringBuilder();

                distributedPlanStringBuilder.append("========== DISTRIBUTED PLAN ==========\n");
                if (distributedPlans == null || distributedPlans.isEmpty()) {
                    plan = "Distributed plan not generated, please set enable_nereids_distribute_planner "
                            + "and enable_pipeline_x_engine to true";
                } else {
                    plan += DistributedPlan.toString(Lists.newArrayList(distributedPlans.values())) + "\n\n";
                }
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
                        + optimizedPlan.treeString() + "\n\n";

                if (distributedPlans != null && !distributedPlans.isEmpty()) {
                    plan += "========== DISTRIBUTED PLAN "
                            + getTimeMetricString(SummaryProfile::getPrettyNereidsDistributeTime) + " ==========\n";
                    plan += DistributedPlan.toString(Lists.newArrayList(distributedPlans.values())) + "\n\n";
                }
                break;
            default:
                plan = super.getExplainString(explainOptions)
                        + MaterializationContext.toSummaryString(cascadesContext.getMaterializationContexts(),
                        this.getPhysicalPlan());
                if (statementContext != null) {
                    if (statementContext.isHasUnknownColStats()) {
                        plan += "\n\nStatistics\n planed with unknown column statistics\n";
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

        setFormatOptions();
        if (physicalPlan instanceof ComputeResultSet) {
            Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
            Optional<ResultSet> resultSet = ((ComputeResultSet) physicalPlan)
                    .computeResultInFe(cascadesContext, sqlCacheContext);
            if (resultSet.isPresent()) {
                return resultSet;
            }
        }

        if (physicalPlan instanceof PhysicalResultSink
                && physicalPlan.child(0) instanceof PhysicalHashAggregate && !getScanNodes().isEmpty()
                && getScanNodes().get(0) instanceof IcebergScanNode) {
            List<Column> columns = Lists.newArrayList();
            NamedExpression output = physicalPlan.getOutput().get(0);
            columns.add(new Column(output.getName(), output.getDataType().toCatalogDataType()));
            if (((IcebergScanNode) getScanNodes().get(0)).rowCount > 0) {
                ResultSetMetaData metadata = new CommonResultSet.CommonResultSetMetaData(columns);
                ResultSet resultSet = new CommonResultSet(metadata, Collections.singletonList(
                        Lists.newArrayList(String.valueOf(((IcebergScanNode) getScanNodes().get(0)).rowCount))));
                // only support one iceberg scan node and one count, e.g. select count(*) from icetbl;
                return Optional.of(resultSet);
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
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

    public FragmentIdMapping<DistributedPlan> getDistributedPlans() {
        return distributedPlans;
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
        return explainOptions == null ? false : explainOptions.showPlanProcess();
    }

    private void keepOrShowPlanProcess(boolean showPlanProcess, Runnable task) {
        if (showPlanProcess) {
            cascadesContext.withPlanProcess(showPlanProcess, task);
        } else {
            task.run();
        }
    }

    @Override
    public List<TopnFilter> getTopnFilters() {
        return cascadesContext.getTopnFilterContext().getTopnFilters();
    }
}
