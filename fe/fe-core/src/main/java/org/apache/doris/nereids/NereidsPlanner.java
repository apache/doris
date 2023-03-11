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
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext.Lock;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.batch.CascadesOptimizer;
import org.apache.doris.nereids.jobs.batch.NereidsRewriter;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.pre.PlanPreprocessors;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {
    public static final Logger LOG = LogManager.getLogger(NereidsPlanner.class);
    private CascadesContext cascadesContext;
    private final StatementContext statementContext;
    private List<ScanNode> scanNodeList = null;
    private DescriptorTable descTable;

    private Plan parsedPlan;
    private Plan analyzedPlan;
    private Plan rewrittenPlan;
    private Plan optimizedPlan;
    // The cost of optimized plan
    private double cost = 0;

    public NereidsPlanner(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    @Override
    public void plan(StatementBase queryStmt, org.apache.doris.thrift.TQueryOptions queryOptions) {
        if (!(queryStmt instanceof LogicalPlanAdapter)) {
            throw new RuntimeException("Wrong type of queryStmt, expected: <? extends LogicalPlanAdapter>");
        }

        LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) queryStmt;

        ExplainLevel explainLevel = getExplainLevel(queryStmt.getExplainOptions());

        LogicalPlan parsedPlan = logicalPlanAdapter.getLogicalPlan();
        PhysicalProperties requireProperties = buildInitRequireProperties(parsedPlan);
        Plan resultPlan = plan(parsedPlan, requireProperties, explainLevel);
        if (explainLevel.isPlanLevel) {
            return;
        }
        PhysicalPlan physicalPlan = (PhysicalPlan) resultPlan;
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator(planTranslatorContext,
                ConnectContext.get().getStatsErrorEstimator());
        if (ConnectContext.get().getSessionVariable().isEnableNereidsTrace()) {
            CounterEvent.clearCounter();
        }
        PlanFragment root = physicalPlanTranslator.translatePlan(physicalPlan);

        scanNodeList = planTranslatorContext.getScanNodes();
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
        return (PhysicalPlan) plan(plan, outputProperties, ExplainLevel.NONE);
    }

    /**
     * Do analyze and optimize for query plan.
     *
     * @param plan wait for plan
     * @param requireProperties request physical properties constraints
     * @return plan generated by this planner
     * @throws AnalysisException throw exception if failed in ant stage
     */
    public Plan plan(LogicalPlan plan, PhysicalProperties requireProperties, ExplainLevel explainLevel) {
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
            analyze();
            if (explainLevel == ExplainLevel.ANALYZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                analyzedPlan = cascadesContext.getRewritePlan();
                if (explainLevel == ExplainLevel.ANALYZED_PLAN) {
                    return analyzedPlan;
                }
            }
            // rule-based optimize
            rewrite();
            if (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                rewrittenPlan = cascadesContext.getRewritePlan();
                if (explainLevel == ExplainLevel.REWRITTEN_PLAN) {
                    return rewrittenPlan;
                }
            }

            initMemo();

            deriveStats();

            optimize();

            //print memo before choose plan.
            //if chooseNthPlan failed, we could get memo to debug
            if (ConnectContext.get().getSessionVariable().isDumpNereidsMemo()) {
                String memo = cascadesContext.getMemo().toString();
                LOG.info(memo);
            }

            int nth = ConnectContext.get().getSessionVariable().getNthOptimizedPlan();
            PhysicalPlan physicalPlan = chooseNthPlan(getRoot(), requireProperties, nth);

            physicalPlan = postProcess(physicalPlan);

            if (ConnectContext.get().getSessionVariable().isDumpNereidsMemo()) {
                String tree = physicalPlan.treeString();
                LOG.info(tree);
            }

            if (explainLevel == ExplainLevel.OPTIMIZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                optimizedPlan = physicalPlan;
            }

            return physicalPlan;
        }
    }

    private LogicalPlan preprocess(LogicalPlan logicalPlan) {
        return new PlanPreprocessors(statementContext).process(logicalPlan);
    }

    private void initCascadesContext(LogicalPlan plan, PhysicalProperties requireProperties) {
        cascadesContext = CascadesContext.newRewriteContext(statementContext, plan, requireProperties);
    }

    private void analyze() {
        cascadesContext.newAnalyzer().analyze();
    }

    /**
     * Logical plan rewrite based on a series of heuristic rules.
     */
    private void rewrite() {
        new NereidsRewriter(cascadesContext).execute();
    }

    private void initMemo() {
        cascadesContext.toMemo();
    }

    private void deriveStats() {
        cascadesContext.pushJob(
                new DeriveStatsJob(getRoot().getLogicalExpression(), cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    private void dpHypOptimize() {
        Group root = getRoot();
        if (root.isJoinGroup()) {
            // If the root group is join group, DPHyp can change the root group.
            // To keep the root group is not changed, we add a project operator above join
            List<Slot> outputs = root.getLogicalExpression().getPlan().getOutput();
            LogicalPlan plan = new LogicalProject(outputs, root.getLogicalExpression().getPlan());
            CopyInResult copyInResult = cascadesContext.getMemo().copyIn(plan, null, false);
            root = copyInResult.correspondingExpression.getOwnerGroup();
        }
        cascadesContext.pushJob(new JoinOrderJob(root, cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    /**
     * Cascades style optimize:
     * Perform equivalent logical plan exploration and physical implementation enumeration,
     * try to find best plan under the guidance of statistic information and cost model.
     */
    private void optimize() {
        if (!statementContext.getConnectContext().getSessionVariable().isDisableJoinReorder()
                && statementContext.getConnectContext().getSessionVariable().isEnableDPHypOptimizer()
                && statementContext.getMaxNAryInnerJoin() > statementContext.getConnectContext()
                .getSessionVariable().getMaxTableCountUseCascadesJoinReorder()) {
            dpHypOptimize();
        }
        new CascadesOptimizer(cascadesContext).execute();
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
                throw new AnalysisException("Result plan must be PhysicalPlan");
            }
            // add groupExpression to plan so that we could print group id in plan.treeString()
            plan = plan.withGroupExpression(Optional.of(groupExpression));
            PhysicalPlan physicalPlan = ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                    groupExpression.getOutputProperties(physicalProperties),
                    groupExpression.getOwnerGroup().getStatistics());
            return physicalPlan;
        } catch (Exception e) {
            String memo = cascadesContext.getMemo().toString();
            LOG.warn("Failed to choose best plan, memo structure:{}", memo, e);
            throw new AnalysisException("Failed to choose best plan", e);
        }
    }

    @Override
    public String getExplainString(ExplainOptions explainOptions) {
        ExplainLevel explainLevel = getExplainLevel(explainOptions);
        switch (explainLevel) {
            case PARSED_PLAN:
                return parsedPlan.treeString();
            case ANALYZED_PLAN:
                return analyzedPlan.treeString();
            case REWRITTEN_PLAN:
                return rewrittenPlan.treeString();
            case OPTIMIZED_PLAN:
                return "cost = " + cost + "\n" + optimizedPlan.treeString();
            case ALL_PLAN:
                return "========== PARSED PLAN ==========\n"
                        + parsedPlan.treeString() + "\n\n"
                        + "========== ANALYZED PLAN ==========\n"
                        + analyzedPlan.treeString() + "\n\n"
                        + "========== REWRITTEN PLAN ==========\n"
                        + rewrittenPlan.treeString() + "\n\n"
                        + "========== OPTIMIZED PLAN ==========\n"
                        + optimizedPlan.treeString();
            default:
                return super.getExplainString(explainOptions);
        }
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

    @VisibleForTesting
    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public static PhysicalProperties buildInitRequireProperties(Plan initPlan) {
        boolean isQuery = !(initPlan instanceof Command) || (initPlan instanceof ExplainCommand);
        return isQuery ? PhysicalProperties.GATHER : PhysicalProperties.ANY;
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
}
