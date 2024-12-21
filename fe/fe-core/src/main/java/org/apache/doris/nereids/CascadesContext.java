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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Analyzer;
import org.apache.doris.nereids.jobs.executor.TableCollector;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.RootPlanTreeRewriteJob.RootRewriteJobContext;
import org.apache.doris.nereids.jobs.scheduler.JobPool;
import org.apache.doris.nereids.jobs.scheduler.JobScheduler;
import org.apache.doris.nereids.jobs.scheduler.JobStack;
import org.apache.doris.nereids.jobs.scheduler.ScheduleContext;
import org.apache.doris.nereids.jobs.scheduler.SimpleJobScheduler;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Context used in memo.
 */
public class CascadesContext implements ScheduleContext {
    private static final Logger LOG = LogManager.getLogger(CascadesContext.class);

    // in analyze/rewrite stage, the plan will storage in this field
    private Plan plan;
    private Optional<RootRewriteJobContext> currentRootRewriteJobContext;
    // in optimize stage, the plan will storage in the memo
    private Memo memo;
    private final StatementContext statementContext;

    private CTEContext cteContext;
    private final RuleSet ruleSet;
    private final JobPool jobPool;
    private final JobScheduler jobScheduler;
    private JobContext currentJobContext;
    // subqueryExprIsAnalyzed: whether the subquery has been analyzed.
    private final Map<SubqueryExpr, Boolean> subqueryExprIsAnalyzed;
    private final RuntimeFilterContext runtimeFilterContext;
    private final TopnFilterContext topnFilterContext = new TopnFilterContext();
    private Optional<Scope> outerScope = Optional.empty();

    private boolean isRewriteRoot;
    private volatile boolean isTimeout = false;

    // current process subtree, represent outer plan if empty
    private final Optional<CTEId> currentTree;
    private final Optional<CascadesContext> parent;

    private final Set<MaterializationContext> materializationContexts;
    private final Set<List<String>> materializationRewrittenSuccessSet = new HashSet<>();
    private boolean isLeadingJoin = false;

    private boolean isLeadingDisableJoinReorder = false;

    private final Map<String, Hint> hintMap = Maps.newLinkedHashMap();
    private final ThreadLocal<Boolean> showPlanProcess = new ThreadLocal<>();

    // This list is used to listen the change event of the plan which
    // trigger by rule and show by `explain plan process` statement
    private final List<PlanProcess> planProcesses = new ArrayList<>();

    // this field is modified by FoldConstantRuleOnFE, it matters current traverse
    // into AggregateFunction with distinct, we can not fold constant in this case
    private int distinctAggLevel;
    private final boolean isEnableExprTrace;

    /**
     * Constructor of OptimizerContext.
     *
     * @param statementContext {@link StatementContext} reference
     * @param memo {@link Memo} reference
     */
    private CascadesContext(Optional<CascadesContext> parent, Optional<CTEId> currentTree,
            StatementContext statementContext, Plan plan, Memo memo,
            CTEContext cteContext, PhysicalProperties requireProperties, boolean isLeadingDisableJoinReorder) {
        this.parent = Objects.requireNonNull(parent, "parent should not null");
        this.currentTree = Objects.requireNonNull(currentTree, "currentTree should not null");
        this.statementContext = Objects.requireNonNull(statementContext, "statementContext should not null");
        this.plan = Objects.requireNonNull(plan, "plan should not null");
        this.memo = memo;
        this.cteContext = Objects.requireNonNull(cteContext, "cteContext should not null");
        this.ruleSet = new RuleSet();
        this.jobPool = new JobStack();
        this.jobScheduler = new SimpleJobScheduler();
        this.currentJobContext = new JobContext(this, requireProperties, Double.MAX_VALUE);
        this.subqueryExprIsAnalyzed = new HashMap<>();
        this.runtimeFilterContext = new RuntimeFilterContext(getConnectContext().getSessionVariable());
        this.materializationContexts = new HashSet<>();
        if (statementContext.getConnectContext() != null) {
            ConnectContext connectContext = statementContext.getConnectContext();
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            this.isEnableExprTrace = sessionVariable != null && sessionVariable.isEnableExprTrace();
        } else {
            this.isEnableExprTrace = false;
        }
        this.isLeadingDisableJoinReorder = isLeadingDisableJoinReorder;
    }

    /**
     * init a brand-new context to process whole tree
     */
    public static CascadesContext initContext(StatementContext statementContext,
            Plan initPlan, PhysicalProperties requireProperties) {
        return newContext(Optional.empty(), Optional.empty(), statementContext,
                initPlan, new CTEContext(), requireProperties, false);
    }

    /**
     * use for analyze cte. we must pass CteContext from outer since we need to get right scope of cte
     */
    public static CascadesContext newContextWithCteContext(CascadesContext cascadesContext,
            Plan initPlan, CTEContext cteContext) {
        return newContext(Optional.of(cascadesContext), Optional.empty(),
                cascadesContext.getStatementContext(), initPlan, cteContext, PhysicalProperties.ANY,
                cascadesContext.isLeadingDisableJoinReorder
        );
    }

    public static CascadesContext newCurrentTreeContext(CascadesContext context) {
        return CascadesContext.newContext(context.getParent(), context.getCurrentTree(), context.getStatementContext(),
                context.getRewritePlan(), context.getCteContext(),
                context.getCurrentJobContext().getRequiredProperties(), context.isLeadingDisableJoinReorder);
    }

    /**
     * New rewrite context copy from current context, used in cbo rewriter.
     */
    public static CascadesContext newSubtreeContext(Optional<CTEId> subtree, CascadesContext context,
            Plan plan, PhysicalProperties requireProperties) {
        return CascadesContext.newContext(Optional.of(context), subtree, context.getStatementContext(),
                plan, context.getCteContext(), requireProperties, context.isLeadingDisableJoinReorder);
    }

    private static CascadesContext newContext(Optional<CascadesContext> parent, Optional<CTEId> subtree,
            StatementContext statementContext, Plan initPlan, CTEContext cteContext,
            PhysicalProperties requireProperties, boolean isLeadingDisableJoinReorder) {
        return new CascadesContext(parent, subtree, statementContext, initPlan, null,
            cteContext, requireProperties, isLeadingDisableJoinReorder);
    }

    public CascadesContext getRoot() {
        CascadesContext root = this;
        while (root.getParent().isPresent()) {
            root = root.getParent().get();
        }
        return root;
    }

    public Optional<CascadesContext> getParent() {
        return parent;
    }

    public Optional<CTEId> getCurrentTree() {
        return currentTree;
    }

    public synchronized void setIsTimeout(boolean isTimeout) {
        this.isTimeout = isTimeout;
    }

    public synchronized boolean isTimeout() {
        return isTimeout;
    }

    public void toMemo() {
        this.memo = new Memo(getConnectContext(), plan);
    }

    public TableCollector newTableCollector() {
        return new TableCollector(this);
    }

    public Analyzer newAnalyzer() {
        return new Analyzer(this);
    }

    @Override
    public void pushJob(Job job) {
        jobPool.push(job);
    }

    public Memo getMemo() {
        return memo;
    }

    public void releaseMemo() {
        this.memo = null;
    }

    public final ConnectContext getConnectContext() {
        return statementContext.getConnectContext();
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    @Override
    public JobPool getJobPool() {
        return jobPool;
    }

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public JobContext getCurrentJobContext() {
        return currentJobContext;
    }

    public RuntimeFilterContext getRuntimeFilterContext() {
        return runtimeFilterContext;
    }

    public TopnFilterContext getTopnFilterContext() {
        return topnFilterContext;
    }

    public void setCurrentJobContext(JobContext currentJobContext) {
        this.currentJobContext = currentJobContext;
    }

    public CascadesContext setJobContext(PhysicalProperties physicalProperties) {
        this.currentJobContext = new JobContext(this, physicalProperties, Double.MAX_VALUE);
        return this;
    }

    public Plan getRewritePlan() {
        return plan;
    }

    public void setRewritePlan(Plan plan) {
        this.plan = plan;
    }

    public void setSubqueryExprIsAnalyzed(SubqueryExpr subqueryExpr, boolean isAnalyzed) {
        subqueryExprIsAnalyzed.put(subqueryExpr, isAnalyzed);
    }

    public boolean subqueryIsAnalyzed(SubqueryExpr subqueryExpr) {
        if (subqueryExprIsAnalyzed.get(subqueryExpr) == null) {
            setSubqueryExprIsAnalyzed(subqueryExpr, false);
            return false;
        }
        return subqueryExprIsAnalyzed.get(subqueryExpr);
    }

    public CascadesContext bottomUpRewrite(RuleFactory... rules) {
        return execute(new RewriteBottomUpJob(memo.getRoot(), currentJobContext, ImmutableList.copyOf(rules)));
    }

    public CascadesContext topDownRewrite(RuleFactory... rules) {
        return execute(new RewriteTopDownJob(memo.getRoot(), currentJobContext, ImmutableList.copyOf(rules)));
    }

    public CTEContext getCteContext() {
        return cteContext;
    }

    public void setCteContext(CTEContext cteContext) {
        this.cteContext = cteContext;
    }

    public void setIsRewriteRoot(boolean isRewriteRoot) {
        this.isRewriteRoot = isRewriteRoot;
    }

    public boolean isRewriteRoot() {
        return isRewriteRoot;
    }

    public Optional<Scope> getOuterScope() {
        return outerScope;
    }

    public void setOuterScope(@Nullable Scope outerScope) {
        this.outerScope = Optional.ofNullable(outerScope);
    }

    public List<MaterializationContext> getMaterializationContexts() {
        return materializationContexts.stream()
                .filter(MaterializationContext::isAvailable)
                .collect(Collectors.toList());
    }

    public void addMaterializationContext(MaterializationContext materializationContext) {
        this.materializationContexts.add(materializationContext);
    }

    public Set<List<String>> getMaterializationRewrittenSuccessSet() {
        return materializationRewrittenSuccessSet;
    }

    public void addMaterializationRewrittenSuccess(List<String> materializationQualifier) {
        this.materializationRewrittenSuccessSet.add(materializationQualifier);
    }

    /**
     * getAndCacheSessionVariable
     */
    public <T> T getAndCacheSessionVariable(String cacheName,
            T defaultValue, Function<SessionVariable, T> variableSupplier) {
        ConnectContext connectContext = getConnectContext();
        if (connectContext == null) {
            return defaultValue;
        }

        return getStatementContext().getOrRegisterCache(cacheName,
                () -> variableSupplier.apply(connectContext.getSessionVariable()));
    }

    /** getAndCacheDisableRules */
    public final BitSet getAndCacheDisableRules() {
        ConnectContext connectContext = getConnectContext();
        StatementContext statementContext = getStatementContext();
        if (connectContext == null || statementContext == null) {
            return new BitSet();
        }
        return statementContext.getOrCacheDisableRules(connectContext.getSessionVariable());
    }

    private CascadesContext execute(Job job) {
        pushJob(job);
        jobScheduler.executeJobPool(this);
        return this;
    }

    public void putCTEIdToConsumer(LogicalCTEConsumer cteConsumer) {
        Set<LogicalCTEConsumer> consumers = this.statementContext.getCteIdToConsumers()
                .computeIfAbsent(cteConsumer.getCteId(), k -> new HashSet<>());
        consumers.add(cteConsumer);
    }

    public Map<CTEId, Set<LogicalCTEConsumer>> getCteIdToConsumers() {
        return this.statementContext.getCteIdToConsumers();
    }

    public void putConsumerIdToFilter(RelationId id, Expression filter) {
        Set<Expression> filters = this.getConsumerIdToFilters().computeIfAbsent(id, k -> new HashSet<>());
        filters.add(filter);
    }

    public Map<RelationId, Set<Expression>> getConsumerIdToFilters() {
        return this.statementContext.getConsumerIdToFilters();
    }

    public void addCTEConsumerGroup(CTEId cteId, Group g, Multimap<Slot, Slot> producerSlotToConsumerSlot) {
        List<Pair<Multimap<Slot, Slot>, Group>> consumerGroups =
                this.statementContext.getCteIdToConsumerGroup().computeIfAbsent(cteId, k -> new ArrayList<>());
        consumerGroups.add(Pair.of(producerSlotToConsumerSlot, g));
    }

    /**
     * Update CTE consumer group as producer's stats update
     */
    public void updateConsumerStats(CTEId cteId, Statistics statistics) {
        List<Pair<Multimap<Slot, Slot>, Group>> consumerGroups
                = this.statementContext.getCteIdToConsumerGroup().get(cteId);
        for (Pair<Multimap<Slot, Slot>, Group> p : consumerGroups) {
            Multimap<Slot, Slot> producerSlotToConsumerSlot = p.first;
            Statistics updatedConsumerStats = new StatisticsBuilder(statistics).build();
            for (Entry<Expression, ColumnStatistic> entry : statistics.columnStatistics().entrySet()) {
                if (!(entry.getKey() instanceof Slot)) {
                    continue;
                }
                for (Slot consumer : producerSlotToConsumerSlot.get((Slot) entry.getKey())) {
                    updatedConsumerStats.addColumnStats(consumer, entry.getValue());
                }
            }
            p.value().setStatistics(updatedConsumerStats);
        }
    }

    public boolean isLeadingJoin() {
        return isLeadingJoin;
    }

    public void setLeadingJoin(boolean leadingJoin) {
        isLeadingJoin = leadingJoin;
    }

    public boolean isLeadingDisableJoinReorder() {
        return isLeadingDisableJoinReorder;
    }

    public void setLeadingDisableJoinReorder(boolean leadingDisableJoinReorder) {
        isLeadingDisableJoinReorder = leadingDisableJoinReorder;
    }

    public Map<String, Hint> getHintMap() {
        return hintMap;
    }

    public void addPlanProcess(PlanProcess planProcess) {
        planProcesses.add(planProcess);
    }

    public void addPlanProcesses(List<PlanProcess> planProcesses) {
        this.planProcesses.addAll(planProcesses);
    }

    public List<PlanProcess> getPlanProcesses() {
        return planProcesses;
    }

    public Optional<RootRewriteJobContext> getCurrentRootRewriteJobContext() {
        return currentRootRewriteJobContext;
    }

    public void setCurrentRootRewriteJobContext(RootRewriteJobContext currentRootRewriteJobContext) {
        this.currentRootRewriteJobContext = Optional.ofNullable(currentRootRewriteJobContext);
    }

    public boolean showPlanProcess() {
        Boolean show = showPlanProcess.get();
        return show != null && show;
    }

    /** set showPlanProcess in task scope */
    public void withPlanProcess(boolean showPlanProcess, Runnable task) {
        Boolean originSetting = this.showPlanProcess.get();
        try {
            this.showPlanProcess.set(showPlanProcess);
            task.run();
        } finally {
            if (originSetting == null) {
                this.showPlanProcess.remove();
            } else {
                this.showPlanProcess.set(originSetting);
            }
        }
    }

    /** keepOrShowPlanProcess */
    public void keepOrShowPlanProcess(boolean showPlanProcess, Runnable task) {
        if (showPlanProcess) {
            withPlanProcess(showPlanProcess, task);
        } else {
            task.run();
        }
    }

    public void printPlanProcess() {
        printPlanProcess(this.planProcesses);
    }

    public static void printPlanProcess(List<PlanProcess> planProcesses) {
        for (PlanProcess row : planProcesses) {
            LOG.info("RULE: {}\nBEFORE:\n{}\nafter:\n{}", row.ruleName, row.beforeShape, row.afterShape);
        }
    }

    public void incrementDistinctAggLevel() {
        this.distinctAggLevel++;
    }

    public void decrementDistinctAggLevel() {
        this.distinctAggLevel--;
    }

    public int getDistinctAggLevel() {
        return distinctAggLevel;
    }

    public boolean isEnableExprTrace() {
        return isEnableExprTrace;
    }
}
