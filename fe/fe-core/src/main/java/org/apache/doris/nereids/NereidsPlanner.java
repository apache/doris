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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.foundation.format.FormatOptions;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.pre.PlanPreprocessors;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.OrderSpec;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.distribute.DistributePlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDictionarySink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.planner.AddLocalExchange;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.TimeBasedChangeVisibleWaiter;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.statistics.query.QueryStatsRecorder;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.tso.MasterTsoProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {
    // private static final AtomicInteger executeCount = new AtomicInteger(0);
    public static final Logger LOG = LogManager.getLogger(NereidsPlanner.class);

    public static AtomicLong runningPlanNum = new AtomicLong(0L);
    protected Plan parsedPlan;
    protected Plan analyzedPlan;
    protected Plan rewrittenPlan;
    protected Plan optimizedPlan;
    protected PhysicalPlan physicalPlan;

    private CascadesContext cascadesContext;
    private final StatementContext statementContext;
    private final List<ScanNode> scanNodeList = Lists.newArrayList();
    private final List<PhysicalRelation> physicalRelations = Lists.newArrayList();
    private DescriptorTable descTable;

    private FragmentIdMapping<DistributedPlan> distributedPlans;
    private PlanTranslatorContext planTranslatorContext;
    // The cost of optimized plan
    private double cost = 0;
    private LogicalPlanAdapter logicalPlanAdapter;

    public NereidsPlanner(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    void pinBeExecVersion(org.apache.doris.thrift.TQueryOptions queryOptions) {
        if (!statementContext.hasBeExecVersion()) {
            statementContext.initializeBeExecVersion(queryOptions.getBeExecVersion());
        }
        queryOptions.setBeExecVersion(statementContext.getBeExecVersion());
    }

    @Override
    public void plan(StatementBase queryStmt, org.apache.doris.thrift.TQueryOptions queryOptions) throws UserException {
        pinBeExecVersion(queryOptions);
        this.queryOptions = queryOptions;
        if (!(queryStmt instanceof LogicalPlanAdapter)) {
            throw new RuntimeException("Wrong type of queryStmt, expected: <? extends LogicalPlanAdapter>");
        }

        logicalPlanAdapter = (LogicalPlanAdapter) queryStmt;

        ExplainLevel explainLevel = getExplainLevel(queryStmt.getExplainOptions());

        LogicalPlan parsedPlan = logicalPlanAdapter.getLogicalPlan();
        setParsedPlan(parsedPlan);

        PhysicalProperties requireProperties = buildInitRequireProperties();
        statementContext.getStopwatch().reset().start();
        NereidsPlanner.runningPlanNum.incrementAndGet();
        try {
            boolean showPlanProcess = showPlanProcess(queryStmt.getExplainOptions());
            planWithLock(parsedPlan, requireProperties, explainLevel, showPlanProcess, plan -> {
                setOptimizedPlan(plan);
                if (plan instanceof PhysicalPlan) {
                    physicalPlan = (PhysicalPlan) plan;
                    distribute(physicalPlan, explainLevel);
                }
            });
        } finally {
            statementContext.getStopwatch().stop();
            NereidsPlanner.runningPlanNum.decrementAndGet();
        }

        if (LOG.isDebugEnabled()) {
            LOG.info(getExplainString(new ExplainOptions(ExplainLevel.SHAPE_PLAN, false)));
            LOG.info(getExplainString(new ExplainOptions(ExplainLevel.DISTRIBUTED_PLAN, false)));
        }
        if (physicalPlan != null) {
            QueryStatsRecorder.record(physicalPlan, statementContext);
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
                if (explainLevel != ExplainLevel.NONE) {
                    this.cascadesContext = CascadesContext.initContext(
                            statementContext, parsedPlan, PhysicalProperties.ANY);
                    switch (explainLevel) {
                        case OPTIMIZED_PLAN:
                        case ALL_PLAN:
                            cascadesContext.addPlanProcess(
                                    new PlanProcess("ImplementSqlCache",
                                            parsedPlan.treeString(false, parsedPlan),
                                            physicalPlan.treeString(false, physicalPlan)
                                    )
                            );
                            break;
                        default: {
                        }
                    }
                }
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
            collectAndLockTable(showAnalyzeProcess(explainLevel, showPlanProcess),
                    explainLevel == ExplainLevel.NONE);
            // after table collector, we should use a new context.
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
        // try to pre mv rewrite
        preMaterializedViewRewrite();
        if (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
            rewrittenPlan = cascadesContext.getRewritePlan();
            if (explainLevel == ExplainLevel.REWRITTEN_PLAN) {
                return rewrittenPlan;
            }
        }

        optimize(showPlanProcess);
        // print memo before choose plan.
        // if chooseNthPlan failed, we could get memo to debug
        if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
            Memo memo = cascadesContext.getMemo();
            if (memo != null) {
                LOG.info("{}\n{}", ConnectContext.get().getQueryIdentifier(), memo.toString());
            } else {
                LOG.info("{}\nMemo is null", ConnectContext.get().getQueryIdentifier());
            }
        }

        // Compute USE_MV hard constraint: USE_MV specified qualifiers that rewrite-successfully
        Set<List<String>> requiredUseMvIdentifiers = computeRequiredUseMvIdentifiers(statementContext);
        Map<Group, Set<List<String>>> mvReachableCache = new HashMap<>();
        if (!requiredUseMvIdentifiers.isEmpty()) {
            computeReachableMvIdentifiers(getRoot(), mvReachableCache, requiredUseMvIdentifiers);
            // Drop the required MVs which the rewrite can not produce, and mark them as UnUsed,
            // USE_MV is best-effort and should not fail the query.
            Set<List<String>> rootReachable = mvReachableCache.get(getRoot());
            requiredUseMvIdentifiers = dropUnreachableUseMvIdentifiers(statementContext,
                    requiredUseMvIdentifiers,
                    rootReachable == null ? ImmutableSet.of() : rootReachable);
        }
        PhysicalPlan physicalPlan;
        if (!requiredUseMvIdentifiers.isEmpty()) {
            PlanWithCost chosenPlan = chooseBestPlanConsideringRequiredUseMv(getRoot(), requireProperties,
                    requiredUseMvIdentifiers, mvReachableCache, cascadesContext);
            physicalPlan = chosenPlan.plan;
            // Keep the cost of the selected alternative for the EXPLAIN output, the cost based
            // selection methods are bypassed on this path.
            cost = chosenPlan.cost;
        } else {
            Set<Integer> requiredGroupIds = cascadesContext.getConnectContext()
                    .getSessionVariable().getRequiredGroupIds();
            if (!requiredGroupIds.isEmpty()) {
                Map<Group, Set<Integer>> reachableCache = new HashMap<>();
                computeReachableGroupIds(getRoot(), reachableCache);
                // Validate all required groups are reachable from root
                Set<Integer> rootReachable = reachableCache.get(getRoot());
                Set<Integer> unreachable = new HashSet<>(requiredGroupIds);
                unreachable.removeAll(rootReachable);
                if (!unreachable.isEmpty()) {
                    throw new AnalysisException("Required group IDs not found in memo: " + unreachable);
                }
                physicalPlan = chooseBestPlanWithRequiredGroups(
                        getRoot(), requireProperties, requiredGroupIds, reachableCache);
            } else {
                int nth = cascadesContext.getConnectContext().getSessionVariable().getNthOptimizedPlan();
                physicalPlan = chooseNthPlan(getRoot(), requireProperties, nth);
            }
        }

        physicalPlan = postProcess(physicalPlan);
        // Finalize USE_MV hint status based on actually chosen MVs in the physical plan
        finalizeUseMvHintStatus(statementContext, physicalPlan);
        if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
            String tree = physicalPlan.treeString();
            LOG.info("{}\n{}", ConnectContext.get().getQueryIdentifier(), tree);
        }
        if (explainLevel == ExplainLevel.OPTIMIZED_PLAN
                || explainLevel == ExplainLevel.ALL_PLAN
                || explainLevel == ExplainLevel.SHAPE_PLAN) {
            optimizedPlan = physicalPlan;
        }
        return physicalPlan;
    }

    protected LogicalPlan preprocess(LogicalPlan logicalPlan) {
        return new PlanPreprocessors(statementContext).process(logicalPlan);
    }

    /**
     * config rf wait time if wait time is the same as default value
     * 1. local mode, config according to max table row count
     *     a. olap table:
     *       row < 1G: 1 sec
     *       1G <= row < 10G: 5 sec
     *       10G < row: 20 sec
     *     b. external table:
     *       row < 1G: 5 sec
     *       1G <= row < 10G: 10 sec
     *       10G < row: 50 sec
     * 2. cloud mode, config it as query time out
     */
    private void configRuntimeFilterWaitTime() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null
                && ConnectContext.get().getSessionVariable().getRuntimeFilterWaitTimeMs()
                == VariableMgr.getDefaultSessionVariable().getRuntimeFilterWaitTimeMs()) {
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (Config.isCloudMode()) {
                sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS,
                        String.valueOf(Math.max(VariableMgr.getDefaultSessionVariable().getRuntimeFilterWaitTimeMs(),
                                1000 * sessionVariable.getQueryTimeoutS())));
            } else {
                List<LogicalCatalogRelation> scans = cascadesContext.getRewritePlan()
                        .collectToList(LogicalCatalogRelation.class::isInstance);
                double maxRow = StatsCalculator.getMaxTableRowCount(scans, cascadesContext);
                boolean hasExternalTable = scans.stream().anyMatch(scan -> !(scan instanceof LogicalOlapScan));
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
    }

    private void initCascadesContext(LogicalPlan plan, PhysicalProperties requireProperties) {
        cascadesContext = CascadesContext.initContext(statementContext, plan, requireProperties);
    }

    protected void collectAndLockTable(boolean showPlanProcess) {
        collectAndLockTable(showPlanProcess, false);
    }

    protected void collectAndLockTable(boolean showPlanProcess, boolean waitForChangeVisible) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start collect and lock table");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newTableCollector(true, true).collect());
        if (statementContext.isChangelogReferenceTsoRequired()) {
            statementContext.getOrRegisterChangelogReferenceTso(
                    () -> MasterTsoProvider.getCurrentTso(statementContext.getConnectContext()));
        }
        // Read the preload result produced by the collect-phase rule before taking internal table locks.
        ExternalMetadataPreloadResult preloadResult = statementContext.getExternalMetadataPreloadResult()
                .orElse(ExternalMetadataPreloadResult.skipped(
                        statementContext.getExternalTablePreloadCandidateCount(), "preload rule did not run"));
        // Record preload timing in the query profile as a dedicated planner sub-stage.
        if (statementContext.getConnectContext().getExecutor() != null && preloadResult.isExecuted()) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .addNereidsPreloadExternalMetadataTime(preloadResult.getElapsedTimeMs());
        }
        // Keep a concise debug summary for the entire preload phase.
        if (LOG.isDebugEnabled()) {
            if (preloadResult.isExecuted()) {
                LOG.debug("{} preloaded external metadata for {} of {} candidate tables in {} ms",
                        statementContext.getConnectContext().getQueryIdentifier(),
                        preloadResult.getPreloadedTableCount(),
                        preloadResult.getCandidateTableCount(),
                        preloadResult.getElapsedTimeMs());
            } else {
                LOG.debug("{} skip external metadata preload before lock: {} [candidateTableCount={}]",
                        statementContext.getConnectContext().getQueryIdentifier(), preloadResult.getSkipReason(),
                        preloadResult.getCandidateTableCount());
            }
        }
        if (waitForChangeVisible) {
            waitForTimeBasedChangeVisibleBeforeLock();
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            // Track only the actual lock() call here so the dedicated preload stage is not double counted.
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsLockTableStartTime(TimeUtils.getStartTimeMs());
        }
        statementContext.lock();
        cascadesContext.setCteContext(new CTEContext());
        if (LOG.isDebugEnabled()) {
            LOG.debug("End collect and lock table");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsLockTableFinishTime(TimeUtils.getStartTimeMs());
        }
    }

    private void waitForTimeBasedChangeVisibleBeforeLock() {
        try {
            if (statementContext.getConnectContext().getExecutor() != null) {
                statementContext.getConnectContext().getExecutor().getSummaryProfile()
                        .setWaitChangeVisibleStartTime(TimeUtils.getStartTimeMs());
            }
            TimeBasedChangeVisibleWaiter.waitForVisible(
                    statementContext.getConnectContext(),
                    cascadesContext.getRewritePlan(),
                    statementContext.getTables());
        } catch (UserException e) {
            throw new NereidsException(e.getMessage(), e);
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setWaitChangeVisibleEndTime(TimeUtils.getStartTimeMs());
        }
    }

    protected void analyze(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start analyze plan");
        }
        statementContext.getPlannerHooks().forEach(hook -> hook.beforeAnalyze(this));
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newAnalyzer().analyze());
        statementContext.getPlannerHooks().forEach(hook -> hook.afterAnalyze(this));
        if (LOG.isDebugEnabled()) {
            LOG.debug("End analyze plan");
        }

        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsAnalysisTime(TimeUtils.getStartTimeMs());
        }
    }

    /**
     * Logical plan rewrite based on a series of heuristic rules.
     */
    protected void rewrite(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start rewrite plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> {
            Rewriter.getWholeTreeRewriter(cascadesContext).execute();
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("End rewrite plan");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsRewriteTime(TimeUtils.getStartTimeMs());
        }
        statementContext.setNeedPreMvRewrite(PreMaterializedViewRewriter.needPreRewrite(cascadesContext));
        // init materialization context for mv rewrite
        cascadesContext.getStatementContext().getPlannerHooks().forEach(hook -> hook.afterRewrite(cascadesContext));
    }

    protected void preMaterializedViewRewrite() {
        if (!cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
            return;
        }
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Start pre rewrite plan by mv");
            }
            List<Plan> tmpPlansForMvRewrite = cascadesContext.getStatementContext().getTmpPlanForMvRewrite();
            Plan originalPlan = cascadesContext.getRewritePlan();
            List<Plan> plansWhichContainMv = new ArrayList<>();
            // because tmpPlansForMvRewrite only one, so timeout is cumulative which is ok
            for (Plan planForRewrite : tmpPlansForMvRewrite) {
                SessionVariable sessionVariable = cascadesContext.getConnectContext()
                        .getSessionVariable();
                int timeoutSecond = sessionVariable.nereidsTimeoutSecond;
                boolean enableTimeout = sessionVariable.enableNereidsTimeout;
                try {
                    // set mv rewrite timeout
                    sessionVariable.nereidsTimeoutSecond = PreMaterializedViewRewriter.convertMillisToCeilingSeconds(
                                    sessionVariable.materializedViewRewriteDurationThresholdMs);
                    sessionVariable.enableNereidsTimeout = true;
                    // pre rewrite
                    Plan rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                            PreMaterializedViewRewriter::rewrite, planForRewrite, planForRewrite, true);
                    Plan ruleOptimizedPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                            childOptContext -> {
                                Rewriter.getWholeTreeRewriterWithoutCostBasedJobs(childOptContext).execute();
                                return childOptContext.getRewritePlan();
                            }, rewrittenPlan, planForRewrite, false);
                    if (ruleOptimizedPlan == null) {
                        continue;
                    }
                    // after rbo, maybe the plan changed a lot, so we need to normalize it with original plan
                    Plan normalizedPlan = MaterializedViewUtils.normalizeSinkExpressions(
                            ruleOptimizedPlan, originalPlan);
                    if (normalizedPlan != null) {
                        plansWhichContainMv.add(normalizedPlan);
                    }
                } catch (Exception e) {
                    LOG.error("pre mv rewrite in rbo rewrite fail, query id is {}",
                            cascadesContext.getConnectContext().getQueryIdentifier(), e);

                } finally {
                    sessionVariable.nereidsTimeoutSecond = timeoutSecond;
                    sessionVariable.enableNereidsTimeout = enableTimeout;
                }
            }
            // clear the rewritten plans which are tmp optimized, should be filled by full optimize later
            statementContext.getRewrittenPlansByMv().clear();
            // if rule-based optimized, would not be rewritten by cbo, so clear materialized hooks
            this.cascadesContext.getStatementContext().setPreMvRewritten(true);
            if (!plansWhichContainMv.isEmpty()) {
                plansWhichContainMv.forEach(statementContext::addRewrittenPlanByMv);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("End pre rewrite plan by mv");
            }
        } finally {
            if (statementContext.getConnectContext().getExecutor() != null) {
                statementContext.getConnectContext().getExecutor().getSummaryProfile()
                        .setNereidsPreRewriteByMvFinishTime(TimeUtils.getStartTimeMs());
            }
        }
    }

    // DependsRules: AddProjectForJoin
    protected void optimize(boolean showPlanProcess) {
        // if we cannot get table row count, skip join reorder
        // except:
        //   1. user set leading hint
        //   2. ut test. In ut test, FeConstants.enableInternalSchemaDb is false or FeConstants.runningUnitTest is true
        if (FeConstants.enableInternalSchemaDb && !FeConstants.runningUnitTest
                && !cascadesContext.isLeadingDisableJoinReorder()) {
            List<CatalogRelation> scans = cascadesContext.getRewritePlan()
                    .collectToList(CatalogRelation.class::isInstance);
            Optional<String> disableJoinReorderReason = StatsCalculator
                    .disableJoinReorderIfStatsInvalid(scans, cascadesContext);
            disableJoinReorderReason.ifPresent(statementContext::setDisableJoinReorderReason);
        }
        configRuntimeFilterWaitTime();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start optimize plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> {
            new Optimizer(cascadesContext).execute();
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("End optimize plan");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsOptimizeTime(TimeUtils.getStartTimeMs());
        }
    }

    /**
     * Collect plan info for hbo usage.
     * @param queryId queryId
     * @param root physical plan
     * @param context PlanTranslatorContext
     */
    private void collectHboPlanInfo(String queryId, PhysicalPlan root, PlanTranslatorContext context) {
        for (Object child : root.children()) {
            collectHboPlanInfo(queryId, (PhysicalPlan) child, context);
        }
        if (root instanceof AbstractPlan) {
            int nodeId = ((AbstractPlan) root).getId();
            PlanNodeId planId = context.getNereidsIdToPlanNodeIdMap().get(nodeId);
            if (planId != null) {
                Map<Integer, PhysicalPlan> idToPlanMap = Env.getCurrentEnv().getHboPlanStatisticsManager()
                        .getHboPlanInfoProvider().getIdToPlanMap(queryId);
                if (idToPlanMap.isEmpty()) {
                    Env.getCurrentEnv().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().putIdToPlanMap(queryId, idToPlanMap);
                }
                idToPlanMap.put(planId.asInt(), root);
                Map<PhysicalPlan, Integer> planToIdMap = Env.getCurrentEnv().getHboPlanStatisticsManager()
                                .getHboPlanInfoProvider().getPlanToIdMap(queryId);
                if (planToIdMap.isEmpty()) {
                    Env.getCurrentEnv().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().putPlanToIdMap(queryId, planToIdMap);
                }
                planToIdMap.put(root, planId.asInt());
            }
        }
    }

    protected void splitFragments(PhysicalPlan resultPlan) {
        if (resultPlan instanceof PhysicalSqlCache) {
            return;
        }

        this.planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator(planTranslatorContext,
                statementContext.getConnectContext().getStatsErrorEstimator());
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        PlanFragment root = physicalPlanTranslator.translatePlan(physicalPlan);
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsTranslateTime(TimeUtils.getStartTimeMs());
        }
        String queryId = DebugUtil.printId(cascadesContext.getConnectContext().queryId());
        if (StatisticsUtil.isEnableHboInfoCollection()) {
            collectHboPlanInfo(queryId, physicalPlan, planTranslatorContext);
        }

        scanNodeList.addAll(planTranslatorContext.getScanNodes());
        physicalRelations.addAll(planTranslatorContext.getPhysicalRelations());
        descTable = planTranslatorContext.getDescTable();
        fragments = new ArrayList<>(planTranslatorContext.getPlanFragments());

        boolean enableQueryCache = sessionVariable.getEnableQueryCache();
        for (int seq = 0; seq < fragments.size(); seq++) {
            PlanFragment fragment = fragments.get(seq);
            fragment.setFragmentSequenceNum(seq);
            if (enableQueryCache) {
                try {
                    QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
                    Optional<TQueryCacheParam> queryCacheParam =
                            normalizer.normalize(cascadesContext.getConnectContext());
                    if (queryCacheParam.isPresent()) {
                        fragment.queryCacheParam = queryCacheParam.get();
                        // after commons-codec 1.14 (include), Hex.encodeHexString will change ByteBuffer.pos,
                        // so we should copy a new byte buffer to print it
                        ByteBuffer digestCopy = fragment.queryCacheParam.digest.duplicate();
                        LOG.info("Use query cache for fragment {}, node id: {}, digest: {}, queryId: {}",
                                seq,
                                fragment.queryCacheParam.node_id,
                                Hex.encodeHexString(digestCopy), queryId);
                    }
                } catch (Throwable t) {
                    // do nothing
                }
            }
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
                column = slotReference.getOneLevelColumn();
                table = slotReference.getOneLevelTable();
            }
            columnLabels.add(output.getName());
            FieldInfo fieldInfo = new FieldInfo(
                    table.isPresent() ? (table.get().getDatabase() != null
                            ? table.get().getDatabase().getFullName() : "") : "",
                    !output.getQualifier().isEmpty() ? output.getQualifier().get(output.getQualifier().size() - 1)
                            : (table.isPresent() ? Util.getTempTableDisplayName(table.get().getName()) : ""),
                    table.isPresent() ? Util.getTempTableDisplayName(table.get().getName()) : "",
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

        // update scan nodes visible version at the end of plan phase for cloud mode.
        try {
            ScanNode.setVisibleVersionForOlapScanNodes(getScanNodes());
        } catch (UserException ue) {
            throw new NereidsException(ue.getMessage(), ue);
        }
    }

    protected void distribute(PhysicalPlan physicalPlan, ExplainLevel explainLevel) {
        boolean canUseNereidsDistributePlanner = SessionVariable.canUseNereidsDistributePlanner()
                || (physicalPlan instanceof PhysicalDictionarySink); // dic sink only supported in new Coordinator
        if ((!canUseNereidsDistributePlanner && explainLevel.isPlanLevel)) {
            return;
        } else if ((canUseNereidsDistributePlanner && explainLevel.isPlanLevel
                && (explainLevel != ExplainLevel.ALL_PLAN && explainLevel != ExplainLevel.DISTRIBUTED_PLAN))) {
            return;
        }

        splitFragments(physicalPlan);
        doDistribute(canUseNereidsDistributePlanner, explainLevel);

        addLocalExchangeAfterDistribute();
    }

    private void addLocalExchangeAfterDistribute() {
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        if (!sessionVariable.isEnableLocalShufflePlanner() || !sessionVariable.isEnableLocalShuffle()) {
            return;
        }
        AddLocalExchange adder = new AddLocalExchange();
        if (distributedPlans != null && !distributedPlans.isEmpty()) {
            adder.addLocalExchange(distributedPlans, planTranslatorContext);
        }
    }

    protected void doDistribute(boolean canUseNereidsDistributePlanner, ExplainLevel explainLevel) {
        if (!canUseNereidsDistributePlanner) {
            return;
        }
        switch (explainLevel) {
            case NONE:
            case ALL_PLAN:
            case DISTRIBUTED_PLAN:
                break;
            default: {
                return;
            }
        }

        boolean notNeedBackend = false;
        // if the query can compute without backend, we can skip check cluster privileges
        if (Config.isCloudMode()
                && cascadesContext.getConnectContext().supportHandleByFe()
                && physicalPlan instanceof ComputeResultSet) {
            Optional<ResultSet> resultSet = ((ComputeResultSet) physicalPlan).computeResultInFe(
                    cascadesContext, physicalPlan.getOutput());
            if (resultSet.isPresent()) {
                notNeedBackend = true;
            }
        }

        boolean useLoadBackendSelection = physicalPlan.anyMatch(PhysicalOlapTableSink.class::isInstance);
        distributedPlans = new DistributePlanner(
                statementContext, fragments, notNeedBackend, false, useLoadBackendSelection).plan();
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsDistributeTime(TimeUtils.getStartTimeMs());
        }
    }

    protected PhysicalPlan postProcess(PhysicalPlan physicalPlan) {
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

    protected PhysicalPlan chooseNthPlan(Group rootGroup, PhysicalProperties physicalProperties, int nthPlan) {
        if (nthPlan <= 1) {
            cost = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                    () -> new AnalysisException("lowestCostPlans with physicalProperties("
                            + physicalProperties + ") doesn't exist in root group")).first.getValue();
            return chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        }
        Memo memo = cascadesContext.getMemo();

        Pair<Long, Double> idCost = memo.rank(nthPlan);
        cost = idCost.second;
        return memo.unrank(idCost.first);
    }

    /**
     * Doc
     */
    public static PhysicalPlan chooseBestPlan(Group rootGroup, PhysicalProperties physicalProperties,
            CascadesContext cascadesContext)
            throws AnalysisException {
        try {
            GroupExpression groupExpression = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                    () -> new AnalysisException("lowestCostPlans with physicalProperties("
                            + physicalProperties + ") doesn't exist in root group")).second;
            if ((groupExpression.getPlan() instanceof PhysicalDistribute
                    && rootGroup.getEnforcerSpecs().containsKey(
                            ((PhysicalDistribute<?>) groupExpression.getPlan()).getDistributionSpec()))
                    || rootGroup.getEnforcers().containsKey(groupExpression)) {
                rootGroup.addChosenEnforcerId(groupExpression.getId().asInt());
                rootGroup.addChosenEnforcerProperties(physicalProperties);
            } else {
                rootGroup.setChosenProperties(physicalProperties);
                rootGroup.setChosenGroupExpressionId(groupExpression.getId().asInt());
            }
            List<PhysicalProperties> inputPropertiesList = groupExpression.getInputPropertiesList(physicalProperties);
            List<Plan> planChildren = Lists.newArrayList();
            for (int i = 0; i < groupExpression.arity(); i++) {
                planChildren.add(chooseBestPlan(groupExpression.child(i), inputPropertiesList.get(i), cascadesContext));
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

    /**
     * Choose the best physical plan considering USE_MV hint as a hard constraint.
     * If USE_MV hint is present with rewrite-success MVs, the returned plan must
     * contain those MVs. Otherwise, falls back to standard cost-based selection.
     *
     * @param rootGroup the root memo group
     * @param physicalProperties required physical properties
     * @param cascadesContext the cascades context
     * @return physical plan respecting USE_MV hard constraint
     */
    public static PhysicalPlan chooseBestPlanConsideringUseMv(Group rootGroup,
            PhysicalProperties physicalProperties, CascadesContext cascadesContext)
            throws AnalysisException {
        StatementContext stmtCtx = cascadesContext.getStatementContext();
        Set<List<String>> requiredUseMvIdentifiers = computeRequiredUseMvIdentifiers(stmtCtx);
        if (requiredUseMvIdentifiers.isEmpty()) {
            return chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        }
        Map<Group, Set<List<String>>> mvReachableCache = new HashMap<>();
        computeReachableMvIdentifiers(rootGroup, mvReachableCache, requiredUseMvIdentifiers);
        Set<List<String>> rootReachable = mvReachableCache.get(rootGroup);
        // Drop the required MVs which the rewrite can not produce, and mark them as UnUsed,
        // USE_MV is best-effort and should not fail the query.
        requiredUseMvIdentifiers = dropUnreachableUseMvIdentifiers(stmtCtx, requiredUseMvIdentifiers,
                rootReachable == null ? ImmutableSet.of() : rootReachable);
        if (requiredUseMvIdentifiers.isEmpty()) {
            return chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        }
        return chooseBestPlanConsideringRequiredUseMv(rootGroup, physicalProperties,
                requiredUseMvIdentifiers, mvReachableCache, cascadesContext).plan;
    }

    /**
     * Choose the best plan which contains at least one of the required materializations.
     * <p>
     * Several hinted materializations may be alternatives of each other and can not be chosen at the
     * same time, so a single one of them satisfies the hint: the selection only requires at least one
     * of the reachable materializations to be contained in the plan.
     */
    private static PlanWithCost chooseBestPlanConsideringRequiredUseMv(Group rootGroup,
            PhysicalProperties physicalProperties, Set<List<String>> requiredUseMvIdentifiers,
            Map<Group, Set<List<String>>> mvReachableCache, CascadesContext cascadesContext)
            throws AnalysisException {
        PlanWithCost chosenPlan = chooseBestPlanWithRequiredMaterializations(rootGroup,
                physicalProperties, requiredUseMvIdentifiers, mvReachableCache, cascadesContext);
        if (chosenPlan == null) {
            // No materialization can be produced together with the required properties, fall back to
            // the cost based plan instead of failing the query.
            markUseMvNotChosen(cascadesContext.getStatementContext(), requiredUseMvIdentifiers);
            return chooseBestPlanWithCost(rootGroup, physicalProperties, cascadesContext);
        }
        return enforceOutputProperties(chosenPlan, physicalProperties, rootGroup, cascadesContext);
    }

    /**
     * Enforce the required output properties on the selected plan, the selected materialization can
     * provide other properties than the costed plan which it replaced.
     */
    private static PlanWithCost enforceOutputProperties(PlanWithCost selectedPlan,
            PhysicalProperties requiredProperties, Group group, CascadesContext cascadesContext) {
        if (selectedPlan.plan instanceof PhysicalSink
                || providesProperties(selectedPlan.plan.getPhysicalProperties(), requiredProperties)) {
            // A sink is the root of the plan and its child is already required to provide the
            // properties, an enforcer above it would be meaningless.
            return selectedPlan;
        }
        return enforceProperties(selectedPlan, requiredProperties, false, group, cascadesContext);
    }

    /**
     * Choose the cost based plan of the group together with its estimated cost.
     */
    private static PlanWithCost chooseBestPlanWithCost(Group rootGroup, PhysicalProperties physicalProperties,
            CascadesContext cascadesContext) throws AnalysisException {
        PhysicalPlan physicalPlan = chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        double planCost = rootGroup.getLowestCostPlan(physicalProperties)
                .map(costAndPlan -> costAndPlan.first.getValue()).orElse(0.0);
        return new PlanWithCost(physicalPlan, planCost);
    }

    /**
     * Drop the required USE_MV identifiers which are not reachable in the memo, and mark them
     * as not chosen in the hint. USE_MV is a best-effort hint: when the materialized view can not
     * be produced by the rewrite, for example the query aggregations can not be answered by the
     * materialized view, the query is planned without this hint instead of failing.
     *
     * @return the required USE_MV identifiers which are reachable, or an empty set if all are dropped
     */
    private static Set<List<String>> dropUnreachableUseMvIdentifiers(StatementContext statementContext,
            Set<List<String>> requiredUseMvIdentifiers, Set<List<String>> reachableUseMvIdentifiers) {
        Set<List<String>> reachableRequired = new HashSet<>(requiredUseMvIdentifiers);
        reachableRequired.retainAll(reachableUseMvIdentifiers);
        if (reachableRequired.size() == requiredUseMvIdentifiers.size()) {
            return requiredUseMvIdentifiers;
        }
        Set<List<String>> unreachable = new HashSet<>(requiredUseMvIdentifiers);
        unreachable.removeAll(reachableUseMvIdentifiers);
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (useMvHint.isPresent()) {
            useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
            useMvHint.get().setErrorMessage("MV(s) not found in memo after rewrite: " + unreachable);
            for (List<String> qualifier : unreachable) {
                useMvHint.get().getUseMvTableColumnMap().put(qualifier, false);
            }
        }
        return reachableRequired;
    }

    /**
     * Mark the USE_MV qualifiers which are not chosen in the final plan, so that the hint is reported
     * as UnUsed together with the reason.
     */
    private static void markUseMvNotChosen(StatementContext statementContext,
            Set<List<String>> notChosenQualifiers) {
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (!useMvHint.isPresent()) {
            return;
        }
        useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
        useMvHint.get().setErrorMessage("MV(s) can not be chosen with the required properties: "
                + notChosenQualifiers);
        for (List<String> qualifier : notChosenQualifiers) {
            useMvHint.get().getUseMvTableColumnMap().put(qualifier, false);
        }
    }

    /**
     * Memoized bottom-up DFS: compute all group IDs reachable from this group's subtree
     * through physical GroupExpressions (including enforcers).
     */
    private static Set<Integer> computeReachableGroupIds(Group group, Map<Group, Set<Integer>> cache) {
        Set<Integer> cached = cache.get(group);
        if (cached != null) {
            return cached;
        }
        Set<Integer> reachable = new HashSet<>();
        reachable.add(group.getGroupId().asInt());
        // Put in cache BEFORE recursing to break cycles (enforcers reference the same group)
        cache.put(group, reachable);
        for (GroupExpression ge : group.getPhysicalExpressions()) {
            for (Group child : ge.children()) {
                reachable.addAll(computeReachableGroupIds(child, cache));
            }
        }
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            for (Group child : enforcer.children()) {
                reachable.addAll(computeReachableGroupIds(child, cache));
            }
        }
        return reachable;
    }

    /**
     * Choose the cheapest physical plan that contains all required group IDs.
     * At each group, picks the cheapest physical GroupExpression whose children's
     * reachable sets collectively cover all remaining required groups, then distributes
     * the remaining requirements among children and recurses.
     */
    private PhysicalPlan chooseBestPlanWithRequiredGroups(
            Group group, PhysicalProperties properties,
            Set<Integer> remaining, Map<Group, Set<Integer>> reachableCache) {
        remaining = new HashSet<>(remaining);
        remaining.remove(group.getGroupId().asInt());

        if (remaining.isEmpty()) {
            return chooseBestPlan(group, properties, cascadesContext);
        }

        GroupExpression chosenGE = null;
        double chosenCost = Double.MAX_VALUE;

        // Check all physical GEs in this group
        for (GroupExpression ge : group.getPhysicalExpressions()) {
            if (!ge.getLowestCostTable().containsKey(properties)) {
                continue;
            }
            double geCost = ge.getCostByProperties(properties);
            if (geCost >= chosenCost) {
                continue;
            }
            Set<Integer> childReachable = new HashSet<>();
            for (Group child : ge.children()) {
                childReachable.addAll(reachableCache.getOrDefault(child, new HashSet()));
            }
            if (childReachable.containsAll(remaining)) {
                chosenGE = ge;
                chosenCost = geCost;
            }
        }

        // Also check enforcers
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            if (!enforcer.getLowestCostTable().containsKey(properties)) {
                continue;
            }
            double eCost = enforcer.getCostByProperties(properties);
            if (eCost >= chosenCost) {
                continue;
            }
            Set<Integer> childReachable = new HashSet<>();
            for (Group child : enforcer.children()) {
                childReachable.addAll(reachableCache.getOrDefault(child, new HashSet()));
            }
            if (childReachable.containsAll(remaining)) {
                chosenGE = enforcer;
                chosenCost = eCost;
            }
        }

        if (chosenGE == null) {
            throw new AnalysisException("Cannot find physical plan containing required group IDs: "
                    + remaining + " at group " + group.getGroupId());
        }

        // Handle enforcer marking (same as chooseBestPlan)
        if ((chosenGE.getPlan() instanceof PhysicalDistribute
                && group.getEnforcerSpecs().containsKey(
                        ((PhysicalDistribute<?>) chosenGE.getPlan()).getDistributionSpec()))
                || group.getEnforcers().containsKey(chosenGE)) {
            group.addChosenEnforcerId(chosenGE.getId().asInt());
            group.addChosenEnforcerProperties(properties);
        } else {
            group.setChosenProperties(properties);
            group.setChosenGroupExpressionId(chosenGE.getId().asInt());
        }

        // Distribute remaining among children and recurse
        List<PhysicalProperties> inputProps = chosenGE.getInputPropertiesList(properties);
        List<Plan> planChildren = Lists.newArrayList();
        for (int i = 0; i < chosenGE.arity(); i++) {
            Group child = chosenGE.child(i);
            Set<Integer> childReachable = reachableCache.getOrDefault(child, new HashSet());
            Set<Integer> childRemaining = new HashSet<>(remaining);
            childRemaining.retainAll(childReachable);
            planChildren.add(chooseBestPlanWithRequiredGroups(
                    child, inputProps.get(i), childRemaining, reachableCache));
        }

        Plan plan = chosenGE.getPlan().withChildren(planChildren);
        if (!(plan instanceof PhysicalPlan)) {
            throw new AnalysisException("Result plan must be PhysicalPlan");
        }
        plan = plan.withGroupExpression(Optional.of(chosenGE));
        PhysicalPlan physicalPlan = ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                properties, group.getStatistics());
        cost = chosenCost;
        return physicalPlan;
    }

    /**
     * Compute the set of USE_MV hint qualifiers that successfully rewrote.
     * Marks USE_MV hint parameters that did NOT rewrite-success as UnUsed.
     */
    private static Set<List<String>> computeRequiredUseMvIdentifiers(StatementContext statementContext) {
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (!useMvHint.isPresent()) {
            return ImmutableSet.of();
        }
        Set<List<String>> useMvQualifiers = useMvHint.get().getUseMvTableColumnMap().keySet();
        Set<List<String>> rewrittenSuccessSet = statementContext.getMaterializationRewrittenSuccessSet();
        Set<List<String>> required = new HashSet<>(useMvQualifiers);
        required.retainAll(rewrittenSuccessSet);
        // Mark USE_MV qualifiers that did not rewrite-success as UnUsed
        Set<List<String>> notRewritten = new HashSet<>(useMvQualifiers);
        notRewritten.removeAll(rewrittenSuccessSet);
        for (List<String> qualifier : notRewritten) {
            // Update hint map to mark as not-chosen (false) with failure reason
            useMvHint.get().getUseMvTableColumnMap().put(qualifier, false);
        }
        if (!notRewritten.isEmpty()) {
            // If some USE_MV hints failed to rewrite, set status to UNUSED with reason
            useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
            useMvHint.get().setErrorMessage("MV(s) not rewrite-success after CBO: " + notRewritten);
        }
        return required;
    }

    /**
     * Extract the MV qualifier(s) from a scan plan if it matches a required USE_MV identifier.
     * Returns the set of MV qualifiers this plan represents.
     */
    private static Set<List<String>> getScanMvIdentifiers(Plan plan,
            Set<List<String>> requiredUseMvIdentifiers) {
        Set<List<String>> result = new HashSet<>();
        if (plan instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) plan;
            OlapTable table = scan.getTable();
            if (table.getBaseIndexId() != scan.getSelectedIndexId()) {
                List<String> qualifier = new ArrayList<>();
                qualifier.add(table.getDatabase().getCatalog().getName());
                qualifier.add(table.getDatabase().getFullName());
                qualifier.add(table.getName());
                qualifier.add(table.getIndexNameById(scan.getSelectedIndexId()));
                if (requiredUseMvIdentifiers.contains(qualifier)) {
                    result.add(qualifier);
                }
            }
        }
        if (plan instanceof PhysicalCatalogRelation) {
            PhysicalCatalogRelation rel = (PhysicalCatalogRelation) plan;
            if (rel.getTable() instanceof MTMV) {
                List<String> qualifier = rel.getTable().getFullQualifiers();
                if (requiredUseMvIdentifiers.contains(qualifier)) {
                    result.add(qualifier);
                }
            }
            // Also check for sync MV via OlapScan (PhysicalOlapScan is a PhysicalCatalogRelation)
            if (rel instanceof PhysicalOlapScan) {
                PhysicalOlapScan scan = (PhysicalOlapScan) rel;
                OlapTable table = scan.getTable();
                if (table.getBaseIndexId() != scan.getSelectedIndexId()) {
                    List<String> qualifier = new ArrayList<>();
                    qualifier.add(table.getDatabase().getCatalog().getName());
                    qualifier.add(table.getDatabase().getFullName());
                    qualifier.add(table.getName());
                    qualifier.add(table.getIndexNameById(scan.getSelectedIndexId()));
                    if (requiredUseMvIdentifiers.contains(qualifier)) {
                        result.add(qualifier);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Memoized bottom-up DFS: compute all MV identifiers reachable from this group's subtree
     * through physical GroupExpressions (including enforcers).
     */
    private static void computeReachableMvIdentifiers(Group group,
            Map<Group, Set<List<String>>> cache, Set<List<String>> requiredUseMvIdentifiers) {
        if (cache.containsKey(group)) {
            return;
        }
        Set<List<String>> reachable = new HashSet<>();
        // Put in cache BEFORE recursing to break cycles
        cache.put(group, reachable);

        for (GroupExpression ge : group.getPhysicalExpressions()) {
            Set<List<String>> geReachable = getScanMvIdentifiers(ge.getPlan(), requiredUseMvIdentifiers);
            for (Group child : ge.children()) {
                computeReachableMvIdentifiers(child, cache, requiredUseMvIdentifiers);
                geReachable.addAll(cache.get(child));
            }
            reachable.addAll(geReachable);
        }
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            Set<List<String>> enforcerReachable = new HashSet<>();
            for (Group child : enforcer.children()) {
                computeReachableMvIdentifiers(child, cache, requiredUseMvIdentifiers);
                enforcerReachable.addAll(cache.get(child));
            }
            reachable.addAll(enforcerReachable);
        }
    }

    /**
     * Choose the cheapest physical plan which contains at least one of the required materializations.
     * <p>
     * At each group, picks the cheapest physical GroupExpression whose children can reach one of the
     * remaining required materializations, then distributes the remaining requirements among children
     * and recurses.
     *
     * @return the chosen plan together with its estimated cost, or null if no materialization can be
     *         produced together with the required properties
     */
    private static PlanWithCost chooseBestPlanWithRequiredMaterializations(
            Group group, PhysicalProperties properties,
            Set<List<String>> remaining, Map<Group, Set<List<String>>> reachableCache,
            CascadesContext cascadesContext) {
        Set<List<String>> requiredMvs = new HashSet<>(remaining);

        // Remove MVs that this group's own scan expressions cover
        Set<List<String>> ownMvs = new HashSet<>();
        for (GroupExpression ge : group.getPhysicalExpressions()) {
            ownMvs.addAll(getScanMvIdentifiers(ge.getPlan(), requiredMvs));
        }
        requiredMvs.removeAll(ownMvs);

        if (requiredMvs.isEmpty()) {
            return chooseBestPlanWithCost(group, properties, cascadesContext);
        }

        PhysicalProperties selectedProperties = properties;
        GroupExpression chosenGE = chooseGroupExpression(group, properties, requiredMvs, reachableCache);
        if (chosenGE == null) {
            // The hinted materialization may be costed only with another property than the required
            // one, for example ANY or its own storage distribution, because the cheapest alternative of
            // the group owns the required property. Select it on an available property and enforce the
            // required one afterwards.
            Pair<GroupExpression, PhysicalProperties> alternative =
                    findAlternativeWithProperty(group, requiredMvs, reachableCache);
            if (alternative == null) {
                return null;
            }
            chosenGE = alternative.first;
            selectedProperties = alternative.second;
        }

        if (group.getEnforcers().containsKey(chosenGE)) {
            return choosePlanWithEnforcer(group, chosenGE, properties, requiredMvs, reachableCache,
                    cascadesContext);
        }

        // Handle enforcer marking (same as chooseBestPlan)
        if (chosenGE.getPlan() instanceof PhysicalDistribute
                && group.getEnforcerSpecs().containsKey(
                        ((PhysicalDistribute<?>) chosenGE.getPlan()).getDistributionSpec())) {
            group.addChosenEnforcerId(chosenGE.getId().asInt());
            group.addChosenEnforcerProperties(selectedProperties);
        } else {
            group.setChosenProperties(selectedProperties);
            group.setChosenGroupExpressionId(chosenGE.getId().asInt());
        }

        // Distribute remaining among children and recurse
        List<PhysicalProperties> inputProps = chosenGE.getInputPropertiesList(selectedProperties);
        List<PlanWithCost> childPlans = Lists.newArrayListWithCapacity(chosenGE.arity());
        double memoChildrenCost = 0.0;
        for (int i = 0; i < chosenGE.arity(); i++) {
            Group child = chosenGE.child(i);
            Set<List<String>> childReachable = reachableCache.getOrDefault(child, new HashSet<>());
            Set<List<String>> childRemaining = new HashSet<>(requiredMvs);
            childRemaining.retainAll(childReachable);
            PlanWithCost childPlan = chooseBestPlanWithRequiredMaterializations(
                    child, inputProps.get(i), childRemaining, reachableCache, cascadesContext);
            if (childPlan == null) {
                return null;
            }
            childPlans.add(childPlan);
            memoChildrenCost += child.getLowestCostPlan(inputProps.get(i))
                    .map(costAndPlan -> costAndPlan.first.getValue()).orElse(0.0);
        }

        // The children were chosen by the materialization reachability instead of the cost, so the
        // property contract of the costed group expression can be stale: a child can be replaced by a
        // materialization which is laid out by another relation. Re-derive the actual children outputs
        // and enforce the missing properties again, instead of keeping the costed contract.
        boolean contractHolds = childrenProvideProperties(inputProps, childPlans);
        boolean alignedChildren = requiresAlignedChildren(chosenGE.getPlan());
        List<Plan> planChildren = Lists.newArrayListWithCapacity(childPlans.size());
        double childrenCost = 0.0;
        for (int i = 0; i < childPlans.size(); i++) {
            // Every hash distributed child of a join or of a set operation is realigned, not only the
            // replaced one: the data layout pairing of the costed plan does not hold anymore once one
            // child of such a group expression is replaced by a materialization.
            boolean forceDistribution = alignedChildren && !contractHolds
                    && inputProps.get(i).getDistributionSpec() instanceof DistributionSpecHash;
            PlanWithCost childPlan = enforceChildProperties(chosenGE.child(i), childPlans.get(i),
                    inputProps.get(i), forceDistribution, cascadesContext);
            planChildren.add(childPlan.plan);
            childrenCost += childPlan.cost;
        }

        Plan plan = chosenGE.getPlan().withChildren(planChildren);
        if (!(plan instanceof PhysicalPlan)) {
            throw new AnalysisException("Result plan must be PhysicalPlan");
        }
        plan = plan.withGroupExpression(Optional.of(chosenGE));
        PhysicalProperties outputProperties = contractHolds
                ? chosenGE.getOutputProperties(selectedProperties)
                : deriveOutputProperties(plan, planChildren, cascadesContext);
        PhysicalPlan physicalPlan = ((PhysicalPlan) plan)
                .withPhysicalPropertiesAndStats(outputProperties, group.getStatistics());
        // The node cost is what the memo costed for the group expression without its costed children,
        // the children are charged with the forced alternatives and the synthesized enforcers instead.
        double nodeCost = Math.max(0.0, chosenGE.getCostByProperties(selectedProperties) - memoChildrenCost);
        return new PlanWithCost(physicalPlan, nodeCost + childrenCost);
    }

    /**
     * Choose the cheapest group expression of the group which can produce the required materializations
     * together with the required properties.
     */
    private static GroupExpression chooseGroupExpression(Group group, PhysicalProperties properties,
            Set<List<String>> requiredMvs, Map<Group, Set<List<String>>> reachableCache) {
        List<GroupExpression> candidates = Lists.newArrayList(group.getPhysicalExpressions());
        candidates.addAll(group.getEnforcers().values());
        GroupExpression chosenGE = null;
        double chosenCost = Double.MAX_VALUE;
        for (GroupExpression candidate : candidates) {
            if (!candidate.getLowestCostTable().containsKey(properties)) {
                continue;
            }
            double candidateCost = candidate.getCostByProperties(properties);
            if (candidateCost >= chosenCost) {
                continue;
            }
            if (reaches(candidate, requiredMvs, reachableCache)) {
                chosenGE = candidate;
                chosenCost = candidateCost;
            }
        }
        return chosenGE;
    }

    /**
     * Find the cheapest group expression which produces the required materializations, together with the
     * property it is costed with.
     */
    private static Pair<GroupExpression, PhysicalProperties> findAlternativeWithProperty(Group group,
            Set<List<String>> requiredMvs, Map<Group, Set<List<String>>> reachableCache) {
        Pair<GroupExpression, PhysicalProperties> chosenAlternative = null;
        double chosenCost = Double.MAX_VALUE;
        for (GroupExpression candidate : group.getPhysicalExpressions()) {
            if (!reaches(candidate, requiredMvs, reachableCache)) {
                continue;
            }
            for (Map.Entry<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> propertyAndCost
                    : candidate.getLowestCostTable().entrySet()) {
                double candidateCost = propertyAndCost.getValue().first.getValue();
                if (candidateCost < chosenCost) {
                    chosenAlternative = Pair.of(candidate, propertyAndCost.getKey());
                    chosenCost = candidateCost;
                }
            }
        }
        return chosenAlternative;
    }

    /**
     * Whether the children of the group expression can reach at least one of the required
     * materializations.
     */
    private static boolean reaches(GroupExpression groupExpression, Set<List<String>> requiredMvs,
            Map<Group, Set<List<String>>> reachableCache) {
        Set<List<String>> childReachable = new HashSet<>();
        for (Group child : groupExpression.children()) {
            Set<List<String>> reachable = reachableCache.get(child);
            if (reachable != null) {
                childReachable.addAll(reachable);
            }
        }
        childReachable.retainAll(requiredMvs);
        return !childReachable.isEmpty();
    }

    /**
     * Plan the materializations behind an enforcer of the group.
     * <p>
     * An enforcer is shared by all the alternatives of the group and the input property it recorded is
     * the one the cheapest alternative provided, so the forced alternative is planned on its own
     * property and the required properties are then enforced on its actual output.
     */
    private static PlanWithCost choosePlanWithEnforcer(Group group, GroupExpression enforcer,
            PhysicalProperties properties, Set<List<String>> requiredMvs,
            Map<Group, Set<List<String>>> reachableCache, CascadesContext cascadesContext) {
        PlanWithCost childPlan = chooseBestPlanWithRequiredMaterializations(group,
                PhysicalProperties.ANY, requiredMvs, reachableCache, cascadesContext);
        if (childPlan == null) {
            return null;
        }
        group.addChosenEnforcerId(enforcer.getId().asInt());
        group.addChosenEnforcerProperties(properties);
        return enforceProperties(childPlan, properties, false, group, cascadesContext);
    }

    /**
     * Whether the children of the plan must be aligned with each other, which is the case for joins and
     * set operations. The other plans only inherit the properties of their single child.
     */
    private static boolean requiresAlignedChildren(Plan plan) {
        return plan instanceof AbstractPhysicalJoin || plan instanceof PhysicalSetOperation;
    }

    /**
     * Check whether the actual physical properties provide the required ones.
     * <p>
     * {@link DistributionSpecHash#satisfy} and {@link DistributionSpecHash#equals} compare the shuffle
     * type and the shuffle columns only, they ignore the relation, the index and the partitions. A
     * materialization which replaces a relation is laid out by its own table, so it compares equal to
     * the distribution of the replaced relation even though the two are not co-located. The layout
     * identity must be compared explicitly when the property regulation of the cost based selection is
     * by-passed.
     */
    private static boolean providesProperties(PhysicalProperties actual, PhysicalProperties required) {
        return providesDistribution(actual.getDistributionSpec(), required.getDistributionSpec())
                && actual.getOrderSpec().satisfy(required.getOrderSpec());
    }

    private static boolean providesDistribution(DistributionSpec actual, DistributionSpec required) {
        if (!actual.satisfy(required)) {
            return false;
        }
        if (!(required instanceof DistributionSpecHash) || !(actual instanceof DistributionSpecHash)) {
            return true;
        }
        DistributionSpecHash requiredHash = (DistributionSpecHash) required;
        ShuffleType requiredShuffleType = requiredHash.getShuffleType();
        if (requiredShuffleType != ShuffleType.NATURAL && requiredShuffleType != ShuffleType.STORAGE_BUCKETED) {
            // The required distribution does not identify a storage layout.
            return true;
        }
        DistributionSpecHash actualHash = (DistributionSpecHash) actual;
        return actualHash.getTableId() == requiredHash.getTableId()
                && actualHash.getSelectedIndexId() == requiredHash.getSelectedIndexId()
                && actualHash.getPartitionIds().equals(requiredHash.getPartitionIds());
    }

    /**
     * Check whether the actual distribution delivers the shuffle columns of the required one. Unlike
     * {@link #providesDistribution}, a distribution with another storage layout is accepted, because a
     * single child only propagates its distribution to the group expression, so replacing it with a
     * materialization does not need a data movement as long as the shuffle columns keep matching.
     */
    private static boolean providesShuffleColumns(DistributionSpec actual, DistributionSpec required) {
        if (!(required instanceof DistributionSpecHash) || !(actual instanceof DistributionSpecHash)) {
            return actual.satisfy(required);
        }
        return actual.satisfy(((DistributionSpecHash) required).withShuffleType(ShuffleType.REQUIRE));
    }

    /**
     * Check whether every forced child provides the properties the memo costed the group expression
     * with.
     */
    private static boolean childrenProvideProperties(List<PhysicalProperties> inputProps,
            List<PlanWithCost> childPlans) {
        for (int i = 0; i < childPlans.size(); i++) {
            if (!providesProperties(childPlans.get(i).plan.getPhysicalProperties(), inputProps.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Enforce the input properties which the group expression was costed with on a forced child.
     *
     * @param forceDistribution whether the distribution is enforced even if the child already delivers
     *         the required shuffle columns
     */
    private static PlanWithCost enforceChildProperties(Group childGroup, PlanWithCost childPlan,
            PhysicalProperties requiredProperties, boolean forceDistribution, CascadesContext cascadesContext) {
        if (!forceDistribution
                && providesProperties(childPlan.plan.getPhysicalProperties(), requiredProperties)) {
            return childPlan;
        }
        return enforceProperties(childPlan, requiredProperties, forceDistribution, childGroup, cascadesContext);
    }

    /**
     * Add the enforcers which deliver the required properties, the same way
     * {@link org.apache.doris.nereids.properties.EnforceMissingPropertiesHelper} does it during the cost
     * based selection. A merge sort reads an already locally sorted input, so the local sort has to be
     * placed below the exchange which gathers the sorted data.
     *
     * @param forceDistribution whether the distribution is enforced even if the child already delivers
     *         the required shuffle columns
     */
    private static PlanWithCost enforceProperties(PlanWithCost childPlan, PhysicalProperties requiredProperties,
            boolean forceDistribution, Group childGroup, CascadesContext cascadesContext) {
        PhysicalProperties actualProperties = childPlan.plan.getPhysicalProperties();
        boolean needDistribution = forceDistribution || !providesShuffleColumns(
                actualProperties.getDistributionSpec(), requiredProperties.getDistributionSpec());
        OrderSpec requiredOrder = requiredProperties.getOrderSpec();
        boolean needOrder = !actualProperties.getOrderSpec().satisfy(requiredOrder);
        if (!needDistribution && !needOrder) {
            return childPlan;
        }
        // A merge sort requires an already locally sorted input, so the local sort is enforced below the
        // exchange when the required distribution is gather.
        boolean mergeSort = needOrder && requiredProperties.getDistributionSpec() instanceof DistributionSpecGather;
        PlanWithCost enforcedPlan = childPlan;
        if (mergeSort) {
            enforcedPlan = addSort(enforcedPlan, requiredOrder, SortPhase.LOCAL_SORT, childGroup, cascadesContext);
        }
        if (needDistribution) {
            enforcedPlan = addExchange(enforcedPlan, requiredProperties.getDistributionSpec(),
                    childGroup, cascadesContext);
        }
        if (mergeSort) {
            enforcedPlan = addSort(enforcedPlan, requiredOrder, SortPhase.MERGE_SORT, childGroup, cascadesContext);
        } else if (needOrder) {
            enforcedPlan = addSort(enforcedPlan, requiredOrder, SortPhase.LOCAL_SORT, childGroup, cascadesContext);
        }
        return enforcedPlan;
    }

    /**
     * Add an exchange which delivers the required distribution. A hash distribution without a shuffle
     * type is delivered by a hash shuffle, the same way
     * {@link org.apache.doris.nereids.properties.EnforceMissingPropertiesHelper} does it.
     */
    private static PlanWithCost addExchange(PlanWithCost childPlan, DistributionSpec requiredDistribution,
            Group childGroup, CascadesContext cascadesContext) {
        DistributionSpec exchangeDistribution = requiredDistribution instanceof DistributionSpecHash
                ? ((DistributionSpecHash) requiredDistribution).withShuffleType(ShuffleType.EXECUTION_BUCKETED)
                : requiredDistribution;
        PhysicalDistribute<Plan> exchange = new PhysicalDistribute<>(exchangeDistribution, Optional.empty(),
                childPlan.plan.getLogicalProperties(), new PhysicalProperties(exchangeDistribution),
                childPlan.plan.getStats(), childPlan.plan);
        return new PlanWithCost(exchange,
                childPlan.cost + calculateEnforcerCost(cascadesContext, exchange, childGroup, childPlan.plan));
    }

    /**
     * Add a sort which delivers the required order.
     */
    private static PlanWithCost addSort(PlanWithCost childPlan, OrderSpec requiredOrder, SortPhase sortPhase,
            Group childGroup, CascadesContext cascadesContext) {
        PhysicalProperties childProperties = childPlan.plan.getPhysicalProperties();
        PhysicalQuickSort<Plan> sort = new PhysicalQuickSort<>(requiredOrder.getOrderKeys(), sortPhase,
                Optional.empty(), childPlan.plan.getLogicalProperties(),
                new PhysicalProperties(childProperties.getDistributionSpec(), requiredOrder),
                childPlan.plan.getStats(), childPlan.plan);
        return new PlanWithCost(sort,
                childPlan.cost + calculateEnforcerCost(cascadesContext, sort, childGroup, childPlan.plan));
    }

    /**
     * Estimate the cost of an enforcer which is synthesized outside of the memo, it is charged the same
     * way as the enforcers which the cost based selection adds.
     */
    private static double calculateEnforcerCost(CascadesContext cascadesContext, PhysicalPlan enforcer,
            Group childGroup, PhysicalPlan childPlan) {
        GroupExpression groupExpression = new GroupExpression(enforcer, Lists.newArrayList(childGroup));
        // The enforcer belongs to the group it enforces, its cost is estimated from the statistics of
        // that group.
        groupExpression.setOwnerGroup(childGroup);
        try {
            Cost cost = CostCalculator.calculateCost(cascadesContext.getConnectContext(), groupExpression,
                    Lists.newArrayList(childPlan.getPhysicalProperties()),
                    cascadesContext.getStatementContext().getCostWeight());
            return cost.getValue();
        } finally {
            // The group expression is only created for the estimation, keep the memo untouched.
            childGroup.removeParentExpression(groupExpression);
        }
    }

    /**
     * A physical plan together with its estimated cost.
     */
    private static class PlanWithCost {
        private final PhysicalPlan plan;
        private final double cost;

        PlanWithCost(PhysicalPlan plan, double cost) {
            this.plan = plan;
            this.cost = cost;
        }
    }

    /**
     * Re-derive the output properties of a forced plan from its actual children, the same way the
     * physical properties are derived during the cost computation.
     */
    private static PhysicalProperties deriveOutputProperties(Plan plan, List<Plan> planChildren,
            CascadesContext cascadesContext) {
        if (plan instanceof PhysicalDistribute) {
            // An exchange is annotated with the distribution it delivers when it is built, the
            // group expression plan kept in the memo is not.
            return new PhysicalProperties(((PhysicalDistribute<?>) plan).getDistributionSpec());
        }
        List<PhysicalProperties> childrenProperties = Lists.newArrayListWithCapacity(planChildren.size());
        for (Plan child : planChildren) {
            childrenProperties.add(getPhysicalProperties(child));
        }
        return plan.accept(new ChildOutputPropertyDeriver(childrenProperties),
                new PlanContext(cascadesContext.getConnectContext(), plan.getGroupExpression().orElse(null),
                        childrenProperties));
    }

    private static PhysicalProperties getPhysicalProperties(Plan plan) {
        return ((PhysicalPlan) plan).getPhysicalProperties();
    }

    /**
     * After final physical plan is chosen, validate USE_MV hint status
     * by checking which required MVs are actually in the chosen plan.
     */
    public static void finalizeUseMvHintStatus(StatementContext statementContext, PhysicalPlan physicalPlan) {
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (!useMvHint.isPresent()) {
            return;
        }
        // Do not override SYNTAX_ERROR status set during init phase
        if (useMvHint.get().isSyntaxError()) {
            return;
        }
        Map<List<String>, Boolean> useMvMap = useMvHint.get().getUseMvTableColumnMap();
        Set<List<String>> useMvQualifiers = new HashSet<>(useMvMap.keySet());
        // The selected scans are the ground truth of which materializations are used. The rewrite
        // success flag of a materialization context can not be used here: a sync materialization is
        // selected directly by the INDEX clause or by the USE_MV hint when the relation is bound, and
        // it then does not go through a successful cost based rewrite.
        Set<List<String>> chosenQualifiers = collectChosenMvIdentifiers(physicalPlan, useMvQualifiers);

        // Check if the required MVs are actually chosen, USE_MV is satisfied when at least one
        // of the hinted MVs is chosen because several hinted MVs may be alternatives of each other
        boolean anyChosen = false;
        for (List<String> qualifier : useMvQualifiers) {
            boolean chosen = chosenQualifiers.contains(qualifier);
            useMvMap.put(qualifier, chosen);
            anyChosen = anyChosen || chosen;
        }
        if (anyChosen) {
            useMvHint.get().setStatus(Hint.HintStatus.SUCCESS);
        } else if (!useMvQualifiers.isEmpty()) {
            useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
            useMvHint.get().setErrorMessage("MV(s) are not used in the final plan: " + useMvQualifiers);
        }
    }

    /**
     * Collect the identifiers of the materializations which are selected in the physical plan.
     *
     * @param candidates the materialization identifiers to collect, the others are ignored
     */
    private static Set<List<String>> collectChosenMvIdentifiers(PhysicalPlan physicalPlan,
            Set<List<String>> candidates) {
        Set<List<String>> chosen = new HashSet<>();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Void>() {
            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation, Void context) {
                chosen.addAll(getScanMvIdentifiers(physicalRelation, candidates));
                return null;
            }

            @Override
            public Void visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan lazyScan,
                    Void context) {
                chosen.addAll(getScanMvIdentifiers(lazyScan, candidates));
                return null;
            }
        }, null);
        return chosen;
    }

    private long getGarbageCollectionTime() {
        if (!ConnectContext.get().getSessionVariable().enableProfile()) {
            return 0;
        }
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
        int distributeHintIndex = 0;
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
        if (getConnectContext().getSessionVariable().enableExplainNone) {
            return "";
        }
        ExplainLevel explainLevel = getExplainLevel(explainOptions);
        String plan = "";
        String mvSummary = "";
        if ((this.getPhysicalPlan() != null || this.getOptimizedPlan() != null) && cascadesContext != null) {
            mvSummary = cascadesContext.getMaterializationContexts().isEmpty() ? "" :
                    "\n\n========== MATERIALIZATIONS ==========\n"
                            + MaterializationContext.toSummaryString(cascadesContext,
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
                Memo memo = cascadesContext.getMemo();
                if (memo == null) {
                    plan = "Memo is null";
                } else {
                    plan = memo.toString()
                        + "\n\n========== OPTIMIZED PLAN ==========\n"
                        + optimizedPlan.treeString()
                        + mvSummary;
                }
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
                        + "========== LOCK TABLE "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsLockTableTime) + " ==========\n"
                        + "\n\n"
                        + "========== ANALYZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsAnalysisTime) + " ==========\n"
                        + analyzedPlan.treeString() + "\n\n"
                        + "========== REWRITTEN PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsRewriteTime) + " ==========\n"
                        + rewrittenPlan.treeString() + "\n\n"
                        + "========== PRE REWRITTEN BY MV "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsPreRewriteByMvTime) + " ==========\n"
                        + "========== OPTIMIZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsOptimizeTime) + " ==========\n"
                        + optimizedPlan.treeString() + "\n\n";
                if (cascadesContext != null && cascadesContext.getMemo() != null) {
                    plan += "========== MEMO " + cascadesContext.getMemo().toString() + "\n\n";
                }

                if (distributedPlans != null && !distributedPlans.isEmpty()) {
                    plan += "========== DISTRIBUTED PLAN "
                            + getTimeMetricString(SummaryProfile::getPrettyNereidsDistributeTime) + " ==========\n";
                    plan += DistributedPlan.toString(Lists.newArrayList(distributedPlans.values())) + "\n\n";
                }
                plan += mvSummary;
                break;
            default:
                plan = super.getExplainString(explainOptions);
                plan += mvSummary;
                plan += "\n\n\n========== STATISTICS ==========\n";
                if (statementContext != null) {
                    if (statementContext.isHasUnknownColStats()) {
                        plan += "planned with unknown column statistics\n";
                    }
                }
        }
        if (ConnectContext.get() != null && cascadesContext != null
                && cascadesContext.getGroupExpressionCount()
                        > ConnectContext.get().getSessionVariable().memoMaxGroupExpressionSize) {
            plan += "\n\n\n group expression count exceeds memo_max_group_expression_size("
                    + ConnectContext.get().getSessionVariable().memoMaxGroupExpressionSize + ")\n";
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
        return new ArrayList<>(cascadesContext.getRuntimeFilterContext().getLegacyFilters());
    }

    @Override
    public Optional<ResultSet> handleQueryInFe(StatementBase parsedStmt) {
        if (!(parsedStmt instanceof LogicalPlanAdapter)) {
            return Optional.empty();
        }

        setFormatOptions();
        if (physicalPlan instanceof ComputeResultSet) {
            Optional<ResultSet> resultSet = ((ComputeResultSet) physicalPlan)
                    .computeResultInFe(cascadesContext, physicalPlan.getOutput());
            if (resultSet.isPresent()) {
                tryAddResultSetToSqlCache(resultSet.get());
                return resultSet;
            }
        }

        return Optional.empty();
    }

    private void tryAddResultSetToSqlCache(ResultSet resultSet) {
        if (physicalPlan instanceof PhysicalSqlCache) {
            return;
        }
        Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
        if (!sqlCacheContext.isPresent()
                || !CacheAnalyzer.canUseSqlCache(statementContext.getConnectContext().getSessionVariable())) {
            return;
        }
        sqlCacheContext.get().setResultSetInFe(resultSet);
        Env.getCurrentEnv().getSqlCacheManager().tryAddFeSqlCache(
                statementContext.getConnectContext(),
                statementContext.getOriginStatement().originStmt);
    }

    private void setFormatOptions() {
        ConnectContext ctx = statementContext.getConnectContext();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        switch (sessionVariable.serdeDialect) {
            case "presto":
            case "trino":
                statementContext.setFormatOptions(FormatOptions.getForPresto());
                break;
            case "hive":
                statementContext.setFormatOptions(FormatOptions.getForHive());
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

    public ConnectContext getConnectContext() {
        return cascadesContext == null ? ConnectContext.get() : cascadesContext.getConnectContext();
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public static PhysicalProperties buildInitRequireProperties() {
        return PhysicalProperties.GATHER;
    }

    protected ExplainLevel getExplainLevel(ExplainOptions explainOptions) {
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

    public void setAnalyzedPlan(Plan analyzedPlan) {
        this.analyzedPlan = analyzedPlan;
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
        return explainOptions != null && explainOptions.showPlanProcess();
    }

    protected void keepOrShowPlanProcess(boolean showPlanProcess, Runnable task) {
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
