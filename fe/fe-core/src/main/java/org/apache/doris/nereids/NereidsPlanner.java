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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
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
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.distribute.DistributePlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDictionarySink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TQueryCacheParam;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {
    // private static final AtomicInteger executeCount = new AtomicInteger(0);
    public static final Logger LOG = LogManager.getLogger(NereidsPlanner.class);

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
        }

        if (LOG.isDebugEnabled()) {
            LOG.info(getExplainString(new ExplainOptions(ExplainLevel.SHAPE_PLAN, false)));
            LOG.info(getExplainString(new ExplainOptions(ExplainLevel.DISTRIBUTED_PLAN, false)));
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
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsLockTableFinishTime(TimeUtils.getStartTimeMs());
        }
    }

    protected void analyze(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start analyze plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newAnalyzer().analyze());
        NereidsTracer.logImportantTime("EndAnalyzePlan");
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
        NereidsTracer.logImportantTime("EndRewritePlan");
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start pre rewrite plan by mv");
        }
        List<Plan> tmpPlansForMvRewrite = cascadesContext.getStatementContext().getTmpPlanForMvRewrite();
        List<Plan> plansWhichContainMv = new ArrayList<>();
        for (Plan planForRewrite : tmpPlansForMvRewrite) {
            if (!planForRewrite.getLogicalProperties().equals(
                    cascadesContext.getRewritePlan().getLogicalProperties())) {
                continue;
            }
            try {
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
                plansWhichContainMv.add(ruleOptimizedPlan);
            } catch (Exception e) {
                LOG.error("pre mv rewrite in rbo rewrite fail, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier(), e);
            }
        }
        // clear the rewritten plans which are tmp optimized, should be filled by full optimize later
        statementContext.getRewrittenPlansByMv().clear();
        // if rule-based optimized, would not be rewritten by cbo, so clear materialized hooks
        this.cascadesContext.getStatementContext().setPreMvRewritten(true);
        if (plansWhichContainMv.isEmpty()) {
            return;
        }
        plansWhichContainMv.forEach(statementContext::addRewrittenPlanByMv);
        NereidsTracer.logImportantTime("EndPreRewritePlanByMv");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End pre rewrite plan by mv");
        }
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsPreRewriteByMvFinishTime(TimeUtils.getStartTimeMs());
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
        NereidsTracer.logImportantTime("EndOptimizePlan");
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

        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator(planTranslatorContext,
                statementContext.getConnectContext().getStatsErrorEstimator());
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        if (sessionVariable.isEnableNereidsTrace()) {
            CounterEvent.clearCounter();
        }
        if (sessionVariable.isPlayNereidsDump()) {
            return;
        }
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
                    cascadesContext, Optional.empty(), physicalPlan.getOutput());
            if (resultSet.isPresent()) {
                notNeedBackend = true;
            }
        }

        distributedPlans = new DistributePlanner(statementContext, fragments, notNeedBackend, false).plan();
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
        ArrayList<RuntimeFilter> runtimeFilters = new ArrayList<>();
        runtimeFilters.addAll(cascadesContext.getRuntimeFilterContext().getLegacyFilters());
        runtimeFilters.addAll(cascadesContext.getRuntimeFilterV2Context().getLegacyFilters());
        return runtimeFilters;
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
