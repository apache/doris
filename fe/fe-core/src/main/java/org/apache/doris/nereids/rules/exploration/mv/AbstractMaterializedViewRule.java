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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.ExpressionInfo;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PartitionRemover;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.TableId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The abstract class for all materialized view rules
 */
public abstract class AbstractMaterializedViewRule implements ExplorationRuleFactory {

    public static final Logger LOG = LogManager.getLogger(AbstractMaterializedViewRule.class);
    public static final Set<JoinType> SUPPORTED_JOIN_TYPE_SET = ImmutableSet.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN);

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * multi materialization every time. if exception it will catch the exception and record it to
     * materialization context.
     */
    public List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<Plan> rewrittenPlans = new ArrayList<>();
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        // if available materialization list is empty, bail out
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (cascadesContext.getMaterializationContexts().isEmpty()) {
            return rewrittenPlans;
        }
        if (statementContext.getMaterializedViewRewriteDuration()
                > sessionVariable.materializedViewRewriteDurationThresholdMs) {
            LOG.warn("materialized view rewrite duration is exceeded, the query queryId is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            MaterializationContext.makeFailWithDurationExceeded(queryPlan, cascadesContext.getMaterializationContexts(),
                    statementContext.getMaterializedViewRewriteDuration());
            return rewrittenPlans;
        }
        for (MaterializationContext materializationContext : cascadesContext.getMaterializationContexts()) {
            statementContext.getMaterializedViewStopwatch().reset().start();
            if (checkIfRewritten(queryPlan, materializationContext)) {
                continue;
            }
            // check mv plan is valid or not
            if (!isMaterializationValid(queryPlan, cascadesContext, materializationContext)) {
                continue;
            }
            // get query struct infos according to the view strut info, if valid query struct infos is empty, bail out
            List<StructInfo> queryStructInfos = getValidQueryStructInfos(queryPlan, cascadesContext,
                    materializationContext);
            if (queryStructInfos.isEmpty()) {
                continue;
            }
            long elapsed = statementContext.getMaterializedViewStopwatch().elapsed(TimeUnit.MILLISECONDS);
            statementContext.addMaterializedViewRewriteDuration(elapsed);
            SummaryProfile profile = SummaryProfile.getSummaryProfile(cascadesContext.getConnectContext());
            if (profile != null) {
                profile.addNereidsMvRewriteTime(elapsed);
            }
            for (StructInfo queryStructInfo : queryStructInfos) {
                statementContext.getMaterializedViewStopwatch().reset().start();
                if (statementContext.getMaterializedViewRewriteDuration()
                        > sessionVariable.materializedViewRewriteDurationThresholdMs) {
                    statementContext.getMaterializedViewStopwatch().stop();
                    LOG.warn("materialized view rewrite duration is exceeded, the queryId is {}",
                            cascadesContext.getConnectContext().getQueryIdentifier());
                    MaterializationContext.makeFailWithDurationExceeded(queryStructInfo.getOriginalPlan(),
                            cascadesContext.getMaterializationContexts(),
                            statementContext.getMaterializedViewRewriteDuration());
                    return rewrittenPlans;
                }
                try {
                    if (rewrittenPlans.size() < sessionVariable.getMaterializedViewRewriteSuccessCandidateNum()) {
                        rewrittenPlans.addAll(doRewrite(queryStructInfo, cascadesContext, materializationContext));
                    }
                } catch (Exception exception) {
                    LOG.warn("Materialized view rule exec fail", exception);
                    materializationContext.recordFailReason(queryStructInfo,
                            "Materialized view rule exec fail", exception::toString);
                } finally {
                    elapsed = statementContext.getMaterializedViewStopwatch().elapsed(TimeUnit.MILLISECONDS);
                    statementContext.addMaterializedViewRewriteDuration(elapsed);
                    if (profile != null) {
                        profile.addNereidsMvRewriteTime(elapsed);
                    }
                }
            }
        }
        return rewrittenPlans;
    }

    /**
     * Get valid query struct infos, if invalid record the invalid reason
     */
    protected List<StructInfo> getValidQueryStructInfos(Plan queryPlan, CascadesContext cascadesContext,
            MaterializationContext materializationContext) {
        List<StructInfo> validStructInfos = new ArrayList<>();
        // For every materialized view we should trigger refreshing struct info map
        List<StructInfo> uncheckedQueryStructInfos = MaterializedViewUtils.extractStructInfoFuzzy(queryPlan, queryPlan,
                cascadesContext, materializationContext.getCommonTableIdSet(cascadesContext.getStatementContext()));
        uncheckedQueryStructInfos.forEach(queryStructInfo -> {
            boolean valid = checkQueryPattern(queryStructInfo, cascadesContext) && queryStructInfo.isValid();
            if (!valid) {
                cascadesContext.getMaterializationContexts().forEach(ctx ->
                        ctx.recordFailReason(queryStructInfo, "Query struct info is invalid",
                                () -> String.format("query table bitmap is %s, plan is %s",
                                        queryStructInfo.getRelations(), queryPlan.treeString())
                        ));
            } else {
                validStructInfos.add(queryStructInfo);
            }
        });
        return validStructInfos;
    }

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * only one materialization every time. Different query pattern should override the sub logic.
     */
    protected List<Plan> doRewrite(StructInfo queryStructInfo, CascadesContext cascadesContext,
            MaterializationContext materializationContext) throws AnalysisException {
        List<Plan> rewriteResults = new ArrayList<>();
        StructInfo viewStructInfo = materializationContext.getStructInfo();
        MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations(),
                cascadesContext);
        if (MatchMode.COMPLETE != matchMode && MatchMode.QUERY_PARTIAL != matchMode) {
            materializationContext.recordFailReason(queryStructInfo, "Match mode is invalid",
                    () -> String.format("matchMode is %s", matchMode));
            return rewriteResults;
        }
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        int materializedViewRelationMappingMaxCount = sessionVariable.getMaterializedViewRelationMappingMaxCount();
        List<RelationMapping> queryToViewTableMappings = RelationMapping.generate(queryStructInfo.getRelations(),
                viewStructInfo.getRelations(), materializedViewRelationMappingMaxCount);
        // if any relation in query and view can not map, bail out.
        if (queryToViewTableMappings == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Query to view table mapping is null", () -> "");
            return rewriteResults;
        }
        for (RelationMapping queryToViewTableMapping : queryToViewTableMappings) {
            SlotMapping queryToViewSlotMapping =
                    materializationContext.getSlotMappingFromCache(queryToViewTableMapping);
            if (queryToViewSlotMapping == null) {
                queryToViewSlotMapping = SlotMapping.generate(queryToViewTableMapping);
                materializationContext.addSlotMappingToCache(queryToViewTableMapping, queryToViewSlotMapping);
            }
            if (queryToViewSlotMapping == null) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Query to view slot mapping is null", () ->
                                String.format("queryToViewTableMapping relation mapping is %s",
                                        queryToViewTableMapping));
                continue;
            }
            SlotMapping viewToQuerySlotMapping = queryToViewSlotMapping.inverse();
            LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                    queryToViewTableMapping, viewToQuerySlotMapping, queryStructInfo, viewStructInfo);
            ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(queryStructInfo, viewStructInfo,
                    compatibilityContext);
            if (comparisonResult.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "The graph logic between query and view is not consistent",
                        comparisonResult::getErrorMessage);
                continue;
            }
            SplitPredicate compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                    viewToQuerySlotMapping, comparisonResult, cascadesContext);
            // Can not compensate, bail out
            if (compensatePredicates.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Predicate compensate fail",
                        () -> String.format("query predicates = %s,\n query equivalenceClass = %s, \n"
                                        + "view predicates = %s,\n query equivalenceClass = %s\n"
                                        + "comparisonResult = %s ", queryStructInfo.getPredicates(),
                                queryStructInfo.getEquivalenceClass(), viewStructInfo.getPredicates(),
                                viewStructInfo.getEquivalenceClass(), comparisonResult));
                continue;
            }
            Plan rewrittenPlan;
            Plan mvScan = materializationContext.getScanPlan(queryStructInfo, cascadesContext);
            Plan queryPlan = queryStructInfo.getTopPlan();
            if (compensatePredicates.isAlwaysTrue()) {
                rewrittenPlan = mvScan;
            } else {
                // Try to rewrite compensate predicates by using mv scan
                List<Expression> rewriteCompensatePredicates = rewriteExpression(compensatePredicates.toList(),
                        queryPlan, materializationContext.getShuttledExprToScanExprMapping(),
                        viewToQuerySlotMapping, compensatePredicates.getRangePredicateMap(), cascadesContext);
                if (rewriteCompensatePredicates.isEmpty()) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Rewrite compensate predicate by view fail",
                            () -> String.format("compensatePredicates = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                            + "viewToQuerySlotMapping = %s",
                                    compensatePredicates, materializationContext.getShuttledExprToScanExprMapping(),
                                    viewToQuerySlotMapping));
                    continue;
                }
                rewrittenPlan = new LogicalFilter<>(Sets.newLinkedHashSet(rewriteCompensatePredicates), mvScan);
            }
            boolean checkResult = rewriteQueryByViewPreCheck(matchMode, queryStructInfo,
                    viewStructInfo, viewToQuerySlotMapping, rewrittenPlan, materializationContext,
                    comparisonResult);
            if (!checkResult) {
                continue;
            }
            // Rewrite query by view
            rewrittenPlan = rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    rewrittenPlan, materializationContext, cascadesContext);
            // This is needed whenever by pre rbo mv rewrite or final cbo rewrite, because the following optimize
            // has the partition prune, this is important for the baseTableNeedUnionPartitionNameSet.addAll code
            // in method calcInvalidPartitions, such as mv has 17, 18, 19 three partitions, 18 is invalid as insert data
            // get mv valid partitions is only 18(query only used 18), 17 and 19 should not be added on base table
            // mvNeedRemovePartitionNameSet is 17,19 (because not partition pruned)
            // baseTableNeedUnionPartitionNameSet contains all mvNeedRemovePartitionNameSet, the result is (17, 19)
            // this is not correct, so the rewrittenPlan need to be partition pruned
            rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getWholeTreeRewriter(childContext).execute();
                        return childContext.getRewritePlan();
                    }, rewrittenPlan, queryPlan, false);
            if (rewrittenPlan == null) {
                continue;
            }
            Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> invalidPartitions;
            if (PartitionCompensator.needUnionRewrite(materializationContext, cascadesContext.getStatementContext())
                    && sessionVariable.isEnableMaterializedViewUnionRewrite()) {
                MTMV mtmv = ((AsyncMaterializationContext) materializationContext).getMtmv();
                Map<List<String>, Set<String>> queryUsedPartitions = PartitionCompensator.getQueryUsedPartitions(
                        cascadesContext.getStatementContext(), queryStructInfo.getRelationBitSet());
                Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
                boolean relateTableUsedPartitionsAnyNull = false;
                boolean relateTableUsedPartitionsAllEmpty = true;
                for (MTMVRelatedTableIf tableIf : pctTables) {
                    Set<String> queryUsedPartition = queryUsedPartitions.get(tableIf.getFullQualifiers());
                    relateTableUsedPartitionsAnyNull |= queryUsedPartition == null;
                    if (queryUsedPartition == null) {
                        continue;
                    }
                    relateTableUsedPartitionsAllEmpty &= queryUsedPartition.isEmpty();
                }
                if (relateTableUsedPartitionsAnyNull || relateTableUsedPartitionsAllEmpty) {
                    materializationContext.recordFailReason(queryStructInfo,
                            String.format("queryUsedPartition is all null or empty but needUnionRewrite, "
                                            + "queryUsedPartitions is %s, queryId is %s",
                                    queryUsedPartitions,
                                    cascadesContext.getConnectContext().getQueryIdentifier()),
                            () -> String.format(
                                    "queryUsedPartition is all null or empty but needUnionRewrite, "
                                            + "queryUsedPartitions is %s, queryId is %s",
                                    queryUsedPartitions,
                                    cascadesContext.getConnectContext().getQueryIdentifier()));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format(
                                "queryUsedPartition is all null or empty but needUnionRewrite, "
                                        + "queryUsedPartitions is %s, queryId is %s",
                                queryUsedPartitions,
                                cascadesContext.getConnectContext().getQueryIdentifier()));
                    }
                    return rewriteResults;
                }
                try {
                    invalidPartitions = calcInvalidPartitions(queryUsedPartitions, rewrittenPlan,
                            cascadesContext, (AsyncMaterializationContext) materializationContext);
                } catch (AnalysisException e) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Calc invalid partitions fail",
                            () -> String.format("Calc invalid partitions fail, mv partition names are %s",
                                    mtmv.getPartitions()));
                    LOG.warn("Calc invalid partitions fail", e);
                    continue;
                }
                if (invalidPartitions == null) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "mv can not offer any partition for query",
                            () -> String.format("mv partition info %s", mtmv.getMvPartitionInfo()));
                    // if mv can not offer any partition for query, query rewrite bail out to avoid cycle run
                    return rewriteResults;
                }
                boolean partitionNeedUnion = PartitionCompensator.needUnionRewrite(invalidPartitions, cascadesContext);
                boolean canUnionRewrite = canUnionRewrite(queryPlan,
                        (AsyncMaterializationContext) materializationContext, cascadesContext);
                if (partitionNeedUnion && !canUnionRewrite) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "need compensate union all, but can not, because the query structInfo",
                            () -> String.format("mv partition info is %s, and the query plan is %s",
                                    mtmv.getMvPartitionInfo(), queryPlan.treeString()));
                    return rewriteResults;
                }
                if (partitionNeedUnion) {
                    Pair<Plan, Boolean> planAndNeedAddFilterPair =
                            StructInfo.addFilterOnTableScan(queryPlan, invalidPartitions.value(), cascadesContext);
                    if (planAndNeedAddFilterPair == null) {
                        materializationContext.recordFailReason(queryStructInfo,
                                "Add filter to base table fail when union rewrite",
                                () -> String.format("invalidPartitions are %s, queryPlan is %s",
                                        invalidPartitions, queryPlan.treeString()));
                        continue;
                    }
                    if (invalidPartitions.value().isEmpty() || !planAndNeedAddFilterPair.value()) {
                        // if invalid base table filter is empty or doesn't need to add filter on base table,
                        // only need remove mv invalid partition
                        rewrittenPlan = rewrittenPlan.accept(new PartitionRemover(), invalidPartitions.key());
                    } else {
                        // For rewrittenPlan which contains materialized view should remove invalid partition ids
                        List<Plan> children = Lists.newArrayList(
                                rewrittenPlan.accept(new PartitionRemover(), invalidPartitions.key()),
                                planAndNeedAddFilterPair.key());
                        // Union query materialized view and source table
                        rewrittenPlan = new LogicalUnion(Qualifier.ALL,
                                queryPlan.getOutput().stream().map(NamedExpression.class::cast)
                                        .collect(Collectors.toList()),
                                children.stream()
                                        .map(plan -> plan.getOutput().stream()
                                                .map(slot -> (SlotReference) slot.toSlot())
                                                .collect(Collectors.toList()))
                                        .collect(Collectors.toList()),
                                ImmutableList.of(),
                                false,
                                children);
                    }
                }
            }
            List<Slot> rewrittenPlanOutput = rewrittenPlan.getOutput();
            rewrittenPlan = MaterializedViewUtils.normalizeExpressions(rewrittenPlan, queryPlan);
            if (rewrittenPlan == null) {
                // maybe virtual slot reference added automatically
                materializationContext.recordFailReason(queryStructInfo,
                        "RewrittenPlan output logical properties is different with target group",
                        () -> String.format("materialized view rule normalizeExpressions, output size between "
                                        + "origin and rewritten plan is different, rewritten output is %s, "
                                        + "origin output is %s",
                                rewrittenPlanOutput, queryPlan.getOutput()));
                continue;
            }
            // Merge project
            rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getCteChildrenRewriter(childContext,
                                ImmutableList.of(Rewriter.bottomUp(new MergeProjectable()))
                        ).execute();
                        return childContext.getRewritePlan();
                    }, rewrittenPlan, queryPlan, false);
            if (!isOutputValid(queryPlan, rewrittenPlan)) {
                LogicalProperties logicalProperties = rewrittenPlan.getLogicalProperties();
                materializationContext.recordFailReason(queryStructInfo,
                        "RewrittenPlan output logical properties is different with target group",
                        () -> String.format("planOutput logical"
                                        + " properties = %s,\n groupOutput logical properties = %s",
                                logicalProperties, queryPlan.getLogicalProperties()));
                continue;
            }
            // need to collect table partition again, because the rewritten plan would contain new relation
            // and the rewritten plan would part in rewritten later, the table used partition info is needed
            // for later rewrite
            // record new mv relation id to table in statement context for nest rewrite, because in nest rewrite,
            // would get query struct info by mv struct info, if not record,
            // MaterializedViewUtils.transformToCommonTableId would fail
            long startTimeMs = TimeUtils.getStartTimeMs();
            try {
                MaterializedViewUtils.collectTableUsedPartitions(rewrittenPlan, cascadesContext);
            } finally {
                SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(cascadesContext.getConnectContext());
                if (summaryProfile != null) {
                    summaryProfile.addCollectTablePartitionTime(TimeUtils.getElapsedTimeMs(startTimeMs));
                }
            }
            trySetStatistics(materializationContext, cascadesContext);
            // Derive the operative column for materialized view scan
            rewrittenPlan = deriveOperativeColumn(rewrittenPlan, queryStructInfo,
                    materializationContext.getShuttledExprToScanExprMapping(), viewToQuerySlotMapping,
                    materializationContext);
            rewriteResults.add(rewrittenPlan);
            recordIfRewritten(queryStructInfo.getOriginalPlan(), materializationContext, cascadesContext);
            resetMaterializationContext(materializationContext, cascadesContext);
        }
        return rewriteResults;
    }

    // reset some materialization context state after one materialized view written successfully
    private void resetMaterializationContext(MaterializationContext currentContext,
                                             CascadesContext cascadesContext) {
        // If rewrite successfully, try to clear mv scan currently because it maybe used again
        currentContext.clearScanPlan(cascadesContext);
    }

    // Set materialization context statistics to statementContext for cost estimate later
    // this should be called before MaterializationContext.clearScanPlan because clearScanPlan change the
    // mv scan plan relation id
    private static void trySetStatistics(MaterializationContext context, CascadesContext cascadesContext) {
        Optional<Pair<Id, Statistics>> materializationPlanStatistics = context.getPlanStatistics(cascadesContext);
        if (materializationPlanStatistics.isPresent() && materializationPlanStatistics.get().key() != null) {
            cascadesContext.getStatementContext().addStatistics(materializationPlanStatistics.get().key(),
                    materializationPlanStatistics.get().value());
        }
    }

    /**
     * Not all query after rewritten successfully can compensate union all
     * Such as:
     * mv def sql is as following, partition column is a
     * select a, b, count(*) from t1 group by a, b
     * Query is as following:
     * select b, count(*) from t1 group by b, after rewritten by materialized view successfully
     * If mv part partition is invalid, can not compensate union all, because result is wrong after
     * compensate union all.
     */
    protected boolean canUnionRewrite(Plan queryPlan, AsyncMaterializationContext context,
            CascadesContext cascadesContext) {
        return true;
    }

    /**
     * Check the logical properties of rewritten plan by mv is the same with source plan
     * if same return true, if different return false
     */
    protected boolean isOutputValid(Plan sourcePlan, Plan rewrittenPlan) {
        if (sourcePlan.getGroupExpression().isPresent() && !rewrittenPlan.getLogicalProperties()
                .equals(sourcePlan.getGroupExpression().get().getOwnerGroup().getLogicalProperties())) {
            return false;
        }
        return sourcePlan.getLogicalProperties().equals(rewrittenPlan.getLogicalProperties());
    }

    /**
     * Partition will be pruned in query then add the pruned partitions to select partitions field of
     * catalog relation.
     * Maybe only some partitions is invalid in materialized view, or base table maybe add new partition
     * So we should calc the invalid partition used in query
     * @return the key in pair is mvNeedRemovePartitionNameSet, the value in pair is baseTableNeedUnionPartitionNameSet
     */
    protected Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calcInvalidPartitions(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap,
            Plan rewrittenPlan,
            CascadesContext cascadesContext,
            AsyncMaterializationContext materializationContext)
            throws AnalysisException {
        return PartitionCompensator.calcInvalidPartitions(queryUsedBaseTablePartitionMap, rewrittenPlan,
                materializationContext, cascadesContext);
    }

    /**
    * Query rewrite result may output origin plan , this will cause loop.
    * if return origin plan, need add check hear.
    */
    protected boolean rewriteQueryByViewPreCheck(MatchMode matchMode, StructInfo queryStructInfo,
            StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan,
            MaterializationContext materializationContext, ComparisonResult comparisonResult) {
        if (materializationContext instanceof SyncMaterializationContext
                && queryStructInfo.getBottomPlan() instanceof LogicalOlapScan) {
            LogicalOlapScan olapScan = (LogicalOlapScan) queryStructInfo.getBottomPlan();
            if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Rewrite query by view, for aggregate or join rewriting should be different inherit class implementation
     */
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        return tempRewritedPlan;
    }

    /**
     * Use target expression to represent the source expression. Visit the source expression,
     * try to replace the source expression with target expression in targetExpressionMapping, if found then
     * replace the source expression by target expression mapping value.
     *
     * @param sourceExpressionsToWrite the source expression to write by target expression
     * @param sourcePlan the source plan witch the source expression belong to
     * @param targetExpressionMapping target expression mapping, if finding the expression in key set of the mapping
     *         then use the corresponding value of mapping to replace it
     */
    protected List<Expression> rewriteExpression(List<? extends Expression> sourceExpressionsToWrite, Plan sourcePlan,
            ExpressionMapping targetExpressionMapping, SlotMapping targetToSourceMapping,
            Map<Expression, ExpressionInfo> queryExprToInfoMap, CascadesContext cascadesContext) {
        // Firstly, rewrite the target expression using source with inverse mapping
        // then try to use the target expression to represent the query. if any of source expressions
        // could not be represented by target expressions, return null.
        // generate target to target replacement expression mapping, and change target expression to source based
        List<? extends Expression> sourceShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                sourceExpressionsToWrite, sourcePlan);
        ExpressionMapping expressionMappingKeySourceBased = targetExpressionMapping.keyPermute(targetToSourceMapping);
        // target to target replacement expression mapping, because mv is 1:1 so get the first element
        List<Map<Expression, Expression>> flattenExpressionMap = expressionMappingKeySourceBased.flattenMap();
        Map<Expression, Expression> targetToTargetReplacementMappingQueryBased =
                flattenExpressionMap.get(0);

        // viewExprParamToDateTruncMap is {slot#0 : date_trunc(slot#0, 'day')}
        Map<Expression, DateTrunc> viewExprParamToDateTruncMap = new HashMap<>();
        targetToTargetReplacementMappingQueryBased.keySet().forEach(expr -> {
            if (expr instanceof DateTrunc) {
                viewExprParamToDateTruncMap.put(expr.child(0), (DateTrunc) expr);
            }
        });

        List<Expression> rewrittenExpressions = new ArrayList<>();
        for (int exprIndex = 0; exprIndex < sourceShuttledExpressions.size(); exprIndex++) {
            Expression expressionShuttledToRewrite = sourceShuttledExpressions.get(exprIndex);
            if (expressionShuttledToRewrite instanceof Literal) {
                rewrittenExpressions.add(expressionShuttledToRewrite);
                continue;
            }
            final Set<Expression> slotsToRewrite =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof Slot);

            final Set<SlotReference> variants =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof SlotReference
                            && ((SlotReference) expression).getDataType() instanceof VariantType);
            extendMappingByVariant(variants, targetToTargetReplacementMappingQueryBased);
            Expression replacedExpression = ExpressionUtils.replace(expressionShuttledToRewrite,
                    targetToTargetReplacementMappingQueryBased);
            Set<Expression> replacedExpressionSlotQueryUsed = replacedExpression.collect(slotsToRewrite::contains);
            if (!replacedExpressionSlotQueryUsed.isEmpty()) {
                // if contains any slot to rewrite, which means could not be rewritten by target,
                // expressionShuttledToRewrite is slot#0 > '2024-01-01' but mv plan output is date_trunc(slot#0, 'day')
                // which would try to rewrite
                if (viewExprParamToDateTruncMap.isEmpty()
                        || expressionShuttledToRewrite.children().isEmpty()
                        || !(expressionShuttledToRewrite instanceof ComparisonPredicate)) {
                    // view doesn't have date_trunc, or
                    // expressionShuttledToRewrite is not ComparisonPredicate, bail out
                    return ImmutableList.of();
                }
                Expression queryShuttledExprParam = expressionShuttledToRewrite.child(0);
                Expression queryOriginalExpr = sourceExpressionsToWrite.get(exprIndex);
                if (!queryExprToInfoMap.containsKey(queryOriginalExpr)
                        || !viewExprParamToDateTruncMap.containsKey(queryShuttledExprParam)) {
                    // query expr contains expression info or mv out contains date_trunc expression,
                    // if not, could not try to be rewritten by view date_trunc, bail out
                    return ImmutableList.of();
                }
                Map<Expression, Expression> datetruncMap = new HashMap<>();
                Literal queryUsedLiteral = queryExprToInfoMap.get(queryOriginalExpr).literal;
                if (!(queryUsedLiteral instanceof DateLiteral)) {
                    return ImmutableList.of();
                }
                datetruncMap.put(queryShuttledExprParam, queryUsedLiteral);
                Expression dateTruncWithLiteral = ExpressionUtils.replace(
                        viewExprParamToDateTruncMap.get(queryShuttledExprParam), datetruncMap);
                Expression foldedExpressionWithLiteral = FoldConstantRuleOnFE.evaluate(dateTruncWithLiteral,
                        new ExpressionRewriteContext(cascadesContext));
                if (!(foldedExpressionWithLiteral instanceof DateLiteral)) {
                    return ImmutableList.of();
                }
                if (((DateLiteral) foldedExpressionWithLiteral).getDouble() == queryUsedLiteral.getDouble()) {
                    // after date_trunc simplify if equals to original expression, expr could be rewritten by mv
                    replacedExpression = ExpressionUtils.replace(expressionShuttledToRewrite,
                            targetToTargetReplacementMappingQueryBased,
                            viewExprParamToDateTruncMap);
                }
                if (replacedExpression.anyMatch(slotsToRewrite::contains)) {
                    // has expression not rewritten successfully, bail out
                    return ImmutableList.of();
                }
            }
            rewrittenExpressions.add(replacedExpression);
        }
        return rewrittenExpressions;
    }

    /**
     * if query contains variant slot reference, extend the expression mapping for rewrite
     * such as targetToTargetReplacementMappingQueryBased is
     * id#0 -> id#8
     * type#1 -> type#9
     * payload#4 -> payload#10
     * query variants is payload['issue']['number']#20
     * then we can add payload['issue']['number']#20 -> element_at(element_at(payload#10, 'issue'), 'number')
     * to targetToTargetReplacementMappingQueryBased
     * */
    private void extendMappingByVariant(Set<SlotReference> queryVariants,
            Map<Expression, Expression> targetToTargetReplacementMappingQueryBased) {
        if (queryVariants.isEmpty()) {
            return;
        }
        Map<List<String>, Expression> viewNameToExprMap = new HashMap<>();
        for (Map.Entry<Expression, Expression> targetExpressionEntry :
                targetToTargetReplacementMappingQueryBased.entrySet()) {
            if (targetExpressionEntry.getKey() instanceof SlotReference
                    && ((SlotReference) targetExpressionEntry.getKey()).getDataType() instanceof VariantType) {
                SlotReference targetSlotReference = (SlotReference) targetExpressionEntry.getKey();
                List<String> nameIdentifier = new ArrayList<>(targetSlotReference.getQualifier());
                nameIdentifier.add(targetSlotReference.getName());
                nameIdentifier.addAll(targetSlotReference.getSubPath());
                viewNameToExprMap.put(nameIdentifier, targetExpressionEntry.getValue());
            }
        }
        if (viewNameToExprMap.isEmpty()) {
            return;
        }
        Map<List<String>, SlotReference> queryNameAndExpressionMap = new HashMap<>();
        for (SlotReference slotReference : queryVariants) {
            List<String> nameIdentifier = new ArrayList<>(slotReference.getQualifier());
            nameIdentifier.add(slotReference.getName());
            nameIdentifier.addAll(slotReference.getSubPath());
            queryNameAndExpressionMap.put(nameIdentifier, slotReference);
        }
        for (Map.Entry<List<String>, ? extends Expression> queryNameEntry : queryNameAndExpressionMap.entrySet()) {
            Expression minExpr = null;
            List<String> minCompensateName = null;
            for (Map.Entry<List<String>, Expression> entry : viewNameToExprMap.entrySet()) {
                if (!containsAllWithOrder(queryNameEntry.getKey(), entry.getKey())) {
                    continue;
                }
                List<String> removedQueryName = new ArrayList<>(queryNameEntry.getKey());
                removedQueryName.removeAll(entry.getKey());
                if (minCompensateName == null) {
                    minCompensateName = removedQueryName;
                    minExpr = entry.getValue();
                }
                if (removedQueryName.size() < minCompensateName.size()) {
                    minCompensateName = removedQueryName;
                    minExpr = entry.getValue();
                }
            }
            if (minExpr != null) {
                targetToTargetReplacementMappingQueryBased.put(queryNameEntry.getValue(),
                        constructElementAt(minExpr, minCompensateName));
            }
        }
    }

    /**
     * Derive the operative column for materialized view scan, if the operative column in query can be
     * represented by the operative column in materialized view, then set the operative column in
     * materialized view scan, otherwise return the materialized view scan without operative column
     */
    private static Plan deriveOperativeColumn(Plan rewrittenPlan, StructInfo queryStructInfo,
            ExpressionMapping targetExpressionMapping, SlotMapping targetToSourceMapping,
            MaterializationContext materializationContext) {
        ExpressionMapping expressionMappingKeySourceBased = targetExpressionMapping.keyPermute(targetToSourceMapping);
        // target to target replacement expression mapping, because mv is 1:1 so get first element
        List<Map<Expression, Expression>> flattenExpressionMap = expressionMappingKeySourceBased.flattenMap();
        Map<Expression, Expression> targetToTargetReplacementMappingQueryBased =
                flattenExpressionMap.get(0);
        final Multimap<NamedExpression, Slot> slotMapping = ArrayListMultimap.create();
        for (Map.Entry<Expression, Expression> entry : targetToTargetReplacementMappingQueryBased.entrySet()) {
            if (entry.getValue() instanceof Slot) {
                entry.getKey().collect(NamedExpression.class::isInstance).forEach(
                        namedExpression -> slotMapping.put(
                                (NamedExpression) namedExpression, (Slot) entry.getValue()));
            }
        }
        Set<Slot> operativeSlots = new HashSet<>();
        for (CatalogRelation relation : queryStructInfo.getRelations()) {
            List<Slot> relationOperativeSlots = relation.getOperativeSlots();
            if (relationOperativeSlots.isEmpty()) {
                continue;
            }
            for (Slot slot : relationOperativeSlots) {
                Collection<Slot> mvOutputSlots = slotMapping.get(slot);
                if (!mvOutputSlots.isEmpty()) {
                    operativeSlots.addAll(mvOutputSlots);
                }
            }
        }
        return rewrittenPlan.accept(new DefaultPlanRewriter<MaterializationContext>() {
            @Override
            public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, MaterializationContext context) {
                if (context.generateMaterializationIdentifier().equals(olapScan.getTable().getFullQualifiers())) {
                    return olapScan.withOperativeSlots(operativeSlots);
                }
                return super.visitLogicalOlapScan(olapScan, context);
            }
        }, materializationContext);
    }

    private static Expression constructElementAt(Expression target, List<String> atList) {
        Expression elementAt = target;
        for (String at : atList) {
            elementAt = new ElementAt(elementAt, new VarcharLiteral(at));
        }
        return elementAt;
    }

    // source names is contain all target with order or not
    private static boolean containsAllWithOrder(List<String> sourceNames, List<String> targetNames) {
        if (sourceNames.size() < targetNames.size()) {
            return false;
        }
        for (int index = 0; index < targetNames.size(); index++) {
            String sourceName = sourceNames.get(index);
            String targetName = targetNames.get(index);
            if (sourceName == null || targetName == null) {
                return false;
            }
            if (!sourceName.equals(targetName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compensate mv predicates by query predicates, compensate predicate result is query based.
     * Such as a > 5 in mv, and a > 10 in query, the compensatory predicate is a > 10.
     * For another example as following:
     * predicate a = b in mv, and a = b and c = d in query, the compensatory predicate is c = d
     */
    protected SplitPredicate predicatesCompensate(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext
    ) {
        // TODO: Use set of list? And consider view expr
        List<Expression> queryPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getQueryExpressions());
        // set pulled up expression to queryStructInfo predicates and update related predicates
        if (!queryPulledUpExpressions.isEmpty()) {
            queryStructInfo = queryStructInfo.withPredicates(
                    queryStructInfo.getPredicates().mergePulledUpPredicates(queryPulledUpExpressions));
        }
        List<Expression> viewPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getViewExpressions());
        // set pulled up expression to viewStructInfo predicates and update related predicates
        if (!viewPulledUpExpressions.isEmpty()) {
            viewStructInfo = viewStructInfo.withPredicates(
                    viewStructInfo.getPredicates().mergePulledUpPredicates(viewPulledUpExpressions));
        }
        // if the join type in query and mv plan is different, we should check query is have the
        // filters which rejects null
        Set<Set<Slot>> requireNoNullableViewSlot = comparisonResult.getViewNoNullableSlot();
        // check query is use the null reject slot which view comparison need
        if (!requireNoNullableViewSlot.isEmpty()) {
            SlotMapping queryToViewMapping = viewToQuerySlotMapping.inverse();
            // try to use
            boolean valid = containsNullRejectSlot(requireNoNullableViewSlot,
                    queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping, queryStructInfo,
                    viewStructInfo, cascadesContext);
            if (!valid) {
                queryStructInfo = queryStructInfo.withPredicates(queryStructInfo.getPredicates()
                        .mergePulledUpPredicates(comparisonResult.getQueryAllPulledUpExpressions()));
                valid = containsNullRejectSlot(requireNoNullableViewSlot,
                        queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping,
                        queryStructInfo, viewStructInfo, cascadesContext);
            }
            if (!valid) {
                return SplitPredicate.INVALID_INSTANCE;
            }
        }
        // compensate couldNot PulledUp Conjunctions
        Map<Expression, ExpressionInfo> couldNotPulledUpCompensateConjunctions =
                Predicates.compensateCouldNotPullUpPredicates(queryStructInfo, viewStructInfo,
                viewToQuerySlotMapping, comparisonResult);
        if (couldNotPulledUpCompensateConjunctions == null) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        // viewEquivalenceClass to query based
        // equal predicate compensate
        final Map<Expression, ExpressionInfo> equalCompensateConjunctions = Predicates.compensateEquivalence(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult);
        // range compensate
        final Map<Expression, ExpressionInfo> rangeCompensatePredicates =
                Predicates.compensateRangePredicate(queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                comparisonResult, cascadesContext);
        // residual compensate
        final Map<Expression, ExpressionInfo> residualCompensatePredicates = Predicates.compensateResidualPredicate(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult);
        if (equalCompensateConjunctions == null || rangeCompensatePredicates == null
                || residualCompensatePredicates == null) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        return SplitPredicate.of(equalCompensateConjunctions, rangeCompensatePredicates, residualCompensatePredicates);
    }

    /**
     * Check the queryPredicates contains the required nullable slot
     */
    private boolean containsNullRejectSlot(Set<Set<Slot>> requireNoNullableViewSlot,
            Set<Expression> queryPredicates,
            SlotMapping queryToViewMapping,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            CascadesContext cascadesContext) {
        Set<Expression> queryPulledUpPredicates = queryPredicates.stream()
                .flatMap(expr -> ExpressionUtils.extractConjunction(expr).stream())
                .map(expr -> {
                    // NOTICE inferNotNull generate Not with isGeneratedIsNotNull = false,
                    //  so, we need set this flag to false before comparison.
                    if (expr instanceof Not) {
                        return ((Not) expr).withGeneratedIsNotNull(false);
                    }
                    return expr;
                })
                .collect(Collectors.toSet());
        Set<Expression> queryNullRejectPredicates =
                ExpressionUtils.inferNotNull(queryPulledUpPredicates, cascadesContext);
        if (queryPulledUpPredicates.containsAll(queryNullRejectPredicates)) {
            // Query has no null reject predicates, return
            return false;
        }
        // Get query null reject predicate slots
        Set<Expression> queryNullRejectSlotSet = new HashSet<>();
        for (Expression queryNullRejectPredicate : queryNullRejectPredicates) {
            Optional<Slot> notNullSlot = TypeUtils.isNotNull(queryNullRejectPredicate);
            if (!notNullSlot.isPresent()) {
                continue;
            }
            queryNullRejectSlotSet.add(notNullSlot.get());
        }
        // query slot need shuttle to use table slot, avoid alias influence
        Set<Expression> queryUsedNeedRejectNullSlotsViewBased = ExpressionUtils.shuttleExpressionWithLineage(
                        new ArrayList<>(queryNullRejectSlotSet), queryStructInfo.getTopPlan()).stream()
                .map(expr -> ExpressionUtils.replace(expr, queryToViewMapping.toSlotReferenceMap()))
                .collect(Collectors.toSet());
        // view slot need shuttle to use table slot, avoid alias influence
        Set<Set<Slot>> shuttledRequireNoNullableViewSlot = new HashSet<>();
        for (Set<Slot> requireNullableSlots : requireNoNullableViewSlot) {
            shuttledRequireNoNullableViewSlot.add(
                    ExpressionUtils.shuttleExpressionWithLineage(new ArrayList<>(requireNullableSlots),
                                    viewStructInfo.getTopPlan()).stream().map(Slot.class::cast)
                            .collect(Collectors.toSet()));
        }
        // query pulledUp predicates should have null reject predicates and contains any require noNullable slot
        return shuttledRequireNoNullableViewSlot.stream().noneMatch(viewRequiredNullSlotSet ->
                Sets.intersection(viewRequiredNullSlotSet, queryUsedNeedRejectNullSlotsViewBased).isEmpty());
    }

    /**
     * Decide the match mode
     *
     * @see MatchMode
     */
    private MatchMode decideMatchMode(List<CatalogRelation> queryRelations, List<CatalogRelation> viewRelations,
            CascadesContext cascadesContext) {
        Set<TableId> queryTables = new HashSet<>();
        for (CatalogRelation catalogRelation : queryRelations) {
            queryTables.add(cascadesContext.getStatementContext().getTableId(catalogRelation.getTable()));
        }
        Set<TableId> viewTables = new HashSet<>();
        for (CatalogRelation catalogRelation : viewRelations) {
            viewTables.add(cascadesContext.getStatementContext().getTableId(catalogRelation.getTable()));
        }
        if (queryTables.equals(viewTables)) {
            return MatchMode.COMPLETE;
        }
        if (queryTables.containsAll(viewTables)) {
            return MatchMode.VIEW_PARTIAL;
        }
        if (viewTables.containsAll(queryTables)) {
            return MatchMode.QUERY_PARTIAL;
        }
        return MatchMode.NOT_MATCH;
    }

    /**
     * Check the pattern of query or materializedView is supported or not.
     */
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        if (structInfo.getRelations().isEmpty()) {
            return false;
        }
        return true;
    }

    protected boolean checkMaterializationPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        return checkQueryPattern(structInfo, cascadesContext);
    }

    protected void recordIfRewritten(Plan plan, MaterializationContext context, CascadesContext cascadesContext) {
        context.setSuccess(true);
        cascadesContext.getStatementContext().addMaterializationRewrittenSuccess(
                context.generateMaterializationIdentifier());
        if (plan.getGroupExpression().isPresent()) {
            context.addMatchedGroup(plan.getGroupExpression().get().getOwnerGroup().getGroupId(), true);
        }
    }

    protected boolean checkIfRewritten(Plan plan, MaterializationContext context) {
        return plan.getGroupExpression().isPresent()
                && context.alreadyRewrite(plan.getGroupExpression().get().getOwnerGroup().getGroupId());
    }

    // check mv plan is valid or not, this can use cache for performance
    private boolean isMaterializationValid(Plan queryPlan, CascadesContext cascadesContext,
            MaterializationContext context) {
        if (!context.getStructInfo().isValid()) {
            context.recordFailReason(context.getStructInfo(),
                    "View original struct info is invalid", () -> String.format("view plan is %s",
                            context.getStructInfo().getOriginalPlan().treeString()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("View struct info is invalid, mv identifier is %s,  query plan is %s,"
                                + "view plan is %s",
                        context.generateMaterializationIdentifier(), queryPlan.treeString(),
                        context.getStructInfo().getTopPlan().treeString()));
            }
            return false;
        }
        long materializationId = context.generateMaterializationIdentifier().hashCode();
        Boolean cachedCheckResult = cascadesContext.getMemo().materializationHasChecked(this.getClass(),
                materializationId);
        if (cachedCheckResult == null) {
            // need check in real time
            boolean checkResult = checkMaterializationPattern(context.getStructInfo(), cascadesContext);
            if (!checkResult) {
                context.recordFailReason(context.getStructInfo(),
                        "View struct info is invalid", () -> String.format("view plan is %s",
                                context.getStructInfo().getOriginalPlan().treeString()));
                // tmp to location question
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("View struct info is invalid, mv identifier is %s, query plan is %s,"
                                    + "view plan is %s",
                            context.generateMaterializationIdentifier(), queryPlan.treeString(),
                            context.getStructInfo().getTopPlan().treeString()));
                }
                cascadesContext.getMemo().recordMaterializationCheckResult(this.getClass(), materializationId,
                        false);
                return false;
            } else {
                cascadesContext.getMemo().recordMaterializationCheckResult(this.getClass(),
                        materializationId, true);
            }
        } else if (!cachedCheckResult) {
            context.recordFailReason(context.getStructInfo(),
                    "View struct info is invalid", () -> String.format("view plan is %s",
                            context.getStructInfo().getOriginalPlan().treeString()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("View struct info is invalid, mv identifier is %s, query plan is %s,"
                                + "view plan is %s",
                        context.generateMaterializationIdentifier(), queryPlan.treeString(),
                        context.getStructInfo().getTopPlan().treeString()));
            }
            return false;
        }
        return true;
    }

    /**
     * Query and mv match node
     */
    protected enum MatchMode {
        /**
         * The tables in query are same to the tables in view
         */
        COMPLETE,
        /**
         * The tables in query contains all the tables in view
         */
        VIEW_PARTIAL,
        /**
         * The tables in view contains all the tables in query
         */
        QUERY_PARTIAL,
        /**
         * Except for COMPLETE and VIEW_PARTIAL and QUERY_PARTIAL
         */
        NOT_MATCH
    }

    /**
     * Try rewrite topN node
     */
    protected Plan tryRewriteTopN(LogicalTopN<Plan> queryTopNode, LogicalTopN<Plan> viewTopNode,
            SlotMapping viewToQuerySlotMapping, Plan tmpRwritePlan, StructInfo queryStructInfo,
            StructInfo viewStructInfo, MaterializationContext materializationContext, CascadesContext cascadesContext) {
        if (queryTopNode == null || viewTopNode == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN rewrite fail, queryLimitNode or viewLimitNode is null",
                    () -> String.format("queryTopNode = %s,\n viewTopNode = %s,\n",
                            queryTopNode, viewTopNode));
            return null;
        }
        Pair<Long, Long> limitAndOffset = AbstractMaterializedViewLimitOrTopNRule.rewriteLimitAndOffset(
                Pair.of(queryTopNode.getLimit(), queryTopNode.getOffset()),
                Pair.of(viewTopNode.getLimit(), viewTopNode.getOffset()));
        if (limitAndOffset == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN limit and offset rewrite fail, query topN is not consistent with view topN",
                    () -> String.format("query topN = %s,\n view topN = %s,\n",
                            queryTopNode.treeString(),
                            viewTopNode.treeString()));
            return null;
        }
        // check the order keys of TopN between query and view is consistent
        List<OrderKey> queryOrderKeys = queryTopNode.getOrderKeys();
        List<OrderKey> viewOrderKeys = viewTopNode.getOrderKeys();
        if (queryOrderKeys.size() > viewOrderKeys.size()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN order keys size is bigger than view topN order keys size",
                    () -> String.format("query topN order keys = %s,\n view topN order keys = %s,\n",
                            queryOrderKeys, viewOrderKeys));
            return null;
        }
        List<Expression> queryOrderKeysExpressions = queryOrderKeys.stream()
                .map(OrderKey::getExpr).collect(Collectors.toList());
        List<? extends Expression> queryOrderByExpressionsShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                queryOrderKeysExpressions, queryStructInfo.getTopPlan());

        List<OrderKey> queryShuttledOrderKeys = new ArrayList<>();
        for (int i = 0; i < queryOrderKeys.size(); i++) {
            OrderKey queryOrderKey = queryOrderKeys.get(i);
            queryShuttledOrderKeys.add(new OrderKey(queryOrderByExpressionsShuttled.get(i), queryOrderKey.isAsc(),
                    queryOrderKey.isNullFirst()));
        }
        List<OrderKey> viewShuttledOrderKeys = new ArrayList<>();
        List<? extends Expression> viewOrderByExpressionsShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                viewOrderKeys.stream().map(OrderKey::getExpr).collect(Collectors.toList()),
                viewStructInfo.getTopPlan());
        List<Expression> viewOrderByExpressionsQueryBasedSet = ExpressionUtils.replace(
                viewOrderByExpressionsShuttled.stream().map(Expression.class::cast).collect(Collectors.toList()),
                viewToQuerySlotMapping.toSlotReferenceMap());
        for (int j = 0; j < viewOrderKeys.size(); j++) {
            OrderKey viewOrderKey = viewOrderKeys.get(j);
            viewShuttledOrderKeys.add(new OrderKey(viewOrderByExpressionsQueryBasedSet.get(j), viewOrderKey.isAsc(),
                    viewOrderKey.isNullFirst()));
        }
        if (!MaterializedViewUtils.isPrefixSameFromStart(queryShuttledOrderKeys, viewShuttledOrderKeys)) {
            materializationContext.recordFailReason(queryStructInfo,
                    "view topN order key doesn't match query order key",
                    () -> String.format("queryShuttledOrderKeys = %s,\n viewShuttledOrderKeys = %s,\n",
                            queryShuttledOrderKeys, viewShuttledOrderKeys));
            return null;
        }

        // try to rewrite the order by expressions using the mv scan slot
        List<Expression> rewrittenExpressions = rewriteExpression(queryOrderKeysExpressions,
                queryStructInfo.getTopPlan(), materializationContext.shuttledExprToScanExprMapping,
                viewToQuerySlotMapping, ImmutableMap.of(), cascadesContext);
        if (rewrittenExpressions.isEmpty()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN order keys rewrite fail, query topN order keys is not consistent "
                            + "with view topN order keys",
                    () -> String.format("query topN order keys = %s,\n shuttledExprToScanExprMapping = %s,\n",
                            queryOrderKeysExpressions, materializationContext.shuttledExprToScanExprMapping));
            return null;
        }
        List<OrderKey> rewrittenOrderKeys = new ArrayList<>();
        for (int i = 0; i < rewrittenExpressions.size(); i++) {
            OrderKey queryOrderKey = queryOrderKeys.get(i);
            rewrittenOrderKeys.add(new OrderKey(rewrittenExpressions.get(i), queryOrderKey.isAsc(),
                    queryOrderKey.isNullFirst()));
        }
        return new LogicalTopN<>(rewrittenOrderKeys, limitAndOffset.key(), limitAndOffset.value(), tmpRwritePlan);
    }
}
