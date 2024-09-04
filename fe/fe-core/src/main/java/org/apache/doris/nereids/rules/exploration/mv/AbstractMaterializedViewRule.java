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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRewriteUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PartitionRemover;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
            JoinType.RIGHT_ANTI_JOIN);

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * multi materialization every time. if exception it will catch the exception and record it to
     * materialization context.
     */
    public List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<Plan> rewrittenPlans = new ArrayList<>();
        // if available materialization list is empty, bail out
        if (cascadesContext.getMaterializationContexts().isEmpty()) {
            return rewrittenPlans;
        }
        for (MaterializationContext context : cascadesContext.getMaterializationContexts()) {
            if (checkIfRewritten(queryPlan, context)) {
                continue;
            }
            // check mv plan is valid or not
            if (!isMaterializationValid(queryPlan, cascadesContext, context)) {
                continue;
            }
            // get query struct infos according to the view strut info, if valid query struct infos is empty, bail out
            List<StructInfo> queryStructInfos = getValidQueryStructInfos(queryPlan, cascadesContext,
                    context.getStructInfo().getTableBitSet());
            if (queryStructInfos.isEmpty()) {
                continue;
            }
            for (StructInfo queryStructInfo : queryStructInfos) {
                try {
                    if (rewrittenPlans.size() < cascadesContext.getConnectContext()
                            .getSessionVariable().getMaterializedViewRewriteSuccessCandidateNum()) {
                        rewrittenPlans.addAll(doRewrite(queryStructInfo, cascadesContext, context));
                    }
                } catch (Exception exception) {
                    context.recordFailReason(queryStructInfo,
                            "Materialized view rule exec fail", exception::toString);
                }
            }
        }
        return rewrittenPlans;
    }

    /**
     * Get valid query struct infos, if invalid record the invalid reason
     */
    protected List<StructInfo> getValidQueryStructInfos(Plan queryPlan, CascadesContext cascadesContext,
            BitSet materializedViewTableSet) {
        List<StructInfo> validStructInfos = new ArrayList<>();
        // For every materialized view we should trigger refreshing struct info map
        List<StructInfo> uncheckedStructInfos = MaterializedViewUtils.extractStructInfo(queryPlan, queryPlan,
                cascadesContext, materializedViewTableSet);
        uncheckedStructInfos.forEach(queryStructInfo -> {
            boolean valid = checkQueryPattern(queryStructInfo, cascadesContext) && queryStructInfo.isValid();
            if (!valid) {
                cascadesContext.getMaterializationContexts().forEach(ctx ->
                        ctx.recordFailReason(queryStructInfo, "Query struct info is invalid",
                                () -> String.format("query table bitmap is %s, plan is %s",
                                        queryStructInfo.getTableBitSet(), queryPlan.treeString())
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
            MaterializationContext materializationContext) {
        List<Plan> rewriteResults = new ArrayList<>();
        StructInfo viewStructInfo = materializationContext.getStructInfo();
        MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations());
        if (MatchMode.COMPLETE != matchMode) {
            materializationContext.recordFailReason(queryStructInfo, "Match mode is invalid",
                    () -> String.format("matchMode is %s", matchMode));
            return rewriteResults;
        }
        List<RelationMapping> queryToViewTableMappings = RelationMapping.generate(queryStructInfo.getRelations(),
                viewStructInfo.getRelations());
        // if any relation in query and view can not map, bail out.
        if (queryToViewTableMappings == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Query to view table mapping is null", () -> "");
            return rewriteResults;
        }
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        int materializedViewRelationMappingMaxCount = sessionVariable.getMaterializedViewRelationMappingMaxCount();
        if (queryToViewTableMappings.size() > materializedViewRelationMappingMaxCount) {
            LOG.warn("queryToViewTableMappings is over limit and be intercepted");
            queryToViewTableMappings = queryToViewTableMappings.subList(0, materializedViewRelationMappingMaxCount);
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
            Plan mvScan = materializationContext.getScanPlan(queryStructInfo);
            Plan queryPlan = queryStructInfo.getTopPlan();
            if (compensatePredicates.isAlwaysTrue()) {
                rewrittenPlan = mvScan;
            } else {
                // Try to rewrite compensate predicates by using mv scan
                List<Expression> rewriteCompensatePredicates = rewriteExpression(compensatePredicates.toList(),
                        queryPlan, materializationContext.getShuttledExprToScanExprMapping(),
                        viewToQuerySlotMapping, queryStructInfo.getTableBitSet());
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
                    viewStructInfo, viewToQuerySlotMapping, rewrittenPlan, materializationContext);
            if (!checkResult) {
                continue;
            }
            // Rewrite query by view
            rewrittenPlan = rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    rewrittenPlan, materializationContext, cascadesContext);
            rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getWholeTreeRewriter(childContext).execute();
                        return childContext.getRewritePlan();
                    }, rewrittenPlan, queryPlan);
            if (rewrittenPlan == null) {
                continue;
            }
            Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> invalidPartitions;
            if (materializationContext instanceof AsyncMaterializationContext) {
                try {
                    invalidPartitions = calcInvalidPartitions(queryPlan, rewrittenPlan,
                            (AsyncMaterializationContext) materializationContext, cascadesContext);
                } catch (AnalysisException e) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Calc invalid partitions fail",
                            () -> String.format("Calc invalid partitions fail, mv partition names are %s",
                                    ((AsyncMaterializationContext) materializationContext).getMtmv().getPartitions()));
                    LOG.warn("Calc invalid partitions fail", e);
                    continue;
                }
                if (invalidPartitions == null) {
                    // if mv can not offer any partition for query, query rewrite bail out to avoid cycle run
                    materializationContext.recordFailReason(queryStructInfo,
                            "mv can not offer any partition for query",
                            () -> String.format("mv partition info %s",
                                    ((AsyncMaterializationContext) materializationContext).getMtmv()
                                            .getMvPartitionInfo()));
                    return rewriteResults;
                }
                boolean partitionNeedUnion = needUnionRewrite(invalidPartitions, cascadesContext);
                final Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> finalInvalidPartitions =
                        invalidPartitions;
                if (partitionNeedUnion) {
                    MTMV mtmv = ((AsyncMaterializationContext) materializationContext).getMtmv();
                    Pair<Plan, Boolean> planAndNeedAddFilterPair =
                            StructInfo.addFilterOnTableScan(queryPlan, invalidPartitions.value(),
                                    mtmv.getMvPartitionInfo().getRelatedCol(), cascadesContext);
                    if (planAndNeedAddFilterPair == null) {
                        materializationContext.recordFailReason(queryStructInfo,
                                "Add filter to base table fail when union rewrite",
                                () -> String.format("invalidPartitions are %s, queryPlan is %s, partition column is %s",
                                        invalidPartitions, queryPlan.treeString(),
                                        mtmv.getMvPartitionInfo().getPartitionCol()));
                        continue;
                    }
                    if (finalInvalidPartitions.value().isEmpty() || !planAndNeedAddFilterPair.value()) {
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
            rewrittenPlan = normalizeExpressions(rewrittenPlan, queryPlan);
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
            if (!isOutputValid(queryPlan, rewrittenPlan)) {
                LogicalProperties logicalProperties = rewrittenPlan.getLogicalProperties();
                materializationContext.recordFailReason(queryStructInfo,
                        "RewrittenPlan output logical properties is different with target group",
                        () -> String.format("planOutput logical"
                                        + " properties = %s,\n groupOutput logical properties = %s",
                                logicalProperties, queryPlan.getLogicalProperties()));
                continue;
            }
            recordIfRewritten(queryStructInfo.getOriginalPlan(), materializationContext);
            trySetStatistics(materializationContext, cascadesContext);
            rewriteResults.add(rewrittenPlan);
            // if rewrite successfully, try to regenerate mv scan because it maybe used again
            materializationContext.tryReGenerateScanPlan(cascadesContext);
        }
        return rewriteResults;
    }

    // Set materialization context statistics to statementContext for cost estimate later
    private static void trySetStatistics(MaterializationContext context, CascadesContext cascadesContext) {
        Optional<Pair<Id, Statistics>> materializationPlanStatistics = context.getPlanStatistics(cascadesContext);
        if (materializationPlanStatistics.isPresent() && materializationPlanStatistics.get().key() != null) {
            cascadesContext.getStatementContext().addStatistics(materializationPlanStatistics.get().key(),
                    materializationPlanStatistics.get().value());
        }
    }

    protected boolean needUnionRewrite(
            Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> invalidPartitions,
            CascadesContext cascadesContext) {
        return invalidPartitions != null
                && (!invalidPartitions.key().isEmpty() || !invalidPartitions.value().isEmpty());
    }

    // Normalize expression such as nullable property and output slot id
    protected Plan normalizeExpressions(Plan rewrittenPlan, Plan originPlan) {
        if (rewrittenPlan.getOutput().size() != originPlan.getOutput().size()) {
            return null;
        }
        // normalize nullable
        List<NamedExpression> normalizeProjects = new ArrayList<>();
        for (int i = 0; i < originPlan.getOutput().size(); i++) {
            normalizeProjects.add(normalizeExpression(originPlan.getOutput().get(i), rewrittenPlan.getOutput().get(i)));
        }
        return new LogicalProject<>(normalizeProjects, rewrittenPlan);
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
    protected Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> calcInvalidPartitions(
            Plan queryPlan, Plan rewrittenPlan,
            AsyncMaterializationContext materializationContext, CascadesContext cascadesContext)
            throws AnalysisException {
        Set<String> mvNeedRemovePartitionNameSet = new HashSet<>();
        Set<String> baseTableNeedUnionPartitionNameSet = new HashSet<>();
        // check partition is valid or not
        MTMV mtmv = materializationContext.getMtmv();
        PartitionInfo mvPartitionInfo = mtmv.getPartitionInfo();
        if (PartitionType.UNPARTITIONED.equals(mvPartitionInfo.getType())) {
            // if not partition, if rewrite success, it means mv is available
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        MTMVPartitionInfo mvCustomPartitionInfo = mtmv.getMvPartitionInfo();
        BaseTableInfo relatedPartitionTable = mvCustomPartitionInfo.getRelatedTableInfo();
        if (relatedPartitionTable == null) {
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        // Collect the mv related base table partitions which query used
        Map<BaseTableInfo, Set<Partition>> queryUsedBaseTablePartitions = new LinkedHashMap<>();
        queryUsedBaseTablePartitions.put(relatedPartitionTable, new HashSet<>());
        queryPlan.accept(new StructInfo.QueryScanPartitionsCollector(), queryUsedBaseTablePartitions);
        // Bail out, not check invalid partition if not olap scan, support later
        if (queryUsedBaseTablePartitions.isEmpty()) {
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        Set<String> queryUsedBaseTablePartitionNameSet = queryUsedBaseTablePartitions.get(relatedPartitionTable)
                .stream()
                .map(Partition::getName)
                .collect(Collectors.toSet());

        Collection<Partition> mvValidPartitions = MTMVRewriteUtil.getMTMVCanRewritePartitions(mtmv,
                cascadesContext.getConnectContext(), System.currentTimeMillis(), false);
        Set<String> mvValidPartitionNameSet = new HashSet<>();
        Set<String> mvValidBaseTablePartitionNameSet = new HashSet<>();
        Set<String> mvValidHasDataRelatedBaseTableNameSet = new HashSet<>();
        Pair<Map<String, Set<String>>, Map<String, String>> partitionMapping = mtmv.calculateDoublyPartitionMappings();
        for (Partition mvValidPartition : mvValidPartitions) {
            mvValidPartitionNameSet.add(mvValidPartition.getName());
            Set<String> relatedBaseTablePartitions = partitionMapping.key().get(mvValidPartition.getName());
            if (relatedBaseTablePartitions != null) {
                mvValidBaseTablePartitionNameSet.addAll(relatedBaseTablePartitions);
            }
            if (!mtmv.selectNonEmptyPartitionIds(ImmutableList.of(mvValidPartition.getId())).isEmpty()) {
                if (relatedBaseTablePartitions != null) {
                    mvValidHasDataRelatedBaseTableNameSet.addAll(relatedBaseTablePartitions);
                }
            }
        }
        if (Sets.intersection(mvValidHasDataRelatedBaseTableNameSet, queryUsedBaseTablePartitionNameSet).isEmpty()) {
            // if mv can not offer any partition for query, query rewrite bail out
            return null;
        }
        // Check when mv partition relates base table partition data change or delete partition
        Set<String> rewrittenPlanUsePartitionNameSet = new HashSet<>();
        List<Object> mvOlapScanList = rewrittenPlan.collectToList(node ->
                node instanceof LogicalOlapScan
                        && Objects.equals(((CatalogRelation) node).getTable().getName(), mtmv.getName()));
        for (Object olapScanObj : mvOlapScanList) {
            LogicalOlapScan olapScan = (LogicalOlapScan) olapScanObj;
            olapScan.getSelectedPartitionIds().forEach(id ->
                    rewrittenPlanUsePartitionNameSet.add(olapScan.getTable().getPartition(id).getName()));
        }
        // If rewritten plan use but not in mv valid partition name set, need remove in mv and base table union
        Sets.difference(rewrittenPlanUsePartitionNameSet, mvValidPartitionNameSet)
                .copyInto(mvNeedRemovePartitionNameSet);
        for (String partitionName : mvNeedRemovePartitionNameSet) {
            baseTableNeedUnionPartitionNameSet.addAll(partitionMapping.key().get(partitionName));
        }
        // If related base table create partitions or mv is created with ttl, need base table union
        Sets.difference(queryUsedBaseTablePartitionNameSet, mvValidBaseTablePartitionNameSet)
                .copyInto(baseTableNeedUnionPartitionNameSet);
        // Construct result map
        Map<BaseTableInfo, Set<String>> mvPartitionNeedRemoveNameMap = new HashMap<>();
        if (!mvNeedRemovePartitionNameSet.isEmpty()) {
            mvPartitionNeedRemoveNameMap.put(new BaseTableInfo(mtmv), mvNeedRemovePartitionNameSet);
        }
        Map<BaseTableInfo, Set<String>> baseTablePartitionNeedUnionNameMap = new HashMap<>();
        if (!baseTableNeedUnionPartitionNameSet.isEmpty()) {
            baseTablePartitionNeedUnionNameMap.put(relatedPartitionTable, baseTableNeedUnionPartitionNameSet);
        }
        return Pair.of(mvPartitionNeedRemoveNameMap, baseTablePartitionNeedUnionNameMap);
    }

    /**
    * Query rewrite result may output origin plan , this will cause loop.
    * if return origin plan, need add check hear.
    */
    protected boolean rewriteQueryByViewPreCheck(MatchMode matchMode, StructInfo queryStructInfo,
            StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan,
            MaterializationContext materializationContext) {
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
            ExpressionMapping targetExpressionMapping, SlotMapping targetToSourceMapping, BitSet sourcePlanBitSet) {
        // Firstly, rewrite the target expression using source with inverse mapping
        // then try to use the target expression to represent the query. if any of source expressions
        // can not be represented by target expressions, return null.
        // generate target to target replacement expression mapping, and change target expression to source based
        List<? extends Expression> sourceShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                sourceExpressionsToWrite, sourcePlan, sourcePlanBitSet);
        ExpressionMapping expressionMappingKeySourceBased = targetExpressionMapping.keyPermute(targetToSourceMapping);
        // target to target replacement expression mapping, because mv is 1:1 so get first element
        List<Map<Expression, Expression>> flattenExpressionMap = expressionMappingKeySourceBased.flattenMap();
        Map<Expression, Expression> targetToTargetReplacementMappingQueryBased =
                flattenExpressionMap.get(0);

        List<Expression> rewrittenExpressions = new ArrayList<>();
        for (Expression expressionShuttledToRewrite : sourceShuttledExpressions) {
            if (expressionShuttledToRewrite instanceof Literal) {
                rewrittenExpressions.add(expressionShuttledToRewrite);
                continue;
            }
            final Set<Object> slotsToRewrite =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof Slot);

            final Set<SlotReference> variants =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof SlotReference
                    && ((SlotReference) expression).getDataType() instanceof VariantType);
            extendMappingByVariant(variants, targetToTargetReplacementMappingQueryBased);
            Expression replacedExpression = ExpressionUtils.replace(expressionShuttledToRewrite,
                    targetToTargetReplacementMappingQueryBased);
            if (replacedExpression.anyMatch(slotsToRewrite::contains)) {
                // if contains any slot to rewrite, which means can not be rewritten by target, bail out
                return ImmutableList.of();
            }
            rewrittenExpressions.add(replacedExpression);
        }
        return rewrittenExpressions;
    }

    /**
     * if query contains variant slot reference, extend the expression mapping for rewrte
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
     * Normalize expression with query, keep the consistency of exprId and nullable props with
     * query
     * Keep the replacedExpression slot property is the same as the sourceExpression
     */
    public static NamedExpression normalizeExpression(
            NamedExpression sourceExpression, NamedExpression replacedExpression) {
        Expression innerExpression = replacedExpression;
        if (replacedExpression.nullable() != sourceExpression.nullable()) {
            // if enable join eliminate, query maybe inner join and mv maybe outer join.
            // If the slot is at null generate side, the nullable maybe different between query and view
            // So need to force to consistent.
            innerExpression = sourceExpression.nullable()
                    ? new Nullable(replacedExpression) : new NonNullable(replacedExpression);
        }
        return new Alias(sourceExpression.getExprId(), innerExpression, sourceExpression.getName());
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
                    queryStructInfo.getPredicates().merge(queryPulledUpExpressions));
        }
        List<Expression> viewPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getViewExpressions());
        // set pulled up expression to viewStructInfo predicates and update related predicates
        if (!viewPulledUpExpressions.isEmpty()) {
            viewStructInfo = viewStructInfo.withPredicates(
                    viewStructInfo.getPredicates().merge(viewPulledUpExpressions));
        }
        // if the join type in query and mv plan is different, we should check query is have the
        // filters which rejects null
        Set<Set<Slot>> requireNoNullableViewSlot = comparisonResult.getViewNoNullableSlot();
        // check query is use the null reject slot which view comparison need
        if (!requireNoNullableViewSlot.isEmpty()) {
            SlotMapping queryToViewMapping = viewToQuerySlotMapping.inverse();
            // try to use
            boolean valid = containsNullRejectSlot(requireNoNullableViewSlot,
                    queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping, cascadesContext);
            if (!valid) {
                queryStructInfo = queryStructInfo.withPredicates(
                        queryStructInfo.getPredicates().merge(comparisonResult.getQueryAllPulledUpExpressions()));
                valid = containsNullRejectSlot(requireNoNullableViewSlot,
                        queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping, cascadesContext);
            }
            if (!valid) {
                return SplitPredicate.INVALID_INSTANCE;
            }
        }
        // viewEquivalenceClass to query based
        // equal predicate compensate
        final Set<Expression> equalCompensateConjunctions = Predicates.compensateEquivalence(
                queryStructInfo,
                viewStructInfo,
                viewToQuerySlotMapping,
                comparisonResult);
        // range compensate
        final Set<Expression> rangeCompensatePredicates = Predicates.compensateRangePredicate(
                queryStructInfo,
                viewStructInfo,
                viewToQuerySlotMapping,
                comparisonResult,
                cascadesContext);
        // residual compensate
        final Set<Expression> residualCompensatePredicates = Predicates.compensateResidualPredicate(
                queryStructInfo,
                viewStructInfo,
                viewToQuerySlotMapping,
                comparisonResult);
        if (equalCompensateConjunctions == null || rangeCompensatePredicates == null
                || residualCompensatePredicates == null) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        if (equalCompensateConjunctions.stream().anyMatch(expr -> expr.containsType(AggregateFunction.class))
                || rangeCompensatePredicates.stream().anyMatch(expr -> expr.containsType(AggregateFunction.class))
                || residualCompensatePredicates.stream().anyMatch(expr ->
                expr.containsType(AggregateFunction.class))) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        return SplitPredicate.of(equalCompensateConjunctions.isEmpty() ? BooleanLiteral.TRUE
                        : ExpressionUtils.and(equalCompensateConjunctions),
                rangeCompensatePredicates.isEmpty() ? BooleanLiteral.TRUE
                        : ExpressionUtils.and(rangeCompensatePredicates),
                residualCompensatePredicates.isEmpty() ? BooleanLiteral.TRUE
                        : ExpressionUtils.and(residualCompensatePredicates));
    }

    /**
     * Check the queryPredicates contains the required nullable slot
     */
    private boolean containsNullRejectSlot(Set<Set<Slot>> requireNoNullableViewSlot,
            Set<Expression> queryPredicates,
            SlotMapping queryToViewMapping,
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
        Set<Expression> nullRejectPredicates = ExpressionUtils.inferNotNull(queryPulledUpPredicates, cascadesContext);
        Set<Expression> queryUsedNeedRejectNullSlotsViewBased = nullRejectPredicates.stream()
                .map(expression -> TypeUtils.isNotNull(expression).orElse(null))
                .filter(Objects::nonNull)
                .map(expr -> ExpressionUtils.replace((Expression) expr, queryToViewMapping.toSlotReferenceMap()))
                .collect(Collectors.toSet());
        // query pulledUp predicates should have null reject predicates and contains any require noNullable slot
        return !queryPulledUpPredicates.containsAll(nullRejectPredicates)
                && requireNoNullableViewSlot.stream().noneMatch(set ->
                Sets.intersection(set, queryUsedNeedRejectNullSlotsViewBased).isEmpty());
    }

    /**
     * Decide the match mode
     *
     * @see MatchMode
     */
    private MatchMode decideMatchMode(List<CatalogRelation> queryRelations, List<CatalogRelation> viewRelations) {

        Set<TableIdentifier> queryTables = new HashSet<>();
        for (CatalogRelation catalogRelation : queryRelations) {
            queryTables.add(new TableIdentifier(catalogRelation.getTable()));
        }
        Set<TableIdentifier> viewTables = new HashSet<>();
        for (CatalogRelation catalogRelation : viewRelations) {
            viewTables.add(new TableIdentifier(catalogRelation.getTable()));
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

    protected void recordIfRewritten(Plan plan, MaterializationContext context) {
        context.setSuccess(true);
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
}
