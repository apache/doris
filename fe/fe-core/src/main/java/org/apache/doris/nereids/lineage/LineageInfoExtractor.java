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

package org.apache.doris.nereids.lineage;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.lineage.LineageInfo.DirectLineageType;
import org.apache.doris.nereids.lineage.LineageInfo.IndirectLineageType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer.ExpressionReplacer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extract lineage information from a Plan tree.
 * This class traverses the plan tree and collects both direct and indirect lineage information.
 *
 * <p>Example SQL used below:
 * INSERT INTO tgt_region_revenue
 * SELECT n.n_name AS nation_name,
 *        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
 * FROM customer c
 * JOIN orders o ON c.c_custkey = o.o_custkey
 * JOIN lineitem l ON o.o_orderkey = l.l_orderkey
 * JOIN nation n ON c.c_nationkey = n.n_nationkey
 * JOIN region r ON n.n_regionkey = r.r_regionkey
 * WHERE r.r_name = 'ASIA'
 * GROUP BY n.n_name;
 */
public class LineageInfoExtractor {
    private static final Logger LOG = LogManager.getLogger(LineageInfoExtractor.class);

    /**
     * Register analyze-plan hook for lineage extraction.
     *
     * <p>Using the example SQL above, this is called before planner.plan(...) so the analyzed plan can be captured.
     */
    public static void registerAnalyzePlanHook(StatementContext statementContext, NereidsPlanner planner) {
        if (statementContext == null || planner == null) {
            return;
        }
        for (PlannerHook hook : statementContext.getPlannerHooks()) {
            if (hook instanceof AnalyzePlanHook) {
                return;
            }
        }
        statementContext.addPlannerHook(new AnalyzePlanHook());
    }

    /**
     * Extract lineage information from a plan.
     *
     * <p>Using the example SQL above, this records direct lineage, indirect lineage, and table lineage.
     *
     * @param plan the plan to extract lineage from
     * @return the extracted lineage information
     */
    public static LineageInfo extractLineageInfo(Plan plan) {
        LineageInfo lineageInfo = new LineageInfo();

        // Step 1: Extract direct lineage using ExpressionLineageReplacer
        ExpressionLineageReplacer.ExpressionReplaceContext replaceContext = extractDirectLineage(plan, lineageInfo);

        // Step 2: Extract indirect lineage and set lineage by traversing plan tree
        LineageCollector collector = new LineageCollector(replaceContext.getExprIdExpressionMap());
        plan.accept(collector, lineageInfo);

        collector.collectExpressionLineage(lineageInfo, replaceContext.getExprIdExpressionMap());
        return lineageInfo;
    }

    /**
     * Extract direct lineage from plan output expressions.
     *
     * <p>Using the example SQL above, nation_name is IDENTITY from n.n_name and
     * revenue is AGGREGATION from SUM(l.l_extendedprice * (1 - l.l_discount)).
     */
    private static ExpressionLineageReplacer.ExpressionReplaceContext extractDirectLineage(Plan plan,
                                                                                           LineageInfo lineageInfo) {
        List<Slot> outputs = plan.getOutput();
        ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                new ExpressionLineageReplacer.ExpressionReplaceContext(outputs);
        plan.accept(ExpressionLineageReplacer.INSTANCE, replaceContext);

        List<? extends Expression> shuttledExpressions = replaceContext.getReplacedExpressions();
        for (int i = 0; i < outputs.size(); i++) {
            Slot outputSlot = outputs.get(i);
            if (!(outputSlot instanceof SlotReference)) {
                continue;
            }
            SlotReference outputSlotRef = (SlotReference) outputSlot;
            Expression shuttledExpr = shuttledExpressions.get(i);
            // Determine direct lineage type based on expression structure
            DirectLineageType type = determineDirectLineageType(shuttledExpr);
            lineageInfo.addDirectLineage(outputSlotRef, type, shuttledExpr);
        }
        return replaceContext;
    }

    /**
     * Determine the direct lineage type based on expression structure.
     *
     * <p>Using the example SQL above, SUM(...) is AGGREGATION and n.n_name is IDENTITY.
     */
    private static DirectLineageType determineDirectLineageType(Expression expr) {
        // Check if expression contains aggregate function
        if (expr.containsType(AggregateFunction.class)) {
            return DirectLineageType.AGGREGATION;
        }
        // Check if expression is a simple slot reference (identity)
        if (expr instanceof SlotReference) {
            return DirectLineageType.IDENTITY;
        }
        // Otherwise it's a transformation
        return DirectLineageType.TRANSFORMATION;
    }

    /**
     * Plan visitor to collect indirect lineage information
     */
    private static class LineageCollector extends DefaultPlanVisitor<Void, LineageInfo> {
        private final Map<ExprId, Expression> exprIdExpressionMap;

        /**
         * Create a collector with a map for shuttling expressions.
         *
         * <p>Using the example SQL above, this map resolves aliases like nation_name -> n.n_name.
         */
        public LineageCollector(Map<ExprId, Expression> exprIdExpressionMap) {
            this.exprIdExpressionMap = exprIdExpressionMap;
        }

        /**
         * Collect indirect lineage based on expressions embedded in direct lineage.
         *
         * <p>Using the example SQL above, WINDOW/CONDITIONAL are collected from direct expressions when present.
         */
        private void collectExpressionLineage(LineageInfo lineageInfo, Map<ExprId, Expression> exprIdExpressionMap) {
            collectWindowLineage(lineageInfo, exprIdExpressionMap);
            collectConditionalLineage(lineageInfo, exprIdExpressionMap);
        }

        /**
         * Capture target table/columns from the sink.
         *
         * <p>Using the example SQL above, target table is tgt_region_revenue.
         */
        @Override
        public Void visitLogicalTableSink(LogicalTableSink<? extends Plan> logicalTableSink, LineageInfo lineageInfo) {
            if (lineageInfo.getTargetTable() == null) {
                lineageInfo.setTargetTable(logicalTableSink.getTargetTable());
                lineageInfo.setTargetColumns(logicalTableSink.getOutput());
            }
            return super.visitLogicalTableSink(logicalTableSink, lineageInfo);
        }

        /**
         * Collect table lineage from scan nodes.
         *
         * <p>Using the example SQL above, tableLineageSet includes customer, orders, lineitem, nation, region.
         */
        @Override
        public Void visitLogicalCatalogRelation(LogicalCatalogRelation relation, LineageInfo lineageInfo) {
            // Collect table lineage
            lineageInfo.addTableLineage(relation.getTable());
            return null;
        }

        /**
         * Collect JOIN indirect lineage from join conditions.
         *
         * <p>Using the example SQL above, JOIN inputs include o.o_orderkey, l.l_orderkey, and other join keys.
         */
        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, LineageInfo lineageInfo) {
            Set<Expression> joinConditions = new HashSet<>();
            joinConditions.addAll(join.getHashJoinConjuncts());
            joinConditions.addAll(join.getOtherJoinConjuncts());
            joinConditions.addAll(join.getMarkJoinConjuncts());
            Set<Expression> shuttled = shuttleExpressions(new ArrayList<>(joinConditions), exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.JOIN, shuttled, lineageInfo);
            return super.visitLogicalJoin(join, lineageInfo);
        }

        /**
         * Collect FILTER indirect lineage from WHERE/HAVING predicates.
         *
         * <p>Using the example SQL above, r.r_name is a FILTER input from WHERE r.r_name = 'ASIA'.
         */
        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, LineageInfo lineageInfo) {
            Set<Expression> predicates = new HashSet<>(filter.getConjuncts());
            Set<Expression> shuttled = shuttleExpressions(new ArrayList<>(predicates), exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.FILTER, shuttled, lineageInfo);
            return super.visitLogicalFilter(filter, lineageInfo);
        }

        /**
         * Collect GROUP_BY indirect lineage from group keys.
         *
         * <p>Using the example SQL above, n.n_name is a GROUP_BY input.
         */
        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, LineageInfo lineageInfo) {
            Set<Expression> shuttled = shuttleExpressions(aggregate.getGroupByExpressions(), exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.GROUP_BY, shuttled, lineageInfo);
            return super.visitLogicalAggregate(aggregate, lineageInfo);
        }

        /**
         * Collect SORT indirect lineage from ORDER BY.
         *
         * <p>Using the example SQL above, there is no ORDER BY, so no SORT inputs are collected.
         */
        @Override
        public Void visitLogicalSort(LogicalSort<? extends Plan> sort, LineageInfo lineageInfo) {
            List<Expression> sortExprs = new ArrayList<>();
            sort.getOrderKeys().forEach(orderKey -> sortExprs.add(orderKey.getExpr()));
            Set<Expression> shuttled = shuttleExpressions(sortExprs, exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.SORT, shuttled, lineageInfo);
            return super.visitLogicalSort(sort, lineageInfo);
        }

        /**
         * Collect SORT indirect lineage from TopN order keys.
         *
         * <p>Using the example SQL above, there is no TopN, so no SORT inputs are collected.
         */
        @Override
        public Void visitLogicalTopN(LogicalTopN<? extends Plan> topN, LineageInfo lineageInfo) {
            List<Expression> sortExprs = new ArrayList<>();
            topN.getOrderKeys().forEach(orderKey -> sortExprs.add(orderKey.getExpr()));
            Set<Expression> shuttled = shuttleExpressions(sortExprs, exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.SORT, shuttled, lineageInfo);
            return super.visitLogicalTopN(topN, lineageInfo);
        }

        /**
         * Collect SORT indirect lineage from defer-materialize TopN.
         *
         * <p>Using the example SQL above, there is no defer-materialize TopN.
         */
        @Override
        public Void visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN,
                                                     LineageInfo lineageInfo) {
            List<Expression> sortExprs = new ArrayList<>();
            topN.getOrderKeys().forEach(orderKey -> sortExprs.add(orderKey.getExpr()));
            Set<Expression> shuttled = shuttleExpressions(sortExprs, exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.SORT, shuttled, lineageInfo);
            return super.visitLogicalDeferMaterializeTopN(topN, lineageInfo);
        }

        /**
         * Replace UNION outputs in direct lineage using child outputs.
         *
         * <p>Using the example SQL above, there is no UNION, so this is a no-op; when UNION exists, each child
         * contributes to direct lineage.
         */
        @Override
        public Void visitLogicalUnion(LogicalUnion union, LineageInfo lineageInfo) {
            if (union == null || union.children().isEmpty()) {
                return null;
            }
            Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap =
                    lineageInfo.getDirectLineageMap();
            if (directLineageMap == null || directLineageMap.isEmpty()) {
                return null;
            }
            List<Slot> unionOutputs = union.getOutput();
            Set<ExprId> unionOutputExprIds = collectUnionOutputExprIds(unionOutputs);
            if (unionOutputExprIds.isEmpty()) {
                return super.visitLogicalUnion(union, lineageInfo);
            }
            List<Map<ExprId, Expression>> childOutputMaps = buildUnionChildOutputMaps(union, unionOutputs);
            if (childOutputMaps.isEmpty()) {
                return super.visitLogicalUnion(union, lineageInfo);
            }
            replaceUnionDirectLineage(directLineageMap, unionOutputExprIds, childOutputMaps);
            return super.visitLogicalUnion(union, lineageInfo);
        }

        /**
         * Collect UNION output exprIds from union outputs.
         *
         * <p>Using the example SQL above, this captures the UNION output slots for later replacement.
         */
        private Set<ExprId> collectUnionOutputExprIds(List<Slot> unionOutputs) {
            Set<ExprId> unionOutputExprIds = new HashSet<>();
            if (unionOutputs == null) {
                return unionOutputExprIds;
            }
            for (Slot outputSlot : unionOutputs) {
                if (outputSlot instanceof SlotReference) {
                    unionOutputExprIds.add(((SlotReference) outputSlot).getExprId());
                }
            }
            return unionOutputExprIds;
        }

        /**
         * Build child output maps keyed by UNION output exprId.
         *
         * <p>Using the example SQL above, each child branch provides an expression per UNION output slot.
         */
        private List<Map<ExprId, Expression>> buildUnionChildOutputMaps(LogicalUnion union, List<Slot> unionOutputs) {
            List<Map<ExprId, Expression>> childOutputMaps = new ArrayList<>();
            for (int childIndex = 0; childIndex < union.children().size(); childIndex++) {
                Map<ExprId, Expression> childMap = new HashMap<>();
                List<SlotReference> childOutputs = union.getRegularChildOutput(childIndex);
                List<Expression> shuttledOutputs =
                        shuttleExpressionsThroughPlan(union.child(childIndex), childOutputs, exprIdExpressionMap);
                int maxIndex = Math.min(unionOutputs.size(), shuttledOutputs.size());
                for (int outputIndex = 0; outputIndex < maxIndex; outputIndex++) {
                    Slot unionOutput = unionOutputs.get(outputIndex);
                    if (!(unionOutput instanceof SlotReference)) {
                        continue;
                    }
                    ExprId unionExprId = ((SlotReference) unionOutput).getExprId();
                    childMap.put(unionExprId, shuttledOutputs.get(outputIndex));
                }
                childOutputMaps.add(childMap);
            }
            return childOutputMaps;
        }

        /**
         * Replace UNION outputs in direct lineage using child output expressions.
         *
         * <p>Using the example SQL above, SUM(price) is replaced with SUM(child_price) per branch.
         */
        private void replaceUnionDirectLineage(
                Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap,
                Set<ExprId> unionOutputExprIds, List<Map<ExprId, Expression>> childOutputMaps) {
            Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageSnapshot =
                    new HashMap<>(directLineageMap);
            Map<Expression, Set<ExprId>> referencedUnionExprIdsCache = new IdentityHashMap<>();
            for (Map.Entry<SlotReference, SetMultimap<DirectLineageType, Expression>> directEntry
                    : directLineageSnapshot.entrySet()) {
                SetMultimap<DirectLineageType, Expression> updatedLineage = HashMultimap.create();
                boolean replaced = false;
                for (Map.Entry<DirectLineageType, Expression> lineageEntry : directEntry.getValue().entries()) {
                    Expression expr = lineageEntry.getValue();
                    Set<ExprId> referencedUnionExprIds =
                            collectUnionExprIds(expr, unionOutputExprIds, referencedUnionExprIdsCache);
                    if (referencedUnionExprIds.isEmpty()) {
                        updatedLineage.put(lineageEntry.getKey(), expr);
                        continue;
                    }
                    replaced = true;
                    for (Map<ExprId, Expression> childMap : childOutputMaps) {
                        if (!childMap.keySet().containsAll(referencedUnionExprIds)) {
                            continue;
                        }
                        Expression replacedExpr = expr.accept(ExpressionReplacer.INSTANCE, childMap);
                        DirectLineageType type = determineDirectLineageType(replacedExpr);
                        updatedLineage.put(type, replacedExpr);
                    }
                }
                if (replaced && !updatedLineage.isEmpty()) {
                    directLineageMap.put(directEntry.getKey(), updatedLineage);
                }
            }
        }

        /**
         * Collect UNION output exprIds referenced by an expression with caching.
         *
         * <p>Using the example SQL above, this finds UNION output slots referenced in a projection.
         */
        private Set<ExprId> collectUnionExprIds(Expression expression, Set<ExprId> unionOutputExprIds,
                                                Map<Expression, Set<ExprId>> referencedUnionExprIdsCache) {
            if (expression == null || unionOutputExprIds == null || unionOutputExprIds.isEmpty()) {
                return Collections.emptySet();
            }
            Set<ExprId> cached = referencedUnionExprIdsCache.get(expression);
            if (cached != null) {
                return cached;
            }
            Set<ExprId> referenced = new HashSet<>();
            for (Object slotRef : expression.collectToList(SlotReference.class::isInstance)) {
                ExprId exprId = ((SlotReference) slotRef).getExprId();
                if (unionOutputExprIds.contains(exprId)) {
                    referenced.add(exprId);
                }
            }
            referencedUnionExprIdsCache.put(expression, referenced);
            return referenced;
        }

        /**
         * Shuttle expressions to replace exprIds with their resolved expressions.
         *
         * <p>Using the example SQL above, nation_name is shuttled to n.n_name.
         */
        private Set<Expression> shuttleExpressions(Collection<? extends Expression> expressions,
                                                   Map<ExprId, Expression> exprIdExpressionMap) {
            if (expressions == null || expressions.isEmpty()) {
                return new HashSet<>();
            }
            return expressions.stream()
                    .map(original ->
                            original.accept(ExpressionReplacer.INSTANCE, exprIdExpressionMap))
                    .collect(Collectors.toSet());
        }

        /**
         * Shuttle a target expression through a specific child plan to resolve aliases.
         *
         * <p>Using the example SQL above, this resolves UNION child output slots to their base expressions.
         */
        private List<Expression> shuttleExpressionsThroughPlan(Plan plan, List<? extends Expression> expressions,
                                                              Map<ExprId, Expression> exprIdExpressionMap) {
            if (expressions == null || expressions.isEmpty()) {
                return new ArrayList<>();
            }
            if (plan == null) {
                List<Expression> original = new ArrayList<>(expressions.size());
                for (Expression expression : expressions) {
                    original.add(expression);
                }
                return original;
            }
            ExpressionLineageReplacer.ExpressionReplaceContext context =
                    new ExpressionLineageReplacer.ExpressionReplaceContext(expressions);
            plan.accept(ExpressionLineageReplacer.INSTANCE, context);
            exprIdExpressionMap.putAll(context.getExprIdExpressionMap());
            List<Expression> replaced = new ArrayList<>(expressions.size());
            for (Expression expression : context.getReplacedExpressions()) {
                replaced.add(expression);
            }
            return replaced;
        }

        /**
         * Add indirect lineage information based on expressions.
         *
         * <p>Using the example SQL above, FILTER on r.r_name is recorded at dataset-level when no output directly
         * references r.r_name.
         *
         * @param type the type of indirect lineage
         * @param expressions the expressions contributing to indirect lineage, should be shuttled conjunction
         * @param lineageInfo the lineage info to update
         */
        private void addIndirectLineage(IndirectLineageType type,
                                        Set<Expression> expressions, LineageInfo lineageInfo) {
            for (Expression expr : expressions) {
                lineageInfo.addDatasetIndirectLineage(type, expr);
            }
        }

        /**
         * Collect WINDOW indirect lineage from window partition/order keys.
         *
         * <p>Using the example SQL above, there are no window expressions.
         */
        private void collectWindowLineage(LineageInfo lineageInfo, Map<ExprId, Expression> exprIdExpressionMap) {
            Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap =
                    lineageInfo.getDirectLineageMap();
            for (Map.Entry<SlotReference, SetMultimap<DirectLineageType, Expression>> entry
                    : directLineageMap.entrySet()) {
                SlotReference outputSlot = entry.getKey();
                Set<Expression> windowInputs = new HashSet<>();
                for (Expression expr : entry.getValue().values()) {
                    for (Object windowExpr : expr.collectToList(WindowExpression.class::isInstance)) {
                        WindowExpression windowExpression = (WindowExpression) windowExpr;
                        windowInputs.addAll(windowExpression.getPartitionKeys());
                        for (OrderExpression orderExpression : windowExpression.getOrderKeys()) {
                            windowInputs.add(orderExpression.child());
                        }
                    }
                }
                for (Expression input : shuttleExpressions(windowInputs, exprIdExpressionMap)) {
                    if (containsSlot(input)) {
                        lineageInfo.addIndirectLineage(outputSlot, IndirectLineageType.WINDOW, input);
                    }
                }
            }
        }

        /**
         * Collect CONDITIONAL indirect lineage from CASE/IF/COALESCE conditions.
         *
         * <p>Using the example SQL above, there are no conditional expressions.
         */
        private void collectConditionalLineage(LineageInfo lineageInfo, Map<ExprId, Expression> exprIdExpressionMap) {
            Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap =
                    lineageInfo.getDirectLineageMap();
            for (Map.Entry<SlotReference, SetMultimap<DirectLineageType, Expression>> entry
                    : directLineageMap.entrySet()) {
                SlotReference outputSlot = entry.getKey();
                Set<Expression> conditionalInputs = new HashSet<>();
                for (Expression expr : entry.getValue().values()) {
                    conditionalInputs.addAll(extractConditionalExpressions(expr));
                }
                for (Expression input : shuttleExpressions(conditionalInputs, exprIdExpressionMap)) {
                    if (containsSlot(input)) {
                        lineageInfo.addIndirectLineage(outputSlot, IndirectLineageType.CONDITIONAL, input);
                    }
                }
            }
        }

        /**
         * Extract conditional expressions from CASE/IF/COALESCE.
         *
         * <p>Using the example SQL above, this returns an empty set.
         */
        private Set<Expression> extractConditionalExpressions(Expression expression) {
            Set<Expression> conditions = new HashSet<>();
            for (Object expr : expression.collectToList(CaseWhen.class::isInstance)) {
                CaseWhen caseWhen = (CaseWhen) expr;
                for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                    conditions.add(whenClause.getOperand());
                }
            }
            for (Object expr : expression.collectToList(If.class::isInstance)) {
                conditions.add(((If) expr).getCondition());
            }
            for (Object expr : expression.collectToList(Coalesce.class::isInstance)) {
                conditions.addAll(((Coalesce) expr).children());
            }
            return conditions;
        }

        /**
         * Check whether an expression contains any slot references.
         *
         * <p>Using the example SQL above, n.n_name contains slots while literals do not.
         */
        private boolean containsSlot(Expression expression) {
            return !expression.collectToList(Slot.class::isInstance).isEmpty();
        }
    }

    private static class AnalyzePlanHook implements PlannerHook {
        /**
         * Hook that captures analyzed plan into the planner for later lineage use.
         *
         * <p>Using the example SQL above, this avoids rewrite-time pruning that could drop scans.
         */
        private AnalyzePlanHook() {
        }

        /**
         * Record analyzed plan after analyzer completes.
         *
         * <p>Using the example SQL above, the analyzed plan keeps table scans for lineage extraction.
         */
        @Override
        public void afterAnalyze(NereidsPlanner planner) {
            if (planner == null || planner.getCascadesContext() == null) {
                return;
            }
            Plan analyzed = planner.getCascadesContext().getRewritePlan();
            if (analyzed != null) {
                planner.setAnalyzedPlan(analyzed);
            }
        }
    }
}
