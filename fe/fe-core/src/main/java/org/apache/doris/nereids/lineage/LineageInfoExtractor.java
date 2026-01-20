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

import org.apache.doris.nereids.lineage.LineageInfo.DirectLineageType;
import org.apache.doris.nereids.lineage.LineageInfo.IndirectLineageType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer.ExpressionReplacer;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extract lineage information from a Plan tree.
 * This class traverses the plan tree and collects both direct and indirect lineage information.
 */
public class LineageInfoExtractor {

    /**
     * Extract lineage information from a LineageEvent
     *
     * @param lineageEvent the event containing plan and source command info
     * @return the extracted lineage information
     */
    public static LineageInfo extractLineageInfo(LineageEvent lineageEvent) {
        LineageInfo lineageInfo = new LineageInfo();
        Plan plan = lineageEvent.getPlan();

        // Set source command type
        lineageInfo.setSourceCommand(lineageEvent.getSourceCommand());
        lineageInfo.setQueryId(lineageEvent.getQueryId());
        lineageInfo.setQueryText(lineageEvent.getQueryText());
        lineageInfo.setUser(lineageEvent.getUser());
        lineageInfo.setDatabase(lineageEvent.getDatabase());
        lineageInfo.setTimestampMs(lineageEvent.getTimestampMs());
        lineageInfo.setDurationMs(lineageEvent.getDurationMs());

        // Step 1: Extract direct lineage using shuttleExpressionWithLineage
        ExpressionLineageReplacer.ExpressionReplaceContext replaceContext = extractDirectLineage(plan, lineageInfo);

        // Step 2: Extract indirect lineage by traversing plan tree
        LineageCollector collector = new LineageCollector(replaceContext.getExprIdExpressionMap());
        plan.accept(collector, lineageInfo);
        return lineageInfo;
    }

    /**
     * Extract direct lineage from plan output expressions
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
            SlotReference outputSlotRef = (SlotReference) outputSlot;
            Expression shuttledExpr = shuttledExpressions.get(i);
            // Determine direct lineage type based on expression structure
            DirectLineageType type = determineDirectLineageType(shuttledExpr);
            lineageInfo.addDirectLineage(outputSlotRef, type, shuttledExpr);
        }
        return replaceContext;
    }

    /**
     * Determine the direct lineage type based on expression structure
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

        public LineageCollector(Map<ExprId, Expression> exprIdExpressionMap) {
            this.exprIdExpressionMap = exprIdExpressionMap;
        }

        @Override
        public Void visitLogicalTableSink(LogicalTableSink<? extends Plan> logicalTableSink, LineageInfo lineageInfo) {
            if (lineageInfo.getTargetTable() == null) {
                lineageInfo.setTargetTable(logicalTableSink.getTargetTable());
                lineageInfo.setTargetColumns(logicalTableSink.getOutput());
            }
            return super.visitLogicalTableSink(logicalTableSink, lineageInfo);
        }

        @Override
        public Void visitLogicalCatalogRelation(LogicalCatalogRelation relation, LineageInfo lineageInfo) {
            // Collect table lineage
            lineageInfo.addTableLineage(relation.getTable());
            return null;
        }

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

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, LineageInfo lineageInfo) {
            Set<Expression> predicates = new HashSet<>(filter.getConjuncts());
            Set<Expression> shuttled = shuttleExpressions(new ArrayList<>(predicates), exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.FILTER, shuttled, lineageInfo);
            return super.visitLogicalFilter(filter, lineageInfo);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, LineageInfo lineageInfo) {
            Set<Expression> shuttled = shuttleExpressions(aggregate.getGroupByExpressions(), exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.GROUP_BY, shuttled, lineageInfo);
            return super.visitLogicalAggregate(aggregate, lineageInfo);
        }

        @Override
        public Void visitLogicalSort(LogicalSort<? extends Plan> sort, LineageInfo lineageInfo) {
            List<Expression> sortExprs = new ArrayList<>();
            sort.getOrderKeys().forEach(orderKey -> sortExprs.add(orderKey.getExpr()));
            Set<Expression> shuttled = shuttleExpressions(sortExprs, exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.SORT, shuttled, lineageInfo);
            return super.visitLogicalSort(sort, lineageInfo);
        }

        @Override
        public Void visitLogicalTopN(LogicalTopN<? extends Plan> topN, LineageInfo lineageInfo) {
            List<Expression> sortExprs = new ArrayList<>();
            topN.getOrderKeys().forEach(orderKey -> sortExprs.add(orderKey.getExpr()));
            Set<Expression> shuttled = shuttleExpressions(sortExprs, exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.SORT, shuttled, lineageInfo);
            return super.visitLogicalTopN(topN, lineageInfo);
        }

        @Override
        public Void visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN,
                                                     LineageInfo lineageInfo) {
            List<Expression> sortExprs = new ArrayList<>();
            topN.getOrderKeys().forEach(orderKey -> sortExprs.add(orderKey.getExpr()));
            Set<Expression> shuttled = shuttleExpressions(sortExprs, exprIdExpressionMap);
            addIndirectLineage(IndirectLineageType.SORT, shuttled, lineageInfo);
            return super.visitLogicalDeferMaterializeTopN(topN, lineageInfo);
        }

        private Set<Expression> shuttleExpressions(List<? extends Expression> expressions,
                                                   Map<ExprId, Expression> exprIdExpressionMap) {
            if (expressions == null || expressions.isEmpty()) {
                return new HashSet<>();
            }
            return expressions.stream()
                    .map(original ->
                            original.accept(ExpressionReplacer.INSTANCE, exprIdExpressionMap))
                    .collect(Collectors.toSet());
        }

        private void addIndirectLineage(IndirectLineageType type,
                                        Set<Expression> expressions, LineageInfo lineageInfo) {

            Set<Slot> exprSlots = expressions.stream()
                    .flatMap(expr -> expr.collectToList(Slot.class::isInstance).stream())
                    .map(Slot.class::cast)
                    .collect(Collectors.toSet());

            Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap
                    = lineageInfo.getDirectLineageMap();
            for (Map.Entry<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineage
                    : directLineageMap.entrySet()) {
                SlotReference outputSlot = directLineage.getKey();
                SetMultimap<DirectLineageType, Expression> value = directLineage.getValue();
                Set<Slot> directSlots = value.values().stream()
                        .flatMap(expr -> expr.collectToList(Slot.class::isInstance).stream())
                        .map(Slot.class::cast)
                        .collect(Collectors.toSet());
                if (!Sets.intersection(exprSlots, directSlots).isEmpty()) {
                    for (Expression expr : expressions) {
                        lineageInfo.addIndirectLineage(outputSlot, type, expr);
                    }
                }
            }
        }
    }
}
