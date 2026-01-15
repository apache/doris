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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        // Step 1: Extract direct lineage using shuttleExpressionWithLineage
        extractDirectLineage(plan, lineageInfo);

        // Step 2: Extract indirect lineage by traversing plan tree
        LineageCollector collector = new LineageCollector(lineageInfo);
        plan.accept(collector, null);

        // Step 3: Extract conditional expressions from direct lineage expressions
        extractConditionalLineage(lineageInfo);

        return lineageInfo;
    }

    /**
     * Extract direct lineage from plan output expressions
     */
    private static void extractDirectLineage(Plan plan, LineageInfo lineageInfo) {
        List<Slot> outputs = plan.getOutput();
        List<? extends Expression> shuttledExprs = ExpressionUtils.shuttleExpressionWithLineage(outputs, plan);

        for (int i = 0; i < outputs.size(); i++) {
            Slot outputSlot = outputs.get(i);
            if (!(outputSlot instanceof SlotReference)) {
                continue;
            }
            SlotReference outputSlotRef = (SlotReference) outputSlot;
            Expression shuttledExpr = shuttledExprs.get(i);

            // Determine direct lineage type based on expression structure
            DirectLineageType type = determineDirectLineageType(shuttledExpr);
            lineageInfo.addDirectLineage(outputSlotRef, type, shuttledExpr);
        }
    }

    /**
     * Determine the direct lineage type based on expression structure
     */
    private static DirectLineageType determineDirectLineageType(Expression expr) {
        // Check if expression contains aggregate function
        if (containsAggregateFunction(expr)) {
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
     * Check if expression contains aggregate function
     */
    private static boolean containsAggregateFunction(Expression expr) {
        if (expr instanceof AggregateFunction) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsAggregateFunction(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extract conditional expressions (CASE WHEN, IF, COALESCE) from direct lineage
     * and add them as CONDITIONAL indirect lineage
     */
    private static void extractConditionalLineage(LineageInfo lineageInfo) {
        Map<SlotReference, ?> directLineageMap = lineageInfo.getDirectLineageMap();

        for (Map.Entry<SlotReference, ?> entry : directLineageMap.entrySet()) {
            SlotReference outputSlot = entry.getKey();
            // Get all expressions associated with this output slot
            @SuppressWarnings("unchecked")
            com.google.common.collect.SetMultimap<DirectLineageType, Expression> typeExprMap =
                    (com.google.common.collect.SetMultimap<DirectLineageType, Expression>) entry.getValue();

            for (Expression expr : typeExprMap.values()) {
                Set<Expression> conditionalExprs = extractConditionalExpressions(expr);
                for (Expression condExpr : conditionalExprs) {
                    lineageInfo.addIndirectLineage(outputSlot, IndirectLineageType.CONDITIONAL, condExpr);
                }
            }
        }
    }

    /**
     * Extract conditional expressions from an expression tree
     */
    private static Set<Expression> extractConditionalExpressions(Expression expr) {
        Set<Expression> result = new HashSet<>();
        extractConditionalExpressionsRecursive(expr, result);
        return result;
    }

    private static void extractConditionalExpressionsRecursive(Expression expr, Set<Expression> result) {
        if (expr instanceof CaseWhen) {
            // For CASE WHEN, extract the condition expressions
            CaseWhen caseWhen = (CaseWhen) expr;
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                result.add(whenClause.getOperand());
            }
        } else if (expr instanceof If) {
            // For IF function, extract the condition (first argument)
            If ifExpr = (If) expr;
            result.add(ifExpr.getArgument(0)); // First argument is the condition
        } else if (expr instanceof Coalesce || expr instanceof Nvl) {
            // For COALESCE/NVL, the conditional logic is implicit
            result.add(expr);
        }

        // Recursively check children
        for (Expression child : expr.children()) {
            extractConditionalExpressionsRecursive(child, result);
        }
    }

    /**
     * Plan visitor to collect indirect lineage information
     */
    private static class LineageCollector extends DefaultPlanVisitor<Void, Void> {
        private final LineageInfo lineageInfo;

        public LineageCollector(LineageInfo lineageInfo) {
            this.lineageInfo = lineageInfo;
        }

        @Override
        public Void visit(Plan plan, Void context) {
            // Continue visiting children
            for (Plan child : plan.children()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitLogicalCatalogRelation(LogicalCatalogRelation relation, Void context) {
            // Collect table lineage
            lineageInfo.addTableLineage(relation.getTable());
            return null;
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
            // Collect JOIN conditions as indirect lineage
            Set<Expression> joinConditions = new HashSet<>();

            // Hash join conjuncts
            joinConditions.addAll(join.getHashJoinConjuncts());

            // Other join conjuncts
            joinConditions.addAll(join.getOtherJoinConjuncts());

            // Mark join conjuncts (if any)
            joinConditions.addAll(join.getMarkJoinConjuncts());

            // Add join conditions to all output slots
            lineageInfo.addIndirectLineageForAll(IndirectLineageType.JOIN, joinConditions);

            // Continue visiting children
            return visit(join, context);
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
            // Collect FILTER predicates as indirect lineage
            Set<Expression> predicates = filter.getConjuncts();
            lineageInfo.addIndirectLineageForAll(IndirectLineageType.FILTER, predicates);

            // Continue visiting children
            return visit(filter, context);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
            // Collect GROUP BY expressions as indirect lineage
            List<Expression> groupByExprs = aggregate.getGroupByExpressions();
            for (Expression groupByExpr : groupByExprs) {
                lineageInfo.addIndirectLineageForAll(IndirectLineageType.GROUP_BY, groupByExpr);
            }

            // Continue visiting children
            return visit(aggregate, context);
        }

        @Override
        public Void visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
            // Collect SORT expressions as indirect lineage
            sort.getOrderKeys().forEach(orderKey -> {
                Expression sortExpr = orderKey.getExpr();
                lineageInfo.addIndirectLineageForAll(IndirectLineageType.SORT, sortExpr);
            });

            // Continue visiting children
            return visit(sort, context);
        }

        @Override
        public Void visitLogicalWindow(LogicalWindow<? extends Plan> window, Void context) {
            // Collect WINDOW partition and order expressions as indirect lineage
            for (NamedExpression windowExpr : window.getWindowExpressions()) {
                Expression expr = windowExpr instanceof Alias ? ((Alias) windowExpr).child() : windowExpr;
                if (expr instanceof org.apache.doris.nereids.trees.expressions.WindowExpression) {
                    org.apache.doris.nereids.trees.expressions.WindowExpression winExpr =
                            (org.apache.doris.nereids.trees.expressions.WindowExpression) expr;

                    // Partition by expressions
                    for (Expression partitionKey : winExpr.getPartitionKeys()) {
                        lineageInfo.addIndirectLineageForAll(IndirectLineageType.WINDOW, partitionKey);
                    }

                    // Order by expressions
                    for (Expression orderKey : winExpr.getOrderKeys()) {
                        lineageInfo.addIndirectLineageForAll(IndirectLineageType.WINDOW, orderKey);
                    }
                }
            }

            // Continue visiting children
            return visit(window, context);
        }

        @Override
        public Void visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
            // Project doesn't add indirect lineage, just continue visiting
            return visit(project, context);
        }
    }
}
