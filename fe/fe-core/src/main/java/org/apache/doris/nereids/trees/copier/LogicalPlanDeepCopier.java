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

package org.apache.doris.nereids.trees.copier;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * deep copy a plan
 */
public class LogicalPlanDeepCopier extends DefaultPlanRewriter<DeepCopierContext> {

    public static LogicalPlanDeepCopier INSTANCE = new LogicalPlanDeepCopier();

    public LogicalPlan deepCopy(LogicalPlan plan, DeepCopierContext context) {
        return (LogicalPlan) plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, DeepCopierContext context) {
        List<NamedExpression> newProjects = emptyRelation.getProjects().stream()
                .map(p -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), newProjects);
    }

    @Override
    public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, DeepCopierContext context) {
        List<NamedExpression> newProjects = oneRowRelation.getProjects().stream()
                .map(p -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(), newProjects);
    }

    @Override
    public Plan visitLogicalApply(LogicalApply<? extends Plan, ? extends Plan> apply, DeepCopierContext context) {
        Plan left = apply.left().accept(this, context);
        Plan right = apply.right().accept(this, context);
        List<Expression> correlationSlot = apply.getCorrelationSlot().stream()
                .map(s -> ExpressionDeepCopier.INSTANCE.deepCopy(s, context))
                .collect(ImmutableList.toImmutableList());
        SubqueryExpr subqueryExpr = (SubqueryExpr) ExpressionDeepCopier.INSTANCE
                .deepCopy(apply.getSubqueryExpr(), context);
        Optional<Expression> correlationFilter = apply.getCorrelationFilter()
                .map(f -> ExpressionDeepCopier.INSTANCE.deepCopy(f, context));
        Optional<MarkJoinSlotReference> markJoinSlotReference = apply.getMarkJoinSlotReference()
                .map(m -> (MarkJoinSlotReference) ExpressionDeepCopier.INSTANCE.deepCopy(m, context));
        return new LogicalApply<>(correlationSlot, subqueryExpr, correlationFilter,
                markJoinSlotReference, apply.isNeedAddSubOutputToProjects(), apply.isInProject(), left, right);
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, DeepCopierContext context) {
        Plan child = aggregate.child().accept(this, context);
        List<Expression> groupByExpressions = aggregate.getGroupByExpressions().stream()
                .map(k -> ExpressionDeepCopier.INSTANCE.deepCopy(k, context))
                .collect(ImmutableList.toImmutableList());
        List<NamedExpression> outputExpressions = aggregate.getOutputExpressions().stream()
                .map(o -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(o, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalAggregate<>(groupByExpressions, outputExpressions, child);
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, DeepCopierContext context) {
        Plan child = repeat.child().accept(this, context);
        List<List<Expression>> groupingSets = repeat.getGroupingSets().stream()
                .map(l -> l.stream()
                        .map(e -> ExpressionDeepCopier.INSTANCE.deepCopy(e, context))
                        .collect(ImmutableList.toImmutableList()))
                .collect(ImmutableList.toImmutableList());
        List<NamedExpression> outputExpressions = repeat.getOutputExpressions().stream()
                .map(e -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(e, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalRepeat<>(groupingSets, outputExpressions, child);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, DeepCopierContext context) {
        Plan child = filter.child().accept(this, context);
        Set<Expression> conjuncts = filter.getConjuncts().stream()
                .map(p -> ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableSet.toImmutableSet());
        return new LogicalFilter<>(conjuncts, child);
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(olapScan.getRelationId())) {
            return context.getRelationReplaceMap().get(olapScan.getRelationId());
        }
        LogicalOlapScan newOlapScan;
        if (olapScan.getManuallySpecifiedPartitions().isEmpty()) {
            newOlapScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                    olapScan.getTable(), olapScan.getQualifier(), olapScan.getSelectedTabletIds(),
                    olapScan.getHints());
        } else {
            newOlapScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                    olapScan.getTable(), olapScan.getQualifier(),
                    olapScan.getManuallySpecifiedPartitions(), olapScan.getSelectedTabletIds(),
                    olapScan.getHints());
        }
        newOlapScan.getOutput();
        context.putRelation(olapScan.getRelationId(), newOlapScan);
        updateReplaceMapWithOutput(olapScan, newOlapScan, context.exprIdReplaceMap);
        return newOlapScan;
    }

    @Override
    public Plan visitLogicalDeferMaterializeOlapScan(LogicalDeferMaterializeOlapScan deferMaterializeOlapScan,
            DeepCopierContext context) {
        LogicalOlapScan newScan = (LogicalOlapScan) visitLogicalOlapScan(
                deferMaterializeOlapScan.getLogicalOlapScan(), context);
        Set<ExprId> newSlotIds = deferMaterializeOlapScan.getDeferMaterializeSlotIds().stream()
                .map(context.exprIdReplaceMap::get)
                .collect(ImmutableSet.toImmutableSet());
        SlotReference newRowId = (SlotReference) ExpressionDeepCopier.INSTANCE
                .deepCopy(deferMaterializeOlapScan.getColumnIdSlot(), context);
        return new LogicalDeferMaterializeOlapScan(newScan, newSlotIds, newRowId);
    }

    @Override
    public Plan visitLogicalSchemaScan(LogicalSchemaScan schemaScan, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(schemaScan.getRelationId())) {
            return context.getRelationReplaceMap().get(schemaScan.getRelationId());
        }
        LogicalSchemaScan newSchemaScan = new LogicalSchemaScan(StatementScopeIdGenerator.newRelationId(),
                schemaScan.getTable(), schemaScan.getQualifier());
        updateReplaceMapWithOutput(schemaScan, newSchemaScan, context.exprIdReplaceMap);
        context.putRelation(schemaScan.getRelationId(), newSchemaScan);
        return newSchemaScan;
    }

    @Override
    public Plan visitLogicalFileScan(LogicalFileScan fileScan, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(fileScan.getRelationId())) {
            return context.getRelationReplaceMap().get(fileScan.getRelationId());
        }
        LogicalFileScan newFileScan = new LogicalFileScan(StatementScopeIdGenerator.newRelationId(),
                fileScan.getTable(), fileScan.getQualifier());
        updateReplaceMapWithOutput(fileScan, newFileScan, context.exprIdReplaceMap);
        context.putRelation(fileScan.getRelationId(), newFileScan);
        Set<Expression> conjuncts = fileScan.getConjuncts().stream()
                .map(p -> ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableSet.toImmutableSet());
        return newFileScan.withConjuncts(conjuncts);
    }

    @Override
    public Plan visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(tvfRelation.getRelationId())) {
            return context.getRelationReplaceMap().get(tvfRelation.getRelationId());
        }
        LogicalTVFRelation newTVFRelation = new LogicalTVFRelation(StatementScopeIdGenerator.newRelationId(),
                tvfRelation.getFunction());
        updateReplaceMapWithOutput(newTVFRelation, tvfRelation, context.exprIdReplaceMap);
        context.putRelation(tvfRelation.getRelationId(), newTVFRelation);
        return newTVFRelation;
    }

    @Override
    public Plan visitLogicalJdbcScan(LogicalJdbcScan jdbcScan, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(jdbcScan.getRelationId())) {
            return context.getRelationReplaceMap().get(jdbcScan.getRelationId());
        }
        LogicalJdbcScan newJdbcScan = new LogicalJdbcScan(StatementScopeIdGenerator.newRelationId(),
                jdbcScan.getTable(), jdbcScan.getQualifier());
        updateReplaceMapWithOutput(jdbcScan, newJdbcScan, context.exprIdReplaceMap);
        context.putRelation(jdbcScan.getRelationId(), newJdbcScan);
        return newJdbcScan;
    }

    @Override
    public Plan visitLogicalEsScan(LogicalEsScan esScan, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(esScan.getRelationId())) {
            return context.getRelationReplaceMap().get(esScan.getRelationId());
        }
        LogicalEsScan newEsScan = new LogicalEsScan(StatementScopeIdGenerator.newRelationId(),
                esScan.getTable(), esScan.getQualifier());
        updateReplaceMapWithOutput(esScan, newEsScan, context.exprIdReplaceMap);
        context.putRelation(esScan.getRelationId(), newEsScan);
        return newEsScan;
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, DeepCopierContext context) {
        Plan child = project.child().accept(this, context);
        List<NamedExpression> newProjects = project.getProjects().stream()
                .map(p -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalProject<>(newProjects, child);
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, DeepCopierContext context) {
        Plan child = sort.child().accept(this, context);
        List<OrderKey> orderKeys = sort.getOrderKeys().stream()
                .map(o -> new OrderKey(ExpressionDeepCopier.INSTANCE.deepCopy(o.getExpr(), context),
                        o.isAsc(), o.isNullFirst()))
                .collect(ImmutableList.toImmutableList());
        return new LogicalSort<>(orderKeys, child);
    }

    @Override
    public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, DeepCopierContext context) {
        Plan child = topN.child().accept(this, context);
        List<OrderKey> orderKeys = topN.getOrderKeys().stream()
                .map(o -> new OrderKey(ExpressionDeepCopier.INSTANCE.deepCopy(o.getExpr(), context),
                        o.isAsc(), o.isNullFirst()))
                .collect(ImmutableList.toImmutableList());
        return new LogicalTopN<>(orderKeys, topN.getLimit(), topN.getOffset(), child);
    }

    @Override
    public Plan visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN,
            DeepCopierContext context) {
        LogicalTopN<? extends Plan> newTopN
                = (LogicalTopN<? extends Plan>) visitLogicalTopN(topN.getLogicalTopN(), context);
        Set<ExprId> newSlotIds = topN.getDeferMaterializeSlotIds().stream()
                .map(context.exprIdReplaceMap::get)
                .collect(ImmutableSet.toImmutableSet());
        SlotReference newRowId = (SlotReference) ExpressionDeepCopier.INSTANCE
                .deepCopy(topN.getColumnIdSlot(), context);
        return new LogicalDeferMaterializeTopN<>(newTopN, newSlotIds, newRowId);
    }

    @Override
    public Plan visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN,
            DeepCopierContext context) {
        Plan child = partitionTopN.child().accept(this, context);
        List<Expression> partitionKeys = partitionTopN.getPartitionKeys().stream()
                .map(p -> ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableList.toImmutableList());
        List<OrderExpression> orderKeys = partitionTopN.getOrderKeys().stream()
                .map(o -> (OrderExpression) ExpressionDeepCopier.INSTANCE.deepCopy(o, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalPartitionTopN<>(partitionTopN.getFunction(), partitionKeys, orderKeys,
                partitionTopN.hasGlobalLimit(), partitionTopN.getPartitionLimit(), child);
    }

    @Override
    public Plan visitLogicalLimit(LogicalLimit<? extends Plan> limit, DeepCopierContext context) {
        Plan child = limit.child().accept(this, context);
        return new LogicalLimit<>(limit.getLimit(), limit.getOffset(), limit.getPhase(), child);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, DeepCopierContext context) {
        List<Plan> children = join.children().stream()
                .map(c -> c.accept(this, context))
                .collect(ImmutableList.toImmutableList());
        List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts().stream()
                .map(c -> ExpressionDeepCopier.INSTANCE.deepCopy(c, context))
                .collect(ImmutableList.toImmutableList());
        List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream()
                .map(c -> ExpressionDeepCopier.INSTANCE.deepCopy(c, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalJoin<>(join.getJoinType(), hashJoinConjuncts, otherJoinConjuncts,
                join.getHint(), join.getMarkJoinSlotReference(), children);
    }

    @Override
    public Plan visitLogicalAssertNumRows(LogicalAssertNumRows<? extends Plan> assertNumRows,
            DeepCopierContext context) {
        Plan child = assertNumRows.child().accept(this, context);
        return new LogicalAssertNumRows<>(assertNumRows.getAssertNumRowsElement(), child);
    }

    @Override
    public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, DeepCopierContext context) {
        Plan child = having.child().accept(this, context);
        Set<Expression> conjuncts = having.getConjuncts().stream()
                .map(p -> ExpressionDeepCopier.INSTANCE.deepCopy(p, context))
                .collect(ImmutableSet.toImmutableSet());
        return new LogicalHaving<>(conjuncts, child);
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, DeepCopierContext context) {
        List<Plan> children = union.children().stream()
                .map(c -> c.accept(this, context))
                .collect(ImmutableList.toImmutableList());
        List<List<NamedExpression>> constantExprsList = union.getConstantExprsList().stream()
                .map(l -> l.stream()
                        .map(e -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(e, context))
                        .collect(ImmutableList.toImmutableList()))
                .collect(ImmutableList.toImmutableList());
        List<NamedExpression> outputs = union.getOutputs().stream()
                .map(o -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(o, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalUnion(union.getQualifier(), outputs, constantExprsList, union.hasPushedFilter(), children);
    }

    @Override
    public Plan visitLogicalExcept(LogicalExcept except, DeepCopierContext context) {
        List<Plan> children = except.children().stream()
                .map(c -> c.accept(this, context))
                .collect(ImmutableList.toImmutableList());
        List<NamedExpression> outputs = except.getOutputs().stream()
                .map(o -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(o, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalExcept(except.getQualifier(), outputs, children);
    }

    @Override
    public Plan visitLogicalIntersect(LogicalIntersect intersect, DeepCopierContext context) {
        List<Plan> children = intersect.children().stream()
                .map(c -> c.accept(this, context))
                .collect(ImmutableList.toImmutableList());
        List<NamedExpression> outputs = intersect.getOutputs().stream()
                .map(o -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(o, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalIntersect(intersect.getQualifier(), outputs, children);
    }

    @Override
    public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, DeepCopierContext context) {
        Plan child = generate.child().accept(this, context);
        List<Function> generators = generate.getGenerators().stream()
                .map(g -> (Function) ExpressionDeepCopier.INSTANCE.deepCopy(g, context))
                .collect(ImmutableList.toImmutableList());
        List<Slot> generatorOutput = generate.getGeneratorOutput().stream()
                .map(o -> (Slot) ExpressionDeepCopier.INSTANCE.deepCopy(o, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalGenerate<>(generators, generatorOutput, child);
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, DeepCopierContext context) {
        Plan child = window.child().accept(this, context);
        List<NamedExpression> windowExpressions = window.getWindowExpressions().stream()
                .map(w -> (NamedExpression) ExpressionDeepCopier.INSTANCE.deepCopy(w, context))
                .collect(ImmutableList.toImmutableList());
        return new LogicalWindow<>(windowExpressions, child);
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> logicalSink, DeepCopierContext context) {
        Plan child = logicalSink.child().accept(this, context);
        return logicalSink.withChildren(child);
    }

    @Override
    public Plan visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, DeepCopierContext context) {
        throw new AnalysisException("plan deep copier could not copy CTEProducer.");
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, DeepCopierContext context) {
        if (context.getRelationReplaceMap().containsKey(cteConsumer.getRelationId())) {
            return context.getRelationReplaceMap().get(cteConsumer.getRelationId());
        }
        Map<Slot, Slot> consumerToProducerOutputMap = new LinkedHashMap<>();
        Map<Slot, Slot> producerToConsumerOutputMap = new LinkedHashMap<>();
        for (Slot consumerOutput : cteConsumer.getOutput()) {
            Slot newOutput = (Slot) ExpressionDeepCopier.INSTANCE.deepCopy(consumerOutput, context);
            consumerToProducerOutputMap.put(newOutput, cteConsumer.getProducerSlot(consumerOutput));
            producerToConsumerOutputMap.put(cteConsumer.getProducerSlot(consumerOutput), newOutput);
        }
        LogicalCTEConsumer newCTEConsumer = new LogicalCTEConsumer(
                StatementScopeIdGenerator.newRelationId(),
                cteConsumer.getCteId(), cteConsumer.getName(),
                consumerToProducerOutputMap, producerToConsumerOutputMap);
        context.putRelation(cteConsumer.getRelationId(), newCTEConsumer);
        return newCTEConsumer;
    }

    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            DeepCopierContext context) {
        throw new AnalysisException("plan deep copier could not copy CTEAnchor.");
    }

    private void updateReplaceMapWithOutput(Plan oldPlan, Plan newPlan, Map<ExprId, ExprId> replaceMap) {
        List<Slot> oldOutput = oldPlan.getOutput();
        List<Slot> newOutput = newPlan.getOutput();
        for (int i = 0; i < newOutput.size(); i++) {
            replaceMap.put(oldOutput.get(i).getExprId(), newOutput.get(i).getExprId());
        }
    }
}
