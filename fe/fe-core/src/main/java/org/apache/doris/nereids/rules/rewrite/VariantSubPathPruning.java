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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning.PruneContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * prune sub path of variant type slot.
 * for example, variant slot v in table t has two sub path: 'c1' and 'c2'
 * after this rule, select v['c1'] from t will only scan one sub path 'c1' of v to reduce scan time
 *
 * This rule accomplishes all the work using two components. The Collector traverses from the top down,
 * collecting all the element_at functions on the variant types, and recording the required path from
 * the original variant slot to the current element_at. The Replacer traverses from the bottom up,
 * generating the slots for the required sub path on scan, union, and cte consumer.
 * Then, it replaces the element_at with the corresponding slot.
 */
public class VariantSubPathPruning extends DefaultPlanRewriter<PruneContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        Context context = new Context();
        plan.accept(VariantSubPathCollector.INSTANCE, context);
        if (context.elementAtToSubPathMap.isEmpty()) {
            return plan;
        } else {
            return plan.accept(VariantSubPathReplacer.INSTANCE, context);
        }
    }

    private static class Context {

        // user for collector
        // record slot to its original expr. for example, Alias(c1, a1) will put a1 -> c1 to this map
        private final Map<Slot, Expression> slotToOriginalExprMap = Maps.newHashMap();
        // record element_at to root slot with sub path. for example, element_at(c1, 'a') as c2 + element_at(c2, 'b')
        // will put element(c2, 'b') -> {c1, ['a', 'b']} and element_at(c1, 'a') -> {c1, ['a']} to this map
        private final Map<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPathMap = Maps.newHashMap();
        // record sub path need from slot.  for example, element_at(c1, 'a') as c2 + element_at(c2, 'b')
        // will put c1 -> [['a'], ['a', 'b']] to this map
        private final Map<SlotReference, Set<List<String>>> slotToSubPathsMap = Maps.newHashMap();

        // we need to record elementAt to consumer slot, and generate right slot when do consumer slot replace
        private final Map<ElementAt, SlotReference> elementAtToCteConsumer = Maps.newHashMap();

        // use for replacer
        // record element_at should be replaced with which slot
        private final Map<ElementAt, SlotReference> elementAtToSlotMap = Maps.newHashMap();
        // record which slots of prefix-matched sub paths need to be replaced
        // in addition to the slots of the exactly matched sub path.
        // for example, we have element_at(c1, 'a') as c2 + element_at(c2, 'b')
        // if element_at(c1, 'a') -> s1, element_at(c2, 'b') -> s2, then
        // in this map we have element_at(c1, 'a') -> {['a', 'b'] -> s2}
        // this is used in replace element_at in project. since upper node may need its sub path, so we must put all
        // slot could be generated from it into project list.
        private final Map<ElementAt, Map<List<String>, SlotReference>> elementAtToSlotsMap = Maps.newHashMap();
        // same as elementAtToSlotsMap, record variant slot should be replaced by which slots.
        private final Map<Slot, Map<List<String>, SlotReference>> slotToSlotsMap = Maps.newHashMap();

        public void putSlotToOriginal(Slot slot, Expression expression) {
            this.slotToOriginalExprMap.put(slot, expression);
            // update existed entry
            //   element_at(3, c) -> 3, ['c']
            //   +
            //   slot3 -> element_at(1, b) -> 1, ['b']
            //   ==>
            //   element_at(3, c) -> 1, ['b', 'c']
            for (Map.Entry<ElementAt, Pair<SlotReference, List<String>>> entry : elementAtToSubPathMap.entrySet()) {
                ElementAt elementAt = entry.getKey();
                Pair<SlotReference, List<String>> oldSlotSubPathPair = entry.getValue();
                if (slot.equals(oldSlotSubPathPair.first)) {
                    if (expression instanceof ElementAt) {
                        Pair<SlotReference, List<String>> newSlotSubPathPair = elementAtToSubPathMap.get(expression);
                        List<String> newPath = Lists.newArrayList(newSlotSubPathPair.second);
                        newPath.addAll(oldSlotSubPathPair.second);
                        elementAtToSubPathMap.put(elementAt, Pair.of(newSlotSubPathPair.first, newPath));
                        slotToSubPathsMap.computeIfAbsent(newSlotSubPathPair.first,
                                k -> Sets.newHashSet()).add(newPath);
                    } else if (expression instanceof Slot) {
                        Pair<SlotReference, List<String>> newSlotSubPathPair
                                = Pair.of((SlotReference) expression, oldSlotSubPathPair.second);
                        elementAtToSubPathMap.put(elementAt, newSlotSubPathPair);
                    }
                }
            }
            if (expression instanceof SlotReference && slotToSubPathsMap.containsKey((SlotReference) slot)) {
                Set<List<String>> subPaths = slotToSubPathsMap
                        .computeIfAbsent((SlotReference) expression, k -> Sets.newHashSet());
                subPaths.addAll(slotToSubPathsMap.get(slot));
            }
        }

        public void putElementAtToSubPath(ElementAt elementAt,
                Pair<SlotReference, List<String>> pair, Slot parent) {
            this.elementAtToSubPathMap.put(elementAt, pair);
            Set<List<String>> subPaths = slotToSubPathsMap.computeIfAbsent(pair.first, k -> Sets.newHashSet());
            subPaths.add(pair.second);
            if (parent != null) {
                for (List<String> parentSubPath : slotToSubPathsMap.computeIfAbsent(
                        (SlotReference) parent, k -> Sets.newHashSet())) {
                    List<String> subPathWithParents = Lists.newArrayList(pair.second);
                    subPathWithParents.addAll(parentSubPath);
                    subPaths.add(subPathWithParents);
                }
            }
        }

        public void putAllElementAtToSubPath(Map<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPathMap) {
            for (Map.Entry<ElementAt, Pair<SlotReference, List<String>>> entry : elementAtToSubPathMap.entrySet()) {
                putElementAtToSubPath(entry.getKey(), entry.getValue(), null);
            }
        }
    }

    private static class VariantSubPathReplacer extends DefaultPlanRewriter<Context> {

        public static VariantSubPathReplacer INSTANCE = new VariantSubPathReplacer();

        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Context context) {
            List<Slot> outputs = olapScan.getOutput();
            Map<String, Set<List<String>>> colToSubPaths = Maps.newHashMap();
            for (Slot slot : outputs) {
                if (slot.getDataType() instanceof VariantType
                        && context.slotToSubPathsMap.containsKey((SlotReference) slot)) {
                    Set<List<String>> subPaths = context.slotToSubPathsMap.get(slot);
                    if (((SlotReference) slot).getColumn().isPresent()) {
                        colToSubPaths.put(((SlotReference) slot).getColumn().get().getName(), subPaths);
                    }
                }
            }
            LogicalOlapScan newScan = olapScan.withColToSubPathsMap(colToSubPaths);
            Map<Slot, Map<List<String>, SlotReference>> oriSlotToSubPathToSlot = newScan.getSubPathToSlotMap();
            generateElementAtMaps(context, oriSlotToSubPathToSlot);
            return newScan;
        }

        @Override
        public Plan visitLogicalUnion(LogicalUnion union, Context context) {
            union = (LogicalUnion) this.visit(union, context);
            if (union.getQualifier() == Qualifier.DISTINCT) {
                return super.visitLogicalUnion(union, context);
            }
            List<List<SlotReference>> regularChildrenOutputs
                    = Lists.newArrayListWithExpectedSize(union.getRegularChildrenOutputs().size());
            List<List<NamedExpression>> constExprs
                    = Lists.newArrayListWithExpectedSize(union.getConstantExprsList().size());
            for (int i = 0; i < union.getRegularChildrenOutputs().size(); i++) {
                regularChildrenOutputs.add(Lists.newArrayListWithExpectedSize(union.getOutput().size() * 2));
            }
            for (int i = 0; i < union.getConstantExprsList().size(); i++) {
                constExprs.add(Lists.newArrayListWithExpectedSize(union.getOutput().size() * 2));
            }
            List<NamedExpression> outputs = Lists.newArrayListWithExpectedSize(union.getOutput().size() * 2);

            Map<Slot, Map<List<String>, SlotReference>> oriSlotToSubPathToSlot = Maps.newHashMap();
            for (int i = 0; i < union.getOutput().size(); i++) {
                // put back original slot
                for (int j = 0; j < regularChildrenOutputs.size(); j++) {
                    regularChildrenOutputs.get(j).add(union.getRegularChildOutput(j).get(i));
                }
                for (int j = 0; j < constExprs.size(); j++) {
                    constExprs.get(j).add(union.getConstantExprsList().get(j).get(i));
                }
                outputs.add(union.getOutputs().get(i));
                // if not variant, no need to process
                if (!union.getOutput().get(i).getDataType().isVariantType()) {
                    continue;
                }
                // put new slots generated by sub path push down
                Map<List<String>, List<SlotReference>> subPathSlots = Maps.newHashMap();
                for (int j = 0; j < regularChildrenOutputs.size(); j++) {
                    List<SlotReference> regularChildOutput = union.getRegularChildOutput(j);
                    Expression output = regularChildOutput.get(i);
                    if (!context.slotToSlotsMap.containsKey(output)
                            || !context.slotToSubPathsMap.containsKey(outputs.get(i))) {
                        // no sub path request for this column
                        continue;
                    }
                    // find sub path generated by union children
                    Expression key = output;
                    while (context.slotToOriginalExprMap.containsKey(key)) {
                        key = context.slotToOriginalExprMap.get(key);
                    }
                    List<String> subPathByChildren = Collections.emptyList();
                    if (key instanceof ElementAt) {
                        // this means need to find common sub path of its slots.
                        subPathByChildren = context.elementAtToSubPathMap.get(key).second;
                    }

                    for (Map.Entry<List<String>, SlotReference> entry : context.slotToSlotsMap.get(output).entrySet()) {
                        List<SlotReference> slotsForSubPath;
                        // remove subPath generated by children,
                        // because context only record sub path generated by parent
                        List<String> parentPaths = entry.getKey()
                                .subList(subPathByChildren.size(), entry.getKey().size());
                        if (!context.slotToSubPathsMap.get(outputs.get(i)).contains(parentPaths)) {
                            continue;
                        }
                        if (j == 0) {
                            // first child, need to put entry to subPathToSlots
                            slotsForSubPath = subPathSlots.computeIfAbsent(parentPaths, k -> Lists.newArrayList());
                        } else {
                            // other children, should find try from map. otherwise bug comes
                            if (!subPathSlots.containsKey(parentPaths)) {
                                throw new AnalysisException("push down variant sub path failed."
                                        + " cannot find sub path for child " + j + "."
                                        + " Sub path set is " + subPathSlots.keySet());
                            }
                            slotsForSubPath = subPathSlots.get(parentPaths);
                        }
                        slotsForSubPath.add(entry.getValue());
                    }
                }
                if (regularChildrenOutputs.isEmpty()) {
                    // use output sub paths exprs to generate subPathSlots
                    for (List<String> subPath : context.slotToSubPathsMap.get(outputs.get(i))) {
                        subPathSlots.put(subPath, ImmutableList.of((SlotReference) outputs.get(i).toSlot()));
                    }

                }
                for (Map.Entry<List<String>, List<SlotReference>> entry : subPathSlots.entrySet()) {
                    for (int j = 0; j < regularChildrenOutputs.size(); j++) {
                        regularChildrenOutputs.get(j).add(entry.getValue().get(j));
                    }
                    for (int j = 0; j < constExprs.size(); j++) {
                        NamedExpression constExpr = union.getConstantExprsList().get(j).get(i);
                        Expression pushDownExpr;
                        if (constExpr instanceof Alias) {
                            pushDownExpr = ((Alias) constExpr).child();
                        } else {
                            pushDownExpr = constExpr;
                        }
                        for (int sp = entry.getKey().size() - 1; sp >= 0; sp--) {
                            VarcharLiteral path = new VarcharLiteral(entry.getKey().get(sp));
                            pushDownExpr = new ElementAt(pushDownExpr, path);
                        }
                        constExprs.get(j).add(new Alias(pushDownExpr));

                    }
                    SlotReference outputSlot = new SlotReference(StatementScopeIdGenerator.newExprId(),
                            entry.getValue().get(0).getName(), VariantType.INSTANCE,
                            true, ImmutableList.of(),
                            null,
                            null,
                            Optional.empty());
                    outputs.add(outputSlot);
                    // update element to slot map
                    Map<List<String>, SlotReference> s = oriSlotToSubPathToSlot.computeIfAbsent(
                            (Slot) outputs.get(i), k -> Maps.newHashMap());
                    s.put(entry.getKey(), outputSlot);
                }
            }
            generateElementAtMaps(context, oriSlotToSubPathToSlot);

            return union.withNewOutputsChildrenAndConstExprsList(outputs, union.children(),
                    regularChildrenOutputs, constExprs);
        }

        @Override
        public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Context context) {
            ImmutableList.Builder<NamedExpression> newProjections
                    = ImmutableList.builderWithExpectedSize(oneRowRelation.getProjects().size());
            for (NamedExpression projection : oneRowRelation.getProjects()) {
                newProjections.add(projection);
                newProjections.addAll(pushDownToProject(context, projection));
            }
            return oneRowRelation.withProjects(newProjections.build());
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Context context) {
            project = (LogicalProject<? extends Plan>) this.visit(project, context);
            ImmutableList.Builder<NamedExpression> newProjections
                    = ImmutableList.builderWithExpectedSize(project.getProjects().size());
            for (NamedExpression projection : project.getProjects()) {
                boolean addOthers = projection.getDataType().isVariantType();
                if (projection instanceof SlotReference) {
                    newProjections.add(projection);
                } else {
                    Expression child = ((Alias) projection).child();
                    NamedExpression newProjection;
                    if (child instanceof SlotReference) {
                        newProjection = projection;
                    } else if (child instanceof ElementAt) {
                        if (context.elementAtToSlotMap.containsKey(child)) {
                            newProjection = (NamedExpression) projection
                                    .withChildren(context.elementAtToSlotMap.get(child));
                        } else {
                            addOthers = false;
                            newProjection = projection;

                            // try push element_at on this slot
                            if (extractSlotToSubPathPair((ElementAt) child) == null) {
                                newProjections.addAll(pushDownToProject(context, projection));
                            }
                        }
                    } else {
                        addOthers = false;
                        newProjection = (NamedExpression) ExpressionUtils.replace(
                                projection, context.elementAtToSlotMap);
                        // try push element_at on this slot
                        newProjections.addAll(pushDownToProject(context, projection));
                    }
                    newProjections.add(newProjection);
                }
                if (addOthers) {
                    Expression key = projection.toSlot();
                    while (key instanceof Slot && context.slotToOriginalExprMap.containsKey(key)) {
                        key = context.slotToOriginalExprMap.get(key);
                    }
                    if (key instanceof ElementAt && context.elementAtToSlotsMap.containsKey(key)) {
                        newProjections.addAll(context.elementAtToSlotsMap.get(key).values());
                        context.slotToSlotsMap.put(projection.toSlot(), context.elementAtToSlotsMap.get(key));
                    } else if (key instanceof Slot && context.slotToSlotsMap.containsKey(key)) {
                        newProjections.addAll(context.slotToSlotsMap.get(key).values());
                        context.slotToSlotsMap.put(projection.toSlot(), context.slotToSlotsMap.get(key));
                    }
                }
            }
            return project.withProjects(newProjections.build());
        }

        @Override
        public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Context context) {
            if (cteConsumer.getProducerToConsumerOutputMap().keySet().stream()
                    .map(ExpressionTrait::getDataType).noneMatch(VariantType.class::isInstance)) {
                return cteConsumer;
            }
            Map<Slot, Slot> consumerToProducerOutputMap = Maps.newHashMap();
            Map<Slot, Slot> producerToConsumerOutputMap = Maps.newHashMap();
            Map<Slot, Map<List<String>, SlotReference>> oriSlotToSubPathToSlot = Maps.newHashMap();
            for (Map.Entry<Slot, Slot> consumerToProducer : cteConsumer.getConsumerToProducerOutputMap().entrySet()) {
                Slot consumer = consumerToProducer.getKey();
                Slot producer = consumerToProducer.getValue();
                consumerToProducerOutputMap.put(consumer, producer);
                producerToConsumerOutputMap.put(producer, consumer);
                if (!(consumer.getDataType() instanceof VariantType)) {
                    continue;
                }

                if (context.slotToSlotsMap.containsKey(producer)) {
                    Map<List<String>, SlotReference> consumerSlots = Maps.newHashMap();
                    for (Map.Entry<List<String>, SlotReference> producerSlot
                            : context.slotToSlotsMap.get(producer).entrySet()) {
                        SlotReference consumerSlot = LogicalCTEConsumer.generateConsumerSlot(
                                cteConsumer.getName(), producerSlot.getValue());
                        consumerToProducerOutputMap.put(consumerSlot, producerSlot.getValue());
                        producerToConsumerOutputMap.put(producerSlot.getValue(), consumerSlot);
                        consumerSlots.put(producerSlot.getKey(), consumerSlot);
                    }
                    context.slotToSlotsMap.put(consumer, consumerSlots);
                    oriSlotToSubPathToSlot.put(consumer, consumerSlots);
                }
            }

            for (Entry<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPath
                    : context.elementAtToSubPathMap.entrySet()) {
                ElementAt elementAt = elementAtToSubPath.getKey();
                Pair<SlotReference, List<String>> slotWithSubPath = elementAtToSubPath.getValue();
                SlotReference key = slotWithSubPath.first;
                if (context.elementAtToCteConsumer.containsKey(elementAt)) {
                    key = context.elementAtToCteConsumer.get(elementAt);
                }
                // find exactly sub-path slot
                if (oriSlotToSubPathToSlot.containsKey(key)) {
                    context.elementAtToSlotMap.put(elementAtToSubPath.getKey(),
                            oriSlotToSubPathToSlot.get(key).get(slotWithSubPath.second));
                }
                // find prefix sub-path slots
                if (oriSlotToSubPathToSlot.containsKey(key)) {
                    Map<List<String>, SlotReference> subPathToSlotMap = oriSlotToSubPathToSlot.get(key);
                    for (Map.Entry<List<String>, SlotReference> subPathWithSlot : subPathToSlotMap.entrySet()) {
                        if (subPathWithSlot.getKey().size() > slotWithSubPath.second.size()
                                && subPathWithSlot.getKey().subList(0, slotWithSubPath.second.size())
                                .equals(slotWithSubPath.second)) {
                            Map<List<String>, SlotReference> slots = context.elementAtToSlotsMap
                                    .computeIfAbsent(elementAt, e -> Maps.newHashMap());
                            slots.put(subPathWithSlot.getKey(), subPathWithSlot.getValue());

                        }
                    }
                }
            }
            return cteConsumer.withTwoMaps(consumerToProducerOutputMap, producerToConsumerOutputMap);
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Context context) {
            filter = (LogicalFilter<? extends Plan>) this.visit(filter, context);
            ImmutableSet.Builder<Expression> newConjuncts
                    = ImmutableSet.builderWithExpectedSize(filter.getConjuncts().size());
            for (Expression conjunct : filter.getConjuncts()) {
                newConjuncts.add(ExpressionUtils.replace(conjunct, context.elementAtToSlotMap));
            }
            return filter.withConjuncts(newConjuncts.build());
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Context context) {
            join = (LogicalJoin<? extends Plan, ? extends Plan>) this.visit(join, context);
            ImmutableList.Builder<Expression> hashConditions
                    = ImmutableList.builderWithExpectedSize(join.getHashJoinConjuncts().size());
            ImmutableList.Builder<Expression> otherConditions
                    = ImmutableList.builderWithExpectedSize(join.getOtherJoinConjuncts().size());
            ImmutableList.Builder<Expression> markConditions
                    = ImmutableList.builderWithExpectedSize(join.getMarkJoinConjuncts().size());
            for (Expression conjunct : join.getHashJoinConjuncts()) {
                hashConditions.add(ExpressionUtils.replace(conjunct, context.elementAtToSlotMap));
            }
            for (Expression conjunct : join.getOtherJoinConjuncts()) {
                otherConditions.add(ExpressionUtils.replace(conjunct, context.elementAtToSlotMap));
            }
            for (Expression conjunct : join.getMarkJoinConjuncts()) {
                markConditions.add(ExpressionUtils.replace(conjunct, context.elementAtToSlotMap));
            }
            return join.withJoinConjuncts(hashConditions.build(), otherConditions.build(),
                    markConditions.build(), join.getJoinReorderContext());
        }

        @Override
        public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Context context) {
            sort = (LogicalSort<? extends Plan>) this.visit(sort, context);
            ImmutableList.Builder<OrderKey> orderKeyBuilder
                    = ImmutableList.builderWithExpectedSize(sort.getOrderKeys().size());
            for (OrderKey orderKey : sort.getOrderKeys()) {
                orderKeyBuilder.add(orderKey.withExpression(
                        ExpressionUtils.replace(orderKey.getExpr(), context.elementAtToSlotMap)));
            }
            return sort.withOrderKeys(orderKeyBuilder.build());
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Context context) {
            topN = (LogicalTopN<? extends Plan>) this.visit(topN, context);
            ImmutableList.Builder<OrderKey> orderKeyBuilder
                    = ImmutableList.builderWithExpectedSize(topN.getOrderKeys().size());
            for (OrderKey orderKey : topN.getOrderKeys()) {
                orderKeyBuilder.add(orderKey.withExpression(
                        ExpressionUtils.replace(orderKey.getExpr(), context.elementAtToSlotMap)));
            }
            return topN.withOrderKeys(orderKeyBuilder.build());
        }

        @Override
        public Plan visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN, Context context) {
            partitionTopN = (LogicalPartitionTopN<? extends Plan>) this.visit(partitionTopN, context);
            ImmutableList.Builder<OrderExpression> orderKeyBuilder
                    = ImmutableList.builderWithExpectedSize(partitionTopN.getOrderKeys().size());
            for (OrderExpression orderExpression : partitionTopN.getOrderKeys()) {
                orderKeyBuilder.add(new OrderExpression(orderExpression.getOrderKey().withExpression(
                        ExpressionUtils.replace(orderExpression.getOrderKey().getExpr(), context.elementAtToSlotMap))
                ));
            }
            ImmutableList.Builder<Expression> partitionKeysBuilder
                    = ImmutableList.builderWithExpectedSize(partitionTopN.getPartitionKeys().size());
            for (Expression partitionKey : partitionTopN.getPartitionKeys()) {
                partitionKeysBuilder.add(ExpressionUtils.replace(partitionKey, context.elementAtToSlotMap));
            }
            return partitionTopN.withPartitionKeysAndOrderKeys(partitionKeysBuilder.build(), orderKeyBuilder.build());
        }

        @Override
        public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Context context) {
            generate = (LogicalGenerate<? extends Plan>) this.visit(generate, context);
            ImmutableList.Builder<Function> generatorBuilder
                    = ImmutableList.builderWithExpectedSize(generate.getGenerators().size());
            for (Function generator : generate.getGenerators()) {
                generatorBuilder.add((Function) ExpressionUtils.replace(generator, context.elementAtToSlotMap));
            }
            return generate.withGenerators(generatorBuilder.build());
        }

        @Override
        public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, Context context) {
            window = (LogicalWindow<? extends Plan>) this.visit(window, context);
            ImmutableList.Builder<NamedExpression> windowBuilder
                    = ImmutableList.builderWithExpectedSize(window.getWindowExpressions().size());
            for (NamedExpression windowFunction : window.getWindowExpressions()) {
                windowBuilder.add((NamedExpression) ExpressionUtils.replace(
                        windowFunction, context.elementAtToSlotMap));
            }
            return window.withExpressionsAndChild(windowBuilder.build(), window.child());
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Context context) {
            aggregate = (LogicalAggregate<? extends Plan>) this.visit(aggregate, context);
            ImmutableList.Builder<NamedExpression> outputsBuilder
                    = ImmutableList.builderWithExpectedSize(aggregate.getOutputExpressions().size());
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                outputsBuilder.add((NamedExpression) ExpressionUtils.replace(
                        output, context.elementAtToSlotMap));
            }
            ImmutableList.Builder<Expression> groupByKeysBuilder
                    = ImmutableList.builderWithExpectedSize(aggregate.getGroupByExpressions().size());
            for (Expression groupByKey : aggregate.getGroupByExpressions()) {
                groupByKeysBuilder.add(ExpressionUtils.replace(groupByKey, context.elementAtToSlotMap));
            }
            return aggregate.withGroupByAndOutput(groupByKeysBuilder.build(), outputsBuilder.build());
        }

        private List<NamedExpression> pushDownToProject(Context context, NamedExpression projection) {
            if (!projection.getDataType().isVariantType()
                    || !context.slotToSubPathsMap.containsKey((SlotReference) projection.toSlot())) {
                return Collections.emptyList();
            }
            List<NamedExpression> newProjections = Lists.newArrayList();
            Expression child = projection.child(0);
            Map<List<String>, SlotReference> subPathToSlot = Maps.newHashMap();
            Set<List<String>> subPaths = context.slotToSubPathsMap
                    .get((SlotReference) projection.toSlot());
            for (List<String> subPath : subPaths) {
                Expression pushDownExpr = child;
                for (int i = subPath.size() - 1; i >= 0; i--) {
                    VarcharLiteral path = new VarcharLiteral(subPath.get(i));
                    pushDownExpr = new ElementAt(pushDownExpr, path);
                }
                Alias alias = new Alias(pushDownExpr);
                newProjections.add(alias);
                subPathToSlot.put(subPath, (SlotReference) alias.toSlot());
            }
            Map<Slot, Map<List<String>, SlotReference>> oriSlotToSubPathToSlot = Maps.newHashMap();
            oriSlotToSubPathToSlot.put(projection.toSlot(), subPathToSlot);
            generateElementAtMaps(context, oriSlotToSubPathToSlot);
            return newProjections;
        }

        private void generateElementAtMaps(Context context, Map<Slot,
                Map<List<String>, SlotReference>> oriSlotToSubPathToSlot) {
            context.slotToSlotsMap.putAll(oriSlotToSubPathToSlot);
            for (Entry<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPath
                    : context.elementAtToSubPathMap.entrySet()) {
                ElementAt elementAt = elementAtToSubPath.getKey();
                Pair<SlotReference, List<String>> slotWithSubPath = elementAtToSubPath.getValue();
                // find exactly sub-path slot
                if (oriSlotToSubPathToSlot.containsKey(slotWithSubPath.first)) {
                    context.elementAtToSlotMap.put(elementAtToSubPath.getKey(), oriSlotToSubPathToSlot.get(
                            slotWithSubPath.first).get(slotWithSubPath.second));
                }
                // find prefix sub-path slots
                if (oriSlotToSubPathToSlot.containsKey(slotWithSubPath.first)) {
                    Map<List<String>, SlotReference> subPathToSlotMap = oriSlotToSubPathToSlot.get(
                            slotWithSubPath.first);
                    for (Map.Entry<List<String>, SlotReference> subPathWithSlot : subPathToSlotMap.entrySet()) {
                        if (subPathWithSlot.getKey().size() > slotWithSubPath.second.size()
                                && subPathWithSlot.getKey().subList(0, slotWithSubPath.second.size())
                                .equals(slotWithSubPath.second)) {
                            Map<List<String>, SlotReference> slots = context.elementAtToSlotsMap
                                    .computeIfAbsent(elementAt, e -> Maps.newHashMap());
                            slots.put(subPathWithSlot.getKey(), subPathWithSlot.getValue());
                        }
                    }
                }
            }
        }
    }

    private static class VariantSubPathCollector extends PlanVisitor<Void, Context> {

        public static VariantSubPathCollector INSTANCE = new VariantSubPathCollector();

        /**
         * Extract sequential element_at from expression tree.
         * if extract success, put it into context map and stop traverse
         * other-wise, traverse its children
         */
        private static class ExtractSlotToSubPathPairFromTree
                extends DefaultExpressionVisitor<Void, Map<ElementAt, Pair<SlotReference, List<String>>>> {

            public static ExtractSlotToSubPathPairFromTree INSTANCE = new ExtractSlotToSubPathPairFromTree();

            @Override
            public Void visitElementAt(ElementAt elementAt, Map<ElementAt, Pair<SlotReference, List<String>>> context) {
                Pair<SlotReference, List<String>> pair = extractSlotToSubPathPair(elementAt);
                if (pair == null) {
                    visit(elementAt, context);
                } else {
                    context.put(elementAt, pair);
                }
                return null;
            }
        }

        @Override
        public Void visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
                Context context) {
            cteAnchor.right().accept(this, context);
            return cteAnchor.left().accept(this, context);
        }

        @Override
        public Void visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Context context) {
            for (Map.Entry<Slot, Slot> consumerToProducer : cteConsumer.getConsumerToProducerOutputMap().entrySet()) {
                Slot consumer = consumerToProducer.getKey();
                if (!(consumer.getDataType() instanceof VariantType)) {
                    continue;
                }
                Slot producer = consumerToProducer.getValue();
                if (context.slotToSubPathsMap.containsKey((SlotReference) consumer)) {
                    Set<List<String>> subPaths = context.slotToSubPathsMap
                            .computeIfAbsent((SlotReference) producer, k -> Sets.newHashSet());
                    subPaths.addAll(context.slotToSubPathsMap.get(consumer));
                }
                for (Entry<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPath
                        : context.elementAtToSubPathMap.entrySet()) {
                    ElementAt elementAt = elementAtToSubPath.getKey();
                    Pair<SlotReference, List<String>> slotWithSubPath = elementAtToSubPath.getValue();
                    if (slotWithSubPath.first.equals(consumer)) {
                        context.elementAtToCteConsumer.putIfAbsent(elementAt, (SlotReference) consumer);
                        context.elementAtToSubPathMap.put(elementAt,
                                Pair.of((SlotReference) producer, slotWithSubPath.second));
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitLogicalUnion(LogicalUnion union, Context context) {
            if (union.getQualifier() == Qualifier.DISTINCT) {
                return super.visitLogicalUnion(union, context);
            }
            for (List<SlotReference> childOutputs : union.getRegularChildrenOutputs()) {
                for (int i = 0; i < union.getOutputs().size(); i++) {
                    Slot unionOutput = union.getOutput().get(i);
                    SlotReference childOutput = childOutputs.get(i);
                    if (context.slotToSubPathsMap.containsKey((SlotReference) unionOutput)) {
                        Set<List<String>> subPaths = context.slotToSubPathsMap
                                .computeIfAbsent(childOutput, k -> Sets.newHashSet());
                        subPaths.addAll(context.slotToSubPathsMap.get(unionOutput));
                    }
                }
            }

            this.visit(union, context);
            return null;
        }

        @Override
        public Void visitLogicalProject(LogicalProject<? extends Plan> project, Context context) {
            for (NamedExpression projection : project.getProjects()) {
                if (!(projection instanceof Alias)) {
                    continue;
                }
                Alias alias = (Alias) projection;
                Expression child = alias.child();
                if (child instanceof SlotReference && child.getDataType() instanceof VariantType) {
                    context.putSlotToOriginal(alias.toSlot(), child);
                }
                // process expression like v['a']['b']['c'] just in root
                // The reason for handling this situation separately is that
                // it will have an impact on the upper level. So, we need to record the mapping of slots to it.
                if (child instanceof ElementAt) {
                    Pair<SlotReference, List<String>> pair = extractSlotToSubPathPair((ElementAt) child);
                    if (pair != null) {
                        context.putElementAtToSubPath((ElementAt) child, pair, alias.toSlot());
                        context.putSlotToOriginal(alias.toSlot(), child);
                        continue;
                    }
                }
                // process other situation of expression like v['a']['b']['c']
                Map<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPathMap = Maps.newHashMap();
                child.accept(ExtractSlotToSubPathPairFromTree.INSTANCE, elementAtToSubPathMap);
                context.putAllElementAtToSubPath(elementAtToSubPathMap);
            }
            this.visit(project, context);
            return null;
        }

        @Override
        public Void visit(Plan plan, Context context) {
            Map<ElementAt, Pair<SlotReference, List<String>>> elementAtToSubPathMap = Maps.newHashMap();
            for (Expression expression : plan.getExpressions()) {
                expression.accept(ExtractSlotToSubPathPairFromTree.INSTANCE, elementAtToSubPathMap);
            }
            context.putAllElementAtToSubPath(elementAtToSubPathMap);
            for (Plan child : plan.children()) {
                child.accept(this, context);
            }
            return null;
        }
    }

    protected static Pair<SlotReference, List<String>> extractSlotToSubPathPair(ElementAt elementAt) {
        List<String> subPath = Lists.newArrayList();
        while (true) {
            if (!(elementAt.left().getDataType() instanceof VariantType)) {
                return null;
            }
            if (!(elementAt.left() instanceof ElementAt || elementAt.left() instanceof SlotReference)) {
                return null;
            }
            if (!(elementAt.right() instanceof StringLikeLiteral)) {
                return null;
            }
            subPath.add(((StringLikeLiteral) elementAt.right()).getStringValue());
            if (elementAt.left() instanceof SlotReference) {
                // ElementAt's left child is SlotReference
                // reverse subPath because we put them by reverse order
                Collections.reverse(subPath);
                return Pair.of((SlotReference) elementAt.left(), subPath);
            } else {
                // ElementAt's left child is ElementAt
                elementAt = (ElementAt) elementAt.left();
            }
        }
    }
}
