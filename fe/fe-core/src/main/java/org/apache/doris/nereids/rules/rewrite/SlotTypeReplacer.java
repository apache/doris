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

import org.apache.doris.analysis.AccessPathInfo;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.SupportPruneNestedColumn;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NestedColumnPrunable;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.thrift.TMetaAccessPath;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/** SlotTypeReplacer */
public class SlotTypeReplacer extends DefaultPlanRewriter<Void> {
    private Plan plan;
    private Map<Integer, AccessPathInfo> replacedDataTypes;
    private IdentityHashMap<Plan, Void> shouldPrunePlans = new IdentityHashMap();

    public SlotTypeReplacer(Map<Integer, AccessPathInfo> bottomReplacedDataTypes, Plan plan) {
        this.replacedDataTypes = Maps.newLinkedHashMap(bottomReplacedDataTypes);
        this.plan = plan;
    }

    /** replace */
    public Plan replace() {
        Set<Integer> shouldReplaceSlots = Sets.newLinkedHashSet();
        plan.foreachUp(p -> {
            if (p instanceof LogicalTVFRelation) {
                TableValuedFunction function = ((LogicalTVFRelation) p).getFunction();
                tryRecordReplaceSlots((Plan) p, function, shouldReplaceSlots);
            } else {
                tryRecordReplaceSlots((Plan) p, p, shouldReplaceSlots);
            }
        });
        replacedDataTypes.keySet().retainAll(shouldReplaceSlots);

        if (replacedDataTypes.isEmpty()) {
            return plan;
        }
        return plan.accept(this, null);
    }

    @Override
    public Plan visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, Void context) {
        return super.visitLogicalCTEProducer(cteProducer, context);
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, Void context) {
        window = visitChildren(this, window, context);
        Pair<Boolean, ? extends List<? extends Expression>> replaced = replaceExpressions(
                window.getExpressions(), false, false);
        if (replaced.first) {
            return window.withExpressionsAndChild((List) replaced.second, window.child());
        }
        return window;
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Void context) {
        Map<Slot, Slot> consumerToProducerOutputMap = cteConsumer.getConsumerToProducerOutputMap();
        Multimap<Slot, Slot> producerToConsumerOutputMap = cteConsumer.getProducerToConsumerOutputMap();

        Map<Slot, Slot> replacedConsumerToProducerOutputMap = new LinkedHashMap<>();
        Builder<Slot, Slot> replacedProducerToConsumerOutputMap = ImmutableMultimap.builder();

        boolean changed = false;
        for (Entry<Slot, Slot> kv : consumerToProducerOutputMap.entrySet()) {
            Slot consumerSlot = kv.getKey();
            Slot producerSlot = kv.getValue();
            AccessPathInfo accessPathInfo = replacedDataTypes.get(producerSlot.getExprId().asInt());
            if (accessPathInfo != null) {
                DataType prunedType = accessPathInfo.getPrunedType();
                if (!prunedType.equals(producerSlot.getDataType())) {
                    replacedDataTypes.put(consumerSlot.getExprId().asInt(), accessPathInfo);
                    changed = true;
                    producerSlot = producerSlot.withNullableAndDataType(producerSlot.nullable(), prunedType);
                    consumerSlot = consumerSlot.withNullableAndDataType(consumerSlot.nullable(), prunedType);
                }
            }
            replacedConsumerToProducerOutputMap.put(consumerSlot, producerSlot);
        }

        for (Entry<Slot, Collection<Slot>> kv : producerToConsumerOutputMap.asMap().entrySet()) {
            Slot producerSlot = kv.getKey();
            Collection<Slot> consumerSlots = kv.getValue();
            AccessPathInfo accessPathInfo = replacedDataTypes.get(producerSlot.getExprId().asInt());
            if (accessPathInfo != null && !accessPathInfo.getPrunedType().equals(producerSlot.getDataType())) {
                DataType replacedDataType = accessPathInfo.getPrunedType();
                changed = true;
                producerSlot = producerSlot.withNullableAndDataType(producerSlot.nullable(), replacedDataType);
                for (Slot consumerSlot : consumerSlots) {
                    consumerSlot = consumerSlot.withNullableAndDataType(consumerSlot.nullable(), replacedDataType);
                    replacedProducerToConsumerOutputMap.put(producerSlot, consumerSlot);
                }
            } else {
                replacedProducerToConsumerOutputMap.putAll(producerSlot, consumerSlots);
            }
        }

        if (changed) {
            return new LogicalCTEConsumer(
                    cteConsumer.getRelationId(), cteConsumer.getCteId(), cteConsumer.getName(),
                    replacedConsumerToProducerOutputMap, replacedProducerToConsumerOutputMap.build()
            );
        }
        return cteConsumer;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        join = visitChildren(this, join, context);
        Pair<Boolean, List<Expression>> replacedHashJoinConjuncts
                = replaceExpressions(join.getHashJoinConjuncts(), false, false);
        Pair<Boolean, List<Expression>> replacedOtherJoinConjuncts
                = replaceExpressions(join.getOtherJoinConjuncts(), false, false);

        if (replacedHashJoinConjuncts.first || replacedOtherJoinConjuncts.first) {
            return join.withJoinConjuncts(
                    replacedHashJoinConjuncts.second,
                    replacedOtherJoinConjuncts.second,
                    join.getJoinReorderContext());
        }
        return join;
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        project = visitChildren(this, project, context);

        Pair<Boolean, List<NamedExpression>> projects = replaceExpressions(project.getProjects(), true, false);
        if (projects.first) {
            return project.withProjects(projects.second);
        }
        return project;
    }

    @Override
    public Plan visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN, Void context) {
        partitionTopN = visitChildren(this, partitionTopN, context);

        Pair<Boolean, List<Expression>> replacedPartitionKeys = replaceExpressions(
                partitionTopN.getPartitionKeys(), false, false);
        Pair<Boolean, List<OrderExpression>> replacedOrderExpressions
                = replaceOrderExpressions(partitionTopN.getOrderKeys());
        if (replacedPartitionKeys.first || replacedOrderExpressions.first) {
            return partitionTopN.withPartitionKeysAndOrderKeys(
                    replacedPartitionKeys.second, replacedOrderExpressions.second);
        }
        return partitionTopN;
    }

    @Override
    public Plan visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN, Void context) {
        topN = visitChildren(this, topN, context);

        LogicalTopN logicalTopN = (LogicalTopN) topN.getLogicalTopN().accept(this, context);
        if (logicalTopN != topN.getLogicalTopN()) {
            SlotReference replacedColumnIdSlot = replaceExpressions(
                    ImmutableList.of(topN.getColumnIdSlot()), false, false).second.get(0);
            return new LogicalDeferMaterializeTopN(
                    logicalTopN, topN.getDeferMaterializeSlotIds(), replacedColumnIdSlot);
        }

        return topN;
    }

    @Override
    public Plan visitLogicalExcept(LogicalExcept except, Void context) {
        except = visitChildren(this, except, context);

        Pair<Boolean, List<List<SlotReference>>> replacedRegularChildrenOutputs = replaceMultiExpressions(
                except.getRegularChildrenOutputs());

        Pair<Boolean, List<NamedExpression>> replacedOutputs
                = replaceExpressions(except.getOutputs(), true, false);

        if (replacedRegularChildrenOutputs.first || replacedOutputs.first) {
            return new LogicalExcept(except.getQualifier(), except.getOutputs(),
                    except.getRegularChildrenOutputs(), except.children());
        }

        return except;
    }

    @Override
    public Plan visitLogicalIntersect(LogicalIntersect intersect, Void context) {
        intersect = visitChildren(this, intersect, context);

        Pair<Boolean, List<List<SlotReference>>> replacedRegularChildrenOutputs = replaceMultiExpressions(
                intersect.getRegularChildrenOutputs());

        Pair<Boolean, List<NamedExpression>> replacedOutputs
                = replaceExpressions(intersect.getOutputs(), true, false);

        if (replacedRegularChildrenOutputs.first || replacedOutputs.first) {
            return new LogicalIntersect(intersect.getQualifier(), intersect.getOutputs(),
                    intersect.getRegularChildrenOutputs(), intersect.children());
        }
        return intersect;
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, Void context) {
        union = visitChildren(this, union, context);

        Pair<Boolean, List<List<SlotReference>>> replacedRegularChildrenOutputs = replaceMultiExpressions(
                union.getRegularChildrenOutputs());

        Pair<Boolean, List<NamedExpression>> replacedOutputs
                = replaceExpressions(union.getOutputs(), true, false);

        if (replacedRegularChildrenOutputs.first || replacedOutputs.first) {
            return new LogicalUnion(
                    union.getQualifier(),
                    replacedOutputs.second,
                    replacedRegularChildrenOutputs.second,
                    union.getConstantExprsList(),
                    union.hasPushedFilter(),
                    union.children()
            );
        }

        return union;
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void context) {
        repeat = visitChildren(this, repeat, context);

        Pair<Boolean, List<List<Expression>>> replacedGroupingSets
                = replaceMultiExpressions(repeat.getGroupingSets());
        Pair<Boolean, List<NamedExpression>> replacedOutputs
                = replaceExpressions(repeat.getOutputExpressions(), true, false);

        if (replacedGroupingSets.first || replacedOutputs.first) {
            return repeat.withGroupSetsAndOutput(replacedGroupingSets.second, replacedOutputs.second);
        }
        return repeat;
    }

    @Override
    public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Void context) {
        generate = visitChildren(this, generate, context);

        Pair<Boolean, List<Function>> replacedGenerators
                = replaceExpressions(generate.getGenerators(), false, false);
        Pair<Boolean, List<Slot>> replacedGeneratorOutput
                = replaceExpressions(generate.getGeneratorOutput(), false, false);
        if (replacedGenerators.first || replacedGeneratorOutput.first) {
            return new LogicalGenerate<>(replacedGenerators.second, replacedGeneratorOutput.second,
                    generate.getExpandColumnAlias(), generate.child());
        }
        return generate;
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        aggregate = visitChildren(this, aggregate, context);

        Pair<Boolean, List<Expression>> replacedGroupBy = replaceExpressions(
                aggregate.getGroupByExpressions(), false, false);
        Pair<Boolean, List<NamedExpression>> replacedOutput = replaceExpressions(
                aggregate.getOutputExpressions(), true, false);

        if (replacedGroupBy.first || replacedOutput.first) {
            return aggregate.withGroupByAndOutput(replacedGroupBy.second, replacedOutput.second);
        }
        return aggregate;
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
        sort = visitChildren(this, sort, context);

        Pair<Boolean, List<OrderKey>> replaced = replaceOrderKeys(sort.getOrderKeys());
        if (replaced.first) {
            return sort.withOrderKeys(replaced.second);
        }
        return sort;
    }

    @Override
    public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
        topN = visitChildren(this, topN, context);

        Pair<Boolean, List<OrderKey>> replaced = replaceOrderKeys(topN.getOrderKeys());
        if (replaced.first) {
            return topN.withOrderKeys(replaced.second);
        }
        return topN;
    }

    @Override
    public Plan visitLogicalDeferMaterializeOlapScan(
            LogicalDeferMaterializeOlapScan deferMaterializeOlapScan, Void context) {

        LogicalOlapScan logicalOlapScan
                = (LogicalOlapScan) deferMaterializeOlapScan.getLogicalOlapScan().accept(this, context);

        if (logicalOlapScan != deferMaterializeOlapScan.getLogicalOlapScan()) {
            SlotReference replacedColumnIdSlot = replaceExpressions(
                    ImmutableList.of(deferMaterializeOlapScan.getColumnIdSlot()), false, false).second.get(0);
            return new LogicalDeferMaterializeOlapScan(
                    logicalOlapScan, deferMaterializeOlapScan.getDeferMaterializeSlotIds(), replacedColumnIdSlot
            );
        }
        return deferMaterializeOlapScan;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        filter = visitChildren(this, filter, context);

        Pair<Boolean, Set<Expression>> replaced = replaceExpressions(filter.getConjuncts(), false, false);
        if (replaced.first) {
            return filter.withConjuncts(replaced.second);
        }
        return filter;
    }

    @Override
    public Plan visitLogicalFileScan(LogicalFileScan fileScan, Void context) {
        if (!shouldPrunePlans.containsKey(fileScan)) {
            return fileScan;
        }

        Pair<Boolean, List<Slot>> replaced = replaceExpressions(fileScan.getOutput(), false, true);
        if (replaced.first) {
            List<Slot> replaceSlots = new ArrayList<>(replaced.second);
            if (fileScan.getTable() instanceof IcebergExternalTable) {
                for (int i = 0; i < replaceSlots.size(); i++) {
                    Slot slot = replaceSlots.get(i);
                    if (!(slot instanceof SlotReference)) {
                        continue;
                    }
                    SlotReference slotReference = (SlotReference) slot;
                    Optional<List<TColumnAccessPath>> allAccessPaths = slotReference.getAllAccessPaths();
                    if (!allAccessPaths.isPresent() || !slotReference.getOriginalColumn().isPresent()) {
                        continue;
                    }
                    List<TColumnAccessPath> allAccessPathsWithId
                            = replaceIcebergAccessPathToId(allAccessPaths.get(), slotReference);
                    List<TColumnAccessPath> predicateAccessPathsWithId = replaceIcebergAccessPathToId(
                            slotReference.getPredicateAccessPaths().get(), slotReference);
                    replaceSlots.set(i, ((SlotReference) slot).withAccessPaths(
                            allAccessPathsWithId,
                            predicateAccessPathsWithId,
                            allAccessPaths.get(),
                            slotReference.getPredicateAccessPaths().get()
                    ));
                }
            }
            return fileScan.withCachedOutput(replaceSlots);
        }
        return fileScan;
    }

    @Override
    public Plan visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, Void context) {
        if (!shouldPrunePlans.containsKey(tvfRelation)) {
            return tvfRelation;
        }
        Pair<Boolean, List<Slot>> replaced
                = replaceExpressions(tvfRelation.getOutput(), false, true);
        if (replaced.first) {
            return tvfRelation.withCachedOutputs(replaced.second);
        }
        return tvfRelation;
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        if (!shouldPrunePlans.containsKey(olapScan)) {
            return olapScan;
        }
        Pair<Boolean, List<Slot>> replaced = replaceExpressions(olapScan.getOutput(), false, true);
        if (replaced.first) {
            return olapScan.withCachedOutput(replaced.second);
        }
        return olapScan;
    }

    @Override
    public Plan visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, Void context) {
        Pair<Boolean, List<NamedExpression>> replacedProjects
                = replaceExpressions(emptyRelation.getProjects(), true, false);

        if (replacedProjects.first) {
            return emptyRelation.withProjects(replacedProjects.second);
        }
        return emptyRelation;
    }

    @Override
    public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Void context) {
        Pair<Boolean, List<NamedExpression>> replacedProjects
                = replaceExpressions(oneRowRelation.getProjects(), true, false);

        if (replacedProjects.first) {
            return oneRowRelation.withProjects(replacedProjects.second);
        }
        return oneRowRelation;
    }

    @Override
    public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink, Void context) {
        logicalResultSink = visitChildren(this, logicalResultSink, context);

        Pair<Boolean, List<NamedExpression>> replacedOutput = replaceExpressions(logicalResultSink.getOutputExprs(),
                false, false);
        if (replacedOutput.first) {
            return logicalResultSink.withOutputExprs(replacedOutput.second);
        }
        return logicalResultSink;
    }

    private Pair<Boolean, List<OrderExpression>> replaceOrderExpressions(List<OrderExpression> orderExpressions) {
        ImmutableList.Builder<OrderExpression> newOrderKeys
                = ImmutableList.builderWithExpectedSize(orderExpressions.size());
        boolean changed = false;
        for (OrderExpression orderExpression : orderExpressions) {
            Expression newOrderKeyExpr = replaceSlot(orderExpression.getOrderKey().getExpr(), false);
            if (newOrderKeyExpr != orderExpression.getOrderKey().getExpr()) {
                newOrderKeys.add(new OrderExpression(orderExpression.getOrderKey().withExpression(newOrderKeyExpr)));
                changed = true;
            } else {
                newOrderKeys.add(orderExpression);
            }
        }
        return Pair.of(changed, newOrderKeys.build());
    }

    private Pair<Boolean, List<OrderKey>> replaceOrderKeys(List<OrderKey> orderKeys) {
        ImmutableList.Builder<OrderKey> newOrderKeys = ImmutableList.builderWithExpectedSize(orderKeys.size());
        boolean changed = false;
        for (OrderKey orderKey : orderKeys) {
            Expression newOrderKeyExpr = replaceSlot(orderKey.getExpr(), false);
            if (newOrderKeyExpr != orderKey.getExpr()) {
                newOrderKeys.add(orderKey.withExpression(newOrderKeyExpr));
                changed = true;
            } else {
                newOrderKeys.add(orderKey);
            }
        }
        return Pair.of(changed, newOrderKeys.build());
    }

    private <C extends Collection<E>, E extends Expression>
            Pair<Boolean, List<C>> replaceMultiExpressions(List<C> expressionsList) {
        ImmutableList.Builder<C> result = ImmutableList.builderWithExpectedSize(expressionsList.size());
        boolean changed = false;
        for (C expressions : expressionsList) {
            Pair<Boolean, C> replaced = replaceExpressions(expressions, false, false);
            changed |= replaced.first;
            result.add(replaced.second);
        }
        return Pair.of(changed, result.build());
    }

    private <C extends Collection<E>, E extends Expression> Pair<Boolean, C> replaceExpressions(
            C expressions, boolean propagateType, boolean fillAccessPaths) {
        ImmutableCollection.Builder<E> newExprs;
        if (expressions instanceof List) {
            newExprs = ImmutableList.builder();
        } else if (expressions instanceof Set) {
            newExprs = ImmutableSet.builder();
        } else {
            throw new AnalysisException("Unsupported expression type: " + expressions.getClass());
        }

        boolean changed = false;
        for (Expression oldExpr : expressions) {
            Expression newExpr = replaceSlot(oldExpr, fillAccessPaths);
            if (newExpr != oldExpr) {
                newExprs.add((E) newExpr);
                changed = true;

                if (propagateType && oldExpr instanceof NamedExpression
                        && !oldExpr.getDataType().equals(newExpr.getDataType())) {
                    replacedDataTypes.put(
                            ((NamedExpression) oldExpr).getExprId().asInt(),
                            // not need access path in the upper slots
                            new AccessPathInfo(newExpr.getDataType(), null, null)
                    );
                }
            } else {
                newExprs.add((E) oldExpr);
            }
        }
        return Pair.of(changed, (C) newExprs.build());
    }

    private Expression replaceSlot(Expression e, boolean fillAccessPath) {
        return MoreFieldsThread.keepFunctionSignature(false,
                () -> doRewriteExpression(e, fillAccessPath)
        );
    }

    private Expression doRewriteExpression(Expression e, boolean fillAccessPath) {
        if (e instanceof Lambda) {
            return rewriteLambda((Lambda) e, fillAccessPath);
        } else if (e instanceof Cast) {
            return rewriteCast((Cast) e, fillAccessPath);
        } else if (e instanceof SlotReference) {
            AccessPathInfo accessPathInfo = replacedDataTypes.get(((SlotReference) e).getExprId().asInt());
            if (accessPathInfo != null) {
                SlotReference newSlot = (SlotReference) ((SlotReference) e).withNullableAndDataType(
                        e.nullable(), accessPathInfo.getPrunedType());
                if (fillAccessPath) {
                    newSlot = newSlot.withAccessPaths(
                            accessPathInfo.getAllAccessPaths(), accessPathInfo.getPredicateAccessPaths()
                    );
                }
                return newSlot;
            }
        }

        ImmutableList.Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(e.arity());
        boolean changed = false;
        for (Expression child : e.children()) {
            Expression newChild = doRewriteExpression(child, fillAccessPath);
            changed |= child != newChild;
            newChildren.add(newChild);
        }
        return changed ? e.withChildren(newChildren.build()) : e;
    }

    private Expression rewriteLambda(Lambda e, boolean fillAccessPath) {
        // we should rewrite ArrayItemReference first, then we can replace the ArrayItemSlot int the lambda
        Expression[] newChildren = new Expression[e.arity()];
        for (int i = 0; i < e.arity(); i++) {
            Expression child = e.child(i);
            if (child instanceof ArrayItemReference) {
                Expression newRef = child.withChildren(doRewriteExpression(child.child(0), fillAccessPath));
                replacedDataTypes.put(((ArrayItemReference) child).getExprId().asInt(),
                        new AccessPathInfo(newRef.getDataType(), null, null));
                newChildren[i] = newRef;
            } else {
                newChildren[i] = child;
            }
        }

        for (int i = 0; i < newChildren.length; i++) {
            Expression child = newChildren[i];
            if (!(child instanceof ArrayItemReference)) {
                newChildren[i] = doRewriteExpression(child, fillAccessPath);
            }
        }

        return e.withChildren(newChildren);
    }

    private Expression rewriteCast(Cast cast, boolean fillAccessPath) {
        Expression newChild = doRewriteExpression(cast.child(0), fillAccessPath);
        if (newChild == cast.child(0)) {
            return cast;
        }

        DataType newType = cast.getDataType();
        if (cast.getDataType() instanceof NestedColumnPrunable
                && newChild.getDataType() instanceof NestedColumnPrunable) {
            DataTypeAccessTree originTree = DataTypeAccessTree.of(cast.child().getDataType(), TAccessPathType.DATA);
            DataTypeAccessTree prunedTree = DataTypeAccessTree.of(newChild.getDataType(), TAccessPathType.DATA);
            DataTypeAccessTree castTree = DataTypeAccessTree.of(cast.getDataType(), TAccessPathType.DATA);
            newType = prunedTree.pruneCastType(originTree, castTree);
        }

        return new Cast(newChild, newType);
    }

    private List<TColumnAccessPath> replaceIcebergAccessPathToId(
            List<TColumnAccessPath> originAccessPaths, SlotReference slotReference) {
        Column column = slotReference.getOriginalColumn().get();
        List<TColumnAccessPath> replacedAccessPaths = new ArrayList<>();
        for (TColumnAccessPath accessPath : originAccessPaths) {
            List<String> icebergColumnAccessPath = new ArrayList<>();
            if (accessPath.type == TAccessPathType.DATA) {
                icebergColumnAccessPath.addAll(accessPath.data_access_path.path);
                replaceIcebergAccessPathToId(
                        icebergColumnAccessPath, 0, slotReference.getDataType(), column
                );
                TColumnAccessPath newAccessPath = new TColumnAccessPath(TAccessPathType.DATA);
                newAccessPath.data_access_path = new TDataAccessPath(icebergColumnAccessPath);
                replacedAccessPaths.add(newAccessPath);
            } else {
                icebergColumnAccessPath.addAll(accessPath.meta_access_path.path);
                replaceIcebergAccessPathToId(
                        icebergColumnAccessPath, 0, slotReference.getDataType(), column
                );
                TColumnAccessPath newAccessPath = new TColumnAccessPath(TAccessPathType.META);
                newAccessPath.meta_access_path = new TMetaAccessPath(icebergColumnAccessPath);
                replacedAccessPaths.add(newAccessPath);
            }
        }
        return replacedAccessPaths;
    }

    private void replaceIcebergAccessPathToId(List<String> originPath, int index, DataType type, Column column) {
        if (index >= originPath.size()) {
            return;
        }
        if (index == 0) {
            originPath.set(index, String.valueOf(column.getUniqueId()));
            replaceIcebergAccessPathToId(originPath, index + 1, type, column);
        } else {
            String fieldName = originPath.get(index);
            if (type instanceof ArrayType) {
                // skip replace *
                replaceIcebergAccessPathToId(
                        originPath, index + 1, ((ArrayType) type).getItemType(), column.getChildren().get(0)
                );
            } else if (type instanceof MapType) {
                if (fieldName.equals(AccessPathInfo.ACCESS_ALL) || fieldName.equals(AccessPathInfo.ACCESS_MAP_VALUES)) {
                    replaceIcebergAccessPathToId(
                            originPath, index + 1, ((MapType) type).getValueType(), column.getChildren().get(1)
                    );
                }
            } else if (type instanceof StructType) {
                for (Column child : column.getChildren()) {
                    if (child.getName().equals(fieldName)) {
                        originPath.set(index, String.valueOf(child.getUniqueId()));
                        DataType childType = ((StructType) type).getNameToFields().get(fieldName).getDataType();
                        replaceIcebergAccessPathToId(originPath, index + 1, childType, child);
                        break;
                    }
                }
            } else {
                originPath.set(index, String.valueOf(column.getUniqueId()));
            }
        }
    }

    private void tryRecordReplaceSlots(Plan plan, Object checkObj, Set<Integer> shouldReplaceSlots) {
        if (checkObj instanceof SupportPruneNestedColumn
                && ((SupportPruneNestedColumn) checkObj).supportPruneNestedColumn()) {
            List<Slot> output = plan.getOutput();
            boolean shouldPrune = false;
            for (Slot slot : output) {
                int slotId = slot.getExprId().asInt();
                if (slot.getDataType() instanceof NestedColumnPrunable && replacedDataTypes.containsKey(slotId)) {
                    shouldReplaceSlots.add(slotId);
                    shouldPrune = true;
                }
            }
            if (shouldPrune) {
                shouldPrunePlans.put(plan, null);
            }
        }
    }
}
