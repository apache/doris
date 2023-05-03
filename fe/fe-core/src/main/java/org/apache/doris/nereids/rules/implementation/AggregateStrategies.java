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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** AggregateStrategies */
@DependsRules({
    NormalizeAggregate.class,
    FoldConstantRuleOnFE.class
})
public class AggregateStrategies implements ImplementationRuleFactory {

    @Override
    public List<Rule> buildRules() {
        PatternDescriptor<LogicalAggregate<GroupPlan>> basePattern = logicalAggregate();

        return ImmutableList.of(
            RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT.build(
                logicalAggregate(
                    logicalOlapScan()
                )
                .when(agg -> agg.isNormalized() && enablePushDownNoGroupAgg())
                .thenApply(ctx -> storageLayerAggregate(ctx.root, null, ctx.root.child(), ctx.cascadesContext))
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT.build(
                logicalAggregate(
                    logicalProject(
                        logicalOlapScan()
                    )
                )
                .when(agg -> agg.isNormalized() && enablePushDownNoGroupAgg())
                .thenApply(ctx -> {
                    LogicalAggregate<LogicalProject<LogicalOlapScan>> agg = ctx.root;
                    LogicalProject<LogicalOlapScan> project = agg.child();
                    LogicalOlapScan olapScan = project.child();
                    return storageLayerAggregate(agg, project, olapScan, ctx.cascadesContext);
                })
            ),
            RuleType.ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 0)
                    .thenApplyMulti(ctx -> onePhaseAggregateWithoutDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 0)
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithoutDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI.build(
                basePattern
                    .when(this::containsCountDistinctMultiExpr)
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithCountDistinctMulti(ctx.root, ctx.cascadesContext))
            ),
            RuleType.THREE_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI.build(
                basePattern
                    .when(this::containsCountDistinctMultiExpr)
                    .thenApplyMulti(ctx -> threePhaseAggregateWithCountDistinctMulti(ctx.root, ctx.cascadesContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1 && enableSingleDistinctColumnOpt())
                    .thenApplyMulti(ctx -> onePhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1 && enableSingleDistinctColumnOpt())
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.THREE_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .thenApplyMulti(ctx -> threePhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() > 1 && !containsCountDistinctMultiExpr(agg))
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.FOUR_PHASE_AGGREGATE_WITH_DISTINCT.build(
                    basePattern
                            .when(agg -> agg.getDistinctArguments().size() == 1)
                            .when(agg -> agg.getGroupByExpressions().isEmpty())
                            .thenApplyMulti(ctx -> fourPhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            )
        );
    }

    /**
     * sql: select count(*) from tbl
     *
     * before:
     *
     *               LogicalAggregate(groupBy=[], output=[count(*)])
     *                                |
     *                       LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *               LogicalAggregate(groupBy=[], output=[count(*)])
     *                                |
     *        PhysicalStorageLayerAggregate(pushAggOp=COUNT, table=PhysicalOlapScan(table=tbl))
     *
     */
    private LogicalAggregate<? extends Plan> storageLayerAggregate(
            LogicalAggregate<? extends Plan> aggregate,
            @Nullable LogicalProject<? extends Plan> project,
            LogicalOlapScan olapScan, CascadesContext cascadesContext) {
        final LogicalAggregate<? extends Plan> canNotPush = aggregate;

        KeysType keysType = olapScan.getTable().getKeysType();
        if (keysType != KeysType.AGG_KEYS && keysType != KeysType.DUP_KEYS) {
            return canNotPush;
        }

        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        if (!groupByExpressions.isEmpty() || !aggregate.getDistinctArguments().isEmpty()) {
            return canNotPush;
        }

        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        Set<Class<? extends AggregateFunction>> functionClasses = aggregateFunctions
                .stream()
                .map(AggregateFunction::getClass)
                .collect(Collectors.toSet());

        Map<Class, PushDownAggOp> supportedAgg = PushDownAggOp.supportedFunctions();
        if (!supportedAgg.keySet().containsAll(functionClasses)) {
            return canNotPush;
        }
        if (functionClasses.contains(Count.class) && keysType != KeysType.DUP_KEYS) {
            return canNotPush;
        }
        if (aggregateFunctions.stream().anyMatch(fun -> fun.arity() > 1)) {
            return canNotPush;
        }

        // we already normalize the arguments to slotReference
        List<Expression> argumentsOfAggregateFunction = aggregateFunctions.stream()
                .flatMap(aggregateFunction -> aggregateFunction.getArguments().stream())
                .collect(ImmutableList.toImmutableList());

        if (project != null) {
            argumentsOfAggregateFunction = Project.findProject(
                        (List<SlotReference>) (List) argumentsOfAggregateFunction, project.getProjects())
                    .stream()
                    .map(p -> p instanceof Alias ? p.child(0) : p)
                    .collect(ImmutableList.toImmutableList());
        }

        boolean onlyContainsSlotOrNumericCastSlot = argumentsOfAggregateFunction
                .stream()
                .allMatch(argument -> {
                    if (argument instanceof SlotReference) {
                        return true;
                    }
                    if (argument instanceof Cast) {
                        return argument.child(0) instanceof SlotReference
                                && argument.getDataType().isNumericType()
                                && argument.child(0).getDataType().isNumericType();
                    }
                    return false;
                });
        if (!onlyContainsSlotOrNumericCastSlot) {
            return canNotPush;
        }

        Set<PushDownAggOp> pushDownAggOps = functionClasses.stream()
                .map(supportedAgg::get)
                .collect(Collectors.toSet());

        PushDownAggOp mergeOp = pushDownAggOps.size() == 1
                ? pushDownAggOps.iterator().next()
                : PushDownAggOp.MIX;

        Set<SlotReference> aggUsedSlots =
                ExpressionUtils.collect(argumentsOfAggregateFunction, SlotReference.class::isInstance);

        List<SlotReference> usedSlotInTable = (List<SlotReference>) (List) Project.findProject(aggUsedSlots,
                (List<NamedExpression>) (List) olapScan.getOutput());

        for (SlotReference slot : usedSlotInTable) {
            Column column = slot.getColumn().get();
            if (keysType == KeysType.AGG_KEYS && !column.isKey()) {
                return canNotPush;
            }
            // The zone map max length of CharFamily is 512, do not
            // over the length: https://github.com/apache/doris/pull/6293
            if (mergeOp == PushDownAggOp.MIN_MAX || mergeOp == PushDownAggOp.MIX) {
                PrimitiveType colType = column.getType().getPrimitiveType();
                if (colType.isComplexType() || colType.isHllType() || colType.isBitmapType()
                         || colType == PrimitiveType.STRING) {
                    return canNotPush;
                }
                if (colType.isCharFamily() && mergeOp != PushDownAggOp.COUNT && column.getType().getLength() > 512) {
                    return canNotPush;
                }
            }
            if (mergeOp == PushDownAggOp.COUNT || mergeOp == PushDownAggOp.MIX) {
                // NULL value behavior in `count` function is zero, so
                // we should not use row_count to speed up query. the col
                // must be not null
                if (column.isAllowNull()) {
                    return canNotPush;
                }
            }
        }

        PhysicalOlapScan physicalOlapScan = (PhysicalOlapScan) new LogicalOlapScanToPhysicalOlapScan()
                .build()
                .transform(olapScan, cascadesContext)
                .get(0);
        if (project != null) {
            return aggregate.withChildren(ImmutableList.of(
                    project.withChildren(
                            ImmutableList.of(new PhysicalStorageLayerAggregate(physicalOlapScan, mergeOp)))
            ));
        } else {
            return aggregate.withChildren(ImmutableList.of(
                    new PhysicalStorageLayerAggregate(physicalOlapScan, mergeOp)
            ));
        }
    }

    /**
     * sql: select count(*) from tbl group by id
     *
     * before:
     *
     *          LogicalAggregate(groupBy=[id], output=[count(*)])
     *                       |
     *               LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *  single node aggregate:
     *
     *             PhysicalHashAggregate(groupBy=[id], output=[count(*)])
     *                              |
     *                 PhysicalDistribute(distributionSpec=GATHER)
     *                             |
     *                     LogicalOlapScan(table=tbl)
     *
     *  distribute node aggregate:
     *
     *            PhysicalHashAggregate(groupBy=[id], output=[count(*)])
     *                                    |
     *           LogicalOlapScan(table=tbl, **already distribute by id**)
     *
     */
    private List<PhysicalHashAggregate<Plan>> onePhaseAggregateWithoutDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        AggregateParam inputToResultParam = AggregateParam.localResult();
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        return new AggregateExpression((AggregateFunction) outputChild, inputToResultParam);
                    }
                    return outputChild;
                });
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), newOutput, Optional.empty(),
                inputToResultParam, false,
                logicalAgg.getLogicalProperties(),
                requireGather, logicalAgg.child());

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of(gatherLocalAgg);
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<Plan> hashLocalAgg = gatherLocalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    .add(gatherLocalAgg)
                    .add(hashLocalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id, name) from tbl group by name
     *
     * before:
     *
     *          LogicalAggregate(groupBy=[name], output=[count(distinct id, name)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *  single node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id])
     *                                |
     *           PhysicalDistribute(distributionSpec=GATHER)
     *                               |
     *                     LogicalOlapScan(table=tbl)
     *
     *  distribute node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id])
     *                                |
     *           PhysicalDistribute(distributionSpec=HASH(name))
     *                               |
     *        LogicalOlapScan(table=tbl, **already distribute by name**)
     *
     */
    private List<PhysicalHashAggregate<Plan>> twoPhaseAggregateWithCountDistinctMulti(
            LogicalAggregate<? extends Plan> logicalAgg, CascadesContext cascadesContext) {
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Collection<Expression> countDistinctArguments = logicalAgg.getDistinctArguments();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(ImmutableSet.<Expression>builder()
                .addAll(logicalAgg.getGroupByExpressions())
                .addAll(countDistinctArguments)
                .build());

        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        List<NamedExpression> localOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) localAggGroupBy.stream()
                        .filter(g -> !(g instanceof Literal))
                        .collect(ImmutableList.toImmutableList()))
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localOutput, Optional.of(partitionExpressions),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                maybeUsingStreamAgg(cascadesContext.getConnectContext(), logicalAgg),
                logicalAgg.getLogicalProperties(), requireGather, logicalAgg.child()
        );

        List<Expression> distinctGroupBy = logicalAgg.getGroupByExpressions();

        LogicalAggregate<? extends Plan> countIfAgg = countDistinctMultiExprToCountIf(
                logicalAgg, cascadesContext).first;

        AggregateParam distinctInputToResultParam
                = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_RESULT);
        AggregateParam globalBufferToResultParam
                = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                countIfAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                        Alias alias = nonDistinctAggFunctionToAliasPhase1.get(aggregateFunction);
                        if (alias == null) {
                            return new AggregateExpression(aggregateFunction, distinctInputToResultParam);
                        } else {
                            return new AggregateExpression(aggregateFunction,
                                    globalBufferToResultParam, alias.toSlot());
                        }
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<Plan> gatherLocalGatherDistinctAgg = new PhysicalHashAggregate<>(
                distinctGroupBy, distinctOutput, Optional.of(partitionExpressions),
                distinctInputToResultParam, false,
                logicalAgg.getLogicalProperties(), requireGather, gatherLocalAgg
        );

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of(gatherLocalGatherDistinctAgg);
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<Plan> hashLocalHashGlobalAgg = gatherLocalGatherDistinctAgg
                    .withRequireTree(requireHash.withChildren(requireHash))
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    .add(gatherLocalGatherDistinctAgg)
                    .add(hashLocalHashGlobalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id, name) from tbl group by name
     *
     * before:
     *
     *          LogicalAggregate(groupBy=[name], output=[count(distinct id, name)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *  single node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                   |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                   |
     *                PhysicalDistribute(distributionSpec=GATHER)
     *                                   |
     *       PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                   |
     *                        LogicalOlapScan(table=tbl)
     *
     *  distribute node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                   |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                   |
     *                PhysicalDistribute(distributionSpec=HASH(name))
     *                                   |
     *       PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                   |
     *                        LogicalOlapScan(table=tbl)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> threePhaseAggregateWithCountDistinctMulti(
            LogicalAggregate<? extends Plan> logicalAgg, CascadesContext cascadesContext) {
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

        Collection<Expression> countDistinctArguments = logicalAgg.getDistinctArguments();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(ImmutableSet.<Expression>builder()
                .addAll(logicalAgg.getGroupByExpressions())
                .addAll(countDistinctArguments)
                .build());

        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        List<NamedExpression> localOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) localAggGroupBy.stream()
                        .filter(g -> !(g instanceof Literal))
                        .collect(ImmutableList.toImmutableList()))
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();
        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localOutput, Optional.of(partitionExpressions),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                maybeUsingStreamAgg(cascadesContext.getConnectContext(), logicalAgg),
                logicalAgg.getLogicalProperties(), requireAny, logicalAgg.child()
        );

        List<Expression> globalAggGroupBy = localAggGroupBy;

        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase2 =
                nonDistinctAggFunctionToAliasPhase1.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(kv -> kv.getKey(), kv -> {
                            AggregateFunction originFunction = kv.getKey();
                            Alias localOutputAlias = kv.getValue();
                            AggregateExpression globalAggExpr = new AggregateExpression(
                                    originFunction, bufferToBufferParam, localOutputAlias.toSlot());
                            return new Alias(globalAggExpr, globalAggExpr.toSql());
                        }));

        Set<SlotReference> slotInCountDistinct = ExpressionUtils.collect(
                ImmutableList.copyOf(countDistinctArguments), SlotReference.class::isInstance);
        List<NamedExpression> globalAggOutput = ImmutableList.copyOf(ImmutableSet.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(slotInCountDistinct)
                .addAll(nonDistinctAggFunctionToAliasPhase2.values())
                .build());

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                globalAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                requireGather, anyLocalAgg);

        LogicalAggregate<? extends Plan> countIfAgg = countDistinctMultiExprToCountIf(
                logicalAgg, cascadesContext).first;

        AggregateParam distinctInputToResultParam
                = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_RESULT);
        AggregateParam globalBufferToResultParam
                = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                countIfAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                        Alias alias = nonDistinctAggFunctionToAliasPhase2.get(aggregateFunction);
                        if (alias == null) {
                            return new AggregateExpression(aggregateFunction, distinctInputToResultParam);
                        } else {
                            return new AggregateExpression(aggregateFunction,
                                    globalBufferToResultParam, alias.toSlot());
                        }
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<Plan> anyLocalGatherGlobalGatherAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), distinctOutput, Optional.empty(),
                distinctInputToResultParam, false,
                logicalAgg.getLogicalProperties(), requireGather, anyLocalGatherGlobalAgg
        );

        RequireProperties requireDistinctHash = RequireProperties.of(
                PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
        PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalGatherDistinctAgg
                = anyLocalGatherGlobalGatherAgg.withChildren(ImmutableList.of(
                        anyLocalGatherGlobalAgg
                                .withRequire(requireDistinctHash)
                                .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
                ));

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalGatherAgg)
                    .add(anyLocalHashGlobalGatherDistinctAgg)
                    .build();
        } else {
            RequireProperties requireGroupByHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<PhysicalHashAggregate<Plan>> anyLocalHashGlobalHashDistinctAgg
                    = anyLocalGatherGlobalGatherAgg.withRequirePropertiesAndChild(requireGroupByHash,
                            anyLocalGatherGlobalAgg
                                    .withRequire(requireGroupByHash)
                                    .withPartitionExpressions(logicalAgg.getGroupByExpressions())
                    )
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalGatherAgg)
                    .add(anyLocalHashGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalHashDistinctAgg)
                    .build();
        }
    }

    /**
     * sql: select name, count(value) from tbl group by name
     *
     * before:
     *
     *          LogicalAggregate(groupBy=[name], output=[name, count(value)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *  single node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=BUFFER_TO_RESULT)
     *                                |
     *               PhysicalDistribute(distributionSpec=GATHER)
     *                                |
     *          PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=INPUT_TO_BUFFER)
     *                                |
     *                     LogicalOlapScan(table=tbl)
     *
     *  distribute node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=BUFFER_TO_RESULT)
     *                                |
     *               PhysicalDistribute(distributionSpec=HASH(name))
     *                                |
     *          PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=INPUT_TO_BUFFER)
     *                                |
     *                     LogicalOlapScan(table=tbl)
     *
     */
    private List<PhysicalHashAggregate<Plan>> twoPhaseAggregateWithoutDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> inputToBufferAliases = logicalAgg.getAggregateFunctions()
                .stream()
                .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                    AggregateExpression inputToBuffer = new AggregateExpression(function, inputToBufferParam);
                    return new Alias(inputToBuffer, inputToBuffer.toSql());
                }));

        List<Expression> localAggGroupBy = logicalAgg.getGroupByExpressions();
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                // we already normalized the group by expressions to List<Slot> by the NormalizeAggregate rule
                .addAll((List) localAggGroupBy)
                .addAll(inputToBufferAliases.values())
                .build();

        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<? extends Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localAggOutput, Optional.of(partitionExpressions),
                inputToBufferParam, maybeUsingStreamAgg(connectContext, logicalAgg),
                logicalAgg.getLogicalProperties(), requireAny,
                logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalAggOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    Alias inputToBufferAlias = inputToBufferAliases.get(outputChild);
                    if (inputToBufferAlias == null) {
                        return outputChild;
                    }
                    AggregateFunction function = (AggregateFunction) outputChild;
                    return new AggregateExpression(function, bufferToResultParam, inputToBufferAlias.toSlot());
                });

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate(
                localAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToResultParam, false, anyLocalAgg.getLogicalProperties(),
                requireGather, anyLocalAgg);

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of(anyLocalGatherGlobalAgg);
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));

            PhysicalHashAggregate<Plan> anyLocalHashGlobalAgg = anyLocalGatherGlobalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    .add(anyLocalGatherGlobalAgg)
                    .add(anyLocalHashGlobalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from tbl group by name
     *
     * before:
     *
     *               LogicalAggregate(groupBy=[name], output=[name, count(distinct id)])
     *                                         |
     *                              LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *  single node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id)], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                     PhysicalDistribute(distributionSpec=GATHER)
     *                                          |
     *                               LogicalOlapScan(table=tbl)
     *
     * distribute node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id)], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                 PhysicalDistribute(distributionSpec=HASH(name))
     *                                          |
     *                LogicalOlapScan(table=tbl, **if distribute by name**)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> twoPhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Set<Expression> distinctArguments = aggregateFunctions.stream()
                .filter(aggregateExpression -> aggregateExpression.isDistinct())
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBy = ImmutableSet.<NamedExpression>builder()
                .addAll((List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBy)
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);

        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(ImmutableList.copyOf(localAggGroupBy),
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                /*
                 * should not use streaming, there has some bug in be will compute wrong result,
                 * see aggregate_strategies.groovy
                 */
                false, Optional.empty(), logicalAgg.getLogicalProperties(),
                requireGather, logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                        if (aggregateFunction.isDistinct()) {
                            Preconditions.checkArgument(aggregateFunction.arity() == 1);
                            AggregateFunction nonDistinct = aggregateFunction
                                    .withDistinctAndChildren(false, aggregateFunction.getArguments());
                            return new AggregateExpression(nonDistinct, AggregateParam.localResult());
                        } else {
                            Alias alias = nonDistinctAggFunctionToAliasPhase1.get(outputChild);
                            return new AggregateExpression(
                                    aggregateFunction, bufferToResultParam, alias.toSlot());
                        }
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<Plan> gatherLocalGatherGlobalAgg
                = new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), globalOutput,
                Optional.empty(), bufferToResultParam, false,
                logicalAgg.getLogicalProperties(), requireGather, gatherLocalAgg);

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            RequireProperties requireDistinctHash = RequireProperties.of(PhysicalProperties.createHash(
                    distinctArguments, ShuffleType.AGGREGATE));
            PhysicalHashAggregate<? extends Plan> hashLocalGatherGlobalAgg = gatherLocalGatherGlobalAgg
                    .withChildren(ImmutableList.of(gatherLocalAgg
                            .withRequire(requireDistinctHash)
                            .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
                    ));
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(gatherLocalGatherGlobalAgg)
                    .add(hashLocalGatherGlobalAgg)
                    .build();
        } else {
            RequireProperties requireGroupByHash = RequireProperties.of(PhysicalProperties.createHash(
                    logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<PhysicalHashAggregate<Plan>> hashLocalHashGlobalAgg = gatherLocalGatherGlobalAgg
                    .withRequirePropertiesAndChild(requireGroupByHash, gatherLocalAgg
                            .withRequire(requireGroupByHash)
                            .withPartitionExpressions(logicalAgg.getGroupByExpressions())
                    )
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(gatherLocalGatherGlobalAgg)
                    .add(hashLocalHashGlobalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from tbl group by name
     *
     * before:
     *
     *               LogicalAggregate(groupBy=[name], output=[name, count(distinct id)])
     *                                         |
     *                              LogicalOlapScan(table=tbl)
     *
     * after:
     *  single node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id)], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                          |
     *                     PhysicalDistribute(distributionSpec=GATHER)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                               LogicalOlapScan(table=tbl)
     *
     *  distribute node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id)], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                          |
     *                     PhysicalDistribute(distributionSpec=HASH(name))
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                               LogicalOlapScan(table=tbl)
     *
     */
    // TODO: support one phase aggregate(group by columns + distinct columns) + two phase distinct aggregate
    private List<PhysicalHashAggregate<? extends Plan>> threePhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Set<Expression> distinctArguments = aggregateFunctions.stream()
                .filter(aggregateExpression -> aggregateExpression.isDistinct())
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBySet = ImmutableSet.<NamedExpression>builder()
                .addAll((List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(localAggGroupBySet);
        boolean maybeUsingStreamAgg = maybeUsingStreamAgg(connectContext, localAggGroupBy);
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(localAggGroupBy,
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                maybeUsingStreamAgg, Optional.empty(), logicalAgg.getLogicalProperties(),
                requireAny, logicalAgg.child());

        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase2 =
                nonDistinctAggFunctionToAliasPhase1.entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(kv -> kv.getKey(), kv -> {
                        AggregateFunction originFunction = kv.getKey();
                        Alias localOutput = kv.getValue();
                        AggregateExpression globalAggExpr = new AggregateExpression(
                                originFunction, bufferToBufferParam, localOutput.toSlot());
                        return new Alias(globalAggExpr, globalAggExpr.toSql());
                    }));

        List<NamedExpression> globalAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase2.values())
                .build();

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                requireGather, anyLocalAgg);

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_RESULT);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) expr;
                        if (aggregateFunction.isDistinct()) {
                            Preconditions.checkArgument(aggregateFunction.arity() == 1);
                            AggregateFunction nonDistinct = aggregateFunction
                                    .withDistinctAndChildren(false, aggregateFunction.getArguments());
                            return new AggregateExpression(nonDistinct,
                                    bufferToResultParam, aggregateFunction.child(0));
                        } else {
                            Alias alias = nonDistinctAggFunctionToAliasPhase2.get(expr);
                            return new AggregateExpression(aggregateFunction,
                                    new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_RESULT),
                                    alias.toSlot());
                        }
                    }
                    return expr;
                });

        PhysicalHashAggregate<Plan> anyLocalGatherGlobalGatherDistinctAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), distinctOutput, Optional.empty(),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                requireGather, anyLocalGatherGlobalAgg);

        RequireProperties requireDistinctHash = RequireProperties.of(
                PhysicalProperties.createHash(logicalAgg.getDistinctArguments(), ShuffleType.AGGREGATE));
        PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalGatherDistinctAgg
                = anyLocalGatherGlobalGatherDistinctAgg
                    .withChildren(ImmutableList.of(anyLocalGatherGlobalAgg
                            .withRequire(requireDistinctHash)
                            .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
                    ));

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // TODO: this plan pattern is not good usually, we remove it temporary.
                    // .add(anyLocalGatherGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalGatherDistinctAgg)
                    .build();
        } else {
            RequireProperties requireGroupByHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalHashDistinctAgg
                    = anyLocalGatherGlobalGatherDistinctAgg
                    .withRequirePropertiesAndChild(requireGroupByHash, anyLocalGatherGlobalAgg
                            .withRequire(requireGroupByHash)
                            .withPartitionExpressions(logicalAgg.getGroupByExpressions())
                    )
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalHashDistinctAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from (...) group by name
     *
     * before:
     *
     *          LogicalAggregate(groupBy=[name], output=[count(distinct id)])
     *                       |
     *                    any plan
     *
     * after:
     *
     *  single node aggregate:
     *
     *             PhysicalHashAggregate(groupBy=[name], output=[multi_distinct_count(id)])
     *                                    |
     *                 PhysicalDistribute(distributionSpec=GATHER)
     *                                    |
     *                                any plan
     *
     *  distribute node aggregate:
     *
     *            PhysicalHashAggregate(groupBy=[name], output=[multi_distinct_count(id)])
     *                                    |
     *                     any plan(**already distribute by name**)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> onePhaseAggregateWithMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        AggregateParam inputToResultParam = AggregateParam.localResult();
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction function = tryConvertToMultiDistinct((AggregateFunction) outputChild);
                        return new AggregateExpression(function, inputToResultParam);
                    }
                    return outputChild;
                });

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<? extends Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), newOutput, inputToResultParam,
                maybeUsingStreamAgg(connectContext, logicalAgg),
                logicalAgg.getLogicalProperties(), requireGather, logicalAgg.child());
        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of(gatherLocalAgg);
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<? extends Plan> hashLocalAgg = gatherLocalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(gatherLocalAgg)
                    .add(hashLocalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from tbl group by name
     *
     * before:
     *
     *          LogicalAggregate(groupBy=[name], output=[name, count(distinct id)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     *
     * after:
     *
     *  single node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=BUFFER_TO_RESULT)
     *                                                 |
     *                                PhysicalDistribute(distributionSpec=GATHER)
     *                                                 |
     *     PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=INPUT_TO_BUFFER)
     *                                                 |
     *                                       LogicalOlapScan(table=tbl)
     *
     *  distribute node aggregate:
     *
     *     PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=BUFFER_TO_RESULT)
     *                                                |
     *                               PhysicalDistribute(distributionSpec=HASH(name))
     *                                                |
     *      PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=INPUT_TO_BUFFER)
     *                                                |
     *                                      LogicalOlapScan(table=tbl)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> twoPhaseAggregateWithMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> aggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                    AggregateFunction multiDistinct = tryConvertToMultiDistinct(function);
                    AggregateExpression localAggExpr = new AggregateExpression(multiDistinct, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                // already normalize group by expression to List<SlotReference>
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(aggFunctionToAliasPhase1.values())
                .build();

        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<? extends Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), localAggOutput,
                inputToBufferParam, maybeUsingStreamAgg(connectContext, logicalAgg),
                logicalAgg.getLogicalProperties(), requireAny, logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        Alias alias = aggFunctionToAliasPhase1.get(outputChild);
                        AggregateExpression localAggExpr = (AggregateExpression) alias.child();
                        return new AggregateExpression(localAggExpr.getFunction(),
                                bufferToResultParam, alias.toSlot());
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<? extends Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), globalOutput, Optional.empty(),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                RequireProperties.of(PhysicalProperties.GATHER), anyLocalAgg);

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            Collection<Expression> distinctArguments = logicalAgg.getDistinctArguments();
            RequireProperties requireDistinctHash = RequireProperties.of(PhysicalProperties.createHash(
                    distinctArguments, ShuffleType.AGGREGATE));
            PhysicalHashAggregate<? extends Plan> hashLocalGatherGlobalAgg = anyLocalGatherGlobalAgg
                    .withChildren(ImmutableList.of(anyLocalAgg
                            .withRequire(requireDistinctHash)
                            .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
                    ));
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalAgg)
                    .add(hashLocalGatherGlobalAgg)
                    .build();
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalAgg = anyLocalGatherGlobalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalAgg)
                    .add(anyLocalHashGlobalAgg)
                    .build();
        }
    }

    private boolean maybeUsingStreamAgg(
            ConnectContext connectContext, LogicalAggregate<? extends Plan> logicalAggregate) {
        return !connectContext.getSessionVariable().disableStreamPreaggregations
                && !logicalAggregate.getGroupByExpressions().isEmpty();
    }

    private boolean maybeUsingStreamAgg(
            ConnectContext connectContext, List<? extends Expression> groupByExpressions) {
        return !connectContext.getSessionVariable().disableStreamPreaggregations
                && !groupByExpressions.isEmpty();
    }

    private List<Expression> getHashAggregatePartitionExpressions(
            LogicalAggregate<? extends Plan> logicalAggregate) {
        return logicalAggregate.getGroupByExpressions().isEmpty()
                ? ImmutableList.copyOf(logicalAggregate.getDistinctArguments())
                : logicalAggregate.getGroupByExpressions();
    }

    private AggregateFunction tryConvertToMultiDistinct(AggregateFunction function) {
        if (function instanceof Count && function.isDistinct()) {
            return new MultiDistinctCount(function.getArgument(0),
                    function.getArguments().subList(1, function.arity()).toArray(new Expression[0]));
        } else if (function instanceof Sum && function.isDistinct()) {
            return new MultiDistinctSum(function.getArgument(0));
        } else if (function instanceof GroupConcat && function.isDistinct()) {
            return ((GroupConcat) function).convertToMultiDistinct();
        }
        return function;
    }

    /**
     * countDistinctMultiExprToCountIf.
     *
     * NOTE: this function will break the normalized output, e.g. from `count(distinct slot1, slot2)` to
     *       `count(if(slot1 is null, null, slot2))`. So if you invoke this method, and separate the
     *       phase of aggregate, please normalize to slot and create a bottom project like NormalizeAggregate.
     */
    private Pair<LogicalAggregate<? extends Plan>, List<Count>> countDistinctMultiExprToCountIf(
                LogicalAggregate<? extends Plan> aggregate, CascadesContext cascadesContext) {
        ImmutableList.Builder<Count> countIfList = ImmutableList.builder();
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof Count) {
                        Count count = (Count) outputChild;
                        if (count.isDistinct() && count.arity() > 1) {
                            Set<Expression> arguments = ImmutableSet.copyOf(count.getArguments());
                            Expression countExpr = count.getArgument(arguments.size() - 1);
                            for (int i = arguments.size() - 2; i >= 0; --i) {
                                Expression argument = count.getArgument(i);
                                If ifNull = new If(new IsNull(argument), NullLiteral.INSTANCE, countExpr);
                                countExpr = assignNullType(ifNull, cascadesContext);
                            }
                            Count countIf = new Count(countExpr);
                            countIfList.add(countIf);
                            return countIf;
                        }
                    }
                    return outputChild;
                });
        return Pair.of(aggregate.withAggOutput(newOutput), countIfList.build());
    }

    private boolean containsCountDistinctMultiExpr(LogicalAggregate<? extends Plan> aggregate) {
        return ExpressionUtils.anyMatch(aggregate.getOutputExpressions(), expr ->
                expr instanceof Count && ((Count) expr).isDistinct() && expr.arity() > 1);
    }

    // don't invoke the ExpressionNormalization, because the expression maybe simplified and get rid of some slots
    private If assignNullType(If ifExpr, CascadesContext cascadesContext) {
        If ifWithCoercion = (If) TypeCoercionUtils.processBoundFunction(ifExpr);
        Expression trueValue = ifWithCoercion.getArgument(1);
        if (trueValue instanceof Cast && trueValue.child(0) instanceof NullLiteral) {
            List<Expression> newArgs = Lists.newArrayList(ifWithCoercion.getArguments());
            // backend don't support null type, so we should set the type
            newArgs.set(1, new NullLiteral(((Cast) trueValue).getDataType()));
            return ifWithCoercion.withChildren(newArgs);
        }
        return ifWithCoercion;
    }

    private boolean enablePushDownNoGroupAgg() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext == null || connectContext.getSessionVariable().enablePushDownNoGroupAgg();
    }

    private boolean enableSingleDistinctColumnOpt() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext == null || connectContext.getSessionVariable().enableSingleDistinctColumnOpt();
    }

    /**
     * sql:
     * select count(distinct name), sum(age) from student;
     *
     * 4 phase plan
     * DISTINCT_GLOBAL, BUFFER_TO_RESULT groupBy(), output[count(name), sum(age#5)], [GATHER]
     * +--DISTINCT_LOCAL, INPUT_TO_BUFFER, groupBy()), output(count(name), partial_sum(age)), hash distribute by name
     *    +--GLOBAL, BUFFER_TO_BUFFER, groupBy(name), output(name, partial_sum(age)), hash_distribute by name
     *       +--LOCAL, INPUT_TO_BUFFER, groupBy(name), output(name, partial_sum(age))
     *          +--scan(name, age)
     */
    private List<PhysicalHashAggregate<? extends Plan>> fourPhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Set<Expression> distinctArguments = aggregateFunctions.stream()
                .filter(aggregateExpression -> aggregateExpression.isDistinct())
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBySet = ImmutableSet.<NamedExpression>builder()
                .addAll((List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(localAggGroupBySet);
        boolean maybeUsingStreamAgg = maybeUsingStreamAgg(connectContext, localAggGroupBy);
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(localAggGroupBy,
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                maybeUsingStreamAgg, Optional.empty(), logicalAgg.getLogicalProperties(),
                requireAny, logicalAgg.child());

        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase2 =
                nonDistinctAggFunctionToAliasPhase1.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(kv -> kv.getKey(), kv -> {
                            AggregateFunction originFunction = kv.getKey();
                            Alias localOutput = kv.getValue();
                            AggregateExpression globalAggExpr = new AggregateExpression(
                                    originFunction, bufferToBufferParam, localOutput.toSlot());
                            return new Alias(globalAggExpr, globalAggExpr.toSql());
                        }));

        List<NamedExpression> globalAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase2.values())
                .build();

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);

        RequireProperties requireDistinctHash = RequireProperties.of(
                PhysicalProperties.createHash(logicalAgg.getDistinctArguments(), ShuffleType.AGGREGATE));

        //phase 2
        PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, globalAggOutput, Optional.of(ImmutableList.copyOf(logicalAgg.getDistinctArguments())),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                requireDistinctHash, anyLocalAgg);

        // phase 3
        AggregateParam distinctLocalParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase3 = new HashMap<>();
        List<NamedExpression> localDistinctOutput = Lists.newArrayList();
        for (int i = 0; i < logicalAgg.getOutputExpressions().size(); i++) {
            NamedExpression outputExpr = logicalAgg.getOutputExpressions().get(i);
            List<AggregateFunction> needUpdateSlot = Lists.newArrayList();
            NamedExpression outputExprPhase3 = (NamedExpression) outputExpr
                    .rewriteDownShortCircuit(expr -> {
                        if (expr instanceof AggregateFunction) {
                            AggregateFunction aggregateFunction = (AggregateFunction) expr;
                            if (aggregateFunction.isDistinct()) {
                                Preconditions.checkArgument(aggregateFunction.arity() == 1);
                                AggregateFunction nonDistinct = aggregateFunction
                                        .withDistinctAndChildren(false, aggregateFunction.getArguments());
                                AggregateExpression nonDistinctAggExpr = new AggregateExpression(nonDistinct,
                                        distinctLocalParam, aggregateFunction.child(0));
                                return nonDistinctAggExpr;
                            } else {
                                needUpdateSlot.add(aggregateFunction);
                                Alias alias = nonDistinctAggFunctionToAliasPhase2.get(expr);
                                return new AggregateExpression(aggregateFunction,
                                        new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_BUFFER),
                                        alias.toSlot());
                            }
                        }
                        return expr;
                    });
            for (AggregateFunction originFunction : needUpdateSlot) {
                nonDistinctAggFunctionToAliasPhase3.put(originFunction, (Alias) outputExprPhase3);
            }
            localDistinctOutput.add(outputExprPhase3);

        }
        PhysicalHashAggregate<? extends Plan> distinctLocal = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), localDistinctOutput, Optional.empty(),
                distinctLocalParam, false, logicalAgg.getLogicalProperties(),
                requireDistinctHash, anyLocalHashGlobalAgg);

        //phase 4
        AggregateParam distinctGlobalParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalDistinctOutput = Lists.newArrayList();
        for (int i = 0; i < logicalAgg.getOutputExpressions().size(); i++) {
            NamedExpression outputExpr = logicalAgg.getOutputExpressions().get(i);
            NamedExpression outputExprPhase4 = (NamedExpression) outputExpr.rewriteDownShortCircuit(expr -> {
                if (expr instanceof AggregateFunction) {
                    AggregateFunction aggregateFunction = (AggregateFunction) expr;
                    if (aggregateFunction.isDistinct()) {
                        Preconditions.checkArgument(aggregateFunction.arity() == 1);
                        AggregateFunction nonDistinct = aggregateFunction
                                .withDistinctAndChildren(false, aggregateFunction.getArguments());
                        int idx = logicalAgg.getOutputExpressions().indexOf(outputExpr);
                        Alias localDistinctAlias = (Alias) (localDistinctOutput.get(idx));
                        return new AggregateExpression(nonDistinct,
                                distinctGlobalParam, localDistinctAlias.toSlot());
                    } else {
                        Alias alias = nonDistinctAggFunctionToAliasPhase3.get(expr);
                        return new AggregateExpression(aggregateFunction,
                                new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_RESULT),
                                alias.toSlot());
                    }
                }
                return expr;
            });
            globalDistinctOutput.add(outputExprPhase4);
        }
        PhysicalHashAggregate<? extends Plan> distinctGlobal = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), globalDistinctOutput, Optional.empty(),
                distinctGlobalParam, false, logicalAgg.getLogicalProperties(),
                requireGather, distinctLocal);

        return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                .add(distinctGlobal)
                .build();
    }
}
