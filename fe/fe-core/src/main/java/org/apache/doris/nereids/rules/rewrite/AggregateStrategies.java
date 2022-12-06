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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rewrite.rules.CountDistinctMultiExprToSingle;
import org.apache.doris.nereids.rules.implementation.ImplementationRuleFactory;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.WrapAggregateExpressionForAggregateFunction;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
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
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** AggregateStrategies */
@DependsRules({
    NormalizeAggregate.class,
    CountDistinctMultiExprToSingle.class,
    WrapAggregateExpressionForAggregateFunction.class
})
public class AggregateStrategies implements ImplementationRuleFactory {

    @Override
    public List<Rule> buildRules() {
        PatternDescriptor<LogicalAggregate<GroupPlan>> basePattern = logicalAggregate()
                .when(LogicalAggregate::isNormalized);

        return ImmutableList.of(
            RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT.build(
                logicalAggregate(
                    logicalOlapScan().when(LogicalOlapScan::supportStorageLayerAggregate)
                )
                .when(LogicalAggregate::supportStorageLayerAggregate)
                .thenApply(ctx -> storageLayerAggregate(ctx.root, null, ctx.root.child(), ctx.cascadesContext))
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT.build(
                logicalAggregate(
                    logicalProject(
                        logicalOlapScan().when(LogicalOlapScan::supportStorageLayerAggregate)
                    )
                )
                .when(LogicalAggregate::supportStorageLayerAggregate)
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
            RuleType.TWO_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI.build(
                basePattern
                        .when(agg -> agg.getDistinctArguments().size() == 1)
                        .thenApply(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.THREE_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .thenApply(ctx -> threePhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() > 1)
                    .thenApply(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            )
        );
    }

    private LogicalAggregate<? extends Plan> storageLayerAggregate(
            LogicalAggregate<? extends Plan> aggregate,
            @Nullable LogicalProject<? extends Plan> project,
            LogicalOlapScan olapScan, CascadesContext cascadesContext) {
        final LogicalAggregate<? extends Plan> canNotPush = aggregate;

        List<AggregateFunction> aggregateFunctions = ExpressionUtils.collectAll(
                aggregate.getOutputExpressions(), AggregateFunction.class::isInstance);

        Set<String> aggNames = aggregateFunctions.stream()
                .map(aggFun -> aggFun.getName().toLowerCase())
                .collect(Collectors.toSet());

        Map<String, PushDownAggOp> supportedAgg = PushDownAggOp.supportedFunctions();
        if (!supportedAgg.keySet().containsAll(aggNames)) {
            return canNotPush;
        }
        KeysType keysType = olapScan.getTable().getKeysType();
        if (aggNames.contains("count") && keysType != KeysType.DUP_KEYS) {
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

        Set<PushDownAggOp> pushDownAggOps = aggNames.stream()
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
                if (colType.isArrayType() || colType.isComplexType() || colType == PrimitiveType.STRING) {
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

        return aggregate.withChildren(ImmutableList.of(
            new PhysicalStorageLayerAggregate(physicalOlapScan, mergeOp)
        ));
    }

    private List<PhysicalHashAggregate<Plan>> onePhaseAggregateWithoutDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        RequestProperties requestGather = RequestProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), logicalAgg.getOutputExpressions(), Optional.empty(),
                AggregateParam.localGather(), useStreamAgg(connectContext, logicalAgg.getGroupByExpressions()),
                logicalAgg.getLogicalProperties(),
                requestGather, logicalAgg.child());

        if (!logicalAgg.getGroupByExpressions().isEmpty()) {
            RequestProperties requestHash = RequestProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    .add(gatherLocalAgg)
                    .add(gatherLocalAgg.withRequest(requestHash))
                    .build();
        } else {
            return ImmutableList.of(gatherLocalAgg);
        }
    }

    private List<PhysicalHashAggregate<Plan>> twoPhaseAggregateWithoutDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateExpression, Alias> inputToBufferAliases = logicalAgg.getAggregateExpressions()
                .stream()
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression inputToBuffer =
                            new AggregateExpression(expr.getFunction(), inputToBufferParam);
                    return new Alias(inputToBuffer, inputToBuffer.toSql());
                }));

        List<Expression> localAggGroupBy = logicalAgg.getGroupByExpressions();
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                // we already normalized the group by expressions to List<Slot> by the NormalizeAggregate rule
                .addAll((List) localAggGroupBy)
                .addAll(inputToBufferAliases.values())
                .build();
        RequestProperties requestAny = RequestProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<? extends Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localAggOutput, Optional.of(partitionExpressions),
                inputToBufferParam, useStreamAgg(connectContext, localAggGroupBy),
                logicalAgg.getLogicalProperties(), requestAny,
                logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalAggOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    Alias inputToBufferAlias = inputToBufferAliases.get(outputChild);
                    if (inputToBufferAlias == null) {
                        return outputChild;
                    }
                    AggregateExpression inputToResult = (AggregateExpression) outputChild;
                    return new AggregateExpression(inputToResult.getFunction(),
                            bufferToResultParam, inputToBufferAlias.toSlot());
                });

        RequestProperties requestGather = RequestProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate(
                localAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToResultParam, false, anyLocalAgg.getLogicalProperties(),
                requestGather, anyLocalAgg);

        if (!partitionExpressions.isEmpty()) {
            RequestProperties requestHash = RequestProperties.of(
                    PhysicalProperties.createHash(partitionExpressions, ShuffleType.AGGREGATE));

            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    .add(anyLocalGatherGlobalAgg)
                    .add(anyLocalGatherGlobalAgg.withRequest(requestHash))
                    .build();
        } else {
            return ImmutableList.of(anyLocalGatherGlobalAgg);
        }
    }

    private List<PhysicalHashAggregate<Plan>> twoPhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        PhysicalHashAggregate<Plan> distinctAgg
                = (PhysicalHashAggregate) threePhaseAggregateWithDistinct(logicalAgg, connectContext);

        PhysicalHashAggregate<Plan> globalAgg
                = (PhysicalHashAggregate) distinctAgg.child();

        PhysicalHashAggregate<Plan> localAgg
                = (PhysicalHashAggregate) globalAgg.child();

        RequestProperties requestGather = RequestProperties.of(PhysicalProperties.GATHER);

        PhysicalHashAggregate<Plan> mergedLocalGatherAgg =
                localAgg.withAggOutput(globalAgg.getOutputExpressions())
                .withRequest(requestGather);

        PhysicalHashAggregate<Plan> gatherLocalGatherDistinctAgg
                = distinctAgg.withRequestPropertiesAndChild(requestGather, mergedLocalGatherAgg);

        PhysicalProperties hashPartition = PhysicalProperties.createHash(
                localAgg.getPartitionExpressions().get(), ShuffleType.AGGREGATE);
        RequestProperties requestHash = RequestProperties.of(hashPartition);
        return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                .add(gatherLocalGatherDistinctAgg)
                .add(gatherLocalGatherDistinctAgg.withRequestTree(
                    requestHash.withChildren(requestHash)
                ))
                .build();
    }

    private PhysicalHashAggregate<? extends Plan> threePhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateExpression> aggregateExpressions = logicalAgg.getAggregateExpressions();

        Set<Expression> distinctArguments = aggregateExpressions.stream()
                .filter(aggregateExpression -> aggregateExpression.isDistinct())
                .flatMap(aggregateExpression -> aggregateExpression.getFunction().children().stream())
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBy = ImmutableSet.<NamedExpression>builder()
                .addAll((List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

        Map<AggregateExpression, Alias> nonDistinctAggExprToAliasPhase1 = aggregateExpressions.stream()
                .filter(aggregateExpression -> !aggregateExpression.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr.getFunction(), inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBy)
                .addAll(nonDistinctAggExprToAliasPhase1.values())
                .build();

        boolean useStreamAgg = useStreamAgg(connectContext, localAggGroupBy);
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(ImmutableList.copyOf(localAggGroupBy),
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                useStreamAgg, Optional.empty(), logicalAgg.getLogicalProperties(),
                RequestProperties.of(PhysicalProperties.ANY), logicalAgg.child());

        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        Map<AggregateExpression, Alias> nonDistinctAggExprToAliasPhase2 =
                nonDistinctAggExprToAliasPhase1.entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(kv -> kv.getKey(), kv -> {
                        AggregateExpression originExpr = kv.getKey();
                        Alias localOutput = kv.getValue();
                        AggregateFunction originFunction = originExpr.getFunction();
                        AggregateExpression globalAggExpr = new AggregateExpression(
                                originFunction, bufferToBufferParam, localOutput.toSlot());
                        return new Alias(globalAggExpr, globalAggExpr.toSql());
                    }));

        List<NamedExpression> globalAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBy)
                .addAll(nonDistinctAggExprToAliasPhase2.values())
                .build();

        RequestProperties requestHash = RequestProperties.of(
                PhysicalProperties.createHash(partitionExpressions, ShuffleType.AGGREGATE));
        PhysicalHashAggregate<Plan> anyLocalHashGlobalAgg = new PhysicalHashAggregate<>(
                ImmutableList.copyOf(localAggGroupBy), globalAggOutput, Optional.of(partitionExpressions),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                requestHash, anyLocalAgg);

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), expr -> {
                    Alias alias = nonDistinctAggExprToAliasPhase2.get(expr);
                    if (alias == null) {
                        return expr;
                    }
                    AggregateExpression globalAggExpr = (AggregateExpression) alias.child();
                    return new AggregateExpression(globalAggExpr.getFunction(), bufferToResultParam, alias.toSlot());
                });

        PhysicalHashAggregate<Plan> anyLocalHashGlobalHashDistinctAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), distinctOutput, Optional.of(partitionExpressions),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                requestHash, anyLocalHashGlobalAgg);

        return anyLocalHashGlobalHashDistinctAgg;
    }

    private Plan twoPhaseAggregateWithMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateExpression> aggregateExpressions = logicalAgg.getAggregateExpressions();
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateExpression, Alias> aggExprToAliasPhase1 = aggregateExpressions.stream()
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateFunction function = expr.getFunction();
                    AggregateFunction rewriteFunction = function;
                    if (function instanceof Count && function.isDistinct()) {
                        rewriteFunction = new MultiDistinctCount(function.getArgument(0),
                                function.getArguments().subList(1, function.arity()).toArray(new Expression[0]));
                    } else if (function instanceof Sum && function.isDistinct()) {
                        rewriteFunction = new MultiDistinctSum(function.getArgument(0));
                    }
                    AggregateExpression localAggExpr = new AggregateExpression(rewriteFunction, inputToBufferParam);
                    return new Alias(localAggExpr, localAggExpr.toSql());
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                // already normalize group by expression to List<SlotReference>
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(aggExprToAliasPhase1.values())
                .build();

        RequestProperties requestAny = RequestProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<? extends Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), localAggOutput,
                inputToBufferParam, useStreamAgg(connectContext, logicalAgg.getGroupByExpressions()),
                logicalAgg.getLogicalProperties(), requestAny, logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateExpression) {
                        Alias alias = aggExprToAliasPhase1.get(outputChild);
                        AggregateExpression localAggExpr = (AggregateExpression) alias.child();
                        return new AggregateExpression(localAggExpr.getFunction(), bufferToResultParam, alias.toSlot());
                    } else {
                        return outputChild;
                    }
                });

        RequestProperties globalAggRequest;
        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            globalAggRequest = RequestProperties.of(PhysicalProperties.GATHER);
        } else {
            globalAggRequest = RequestProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.AGGREGATE));
        }

        PhysicalHashAggregate<? extends Plan> gatherOrHashGlobalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), globalOutput, Optional.of(logicalAgg.getGroupByExpressions()),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                globalAggRequest, anyLocalAgg);
        return gatherOrHashGlobalAgg;
    }

    private boolean useStreamAgg(ConnectContext connectContext, Collection<? extends Expression> groupByExpressions) {
        return !connectContext.getSessionVariable().disableStreamPreaggregations
                && !groupByExpressions.isEmpty();
    }

    private List<Expression> getHashAggregatePartitionExpressions(
            LogicalAggregate<? extends Plan> logicalAggregate) {
        List<Expression> partitionExpressions = logicalAggregate.getGroupByExpressions().isEmpty()
                ? ImmutableList.copyOf(logicalAggregate.getDistinctArguments())
                : logicalAggregate.getGroupByExpressions();
        Preconditions.checkState(partitionExpressions.stream().allMatch(p -> p instanceof Slot),
                "group by and arguments of aggregate function should be slot: " + logicalAggregate);

        return partitionExpressions;
    }
}
