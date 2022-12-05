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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.ImplementationRuleFactory;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
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

/**
 * Used to generate the merge agg node for distributed execution.
 * NOTICE: GLOBAL output expressions' ExprId should SAME with ORIGIN output expressions' ExprId.
 * <pre>
 * If we have a query: SELECT SUM(v1 * v2) + 1 FROM t GROUP BY k + 1
 * the initial plan is:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(k + 1) #1, Alias(SUM(v1 * v2) + 1) #2], groupByExpr: [k + 1])
 *   +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(b) #1, Alias(SUM(a) + 1) #2], groupByExpr: [b])
 *   +-- Aggregate(phase: [LOCAL], outputExpr: [SUM(v1 * v2) as a, (k + 1) as b], groupByExpr: [k + 1])
 *       +-- childPlan
 * </pre>
 */
@DependsRules(NormalizeAggregate.class)
public class AggregateStrategies implements ImplementationRuleFactory {

    @Override
    public List<Rule> buildRules() {
        PatternDescriptor<LogicalAggregate<GroupPlan>> basePattern = logicalAggregate()
                .when(agg -> agg.isNormalized());

        return ImmutableList.of(
            RuleType.DISASSEMBLE_ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 0)
                    .thenApplyMulti(ctx -> onePhaseAggregateWithoutDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.DISASSEMBLE_TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 0)
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithoutDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.DISASSEMBLE_TWO_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .thenApply(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.DISASSEMBLE_THREE_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .thenApply(ctx -> threePhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.DISASSEMBLE_TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() > 1)
                    .thenApply(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            )
        );
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

        // distinctAgg must be at the same fragment, so don't need any partition expressions, the bottom
        // globalAgg will provide the partition expressions
        PhysicalHashAggregate<Plan> anyLocalHashGlobalHashDistinctAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), distinctOutput, Optional.empty(),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                requestHash, anyLocalHashGlobalAgg);

        return anyLocalHashGlobalHashDistinctAgg;
    }

    private Plan twoPhaseAggregateWithMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        return logicalAgg;
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
