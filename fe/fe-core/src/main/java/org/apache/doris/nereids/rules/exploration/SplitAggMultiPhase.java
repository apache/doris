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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**SplitAggMultiPhase
 * only process agg with distinct function, split Agg into multi phase
 * */
public class SplitAggMultiPhase extends SplitAggRule implements ExplorationRuleFactory {
    public static final SplitAggMultiPhase INSTANCE = new SplitAggMultiPhase();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate()
                        .whenNot(agg -> agg.getAggregateParam().isSplit)
                        .when(Aggregate::isAggregateDistinct)
                        .when(agg -> !agg.getGroupByExpressions().isEmpty())
                        .thenApplyMulti(ctx -> rewrite(ctx.root, ctx.connectContext))
                        .toRule(RuleType.SPLIT_AGG_MULTI_PHASE)
        );
    }

    private List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        return ImmutableList.<Plan>builder()
                .add(splitToThreePhase(aggregate, connectContext))
                .add(splitToThreePhaseChildSatisfyGroupByKey(aggregate, connectContext))
                .add(splitToFourPhase(aggregate, connectContext))
                .add(splitToOnePlusTwoPhase(aggregate, connectContext))
                .build();
    }

    private Plan splitToThreePhase(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
        Plan middleAgg = splitLocalTwoPhase(aggregate, middleAggFunctionToAlias, aggregate.getGroupByExpressions(),
                localAggGroupBySet, connectContext);

        // third phase
        AggregateParam inputToResultParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT, true);
        return splitDistinctGlobalAgg(aggregate, inputToResultParam, middleAggFunctionToAlias, middleAgg);
    }

    private Plan splitToThreePhaseChildSatisfyGroupByKey(LogicalAggregate<? extends Plan> aggregate,
            ConnectContext connectContext) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        // first phase
        // isSplit设置为true,防止被拆分开
        AggregateParam inputToResultParamFirst = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER, true);
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        Plan localAgg = splitDeduplicateAgg(aggregate, localAggGroupBySet, inputToResultParamFirst, connectContext,
                localAggFunctionToAlias, aggregate.child(), ImmutableList.of());

        // second phase
        AggregateParam inputToResultParamSecond = new AggregateParam(AggPhase.DISTINCT_GLOBAL,
                AggMode.INPUT_TO_RESULT, true);
        return splitDistinctGlobalAgg(aggregate, inputToResultParamSecond, localAggFunctionToAlias, localAgg);
    }

    private Plan splitToFourPhase(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
        Plan middleAgg = splitLocalTwoPhase(aggregate, middleAggFunctionToAlias,
                Utils.fastToImmutableList(localAggGroupBySet), localAggGroupBySet, connectContext);

        return splitDistinctTwoPhase(aggregate, middleAggFunctionToAlias, middleAgg);
    }

    private Plan splitToOnePlusTwoPhase(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        // first phase
        // isSplit设置为true,防止被拆分开
        AggregateParam inputToResultParamFirst = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER, true);
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        Plan localAgg = splitDeduplicateAgg(aggregate, localAggGroupBySet, inputToResultParamFirst, connectContext,
                localAggFunctionToAlias, aggregate.child(), ImmutableList.of());
        return splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, localAgg);
    }

    private Plan splitDistinctGlobalAgg(LogicalAggregate<? extends Plan> aggregate,
            AggregateParam inputToResultParamSecond, Map<AggregateFunction, Alias> childAggFuncMap, Plan child) {
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            // 测试一下为什么需要checkArgument here
                            return new AggregateExpression(
                                    aggFunc.withDistinctAndChildren(false, aggFunc.children()),
                                    inputToResultParamSecond);
                        } else {
                            return new AggregateExpression(aggFunc,
                                    new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT, true),
                                    childAggFuncMap.get(aggFunc).toSlot());
                        }
                    }
                    return expr;
                });
        return aggregate.withAggParam(globalOutput, aggregate.getGroupByExpressions(),
                false, inputToResultParamSecond, aggregate.getLogicalProperties(),
                Optional.of(aggregate.getGroupByExpressions()), child);
    }

}
