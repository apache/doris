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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**SplitAggMultiPhaseWithoutGbyKey*/
public class SplitAggMultiPhaseWithoutGbyKey extends SplitAggRule implements ExplorationRuleFactory {
    public static final SplitAggMultiPhaseWithoutGbyKey INSTANCE = new SplitAggMultiPhaseWithoutGbyKey();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalAggregate()
                    .when(agg -> agg.getGroupByExpressions().isEmpty())
                    .when(Aggregate::isAggregateDistinct)
                    .thenApplyMulti(ctx -> rewrite(ctx.root, ctx.connectContext))
                    .toRule(RuleType.SPLIT_AGG_MULTI_PHASE_WITHOUT_GBY_KEY)
        );
    }

    List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        return ImmutableList.of(
                splitToThreePhase(aggregate, connectContext),
                splitToFourPhase(aggregate, connectContext)
        );
    }

    Plan splitToFourPhase(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        Map<AggregateFunction, Alias> localAggFuncToAlias = new LinkedHashMap<>();
        LogicalAggregate<? extends Plan> secondAgg = splitLocalTwoPhase(aggregate, localAggFuncToAlias,
                Utils.fastToImmutableList(aggregate.getDistinctArguments()), (Set) aggregate.getDistinctArguments(),
                connectContext);

        return splitDistinctTwoPhase(aggregate, localAggFuncToAlias, secondAgg);
    }

    Plan splitToThreePhase(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        //防止被拆分
        AggregateParam inputToResult = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER, true);
        Map<AggregateFunction, Alias> localAggFuncToAlias = new LinkedHashMap<>();
        Set<NamedExpression> keySet = getAllKeySet(aggregate);
        LogicalAggregate<? extends Plan> localAgg = splitDeduplicateAgg(aggregate, keySet, inputToResult,
                connectContext, localAggFuncToAlias, aggregate.child(), Utils.fastToImmutableList(keySet));
        return splitDistinctTwoPhase(aggregate, localAggFuncToAlias, localAgg);
    }

    // 如果是sum/count等场景,可以使用

}
