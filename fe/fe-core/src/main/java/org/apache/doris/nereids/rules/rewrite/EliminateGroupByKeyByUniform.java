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

import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * +--aggregate(group by a,b output a,b,max(c))
 * (a is uniform and not null: e.g. a is projection 2 as a in logicalProject)
 * ->
 * +--aggregate(group by b output b,any_value(a) as a,max(c))
 * */
public class EliminateGroupByKeyByUniform extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().when(agg -> !agg.getSourceRepeat().isPresent())
                .whenNot(agg -> agg.getGroupByExpressions().isEmpty())
                .then(EliminateGroupByKeyByUniform::eliminate)
                .toRule(RuleType.ELIMINATE_GROUP_BY_KEY_BY_UNIFORM);

    }

    private static Plan eliminate(LogicalAggregate<Plan> agg) {
        DataTrait aggChildTrait = agg.child().getLogicalProperties().getTrait();
        // Get the Group by column of agg. If there is a uniform one, delete the group by key.
        List<Expression> removedExpression = new ArrayList<>();
        List<Expression> newGroupBy = new ArrayList<>();
        for (Expression groupBy : agg.getGroupByExpressions()) {
            if (!(groupBy instanceof Slot)) {
                newGroupBy.add(groupBy);
                continue;
            }
            if (aggChildTrait.isUniformAndNotNull((Slot) groupBy)) {
                removedExpression.add(groupBy);
            } else {
                newGroupBy.add(groupBy);
            }
        }
        // TODO Consider whether there are other opportunities for optimization when newGroupBy is empty
        if (newGroupBy.isEmpty()) {
            return null;
        }

        List<NamedExpression> newOutputs = new ArrayList<>();
        // If this output appears in the removedExpression column, replace it with anyvalue
        for (NamedExpression output : agg.getOutputExpressions()) {
            if (output instanceof Slot) {
                if (removedExpression.contains(output)) {
                    newOutputs.add(new Alias(output.getExprId(), new AnyValue(false, output), output.getName()));
                } else {
                    newOutputs.add(output);
                }
            } else if (output instanceof Alias) {
                if (removedExpression.contains(output.child(0))) {
                    newOutputs.add(new Alias(output.getExprId(),
                            new AnyValue(false, output.child(0)), output.getName()));
                } else {
                    newOutputs.add(output);
                }
            } else {
                newOutputs.add(output);
            }
        }

        // Adjust the order of this new output so that aggregate functions are placed at the back
        // and non-aggregated functions are placed at the front.
        List<NamedExpression> aggFuncs = new ArrayList<>();
        List<NamedExpression> orderOutput = new ArrayList<>();
        for (NamedExpression output : newOutputs) {
            if (output.anyMatch(e -> e instanceof AggregateFunction)) {
                aggFuncs.add(output);
            } else {
                orderOutput.add(output);
            }
        }
        orderOutput.addAll(aggFuncs);
        return agg.withGroupByAndOutput(newGroupBy, orderOutput);
    }
}
