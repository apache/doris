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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * prune its child output according to agg.
 * pattern: agg()
 * table a: k1,k2,k3,v1
 * select k1,sum(v1) from a group by k1
 * plan tree:
 *    agg
 *     |
 *    scan(k1,k2,k3,v1)
 * transformed:
 *    agg
 *     |
 *   project(k1,v1)
 *     |
 *    scan(k1,k2,k3,v1)
 */
public class PruneAggChildColumns extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return RuleType.COLUMN_PRUNE_AGGREGATION_CHILD.build(logicalAggregate().then(agg -> {
            List<Slot> childOutput = agg.child().getOutput();
            if (isAggregateWithConstant(agg)) {
                Slot slot = ExpressionUtils.selectMinimumColumn(childOutput);
                if (childOutput.size() == 1 && childOutput.get(0).equals(slot)) {
                    return agg;
                }
                return agg.withChildren(ImmutableList.of(new LogicalProject<>(ImmutableList.of(slot), agg.child())));
            }
            Set<Slot> aggInputSlots = agg.getInputSlots();
            List<NamedExpression> prunedOutputs = childOutput.stream().filter(aggInputSlots::contains)
                    .collect(Collectors.toList());
            if (prunedOutputs.size() == agg.child().getOutput().size()) {
                return agg;
            }
            return agg.withChildren(ImmutableList.of(new LogicalProject<>(prunedOutputs, agg.child())));
        }));
    }

    /**
     * For these aggregate function with constant param. Such as:
     *  count(*), count(1), sum(1)..etc.
     * @return null, if there exists an aggregation function that its parameters contains non-constant expr.
     *               else return a slot with min data type.
     */
    private boolean isAggregateWithConstant(LogicalAggregate<GroupPlan> agg) {
        for (NamedExpression output : agg.getOutputExpressions()) {
            if (output.anyMatch(SlotReference.class::isInstance)) {
                return false;
            }
        }
        return true;
    }
}
