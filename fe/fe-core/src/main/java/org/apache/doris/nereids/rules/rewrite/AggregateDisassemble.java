// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.AggPhase;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO: if instance count is 1, shouldn't disassemble the agg operator
 * Used to generate the merge agg node for distributed execution.
 * Do this in following steps:
 *  1. clone output expr list, find all agg function
 *  2. set found agg function intermediaType
 *  3. create new child plan rooted at new local agg
 *  4. update the slot referenced by expr of merge agg
 *  5. create plan rooted at merge agg, return it.
 */
public class AggregateDisassemble extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalAggregate().when(p -> {
            LogicalAggregate logicalAggregation = p.getOperator();
            return !logicalAggregation.isDisassembled();
        }).thenApply(ctx -> {
            Plan plan = ctx.root;
            Operator operator = plan.getOperator();
            LogicalAggregate agg = (LogicalAggregate) operator;
            List<NamedExpression> outputExpressionList = agg.getOutputExpressionList();
            List<NamedExpression> intermediateAggExpressionList = Lists.newArrayList();
            // TODO: shouldn't extract agg function from this field.
            for (NamedExpression namedExpression : outputExpressionList) {
                namedExpression = (NamedExpression) namedExpression.clone();
                intermediateAggExpressionList.add(namedExpression);
            }
            LogicalAggregate localAgg = new LogicalAggregate(
                    agg.getGroupByExprList().stream().map(Expression::clone).collect(Collectors.toList()),
                    intermediateAggExpressionList,
                    true,
                    AggPhase.FIRST
            );

            Plan childPlan = plan(localAgg, plan.child(0));

            List<NamedExpression> mergeOutputExpressionList = Lists.newArrayList();
            for (int i = 0; i < agg.getOutputExpressionList().size(); i++) {
                NamedExpression mergeOutput = (NamedExpression) new ReplaceSlotReference()
                        .visit(agg.getOutputExpressionList().get(i),
                                (SlotReference) localAgg.getOutputExpressionList().get(i).toSlot());
                mergeOutputExpressionList.add(mergeOutput);
            }

            LogicalAggregate mergeAgg = new LogicalAggregate(
                    agg.getGroupByExprList(),
                    mergeOutputExpressionList,
                    true,
                    AggPhase.FIRST_MERGE
            );
            return plan(mergeAgg, childPlan);
        }).toRule(RuleType.AGGREGATE_DISASSEMBLE);
    }

    private class ReplaceSlotReference extends DefaultExpressionRewriter<SlotReference> {
        @Override
        public Expression visitSlotReference(SlotReference slotReference, SlotReference newSlotReference) {
            return newSlotReference;
        }
    }
}
