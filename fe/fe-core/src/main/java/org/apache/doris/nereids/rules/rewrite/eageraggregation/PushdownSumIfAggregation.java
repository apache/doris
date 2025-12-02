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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * PushdownAggregationThroughJoinV2
 */
public class PushdownSumIfAggregation extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private final Set<Class> pushDownAggFunctionSet = Sets.newHashSet(
            Sum.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        int mode = ConnectContext.get().getSessionVariable().eagerAggregationMode;
        if (mode < 0) {
            return plan;
        } else {
            return plan.accept(this, jobContext);
        }
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, JobContext context) {
        Plan newChild = agg.child().accept(this, context);
        if (newChild != agg.child()) {
            // TODO : push down upper aggregations
            return agg.withChildren(newChild);
        }

        if (agg.getSourceRepeat().isPresent()) {
            return agg;
        }

        List<NamedExpression> aliasToBePushDown = Lists.newArrayList();
        List<EqualTo> ifConditions = Lists.newArrayList();
        List<SlotReference> ifThenSlots = Lists.newArrayList();
        boolean patternMatch = true;
        for (NamedExpression aggOutput : agg.getOutputExpressions()) {
            if (aggOutput instanceof Alias) {
                Expression body = aggOutput.child(0);
                if (body instanceof Sum) {
                    Expression sumBody = ((Sum) body).child();
                    if (sumBody instanceof If) {
                        If ifBody = (If) sumBody;
                        if (ifBody.child(0) instanceof EqualTo
                                && ifBody.child(1) instanceof SlotReference
                                && ifBody.child(2) instanceof NullLiteral) {
                            ifConditions.add((EqualTo) ifBody.child(0));
                            ifThenSlots.add((SlotReference) ifBody.child(1));
                            aliasToBePushDown.add(aggOutput);
                            continue;
                        }
                    }
                }
                patternMatch = false;
            }
        }
        if (!patternMatch) {
            return agg;
        }
        if (ifThenSlots.isEmpty()) {
            return agg;
        }
        ifThenSlots = Lists.newArrayList(Sets.newHashSet(ifThenSlots));

        List<SlotReference> groupKeys = new ArrayList<>();
        for (Expression groupKey : agg.getGroupByExpressions()) {
            if (groupKey instanceof SlotReference) {
                groupKeys.add((SlotReference) groupKey);
            } else {
                if (SessionVariable.isFeDebug()) {
                    throw new RuntimeException("PushDownAggregation failed: agg is not normalized\n "
                            + agg.treeString());
                } else {
                    return agg;
                }
            }
        }

        SumAggContext sumAggContext = new SumAggContext(aliasToBePushDown, ifConditions, ifThenSlots, groupKeys);
        SumAggWriter writer = new SumAggWriter();
        Plan child = agg.child().accept(writer, sumAggContext);
        if (child != agg.child()) {
            List<NamedExpression> outputExpressions = agg.getOutputExpressions();
            List<NamedExpression> newOutputExpressions = new ArrayList<>();
            for (NamedExpression output : outputExpressions) {
                if (output instanceof SlotReference) {
                    newOutputExpressions.add(output);
                } else if (output instanceof Alias
                        && output.child(0) instanceof Sum
                        && output.child(0).child(0) instanceof If
                        && output.child(0).child(0).child(1) instanceof SlotReference) {
                    SlotReference targetSlot = (SlotReference) output.child(0).child(0).child(1);
                    Slot toReplace = null;
                    for (Slot slot : child.getOutput()) {
                        if (slot.getExprId().equals(targetSlot.getExprId())) {
                            toReplace = slot;
                        }
                    }
                    if (toReplace != null) {
                        Alias newOutput = (Alias) ((Alias) output).withChildren(
                                new Sum(
                                        new If(
                                                output.child(0).child(0).child(0),
                                                toReplace,
                                                new NullLiteral()
                                    )
                            )
                        );
                        newOutputExpressions.add(newOutput);
                    } else {
                        return agg;
                    }

                }
            }
            return agg.withAggOutputChild(newOutputExpressions, child);
        }
        return agg;
    }
}
