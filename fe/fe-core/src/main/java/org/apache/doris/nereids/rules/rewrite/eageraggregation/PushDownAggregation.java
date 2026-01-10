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
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.AdjustNullable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * push down aggregation
 */
public class PushDownAggregation extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(PushDownAggregation.class);

    public final EagerAggRewriter writer = new EagerAggRewriter();

    private final Set<Class> pushDownAggFunctionSet = Sets.newHashSet(
            Sum.class,
            Max.class,
            Min.class);

    private final Set<Class> acceptNodeType = Sets.newHashSet(
            LogicalUnion.class,
            LogicalProject.class,
            LogicalFilter.class,
            LogicalRelation.class,
            LogicalJoin.class);

    private CheckAfterRewrite checker = new CheckAfterRewrite();
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        int mode = SessionVariable.getEagerAggregationMode();
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

        List<AggregateFunction> aggFunctions = new ArrayList<>();

        for (AggregateFunction aggFunction : agg.getAggregateFunctions()) {
            if (pushDownAggFunctionSet.contains(aggFunction.getClass())
                    && !aggFunction.isDistinct()) {
                if (aggFunction instanceof Sum && ((Sum) aggFunction).child() instanceof If) {
                    If body = (If) ((Sum) aggFunction).child();
                    aggFunctions.add(new Sum(body.getTrueValue()));
                    if (!(body.getFalseValue() instanceof NullLiteral)) {
                        aggFunctions.add(new Sum(body.getFalseValue()));
                    }
                    groupKeys.addAll(body.getCondition().getInputSlots()
                            .stream().map(slot -> (SlotReference) slot).collect(Collectors.toList()));
                } else {
                    aggFunctions.add(aggFunction);
                }
            } else {
                return agg;
            }
        }
        aggFunctions = aggFunctions.stream().distinct().collect(Collectors.toList());
        groupKeys = groupKeys.stream().distinct().collect(Collectors.toList());
        if (!checkSubTreePattern(agg.child())) {
            return agg;
        }

        if (!checkSubTreePattern(agg.child())) {
            return agg;
        }

        PushDownAggContext pushDownContext = new PushDownAggContext(new ArrayList<>(aggFunctions),
                groupKeys, context.getCascadesContext());
        try {
            Plan child = agg.child().accept(writer, pushDownContext);
            if (child != agg.child()) {
                // agg has been pushed down, rewrite agg output expressions
                // before: agg[sum(A), by (B)]
                //                 ->join(C=D)
                //                      ->scan(T1[A...])
                //                      ->scan(T2)
                // after: agg[sum(x), by(B)]
                //                 ->join(C=D)
                //                       ->agg[sum(A) as x, by(B,C)]
                //                                 ->scan(T1[A...])
                //                       ->scan(T2)
                List<NamedExpression> newOutputExpressions = new ArrayList<>();
                Map<Expression, Slot> replaceMap = new HashMap<>();
                for (Expression x : pushDownContext.getAliasMap().keySet()) {
                    replaceMap.put(x.child(0), pushDownContext.getAliasMap().get(x).toSlot());
                }

                for (NamedExpression ne : agg.getOutputExpressions()) {
                    if (ne instanceof SlotReference) {
                        newOutputExpressions.add(ne);
                    //} else if (ne instanceof Alias
                    //        && ne.child(0) instanceof Sum
                    //        && ne.child(0).child(0) instanceof If
                    //        && ne.child(0).child(0).child(1) instanceof SlotReference) {
                    //    SlotReference targetSlot = (SlotReference) ne.child(0).child(0).child(1);
                    //    Slot toReplace = null;
                    //    for (Slot slot : child.getOutput()) {
                    //        if (slot.getExprId().equals(targetSlot.getExprId())) {
                    //            toReplace = slot;
                    //        }
                    //    }
                    //    if (toReplace != null) {
                    //        Alias newOutput = (Alias) ((Alias) ne).withChildren(
                    //                new Sum(
                    //                        new If(
                    //                                ne.child(0).child(0).child(0),
                    //                                toReplace,
                    //                                new NullLiteral(toReplace.getDataType())
                    //                        )
                    //                )
                    //        );
                    //        newOutputExpressions.add(newOutput);
                    //    } else {
                    //        return agg;
                    //    }

                    } else {
                        NamedExpression replaceAliasExpr = (NamedExpression) ExpressionUtils.replace(ne, replaceMap);
                        replaceAliasExpr = (NamedExpression) ExpressionUtils.rebuildSignature(replaceAliasExpr);
                        newOutputExpressions.add(replaceAliasExpr);
                    }
                }
                LogicalAggregate<Plan> eagerAgg =
                        agg.withAggOutputChild(newOutputExpressions, child);
                checker.checkTreeAllSlotReferenceFromChildren(eagerAgg);
                NormalizeAggregate normalizeAggregate = new NormalizeAggregate();
                LogicalPlan normalized = normalizeAggregate.normalizeAgg(eagerAgg, Optional.empty(),
                        context.getCascadesContext());
                AdjustNullable adjustNullable = new AdjustNullable(false, false);
                return adjustNullable.rewriteRoot(normalized, null);
            }
        } catch (RuntimeException e) {
            LOG.info("PushDownAggregation failed: " + e.getMessage() + "\n" + agg.treeString());
            if (SessionVariable.isFeDebug()) {
                throw e;
            }
        }
        return agg;
    }

    private boolean checkSubTreePattern(Plan root) {
        return containsPushDownJoin(root)
                && isSPJ(root);
    }

    private boolean containsPushDownJoin(Plan root) {
        if (root instanceof LogicalJoin && !((LogicalJoin) root).isMarkJoin()) {
            return true;
        }
        if (root.children().isEmpty()) {
            return false;
        }
        return root.children().stream().anyMatch(this::containsPushDownJoin);
    }

    private boolean isSPJ(Plan root) {
        boolean accepted = acceptNodeType.stream()
                .anyMatch(clazz -> clazz.isAssignableFrom(root.getClass()));
        if (!accepted) {
            return false;
        }
        for (Plan child : root.children()) {
            if (!isSPJ(child)) {
                return false;
            }
        }
        return true;
    }
}
