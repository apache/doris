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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Convert InApply to LogicalJoin.
 * <p>
 * Not In -> NULL_AWARE_LEFT_ANTI_JOIN
 * In -> LEFT_SEMI_JOIN
 */
public class InApplyToJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply().when(LogicalApply::isIn).then(apply -> {
            if (needBitmapUnion(apply)) {
                if (apply.isCorrelated()) {
                    throw new AnalysisException("In bitmap does not support correlated subquery");
                }
                /*
                case 1: in
                select t1.k1 from bigtable t1 where t1.k1 in (select t2.k2 from bitmap_table t2);
                =>
                select t1.k1 from bigtable t1 where t1.k1 in (select bitmap_union(k2) from bitmap_table t2);
                =>
                select t1.k1 from bigtable t1 left semi join (select bitmap_union(k2) x from bitmap_table ) t2
                on bitmap_contains(x, t1.k1);

                case 2: not in
                select t1.k1 from bigtable t1 where t1.k1 not in (select t2.k2 from bitmap_table t2);
                =>
                select t1.k1 from bigtable t1 where t1.k1 not in (select bitmap_union(k2) from bitmap_table t2);
                =>
                select t1.k1 from bigtable t1 left semi join (select bitmap_union(k2) x from bitmap_table ) t2
                on not bitmap_contains(x, t1.k1);
                 */
                List<Expression> groupExpressions = ImmutableList.of();
                Expression bitmapCol = apply.right().getOutput().get(0);
                BitmapUnion union = new BitmapUnion(bitmapCol);
                Alias alias = new Alias(union);
                List<NamedExpression> outputExpressions = Lists.newArrayList(alias);

                LogicalAggregate agg = new LogicalAggregate(groupExpressions, outputExpressions, apply.right());
                Expression compareExpr = ((InSubquery) apply.getSubqueryExpr()).getCompareExpr();
                Expression expr = new BitmapContains(agg.getOutput().get(0), compareExpr);
                if (((InSubquery) apply.getSubqueryExpr()).isNot()) {
                    expr = new Not(expr);
                }
                return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                        Lists.newArrayList(expr),
                        new DistributeHint(DistributeType.NONE),
                        apply.getMarkJoinSlotReference(),
                        apply.left(), agg, null);
            }

            //in-predicate to equal
            InSubquery inSubquery = ((InSubquery) apply.getSubqueryExpr());
            Expression predicate;
            Expression left = inSubquery.getCompareExpr();
            // TODO: trick here, because when deep copy logical plan the apply right child
            //  is not same with query plan in subquery expr, since the scan node copy twice
            Expression right = inSubquery.getSubqueryOutput((LogicalPlan) apply.right());
            if (apply.isMarkJoin()) {
                List<Expression> joinConjuncts = apply.getCorrelationFilter().isPresent()
                        ? ExpressionUtils.extractConjunction(apply.getCorrelationFilter().get())
                        : Lists.newArrayList();
                predicate = new EqualTo(left, right);
                List<Expression> markConjuncts = Lists.newArrayList(predicate);
                if (!predicate.nullable() || (apply.isMarkJoinSlotNotNull() && !inSubquery.isNot())) {
                    // we can merge mark conjuncts with hash conjuncts in 2 scenarios
                    // 1. the mark join predicate is not nullable, so no null value would be produced
                    // 2. semi join with non-nullable mark slot.
                    // because semi join only care about mark slot with true value and discard false and null
                    // it's safe the use false instead of null in this case
                    joinConjuncts.addAll(markConjuncts);
                    markConjuncts.clear();
                }
                return new LogicalJoin<>(
                        inSubquery.isNot() ? JoinType.LEFT_ANTI_JOIN : JoinType.LEFT_SEMI_JOIN,
                        Lists.newArrayList(), joinConjuncts, markConjuncts,
                        new DistributeHint(DistributeType.NONE), apply.getMarkJoinSlotReference(),
                        apply.children(), null);
            } else {
                if (apply.isCorrelated()) {
                    if (inSubquery.isNot()) {
                        predicate = ExpressionUtils.and(ExpressionUtils.or(new EqualTo(left, right),
                                        new IsNull(left), new IsNull(right)),
                                apply.getCorrelationFilter().get());
                    } else {
                        predicate = ExpressionUtils.and(new EqualTo(left, right),
                                apply.getCorrelationFilter().get());
                    }
                } else {
                    predicate = new EqualTo(left, right);
                }

                List<Expression> conjuncts = ExpressionUtils.extractConjunction(predicate);
                if (inSubquery.isNot()) {
                    return new LogicalJoin<>(
                            predicate.nullable() && !apply.isCorrelated()
                                    ? JoinType.NULL_AWARE_LEFT_ANTI_JOIN
                                    : JoinType.LEFT_ANTI_JOIN,
                            Lists.newArrayList(), conjuncts, new DistributeHint(DistributeType.NONE),
                            apply.getMarkJoinSlotReference(), apply.children(), null);
                } else {
                    return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                            conjuncts,
                            new DistributeHint(DistributeType.NONE), apply.getMarkJoinSlotReference(),
                            apply.children(), null);
                }
            }
        }).toRule(RuleType.IN_APPLY_TO_JOIN);
    }

    private boolean needBitmapUnion(LogicalApply<Plan, Plan> apply) {
        return apply.right().getOutput().get(0).getDataType().isBitmapType()
                && !((InSubquery) apply.getSubqueryExpr()).getCompareExpr().getDataType().isBitmapType();
    }
}
