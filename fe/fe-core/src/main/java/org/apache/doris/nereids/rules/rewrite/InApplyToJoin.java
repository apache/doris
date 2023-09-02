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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.JoinHint;
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
                Alias alias = new Alias(union, union.toSql());
                List<NamedExpression> outputExpressions = Lists.newArrayList(alias);

                LogicalAggregate agg = new LogicalAggregate(groupExpressions, outputExpressions, apply.right());
                Expression compareExpr = ((InSubquery) apply.getSubqueryExpr()).getCompareExpr();
                Expression expr = new BitmapContains(agg.getOutput().get(0), compareExpr);
                if (((InSubquery) apply.getSubqueryExpr()).isNot()) {
                    expr = new Not(expr);
                }
                return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                        Lists.newArrayList(expr),
                        JoinHint.NONE,
                        apply.left(), agg);
            }

            //in-predicate to equal
            Expression predicate;
            Expression left = ((InSubquery) apply.getSubqueryExpr()).getCompareExpr();
            // TODO: trick here, because when deep copy logical plan the apply right child
            //  is not same with query plan in subquery expr, since the scan node copy twice
            Expression right = apply.getSubqueryExpr().getSubqueryOutput((LogicalPlan) apply.right());
            if (apply.isCorrelated()) {
                predicate = ExpressionUtils.and(new EqualTo(left, right),
                        apply.getCorrelationFilter().get());
            } else {
                predicate = new EqualTo(left, right);
            }

            List<Expression> conjuncts = ExpressionUtils.extractConjunction(predicate);
            if (((InSubquery) apply.getSubqueryExpr()).isNot()) {
                return new LogicalJoin<>(
                        predicate.nullable() ? JoinType.NULL_AWARE_LEFT_ANTI_JOIN : JoinType.LEFT_ANTI_JOIN,
                        Lists.newArrayList(),
                        conjuncts,
                        JoinHint.NONE, apply.getMarkJoinSlotReference(),
                        apply.children());
            } else {
                return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                        conjuncts,
                        JoinHint.NONE, apply.getMarkJoinSlotReference(),
                        apply.children());
            }
        }).toRule(RuleType.IN_APPLY_TO_JOIN);
    }

    private boolean needBitmapUnion(LogicalApply<Plan, Plan> apply) {
        return apply.right().getOutput().get(0).getDataType().isBitmapType()
                && !((InSubquery) apply.getSubqueryExpr()).getCompareExpr().getDataType().isBitmapType();
    }
}
