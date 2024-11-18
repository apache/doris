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

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.PullUpPredicates;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *  LogicalJoin(hashConjuncts:t1.a=t2.a)
 *    +--Plan1(output:t1.a)
 *    +--Plan2(output:t2.a)
 *  ->
 *  LogicalUnion
 *    +--LogicalFilter(t1.a is null)
 *      +--Plan1
 *    +--LogicalJoin(t1.a=t2.a)
 *      +--LogicalFilter(t1.a is not null)
 *        +--Plan1
 *      +--Plan2
 * */
public class JoinSplitForNullSkew extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin(any(), any())
                .when(join -> join.getJoinType().isLeftJoin())
                .when(join -> join.getHashJoinConjuncts().size() == 1)
                .thenApply(ctx -> splitJoin(ctx.root, ctx.cascadesContext))
                .toRule(RuleType.JOIN_SPLIT_FOR_NULL_SKEW);
    }

    private Plan splitJoin(LogicalJoin<Plan, Plan> join, CascadesContext cascadesContext) {
        // 需要把左边的join key取出，
        Plan left = join.left();
        Plan right = join.right();
        // 找到哪个slot是来自左边 哪个来自于右边
        Expression conjunct = join.getHashJoinConjuncts().get(0);
        if (!(conjunct instanceof EqualTo)) {
            return null;
        }
        EqualTo equalTo = (EqualTo) conjunct;
        Expression leftSlot;
        if (left.getOutputSet().contains(equalTo.left())) {
            leftSlot = equalTo.left();
        } else {
            leftSlot = equalTo.right();
        }
        // 可以上拉一下，如果join的左孩子，在join key上已经有这个 is not null的谓词了，那么就不做这个优化
        PullUpPredicates pullUpPredicates = new PullUpPredicates(false);
        Set<Expression> leftChildPredicates = left.accept(pullUpPredicates, null);
        if (leftChildPredicates.contains(new Not(new IsNull(leftSlot)))) {
            return null;
        }
        // Plan deepCopyLeft = new LogicalPlanDeepCopier().deepCopy((LogicalPlan) left, new DeepCopierContext());
        // 创建isnull侧的
        LogicalFilter<Plan> isNullFilter = new LogicalFilter<>(ImmutableSet.of(new IsNull(leftSlot)), left);
        List<NamedExpression> newPrjects = new ArrayList<>();
        newPrjects.addAll(isNullFilter.getOutput());
        for (int i = 0; i < right.getOutput().size(); ++i) {
            newPrjects.add(new Alias(NullLiteral.INSTANCE));
        }
        LogicalProject<Plan> isNullProject = new LogicalProject<>(newPrjects, isNullFilter);
        // 创建is not null 侧的
        LogicalFilter<Plan> newJoinLeftChild = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(leftSlot))), left);
        LogicalJoin<Plan, Plan> newJoin = join.withChildren(ImmutableList.of(newJoinLeftChild, right));
        List<List<SlotReference>> regularChildrenOutputs = new ArrayList<>();
        regularChildrenOutputs.add((List) isNullProject.getOutput());
        regularChildrenOutputs.add((List) newJoin.getOutput());
        return new LogicalUnion(Qualifier.ALL, (List) join.getOutput(), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(isNullProject, newJoin));
    }
}
