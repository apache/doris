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
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
        if (left.getOutputSet().containsAll(equalTo.left().getInputSlots())) {
            leftSlot = equalTo.left();
        } else {
            leftSlot = equalTo.right();
        }

        // 创建is not null 侧的
        LogicalFilter<Plan> newJoinLeftChild = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(leftSlot))), left);
        LogicalJoin<Plan, Plan> newJoin = join.withChildren(ImmutableList.of(newJoinLeftChild, right));
        Plan deepCopyJoin = LogicalPlanDeepCopier.INSTANCE.deepCopy(newJoin, new DeepCopierContext());

        if (left instanceof LogicalFilter) {
            // 制作一个map，判断是否重复应用这个规则了。
            Map<Expression, Expression> newJoinOutputToOriginJoinOutput = new HashMap<>();
            for (int i = 0; i < left.getOutput().size(); ++i) {
                newJoinOutputToOriginJoinOutput.put(deepCopyJoin.child(0).getOutput().get(i), left.getOutput().get(i));
            }
            Set<Expression> originConjuncts = ((LogicalFilter<?>) left).getConjuncts();
            Set<Expression> replacedNewConjuncts = new HashSet<>();
            for (Expression newConjunct : newJoinLeftChild.getConjuncts()) {
                replacedNewConjuncts.add(newConjunct.rewriteUp(e -> newJoinOutputToOriginJoinOutput
                        .getOrDefault(e, e)));
            }
            if (replacedNewConjuncts.equals(originConjuncts)) {
                return null;
            }
        }

        // 创建isnull侧的
        LogicalFilter<Plan> isNullFilter = new LogicalFilter<>(ImmutableSet.of(new IsNull(leftSlot)), left);
        Plan deepCopyLeft = LogicalPlanDeepCopier.INSTANCE.deepCopy(isNullFilter, new DeepCopierContext());
        List<NamedExpression> newPrjects = new ArrayList<>();
        newPrjects.addAll(deepCopyLeft.getOutput());
        for (Slot slot : right.getOutput()) {
            newPrjects.add(new Alias(new NullLiteral(slot.getDataType())));
        }
        LogicalProject<Plan> isNullProject = new LogicalProject<>(newPrjects, deepCopyLeft);

        // 创建regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = new ArrayList<>();
        regularChildrenOutputs.add((List) isNullProject.getOutput());
        regularChildrenOutputs.add((List) deepCopyJoin.getOutput());

        return new LogicalUnion(Qualifier.ALL, (List) join.getOutput(), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(isNullProject, deepCopyJoin));
    }
}
