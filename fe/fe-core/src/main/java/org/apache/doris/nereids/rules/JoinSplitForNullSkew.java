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
                .thenApply(ctx -> {
                    Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                            .getSessionVariable().getEnableNereidsRules();
                    if (!enableNereidsRules.contains(RuleType.JOIN_SPLIT_FOR_NULL_SKEW.type())) {
                        return null;
                    }
                    return splitJoin(ctx.root);
                })
                .toRule(RuleType.JOIN_SPLIT_FOR_NULL_SKEW);
    }

    private Plan splitJoin(LogicalJoin<Plan, Plan> join) {
        Plan left = join.left();
        Plan right = join.right();

        Expression conjunct = join.getHashJoinConjuncts().get(0);
        if (!(conjunct instanceof EqualTo)) {
            return null;
        }
        EqualTo equalTo = (EqualTo) conjunct;
        Expression leftExpr;
        if (left.getOutputSet().containsAll(equalTo.left().getInputSlots())) {
            leftExpr = equalTo.left();
        } else {
            leftExpr = equalTo.right();
        }

        // is not null side construct
        LogicalFilter<Plan> newJoinLeftChild = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(leftExpr))), left);
        LogicalJoin<Plan, Plan> newJoin = join.withChildren(ImmutableList.of(newJoinLeftChild, right));
        Plan deepCopyJoin = LogicalPlanDeepCopier.INSTANCE.deepCopy(newJoin, new DeepCopierContext());

        // avoid duplicate application of rules
        if (left instanceof LogicalFilter) {
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

        // is null side construct
        LogicalFilter<Plan> isNullFilter = new LogicalFilter<>(ImmutableSet.of(new IsNull(leftExpr)), left);
        Plan deepCopyLeft = LogicalPlanDeepCopier.INSTANCE.deepCopy(isNullFilter, new DeepCopierContext());
        List<NamedExpression> newProjects = new ArrayList<>(deepCopyLeft.getOutput());
        for (Slot slot : right.getOutput()) {
            newProjects.add(new Alias(new NullLiteral(slot.getDataType())));
        }
        LogicalProject<Plan> isNullProject = new LogicalProject<>(newProjects, deepCopyLeft);

        // regularChildrenOutputs construct
        List<List<SlotReference>> regularChildrenOutputs = new ArrayList<>();
        regularChildrenOutputs.add((List) isNullProject.getOutput());
        regularChildrenOutputs.add((List) deepCopyJoin.getOutput());

        return new LogicalUnion(Qualifier.ALL, (List) join.getOutput(), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(isNullProject, deepCopyJoin));
    }
}
