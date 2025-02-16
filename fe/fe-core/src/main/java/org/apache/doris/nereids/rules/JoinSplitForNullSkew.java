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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  LogicalLeftOuterJoin(hashConjuncts:t1.a=t2.a)
 *    +--Plan1(output:t1.a)
 *    +--Plan2(output:t2.a)
 *  ->
 *  LogicalUnion
 *    +--LogicalProject
 *      +--LogicalFilter(t1.a is null)
 *        +--Plan1
 *    +--LogicalLeftOuterJoin(t1.a=t2.a)
 *      +--LogicalFilter(t1.a is not null)
 *        +--Plan1
 *      +--Plan2
 *
 *  LogicalRightOuterJoin(hashConjuncts:t1.a=t2.a)
 *    +--Plan1(output:t1.a)
 *    +--Plan2(output:t2.a)
 *  ->
 *  LogicalUnion
 *    +--LogicalProject
 *      +--LogicalFilter(t2.a is null)
 *        +--Plan2
 *    +--LogicalRightOuterJoin(t1.a=t2.a)
 *      +--Plan1
 *      +--LogicalFilter(t2.a is not null)
 *        +--Plan2
 * */
public class JoinSplitForNullSkew extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getJoinType().isOneSideOuterJoin())
                .whenNot(join -> join.isMarkJoin() || !join.getMarkJoinConjuncts().isEmpty())
                .when(join -> join.getHashJoinConjuncts().size() == 1)
                .then(this::splitJoin)
                .toRule(RuleType.JOIN_SPLIT_FOR_NULL_SKEW);
    }

    private Plan splitJoin(LogicalJoin<Plan, Plan> join) {
        boolean isLeftJoin = join.getJoinType().isLeftOuterJoin();
        Plan primarySide = isLeftJoin ? join.left() : join.right();
        Plan associatedSide = isLeftJoin ? join.right() : join.left();
        Expression conjunct = join.getHashJoinConjuncts().get(0);
        if (!(conjunct instanceof EqualTo)) {
            return null;
        }
        EqualTo equalTo = (EqualTo) conjunct;
        Expression splitExpr;
        if (primarySide.getOutputSet().containsAll(equalTo.left().getInputSlots())) {
            splitExpr = equalTo.left();
        } else {
            splitExpr = equalTo.right();
        }
        if (!splitExpr.nullable()) {
            return null;
        }

        // is not null side construct
        LogicalFilter<Plan> isNotNullFilter = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(splitExpr))), primarySide);
        LogicalJoin<Plan, Plan> newJoin;
        if (isLeftJoin) {
            newJoin = join.withChildren(ImmutableList.of(isNotNullFilter, associatedSide));
        } else {
            newJoin = join.withChildren(ImmutableList.of(associatedSide, isNotNullFilter));
        }
        Plan deepCopyJoin = LogicalPlanDeepCopier.INSTANCE.deepCopy(newJoin, new DeepCopierContext());

        // avoid duplicate application of rules
        if (primarySide instanceof LogicalFilter) {
            int primaryIndex = isLeftJoin ? 0 : 1;
            Map<Expression, Expression> newJoinOutputToOriginJoinOutput = new HashMap<>();
            for (int i = 0; i < primarySide.getOutput().size(); ++i) {
                newJoinOutputToOriginJoinOutput.put(deepCopyJoin.child(primaryIndex).getOutput().get(i),
                        primarySide.getOutput().get(i));
            }
            Set<Expression> conjuncts = ((LogicalFilter<Plan>) deepCopyJoin.child(primaryIndex)).getConjuncts();
            Set<Expression> replacedConjuncts = conjuncts.stream()
                    .map(c -> c.rewriteUp(e -> newJoinOutputToOriginJoinOutput.getOrDefault(e, e)))
                    .collect(Collectors.toSet());
            if (((LogicalFilter<?>) primarySide).getConjuncts().equals(replacedConjuncts)) {
                return null;
            }
        }

        // is null side construct
        LogicalFilter<Plan> isNullFilter = new LogicalFilter<>(ImmutableSet.of(new IsNull(splitExpr)), primarySide);
        Plan deepCopyFilter = LogicalPlanDeepCopier.INSTANCE.deepCopy(isNullFilter, new DeepCopierContext());
        List<NamedExpression> newProjects = new ArrayList<>(join.getOutput().size());
        if (isLeftJoin) {
            newProjects.addAll(deepCopyFilter.getOutput());
            for (Slot slot : associatedSide.getOutput()) {
                newProjects.add(new Alias(new NullLiteral(slot.getDataType())));
            }
        } else {
            for (Slot slot : associatedSide.getOutput()) {
                newProjects.add(new Alias(new NullLiteral(slot.getDataType())));
            }
            newProjects.addAll(deepCopyFilter.getOutput());
        }
        LogicalProject<Plan> isNullProject = new LogicalProject<>(newProjects, deepCopyFilter);

        // regularChildrenOutputs construct
        List<List<SlotReference>> regularChildrenOutputs = new ArrayList<>();
        regularChildrenOutputs.add((List) isNullProject.getOutput());
        regularChildrenOutputs.add((List) deepCopyJoin.getOutput());

        return new LogicalUnion(Qualifier.ALL, (List) join.getOutput(), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(isNullProject, deepCopyJoin));
    }
}
