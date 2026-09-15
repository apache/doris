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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Eliminate constant conditions in Join Condition.
 */
public class EliminateJoinCondition extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalJoin()
                .then(EliminateJoinCondition::eliminateJoinCondition)
                .toRule(RuleType.ELIMINATE_JOIN_CONDITION);
    }

    static Plan eliminateJoinCondition(LogicalJoin<? extends Plan, ? extends Plan> join) {
        List<Expression> hashJoinConjuncts = removeTrueConjuncts(join.getHashJoinConjuncts());
        List<Expression> otherJoinConjuncts = removeTrueConjuncts(join.getOtherJoinConjuncts());
        List<Expression> markJoinConjuncts = removeTrueConjuncts(join.getMarkJoinConjuncts());

        if (!join.isMarkJoin() && (containsFalseOrNull(hashJoinConjuncts)
                || containsFalseOrNull(otherJoinConjuncts))) {
            switch (join.getJoinType()) {
                case INNER_JOIN:
                case CROSS_JOIN:
                    return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), join.getOutput());
                case LEFT_OUTER_JOIN:
                    return projectNullPaddedJoinOutput(join, join.left());
                case RIGHT_OUTER_JOIN:
                    return projectNullPaddedJoinOutput(join, join.right());
                default:
                    break;
            }
        }

        if (hashJoinConjuncts.size() == join.getHashJoinConjuncts().size()
                && otherJoinConjuncts.size() == join.getOtherJoinConjuncts().size()
                && markJoinConjuncts.size() == join.getMarkJoinConjuncts().size()) {
            return null;
        }
        return join.withJoinConjuncts(hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                join.getJoinReorderContext());
    }

    private static List<Expression> removeTrueConjuncts(List<Expression> conjuncts) {
        return conjuncts.stream()
                .filter(expression -> !expression.equals(BooleanLiteral.TRUE))
                .collect(Collectors.toList());
    }

    private static boolean containsFalseOrNull(List<Expression> conjuncts) {
        return conjuncts.stream()
                .anyMatch(expression -> expression.equals(BooleanLiteral.FALSE) || expression.isNullLiteral());
    }

    private static LogicalProject<Plan> projectNullPaddedJoinOutput(
            LogicalJoin<? extends Plan, ? extends Plan> join, Plan preservedChild) {
        Set<Slot> preservedOutput = preservedChild.getOutputSet();
        ImmutableList.Builder<NamedExpression> projects =
                ImmutableList.builderWithExpectedSize(join.getOutput().size());
        for (Slot output : join.getOutput()) {
            if (preservedOutput.contains(output)) {
                projects.add(output);
            } else {
                projects.add(new Alias(output.getExprId(), new NullLiteral(output.getDataType()),
                        output.getName()));
            }
        }
        return new LogicalProject<>(projects.build(), preservedChild);
    }
}
