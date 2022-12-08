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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A MultiJoin represents a join of N inputs (NAry-Join).
 * The regular Join represent strictly binary input (Binary-Join).
 * <p>
 * One {@link MultiJoin} just contains one {@link JoinType} of SEMI/ANTI/OUTER Join.
 * <p>
 * joinType is FULL OUTER JOIN, children.size() == 2.
 * leftChild [FULL OUTER JOIN] rightChild.
 * <p>
 * joinType is LEFT (OUTER/SEMI/ANTI) JOIN,
 * children[0, last) [LEFT (OUTER/SEMI/ANTI) JOIN] lastChild.
 * eg: MJ([LOJ] A, B, C, D) is {A B C} [LOJ] {D}.
 * <p>
 * joinType is RIGHT (OUTER/SEMI/ANTI) JOIN,
 * firstChild [RIGHT (OUTER/SEMI/ANTI) JOIN] children[1, last].
 * eg: MJ([ROJ] A, B, C, D) is {A} [ROJ] {B C D}.
 */
public class MultiJoin extends AbstractLogicalPlan {
    /*
     *        topJoin
     *        /     \            MultiJoin
     *   bottomJoin  C  -->     /    |    \
     *     /    \              A     B     C
     *    A      B
     */

    // Push predicates into it.
    // But joinFilter shouldn't contain predicate which just contains one predicate like `T.key > 1`.
    // Because these predicate should be pushdown.
    private final List<Expression> joinFilter;
    // MultiJoin just contains one OUTER/SEMI/ANTI.
    private final JoinType joinType;
    // When contains one OUTER/SEMI/ANTI join, keep separately its condition.
    private final List<Expression> notInnerJoinConditions;

    public MultiJoin(List<Plan> inputs, List<Expression> joinFilter, JoinType joinType,
            List<Expression> notInnerJoinConditions) {
        super(PlanType.LOGICAL_MULTI_JOIN, inputs.toArray(new Plan[0]));
        this.joinFilter = Objects.requireNonNull(joinFilter);
        this.joinType = joinType;
        this.notInnerJoinConditions = Objects.requireNonNull(notInnerJoinConditions);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public List<Expression> getJoinFilter() {
        return joinFilter;
    }

    public List<Expression> getNotInnerJoinConditions() {
        return notInnerJoinConditions;
    }

    @Override
    public MultiJoin withChildren(List<Plan> children) {
        return new MultiJoin(children, joinFilter, joinType, notInnerJoinConditions);
    }

    @Override
    public List<Slot> computeOutput() {
        Builder<Slot> builder = ImmutableList.builder();

        if (joinType.isInnerOrCrossJoin()) {
            // INNER/CROSS
            for (Plan child : children) {
                builder.addAll(child.getOutput());
            }
            return builder.build();
        }

        // FULL OUTER JOIN
        if (joinType.isFullOuterJoin()) {
            for (Plan child : children) {
                builder.addAll(child.getOutput().stream()
                        .map(o -> o.withNullable(true))
                        .collect(Collectors.toList()));
            }
            return builder.build();
        }

        // RIGHT OUTER | RIGHT_SEMI/ANTI
        if (joinType.isRightJoin()) {
            // RIGHT OUTER
            if (joinType.isRightOuterJoin()) {
                builder.addAll(children.get(0).getOutput().stream()
                        .map(o -> o.withNullable(true))
                        .collect(Collectors.toList()));
            }
            for (int i = 1; i < children.size(); i++) {
                builder.addAll(children.get(i).getOutput());
            }

            return builder.build();
        }

        // LEFT OUTER | LEFT_SEMI/ANTI
        if (joinType.isLeftJoin()) {
            for (int i = 0; i < children.size() - 1; i++) {
                builder.addAll(children.get(i).getOutput());
            }
            // LEFT OUTER
            if (joinType.isLeftOuterJoin()) {
                builder.addAll(children.get(arity() - 1).getOutput().stream()
                        .map(o -> o.withNullable(true))
                        .collect(Collectors.toList()));
            }

            return builder.build();
        }

        throw new RuntimeException("unreachable");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        throw new RuntimeException("multiJoin can't invoke accept");
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new Builder<Expression>()
                .addAll(joinFilter)
                .addAll(notInnerJoinConditions)
                .build();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        throw new RuntimeException("multiJoin can't invoke withGroupExpression");
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        throw new RuntimeException("multiJoin can't invoke withLogicalProperties");
    }

    @Override
    public String toString() {
        return Utils.toSqlString("MultiJoin",
                "joinType", joinType,
                "joinFilter", joinFilter,
                "notInnerJoinConditions", notInnerJoinConditions
        );
    }
}
