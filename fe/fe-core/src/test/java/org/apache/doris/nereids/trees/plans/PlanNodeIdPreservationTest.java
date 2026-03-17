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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * Verifies that withXxx() copy methods on plan nodes preserve the original node's ObjectId.
 */
class PlanNodeIdPreservationTest {

    private LogicalOneRowRelation oneRow(int relationId) {
        return new LogicalOneRowRelation(
                new RelationId(relationId),
                ImmutableList.of(new Alias(Literal.of(1L), "a"))
        );
    }

    @Test
    void testLogicalProjectWithChildrenPreservesId() {
        LogicalOneRowRelation child = oneRow(1);
        List<NamedExpression> projects = ImmutableList.of(new Alias(Literal.of(1L), "x"));
        LogicalProject<Plan> original = new LogicalProject<>(projects, child);

        LogicalOneRowRelation newChild = oneRow(2);
        LogicalProject<Plan> copy = original.withChildren(ImmutableList.of(newChild));

        Assertions.assertEquals(original.getId(), copy.getId(),
                "withChildren should preserve the node id");
    }

    @Test
    void testLogicalProjectWithProjectsPreservesId() {
        LogicalOneRowRelation child = oneRow(1);
        List<NamedExpression> projects = ImmutableList.of(new Alias(Literal.of(1L), "x"));
        LogicalProject<Plan> original = new LogicalProject<>(projects, child);

        List<NamedExpression> newProjects = ImmutableList.of(new Alias(Literal.of(2L), "y"));
        LogicalProject<Plan> copy = original.withProjects(newProjects);

        Assertions.assertEquals(original.getId(), copy.getId(),
                "withProjects should preserve the node id");
    }

    @Test
    void testLogicalFilterWithChildrenPreservesId() {
        LogicalOneRowRelation child = oneRow(1);
        LogicalFilter<Plan> original = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(1))), child);

        LogicalOneRowRelation newChild = oneRow(2);
        LogicalFilter<Plan> copy = original.withChildren(ImmutableList.of(newChild));

        Assertions.assertEquals(original.getId(), copy.getId(),
                "withChildren on LogicalFilter should preserve the node id");
    }

    @Test
    void testLogicalJoinWithChildrenPreservesId() {
        LogicalOneRowRelation left = oneRow(1);
        LogicalOneRowRelation right = oneRow(2);
        LogicalJoin<Plan, Plan> original = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                left,
                right,
                new JoinReorderContext()
        );

        LogicalOneRowRelation newLeft = oneRow(3);
        LogicalOneRowRelation newRight = oneRow(4);
        LogicalJoin<Plan, Plan> copy = (LogicalJoin<Plan, Plan>) original.withChildren(
                ImmutableList.of(newLeft, newRight));

        Assertions.assertEquals(original.getId(), copy.getId(),
                "withChildren on LogicalJoin should preserve the node id");
    }

    @Test
    void testWithGroupExpressionPreservesId() {
        LogicalOneRowRelation child = oneRow(1);
        List<NamedExpression> projects = ImmutableList.of(new Alias(Literal.of(1L), "x"));
        LogicalProject<Plan> original = new LogicalProject<>(projects, child);

        LogicalProject<Plan> copy = original.withGroupExpression(Optional.empty());

        Assertions.assertEquals(original.getId(), copy.getId(),
                "withGroupExpression should preserve the node id");
    }

    @Test
    void testDistinctNodesHaveDifferentIds() {
        LogicalOneRowRelation child = oneRow(1);
        List<NamedExpression> projects = ImmutableList.of(new Alias(Literal.of(1L), "x"));
        LogicalProject<Plan> node1 = new LogicalProject<>(projects, child);
        LogicalProject<Plan> node2 = new LogicalProject<>(projects, child);

        Assertions.assertNotEquals(node1.getId(), node2.getId(),
                "independently constructed nodes must have different ids");
    }
}
