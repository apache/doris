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

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class InferPredicatesMarkJoinTest implements MemoPatternMatchSupported {

    @Test
    void testInferPredicateIntoRightSideOfMarkJoin() {
        // Plan shape equivalent to:
        // SELECT p.id, CASE WHEN EXISTS (SELECT 1 FROM score s WHERE s.id = p.id) THEN 1 ELSE 0 END
        // FROM student p WHERE p.id = 1
        // The EXISTS is used in the outer CASE expression, so the correlated apply is converted
        // into a mark join. InferPredicates must still combine the outer constant with the join
        // condition and push the derived predicate (s.id = 1) into the right side's scan.
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "student", 1);
        Slot leftId = left.getOutput().get(0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "score", 1);
        Slot rightId = right.getOutput().get(0);

        LogicalFilter<LogicalOlapScan> leftFilter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(leftId, new IntegerLiteral(1))), left);
        LogicalJoin<LogicalFilter<LogicalOlapScan>, LogicalOlapScan> markJoin = new LogicalJoin<>(
                JoinType.LEFT_SEMI_JOIN,
                ImmutableList.of(new EqualTo(rightId, leftId)),
                ImmutableList.of(),
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("mark_join_slot")),
                leftFilter, right, null);

        PlanChecker.from(MemoTestUtils.createConnectContext(), markJoin)
                .customRewrite(new InferPredicates())
                .matches(
                        logicalJoin(
                                logicalFilter(logicalOlapScan())
                                        .when(f -> f.getPredicate().toSql().contains("id = 1")),
                                logicalFilter(logicalOlapScan())
                                        .when(f -> f.getPredicate().toSql().contains("id = 1"))
                        )
                );
    }

    @Test
    void testDoNotInferPredicateIntoLeftSideOfMarkJoin() {
        // A mark join keeps all left rows and only appends the mark column, so a predicate
        // derived from the right side (s.id = 5) must NOT be pushed into the left side:
        // filtering the left side would drop rows and change the query result.
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "student", 1);
        Slot leftId = left.getOutput().get(0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "score", 1);
        Slot rightId = right.getOutput().get(0);

        LogicalFilter<LogicalOlapScan> rightFilter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(rightId, new IntegerLiteral(5))), right);
        LogicalJoin<LogicalOlapScan, LogicalFilter<LogicalOlapScan>> markJoin = new LogicalJoin<>(
                JoinType.LEFT_SEMI_JOIN,
                ImmutableList.of(new EqualTo(rightId, leftId)),
                ImmutableList.of(),
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("mark_join_slot")),
                left, rightFilter, null);

        PlanChecker.from(MemoTestUtils.createConnectContext(), markJoin)
                .customRewrite(new InferPredicates())
                .matches(
                        logicalJoin(
                                // left side keeps all rows: no inferred filter is pushed into it
                                logicalOlapScan(),
                                logicalFilter(logicalOlapScan())
                        )
                );
    }
}
