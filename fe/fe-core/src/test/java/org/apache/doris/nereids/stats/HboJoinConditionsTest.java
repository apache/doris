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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the key of an injected join expansion entry: the factors are keyed by the join
 * conditions, and two joins which only look alike must not share a factor.
 */
public class HboJoinConditionsTest {

    @Test
    public void testThePreservedSideIsPartOfAnOuterExpansionKey() {
        LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "b", 0);
        EqualTo equal = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
        // `A LEFT JOIN B` and `B LEFT JOIN A` keep the join type and the (sorted) equality pair, but they
        // preserve different rows and expand a different side
        String left = HboJoinConditions.canonicalOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), a, b, null)).get();
        String swapped = HboJoinConditions.canonicalOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), b, a, null)).get();
        Assertions.assertTrue(left.contains("sides:["), left);
        Assertions.assertNotEquals(left, swapped);
        Assertions.assertNotEquals(HboJoinConditions.fingerprintOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), a, b, null)),
                HboJoinConditions.fingerprintOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.LEFT_OUTER_JOIN,
                        ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), b, a, null)));
        // an inner join is commutative: the same pair with the sides swapped keeps its key
        Assertions.assertEquals(
                HboJoinConditions.canonicalOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.INNER_JOIN,
                        ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), a, b, null)),
                HboJoinConditions.canonicalOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.INNER_JOIN,
                        ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), b, a, null)));
    }

    @Test
    public void testTheLiteralsOfAResidualArePartOfTheExpansionKey() {
        LogicalOlapScan t = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        LogicalOlapScan u = PlanConstructor.newLogicalOlapScan(1, "u", 0);
        EqualTo equal = new EqualTo(t.getOutput().get(0), u.getOutput().get(0));
        Expression one = new GreaterThan(t.getOutput().get(1),
                new Add(u.getOutput().get(1), new IntegerLiteral(1)));
        Expression two = new GreaterThan(t.getOutput().get(1),
                new Add(u.getOutput().get(1), new IntegerLiteral(2)));
        String withOne = HboJoinConditions.canonicalOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.INNER_JOIN,
                ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(one), t, u, null)).get();
        String withTwo = HboJoinConditions.canonicalOf(new LogicalJoin<LogicalOlapScan, LogicalOlapScan>(JoinType.INNER_JOIN,
                ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(two), t, u, null)).get();
        // a residual decides how many partner rows a row finds, so its constant is never folded
        Assertions.assertTrue(withOne.contains("lit(1:INT)"), withOne);
        Assertions.assertNotEquals(withOne, withTwo);
    }
}
