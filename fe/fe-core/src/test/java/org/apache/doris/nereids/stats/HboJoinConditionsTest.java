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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the key of an injected join expansion entry: the factors are keyed by the join type, its
 * conditions and the columns of the tables it reads, so two joins which only look alike must not share a
 * factor, and a join whose operands are not table columns has no key at all.
 */
public class HboJoinConditionsTest {

    private static final String CHARSET = "UTF-8";

    @Test
    public void testThePreservedSideIsPartOfAnOuterExpansionKey() {
        LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "b", 0);
        EqualTo equal = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
        Assertions.assertNotEquals(
                canonical(JoinType.LEFT_OUTER_JOIN, equal, a, b),
                canonical(JoinType.LEFT_OUTER_JOIN, equal, b, a));
        // an inner join is commutative: the same pair with the sides swapped keeps its key
        Assertions.assertEquals(
                canonical(JoinType.INNER_JOIN, equal, a, b),
                canonical(JoinType.INNER_JOIN, equal, b, a));
    }

    @Test
    public void testTheLiteralsOfAResidualArePartOfTheExpansionKey() {
        LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "b", 0);
        EqualTo equal = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> one = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.<Expression>of(equal),
                ImmutableList.<Expression>of(new GreaterThan(a.getOutput().get(1),
                        new Add(b.getOutput().get(1), new IntegerLiteral(1)))), a, b, null);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> many = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.<Expression>of(equal),
                ImmutableList.<Expression>of(new GreaterThan(a.getOutput().get(1),
                        new Add(b.getOutput().get(1), new IntegerLiteral(1000)))), a, b, null);
        Assertions.assertNotEquals(HboJoinConditions.canonicalOf(one), HboJoinConditions.canonicalOf(many));
        Assertions.assertNotEquals(HboJoinConditions.fingerprintOf(one), HboJoinConditions.fingerprintOf(many));
    }

    @Test
    public void testTheAliasesDoNotHideTheTablesOfAnInnerExpansion() {
        // `t1 AS a JOIN t2 AS b` and `t3 AS a JOIN t4 AS b` render the same `a.k = b.k`, while the two
        // joins read other tables and expand differently
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0).withTableAlias("a");
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0).withTableAlias("b");
        LogicalOlapScan t3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0).withTableAlias("a");
        LogicalOlapScan t4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0).withTableAlias("b");
        String first = canonical(JoinType.INNER_JOIN,
                new EqualTo(t1.getOutput().get(0), t2.getOutput().get(0)), t1, t2);
        String second = canonical(JoinType.INNER_JOIN,
                new EqualTo(t3.getOutput().get(0), t4.getOutput().get(0)), t3, t4);
        Assertions.assertNotEquals(first, second);
        Assertions.assertTrue(first.contains("operands:[null.null.t1"), first);
    }

    @Test
    public void testAComputedOperandHasNoExpansionKey() {
        LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "b", 0);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> derived = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.<Expression>of(new EqualTo(
                        new Cast(a.getOutput().get(0), BigIntType.INSTANCE), b.getOutput().get(0))),
                ImmutableList.<Expression>of(), a, b, null);
        Assertions.assertFalse(HboJoinConditions.canonicalOf(derived).isPresent());
        Assertions.assertFalse(HboJoinConditions.fingerprintOf(derived).isPresent());
    }

    @Test
    public void testTheExpansionKeyIsNotTheTextHash() throws Exception {
        LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "b", 0);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.<Expression>of(new EqualTo(a.getOutput().get(0), b.getOutput().get(0))),
                ImmutableList.<Expression>of(), a, b, null);
        String canonical = HboJoinConditions.canonicalOf(join).get();
        Assertions.assertNotEquals(Hashing.sha256().hashString(canonical, java.nio.charset.StandardCharsets.UTF_8)
                .toString(), HboJoinConditions.fingerprintOf(join).get());
    }

    @Test
    public void testTheSideOfAnOuterSelfJoinIsPartOfTheExpansionKey() {
        // `t a LEFT JOIN t b ON a.k = b.v` and the same join on `a.v = b.k` read the same table and the
        // same columns, yet they keep a different number of rows: the operand of the left side is written
        // first, so the two pairs stay apart
        LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "t", 0);
        String leftV = canonical(JoinType.LEFT_OUTER_JOIN,
                new EqualTo(a.getOutput().get(0), b.getOutput().get(1)), a, b);
        String rightV = canonical(JoinType.LEFT_OUTER_JOIN,
                new EqualTo(a.getOutput().get(1), b.getOutput().get(0)), a, b);
        Assertions.assertNotEquals(leftV, rightV, "the reversed columns of an outer self join share a key");
        // the same pair written in either order keeps one key
        Assertions.assertEquals(leftV, canonical(JoinType.LEFT_OUTER_JOIN,
                new EqualTo(b.getOutput().get(1), a.getOutput().get(0)), a, b));
        // an inner join is commutative: its count does not depend on which occurrence is the left one
        Assertions.assertEquals(
                canonical(JoinType.INNER_JOIN, new EqualTo(a.getOutput().get(0), b.getOutput().get(1)), a, b),
                canonical(JoinType.INNER_JOIN, new EqualTo(a.getOutput().get(1), b.getOutput().get(0)), a, b));
    }

    private static String canonical(JoinType type, EqualTo equal, LogicalOlapScan left, LogicalOlapScan right) {
        return HboJoinConditions.canonicalOf(new LogicalJoin<>(type,
                ImmutableList.<Expression>of(equal), ImmutableList.<Expression>of(), left, right, null)).get();
    }
}
