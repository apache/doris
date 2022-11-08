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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class DistributionSpecHashTest {

    @Test
    public void testMerge() {
        Map<ExprId, Integer> naturalMap = Maps.newHashMap();
        naturalMap.put(new ExprId(0), 0);
        naturalMap.put(new ExprId(1), 0);
        naturalMap.put(new ExprId(2), 1);
        naturalMap.put(new ExprId(3), 1);
        DistributionSpecHash natural = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.NATURAL,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                naturalMap
        );

        Map<ExprId, Integer> joinMap = Maps.newHashMap();
        joinMap.put(new ExprId(1), 0);
        joinMap.put(new ExprId(4), 0);
        joinMap.put(new ExprId(3), 1);
        joinMap.put(new ExprId(5), 1);
        DistributionSpecHash join = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(5)),
                ShuffleType.JOIN,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1), new ExprId(4)), Sets.newHashSet(new ExprId(3), new ExprId(5))),
                joinMap
        );

        Map<ExprId, Integer> expectedMap = Maps.newHashMap();
        expectedMap.put(new ExprId(0), 0);
        expectedMap.put(new ExprId(1), 0);
        expectedMap.put(new ExprId(4), 0);
        expectedMap.put(new ExprId(2), 1);
        expectedMap.put(new ExprId(3), 1);
        expectedMap.put(new ExprId(5), 1);
        DistributionSpecHash expected = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.NATURAL,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1), new ExprId(4)), Sets.newHashSet(new ExprId(2), new ExprId(3), new ExprId(5))),
                expectedMap
        );

        Assertions.assertEquals(expected, DistributionSpecHash.merge(natural, join));
    }

    @Test
    public void testProject() {
        Map<ExprId, Integer> naturalMap = Maps.newHashMap();
        naturalMap.put(new ExprId(0), 0);
        naturalMap.put(new ExprId(1), 0);
        naturalMap.put(new ExprId(2), 1);
        naturalMap.put(new ExprId(3), 1);

        DistributionSpecHash original = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.NATURAL,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                naturalMap
        );

        Map<ExprId, ExprId> projects = Maps.newHashMap();
        projects.put(new ExprId(2), new ExprId(5));
        Set<ExprId> obstructions = Sets.newHashSet();

        DistributionSpec after = original.project(projects, obstructions);
        Assertions.assertTrue(after instanceof DistributionSpecHash);
        DistributionSpecHash afterHash = (DistributionSpecHash) after;
        Assertions.assertEquals(Lists.newArrayList(new ExprId(0), new ExprId(5)), afterHash.getOrderedShuffledColumns());
        Assertions.assertEquals(
                Lists.newArrayList(
                        Sets.newHashSet(new ExprId(0), new ExprId(1)),
                        Sets.newHashSet(new ExprId(5), new ExprId(3))),
                afterHash.getEquivalenceExprIds());
        Map<ExprId, Integer> actualMap = Maps.newHashMap();
        actualMap.put(new ExprId(0), 0);
        actualMap.put(new ExprId(1), 0);
        actualMap.put(new ExprId(5), 1);
        actualMap.put(new ExprId(3), 1);
        Assertions.assertEquals(actualMap, afterHash.getExprIdToEquivalenceSet());

        // have obstructions
        obstructions.add(new ExprId(3));
        after = original.project(projects, obstructions);
        Assertions.assertTrue(after instanceof DistributionSpecAny);
    }

    @Test
    public void testSatisfyAny() {
        DistributionSpec required = DistributionSpecAny.INSTANCE;
        DistributionSpecHash join = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.JOIN);
        DistributionSpecHash aggregate = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.AGGREGATE);
        DistributionSpecHash enforce = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.ENFORCED);
        DistributionSpecHash natural = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.NATURAL);

        Assertions.assertTrue(join.satisfy(required));
        Assertions.assertTrue(aggregate.satisfy(required));
        Assertions.assertTrue(enforce.satisfy(required));
        Assertions.assertTrue(natural.satisfy(required));
    }

    @Test
    public void testNotSatisfyOther() {
        DistributionSpecHash join = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.JOIN);
        DistributionSpecHash aggregate = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.AGGREGATE);
        DistributionSpecHash enforce = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.ENFORCED);
        DistributionSpecHash natural = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.NATURAL);

        DistributionSpec gather = DistributionSpecGather.INSTANCE;
        Assertions.assertFalse(join.satisfy(gather));
        Assertions.assertFalse(aggregate.satisfy(gather));
        Assertions.assertFalse(enforce.satisfy(gather));
        Assertions.assertFalse(natural.satisfy(gather));

        DistributionSpec replicated = DistributionSpecReplicated.INSTANCE;
        Assertions.assertFalse(join.satisfy(replicated));
        Assertions.assertFalse(aggregate.satisfy(replicated));
        Assertions.assertFalse(enforce.satisfy(replicated));
        Assertions.assertFalse(natural.satisfy(replicated));
    }

    @Test
    public void testSatisfyNaturalHash() {
        Map<ExprId, Integer> natural1Map = Maps.newHashMap();
        natural1Map.put(new ExprId(0), 0);
        natural1Map.put(new ExprId(1), 0);
        natural1Map.put(new ExprId(2), 1);
        natural1Map.put(new ExprId(3), 1);
        DistributionSpecHash natural1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.NATURAL,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                natural1Map
        );

        Map<ExprId, Integer> natural2Map = Maps.newHashMap();
        natural2Map.put(new ExprId(1), 0);
        natural2Map.put(new ExprId(2), 1);
        DistributionSpecHash natural2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                natural2Map
        );

        Map<ExprId, Integer> natural3Map = Maps.newHashMap();
        natural3Map.put(new ExprId(2), 0);
        natural3Map.put(new ExprId(1), 1);
        DistributionSpecHash natural3 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2), new ExprId(1)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(2)), Sets.newHashSet(new ExprId(1))),
                natural3Map
        );

        DistributionSpecHash join = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.JOIN,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                natural2Map
        );

        DistributionSpecHash enforce = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                natural2Map
        );

        DistributionSpecHash aggregate = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.AGGREGATE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                natural2Map
        );

        // require is same order
        Assertions.assertTrue(natural1.satisfy(natural2));
        // require contains all sets but order is not same
        Assertions.assertFalse(natural1.satisfy(natural3));
        // require slots is not contained by target
        Assertions.assertFalse(natural2.satisfy(natural1));
        // other shuffle type with same order
        Assertions.assertFalse(join.satisfy(natural2));
        Assertions.assertFalse(aggregate.satisfy(natural2));
        Assertions.assertFalse(enforce.satisfy(natural2));
    }

    @Test
    public void testSatisfyJoinHash() {
        Map<ExprId, Integer> join1Map = Maps.newHashMap();
        join1Map.put(new ExprId(0), 0);
        join1Map.put(new ExprId(1), 0);
        join1Map.put(new ExprId(2), 1);
        join1Map.put(new ExprId(3), 1);
        DistributionSpecHash join1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.JOIN,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                join1Map
        );

        Map<ExprId, Integer> join2Map = Maps.newHashMap();
        join2Map.put(new ExprId(1), 0);
        join2Map.put(new ExprId(2), 1);
        DistributionSpecHash join2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.JOIN,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                join2Map
        );

        Map<ExprId, Integer> join3Map = Maps.newHashMap();
        join3Map.put(new ExprId(2), 0);
        join3Map.put(new ExprId(1), 1);
        DistributionSpecHash join3 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2), new ExprId(1)),
                ShuffleType.JOIN,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(2)), Sets.newHashSet(new ExprId(1))),
                join3Map
        );

        DistributionSpecHash natural = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                join2Map
        );

        DistributionSpecHash enforce = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                join2Map
        );

        DistributionSpecHash aggregate = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.AGGREGATE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                join2Map
        );

        // require is same order
        Assertions.assertTrue(join1.satisfy(join2));
        // require contains all sets but order is not same
        Assertions.assertFalse(join1.satisfy(join3));
        // require slots is not contained by target
        Assertions.assertFalse(join3.satisfy(join1));
        // other shuffle type with same order
        Assertions.assertTrue(natural.satisfy(join2));
        Assertions.assertTrue(aggregate.satisfy(join2));
        Assertions.assertTrue(enforce.satisfy(join2));
        // other shuffle type contain all set but order is not same
        Assertions.assertFalse(natural.satisfy(join3));
        Assertions.assertFalse(aggregate.satisfy(join3));
        Assertions.assertFalse(enforce.satisfy(join3));
    }

    @Test
    public void testSatisfyAggregateHash() {
        Map<ExprId, Integer> aggregate1Map = Maps.newHashMap();
        aggregate1Map.put(new ExprId(0), 0);
        aggregate1Map.put(new ExprId(1), 0);
        aggregate1Map.put(new ExprId(2), 1);
        aggregate1Map.put(new ExprId(3), 1);
        DistributionSpecHash aggregate1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.AGGREGATE,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                aggregate1Map
        );

        Map<ExprId, Integer> aggregate2Map = Maps.newHashMap();
        aggregate2Map.put(new ExprId(1), 0);
        aggregate2Map.put(new ExprId(2), 1);
        DistributionSpecHash aggregate2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.AGGREGATE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                aggregate2Map
        );

        Map<ExprId, Integer> aggregate3Map = Maps.newHashMap();
        aggregate3Map.put(new ExprId(2), 0);
        aggregate3Map.put(new ExprId(1), 1);
        DistributionSpecHash aggregate3 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2), new ExprId(1)),
                ShuffleType.AGGREGATE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(2)), Sets.newHashSet(new ExprId(1))),
                aggregate3Map
        );

        Map<ExprId, Integer> aggregate4Map = Maps.newHashMap();
        aggregate4Map.put(new ExprId(2), 0);
        aggregate4Map.put(new ExprId(3), 1);
        DistributionSpecHash aggregate4 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2), new ExprId(3)),
                ShuffleType.AGGREGATE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(2)), Sets.newHashSet(new ExprId(3))),
                aggregate4Map
        );

        DistributionSpecHash natural = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                aggregate2Map
        );

        DistributionSpecHash enforce = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                aggregate2Map
        );

        DistributionSpecHash join = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.JOIN,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                aggregate2Map
        );

        // require is same order
        Assertions.assertTrue(aggregate1.satisfy(aggregate2));
        // require contains all sets but order is not same
        Assertions.assertTrue(aggregate1.satisfy(aggregate3));
        // require do not contain all set but has the same number slot
        Assertions.assertFalse(aggregate1.satisfy(aggregate4));
        // require slots is not contained by target
        Assertions.assertFalse(aggregate3.satisfy(aggregate1));
        // other shuffle type with same order
        Assertions.assertTrue(natural.satisfy(aggregate2));
        Assertions.assertTrue(join.satisfy(aggregate2));
        Assertions.assertTrue(enforce.satisfy(aggregate2));
        // other shuffle type contain all set but order is not same
        Assertions.assertTrue(natural.satisfy(aggregate3));
        Assertions.assertTrue(join.satisfy(aggregate3));
        Assertions.assertTrue(enforce.satisfy(aggregate3));
    }

    @Test
    public void testSatisfyEnforceHash() {
        Map<ExprId, Integer> enforce1Map = Maps.newHashMap();
        enforce1Map.put(new ExprId(0), 0);
        enforce1Map.put(new ExprId(1), 0);
        enforce1Map.put(new ExprId(2), 1);
        enforce1Map.put(new ExprId(3), 1);
        DistributionSpecHash enforce1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.ENFORCED,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                enforce1Map
        );

        Map<ExprId, Integer> enforce2Map = Maps.newHashMap();
        enforce2Map.put(new ExprId(1), 0);
        enforce2Map.put(new ExprId(2), 1);
        DistributionSpecHash enforce2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                enforce2Map
        );

        Map<ExprId, Integer> enforce3Map = Maps.newHashMap();
        enforce3Map.put(new ExprId(2), 0);
        enforce3Map.put(new ExprId(1), 1);
        DistributionSpecHash enforce3 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2), new ExprId(1)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(2)), Sets.newHashSet(new ExprId(1))),
                enforce3Map
        );

        DistributionSpecHash join = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.JOIN,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                enforce2Map
        );

        DistributionSpecHash natural = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                enforce2Map
        );

        DistributionSpecHash aggregate = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.AGGREGATE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                enforce2Map
        );

        // require is same order
        Assertions.assertTrue(enforce1.satisfy(enforce2));
        // require contains all sets but order is not same
        Assertions.assertFalse(enforce1.satisfy(enforce3));
        // require slots is not contained by target
        Assertions.assertFalse(enforce2.satisfy(enforce1));
        // other shuffle type with same order
        Assertions.assertTrue(join.satisfy(enforce2));
        Assertions.assertTrue(aggregate.satisfy(enforce2));
        Assertions.assertTrue(natural.satisfy(enforce2));
    }

    @Test
    public void testHashEqualSatisfyWithDifferentLength() {
        Map<ExprId, Integer> enforce1Map = Maps.newHashMap();
        enforce1Map.put(new ExprId(0), 0);
        enforce1Map.put(new ExprId(1), 1);
        enforce1Map.put(new ExprId(2), 2);
        DistributionSpecHash enforce1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(1), new ExprId(2)),
                ShuffleType.ENFORCED,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0)), Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                enforce1Map
        );

        Map<ExprId, Integer> enforce2Map = Maps.newHashMap();
        enforce2Map.put(new ExprId(0), 0);
        enforce2Map.put(new ExprId(1), 1);
        DistributionSpecHash enforce2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(1)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0)), Sets.newHashSet(new ExprId(1))),
                enforce2Map
        );

        Assertions.assertFalse(enforce1.satisfy(enforce2));
        Assertions.assertFalse(enforce2.satisfy(enforce1));
    }
}
