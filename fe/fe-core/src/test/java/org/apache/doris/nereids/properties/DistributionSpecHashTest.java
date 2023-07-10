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

        Map<ExprId, Integer> requireMap = Maps.newHashMap();
        requireMap.put(new ExprId(1), 0);
        requireMap.put(new ExprId(4), 0);
        requireMap.put(new ExprId(3), 1);
        requireMap.put(new ExprId(5), 1);
        DistributionSpecHash join = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(5)),
                ShuffleType.REQUIRE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1), new ExprId(4)), Sets.newHashSet(new ExprId(3), new ExprId(5))),
                requireMap
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

        DistributionSpec after = original.project(projects, obstructions, DistributionSpecAny.INSTANCE);
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
        after = original.project(projects, obstructions, DistributionSpecAny.INSTANCE);
        Assertions.assertTrue(after instanceof DistributionSpecAny);
    }

    @Test
    public void testSatisfyAny() {
        DistributionSpec required = DistributionSpecAny.INSTANCE;
        DistributionSpecHash require = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.REQUIRE);
        DistributionSpecHash storageBucketed = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.STORAGE_BUCKETED);
        DistributionSpecHash executionBucketed = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.EXECUTION_BUCKETED);
        DistributionSpecHash natural = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.NATURAL);

        Assertions.assertTrue(require.satisfy(required));
        Assertions.assertTrue(storageBucketed.satisfy(required));
        Assertions.assertTrue(executionBucketed.satisfy(required));
        Assertions.assertTrue(natural.satisfy(required));
    }

    @Test
    public void testNotSatisfyOther() {
        DistributionSpecHash require = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.REQUIRE);
        DistributionSpecHash storageBucketed = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.STORAGE_BUCKETED);
        DistributionSpecHash executionBucketed = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.EXECUTION_BUCKETED);
        DistributionSpecHash natural = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.NATURAL);

        DistributionSpec gather = DistributionSpecGather.INSTANCE;
        Assertions.assertFalse(require.satisfy(gather));
        Assertions.assertFalse(storageBucketed.satisfy(gather));
        Assertions.assertFalse(executionBucketed.satisfy(gather));
        Assertions.assertFalse(natural.satisfy(gather));

        DistributionSpec replicated = DistributionSpecReplicated.INSTANCE;
        Assertions.assertFalse(require.satisfy(replicated));
        Assertions.assertFalse(storageBucketed.satisfy(replicated));
        Assertions.assertFalse(executionBucketed.satisfy(replicated));
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

        DistributionSpecHash require = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.REQUIRE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                natural2Map
        );

        DistributionSpecHash storageBucketed = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.STORAGE_BUCKETED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                natural2Map
        );

        DistributionSpecHash executionBucketed = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.EXECUTION_BUCKETED,
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
        Assertions.assertFalse(require.satisfy(natural2));
        Assertions.assertFalse(executionBucketed.satisfy(natural2));
        Assertions.assertFalse(storageBucketed.satisfy(natural2));
    }

    @Test
    public void testSatisfyRequiredHash() {
        Map<ExprId, Integer> require1Map = Maps.newHashMap();
        require1Map.put(new ExprId(0), 0);
        require1Map.put(new ExprId(1), 0);
        require1Map.put(new ExprId(2), 1);
        require1Map.put(new ExprId(3), 1);
        DistributionSpecHash require1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(2)),
                ShuffleType.REQUIRE,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0), new ExprId(1)), Sets.newHashSet(new ExprId(2), new ExprId(3))),
                require1Map
        );

        Map<ExprId, Integer> require2Map = Maps.newHashMap();
        require2Map.put(new ExprId(1), 0);
        require2Map.put(new ExprId(2), 1);
        DistributionSpecHash require2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.REQUIRE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                require2Map
        );

        Map<ExprId, Integer> require3Map = Maps.newHashMap();
        require3Map.put(new ExprId(2), 0);
        require3Map.put(new ExprId(1), 1);
        DistributionSpecHash require3 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2), new ExprId(1)),
                ShuffleType.REQUIRE,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(2)), Sets.newHashSet(new ExprId(1))),
                require3Map
        );

        DistributionSpecHash natural = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                require2Map
        );

        DistributionSpecHash storageBucketed = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.STORAGE_BUCKETED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                require2Map
        );

        DistributionSpecHash executionBucketed = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1), new ExprId(2)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                require2Map
        );

        // require is same order
        Assertions.assertTrue(require1.satisfy(require2));
        // require contains all sets but order is not same
        Assertions.assertTrue(require1.satisfy(require3));
        // require slots is not contained by target
        Assertions.assertFalse(require3.satisfy(require1));
        // other shuffle type with same order
        Assertions.assertTrue(natural.satisfy(require2));
        Assertions.assertTrue(executionBucketed.satisfy(require2));
        Assertions.assertTrue(storageBucketed.satisfy(require2));
        // other shuffle type contain all set but order is not same
        Assertions.assertTrue(natural.satisfy(require3));
        Assertions.assertTrue(executionBucketed.satisfy(require3));
        Assertions.assertTrue(storageBucketed.satisfy(require3));
    }

    @Test
    public void testHashEqualSatisfyWithDifferentLength() {
        Map<ExprId, Integer> bucketed1Map = Maps.newHashMap();
        bucketed1Map.put(new ExprId(0), 0);
        bucketed1Map.put(new ExprId(1), 1);
        bucketed1Map.put(new ExprId(2), 2);
        DistributionSpecHash bucketed1 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(1), new ExprId(2)),
                ShuffleType.EXECUTION_BUCKETED,
                0,
                Sets.newHashSet(0L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0)), Sets.newHashSet(new ExprId(1)), Sets.newHashSet(new ExprId(2))),
                bucketed1Map
        );

        Map<ExprId, Integer> bucketed2Map = Maps.newHashMap();
        bucketed2Map.put(new ExprId(0), 0);
        bucketed2Map.put(new ExprId(1), 1);
        DistributionSpecHash bucketed2 = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0), new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L),
                Lists.newArrayList(Sets.newHashSet(new ExprId(0)), Sets.newHashSet(new ExprId(1))),
                bucketed2Map
        );

        Assertions.assertFalse(bucketed1.satisfy(bucketed2));
        Assertions.assertFalse(bucketed2.satisfy(bucketed1));
    }
}
