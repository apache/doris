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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.SubsetIterator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.Counter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.HyperGraphBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;

public class SubgraphEnumeratorTest {
    @Test
    void testStarQuery() {
        //      t2
        //      |
        //t3-- t1 -- t4
        //      |
        //     t5
        HyperGraph hyperGraph = new HyperGraphBuilder()
                .init(10, 20, 30, 40, 50)
                .addEdge(JoinType.INNER_JOIN, 0, 1)
                .addEdge(JoinType.INNER_JOIN, 0, 2)
                .addEdge(JoinType.INNER_JOIN, 0, 3)
                .addEdge(JoinType.INNER_JOIN, 0, 4)
                .build();
        Counter counter = new Counter();
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
        subgraphEnumerator.enumerate();
        BitSet fullSet = new BitSet();
        fullSet.set(0, 5);
        HashMap<BitSet, Integer> cache = new HashMap<>();
        countAndCheck(fullSet, hyperGraph, counter.getAllCount(), cache);
    }

    @Test
    void testCircleQuery() {
        //    .--t0\
        //   /    | \
        //   |   t1  t3
        //   \    | /
        //    `--t2/
        HyperGraph hyperGraph = new HyperGraphBuilder()
                .init(10, 20, 30, 40)
                .addEdge(JoinType.INNER_JOIN, 0, 1)
                .addEdge(JoinType.INNER_JOIN, 0, 2)
                .addEdge(JoinType.INNER_JOIN, 0, 3)
                .addEdge(JoinType.INNER_JOIN, 1, 2)
                .addEdge(JoinType.INNER_JOIN, 2, 3)
                .build();
        BitSet fullSet = new BitSet();
        fullSet.set(0, 4);
        Counter counter = new Counter();
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
        subgraphEnumerator.enumerate();
        HashMap<BitSet, Integer> cache = new HashMap<>();
        countAndCheck(fullSet, hyperGraph, counter.getAllCount(), cache);
    }

    @Test
    void testRandomQuery() {
        int tableNum = 10;
        int edgeNum = 40;
        BitSet fullSet = new BitSet();
        fullSet.set(0, tableNum);
        for (int i = 0; i < 10; i++) {
            HyperGraph hyperGraph = new HyperGraphBuilder().randomBuildWith(tableNum, edgeNum);
            Counter counter = new Counter();
            SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
            subgraphEnumerator.enumerate();
            HashMap<BitSet, Integer> cache = new HashMap<>();
            countAndCheck(fullSet, hyperGraph, counter.getAllCount(), cache);
        }
    }

    @Test
    void testTime() {
        int tableNum = 10;
        int edgeNum = 40;
        HyperGraph hyperGraph = new HyperGraphBuilder().randomBuildWith(tableNum, edgeNum);

        Counter counter = new Counter();
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
        double startTime = System.currentTimeMillis();
        subgraphEnumerator.enumerate();
        double endTime = System.currentTimeMillis();
        System.out.println(
                String.format("enumerate %d tables %d edges cost %f ms", tableNum, edgeNum, endTime - startTime));
    }

    private int countAndCheck(BitSet bitSet, HyperGraph hyperGraph, HashMap<BitSet, Integer> counter,
            HashMap<BitSet, Integer> cache) {
        if (cache.containsKey(bitSet)) {
            return cache.get(bitSet);
        }
        if (bitSet.cardinality() == 1) {
            Assertions.assertEquals(counter.get(bitSet), 1,
                    String.format("The csg-cmp pairs of %s should be %d rather than %s", bitSet, 1,
                            counter.get(bitSet)));
            cache.put(bitSet, 1);
            return 1;
        }
        SubsetIterator subsetIterator = new SubsetIterator(bitSet);
        int count = 0;
        HashSet<BitSet> visited = new HashSet<>();
        for (BitSet subset : subsetIterator) {
            BitSet left = subset;
            BitSet right = new BitSet();
            right.or(bitSet);
            right.andNot(left);
            if (visited.contains(left) || visited.contains(right)) {
                continue;
            }
            visited.add(left);
            visited.add(right);
            for (Edge edge : hyperGraph.getEdges()) {
                if ((Bitmap.isSubset(edge.getLeft(), left) && Bitmap.isSubset(edge.getRight(), right)) || (
                        Bitmap.isSubset(edge.getLeft(), right) && Bitmap.isSubset(edge.getRight(), left))) {
                    count += countAndCheck(left, hyperGraph, counter, cache) * countAndCheck(right, hyperGraph,
                            counter, cache);
                    break;
                }
            }
        }
        if (count == 0) {
            Assertions.assertEquals(counter.get(bitSet), null,
                    String.format("The plan %s should be invalid", bitSet));
        } else {
            Assertions.assertEquals(counter.get(bitSet), count,
                    String.format("The csg-cmp pairs of %s should be %d rather than %d", bitSet, count,
                            counter.get(bitSet)));
        }
        cache.put(bitSet, count);
        return count;
    }
}
