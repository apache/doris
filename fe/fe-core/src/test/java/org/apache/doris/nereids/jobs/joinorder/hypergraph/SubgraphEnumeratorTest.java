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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmapSubsetIterator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.Counter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.HyperGraphBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;

public class SubgraphEnumeratorTest {
    @Test
    void testStarQuery() {
        //      t2
        //      |
        //t3-- t0 -- t4
        //      |
        //     t1
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
        long fullSet = LongBitmap.newBitmapBetween(0, 5);
        HashMap<Long, Integer> cache = new HashMap<>();
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
        long fullSet = LongBitmap.newBitmapBetween(0, 4);
        Counter counter = new Counter();
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
        subgraphEnumerator.enumerate();
        HashMap<Long, Integer> cache = new HashMap<>();
        countAndCheck(fullSet, hyperGraph, counter.getAllCount(), cache);
    }

    @Test
    void testRandomQuery() {
        int tableNum = 10;
        int edgeNum = 40;
        long fullSet = LongBitmap.newBitmapBetween(0, tableNum);
        for (int i = 0; i < 10; i++) {
            HyperGraph hyperGraph = new HyperGraphBuilder().randomBuildWith(tableNum, edgeNum);
            Counter counter = new Counter();
            SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
            subgraphEnumerator.enumerate();
            HashMap<Long, Integer> cache = new HashMap<>();
            countAndCheck(fullSet, hyperGraph, counter.getAllCount(), cache);
        }
    }

    @Test
    void testTime() {
        int tableNum = 20;
        int edgeNum = 21;
        HyperGraph hyperGraph = new HyperGraphBuilder().randomBuildWith(tableNum, edgeNum);

        Counter counter = new Counter();
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
        double startTime = System.currentTimeMillis();
        subgraphEnumerator.enumerate();
        double endTime = System.currentTimeMillis();
        System.out.println(
                String.format("enumerate %d tables %d edges cost %f ms", tableNum, edgeNum, endTime - startTime));
    }

    private int countAndCheck(long bitmap, HyperGraph hyperGraph, HashMap<Long, Integer> counter,
            HashMap<Long, Integer> cache) {
        if (cache.containsKey(bitmap)) {
            return cache.get(bitmap);
        }
        if (LongBitmap.getCardinality(bitmap) == 1) {
            Assertions.assertEquals(counter.get(bitmap), 1,
                    String.format("The csg-cmp pairs of %s should be %d rather than %s", bitmap, 1,
                            counter.get(bitmap)));
            cache.put(bitmap, 1);
            return 1;
        }
        LongBitmapSubsetIterator subsetIterator = new LongBitmapSubsetIterator(bitmap);
        int count = 0;
        HashSet<Long> visited = new HashSet<>();
        for (long subset : subsetIterator) {
            long left = subset;
            long right = LongBitmap.clone(bitmap);
            right = LongBitmap.andNot(right, left);
            if (visited.contains(left) || visited.contains(right)) {
                continue;
            }
            visited.add(left);
            visited.add(right);
            for (Edge edge : hyperGraph.getJoinEdges()) {
                if ((LongBitmap.isSubset(edge.getLeftExtendedNodes(), left) && LongBitmap.isSubset(edge.getRightExtendedNodes(), right)) || (
                        LongBitmap.isSubset(edge.getLeftExtendedNodes(), right) && LongBitmap.isSubset(edge.getRightExtendedNodes(), left))) {
                    count += countAndCheck(left, hyperGraph, counter, cache) * countAndCheck(right, hyperGraph,
                            counter, cache);
                    break;
                }
            }
        }
        if (count == 0) {
            Assertions.assertEquals(counter.get(bitmap), null,
                    String.format("The plan %s should be invalid", bitmap));
        } else {
            Assertions.assertEquals(counter.get(bitmap), count,
                    String.format("The csg-cmp pairs of %s should be %d rather than %d", bitmap, count,
                            counter.get(bitmap)));
        }
        cache.put(bitmap, count);
        return count;
    }
}
