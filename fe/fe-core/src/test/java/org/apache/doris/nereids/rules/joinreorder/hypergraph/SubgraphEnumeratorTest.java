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

package org.apache.doris.nereids.rules.joinreorder.hypergraph;

import org.apache.doris.nereids.rules.joinreorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.receiver.Counter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.HyperGraphBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(1)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(2)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(3)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(4)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 2)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 3)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 4)), 1);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 2)), 2);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 3)), 2);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 4)), 2);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 2, 3)), 2);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 2, 4)), 2);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 3, 4)), 2);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 2, 3)), 3);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 2, 4)), 3);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 3, 4)), 3);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 2, 3, 4)), 3);
        Assertions.assertEquals(counter.getCount(Bitmap.newBitmap(0, 1, 2, 3, 4)), 4);
    }

    @Test
    void testTime() {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
        // generate 14 tables and 90 edges
        int tableNum = 5;
        int edgeNum = 10;
        int[] rowCounts = new int[tableNum];
        for (int i = 0; i < tableNum; i++) {
            rowCounts[i] = i + 1;
        }
        hyperGraphBuilder.init(rowCounts);

        int left = 0;
        int right = 1;
        for (int i = 0; i < edgeNum; i++) {
            hyperGraphBuilder.addEdge(JoinType.INNER_JOIN, left, right);
            left += (right + 1) / tableNum;
            right = (right + 1) % tableNum;
            if (left == right) {
                left += (right + 1) / tableNum;
                right = (right + 1) % tableNum;
            }
        }

        HyperGraph hyperGraph = hyperGraphBuilder.build();
        Assertions.assertEquals(hyperGraph.getEdges().size(), edgeNum);
        Assertions.assertEquals(hyperGraph.getNodes().size(), tableNum);

        Counter counter = new Counter();
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(counter, hyperGraph);
        double startTime = System.currentTimeMillis();
        subgraphEnumerator.enumerate();
        double endTime = System.currentTimeMillis();
        System.out.println(
                String.format("enumerate %d tables %d edges cost %f ms", tableNum, edgeNum, endTime - startTime));
    }
}
