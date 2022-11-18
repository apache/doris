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

import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.HyperGraphBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class GraphSimplifierTest {
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
        GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
        graphSimplifier.initFirstStep();
        while (graphSimplifier.applySimplificationStep()) {
        }
        List<Edge> edges = hyperGraph.getEdges();
        Assertions.assertEquals(edges.get(0).toString(), "<{0} - {1}>");
        Assertions.assertEquals(edges.get(1).toString(), "<{0, 1} - {2}>");
        Assertions.assertEquals(edges.get(1).toString(), "<{0, 1} - {2}>");
        Assertions.assertEquals(edges.get(3).toString(), "<{0, 1, 2, 3} - {4}>");
    }

    @Test
    void testCircleGraph() {
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
        GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
        graphSimplifier.initFirstStep();
        while (graphSimplifier.applySimplificationStep()) {
        }
        List<Edge> edges = hyperGraph.getEdges();
        Assertions.assertEquals(edges.get(0).toString(), "<{0} - {1}>");
        Assertions.assertEquals(edges.get(1).toString(), "<{0, 1} - {2}>");
        Assertions.assertEquals(edges.get(2).toString(), "<{0} - {1, 2, 3}>");
        Assertions.assertEquals(edges.get(3).toString(), "<{1} - {0, 2}>");
        Assertions.assertEquals(edges.get(4).toString(), "<{0, 1, 2} - {3}>");
    }

    @Test
    void testClique() {
        //    .--t0\
        //   /    | \
        //   |   t1- t3
        //   \    | /
        //    `--t2/
        HyperGraph hyperGraph = new HyperGraphBuilder()
                .init(10, 20, 30, 40)
                .addEdge(JoinType.INNER_JOIN, 0, 1)
                .addEdge(JoinType.INNER_JOIN, 0, 2)
                .addEdge(JoinType.INNER_JOIN, 0, 3)
                .addEdge(JoinType.INNER_JOIN, 1, 2)
                .addEdge(JoinType.INNER_JOIN, 1, 3)
                .addEdge(JoinType.INNER_JOIN, 2, 3)
                .build();
        GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
        graphSimplifier.initFirstStep();
        while (graphSimplifier.applySimplificationStep()) {
        }
        List<Edge> edges = hyperGraph.getEdges();
        Assertions.assertEquals(edges.get(0).toString(), "<{0} - {1}>");
        Assertions.assertEquals(edges.get(1).toString(), "<{0, 1} - {2}>");
        Assertions.assertEquals(edges.get(2).toString(), "<{0} - {1, 2, 3}>");
        Assertions.assertEquals(edges.get(3).toString(), "<{1} - {0, 2}>");
        Assertions.assertEquals(edges.get(4).toString(), "<{1} - {0, 2, 3}>");
        Assertions.assertEquals(edges.get(5).toString(), "<{0, 1, 2} - {3}>");
    }

    @Test
    void testHugeStar() {
        //  t11 t3 t4 t5 t12
        //    `  \ | / '
        //    t1--t0--t2
        //    '  / | \  `
        //   t9 t6 t7 t8  t10
        HyperGraph hyperGraph = new HyperGraphBuilder()
                .init(10, 20, 30, 40, 50, 70, 60, 80, 90, 100, 110, 120)
                .addEdge(JoinType.INNER_JOIN, 0, 1)
                .addEdge(JoinType.INNER_JOIN, 0, 2)
                .addEdge(JoinType.INNER_JOIN, 0, 3)
                .addEdge(JoinType.INNER_JOIN, 0, 4)
                .addEdge(JoinType.INNER_JOIN, 0, 5)
                .addEdge(JoinType.INNER_JOIN, 0, 6)
                .addEdge(JoinType.INNER_JOIN, 0, 7)
                .addEdge(JoinType.INNER_JOIN, 0, 8)
                .addEdge(JoinType.INNER_JOIN, 0, 9)
                .addEdge(JoinType.INNER_JOIN, 0, 10)
                .addEdge(JoinType.INNER_JOIN, 0, 11)
                .build();
        GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
        graphSimplifier.initFirstStep();
        while (graphSimplifier.applySimplificationStep()) {
        }
        List<Edge> edges = hyperGraph.getEdges();
        Assertions.assertEquals(edges.get(0).toString(), "<{0} - {1}>");
        Assertions.assertEquals(edges.get(1).toString(), "<{0, 1} - {2}>");
        Assertions.assertEquals(edges.get(2).toString(), "<{0, 1, 2} - {3}>");
        Assertions.assertEquals(edges.get(3).toString(), "<{0, 1, 2, 3} - {4}>");
        Assertions.assertEquals(edges.get(4).toString(), "<{0, 1, 2, 3, 4, 6} - {5}>");
        Assertions.assertEquals(edges.get(5).toString(), "<{0, 1, 2, 3, 4} - {6}>");
        Assertions.assertEquals(edges.get(6).toString(), "<{0, 1, 2, 3, 4, 5, 6} - {7}>");
        Assertions.assertEquals(edges.get(7).toString(), "<{0, 1, 2, 3, 4, 5, 6, 7} - {8}>");
        Assertions.assertEquals(edges.get(8).toString(), "<{0, 1, 2, 3, 4, 5, 6, 7, 8} - {9}>");
        Assertions.assertEquals(edges.get(9).toString(), "<{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} - {10}>");
        Assertions.assertEquals(edges.get(10).toString(), "<{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10} - {11}>");
    }
}
