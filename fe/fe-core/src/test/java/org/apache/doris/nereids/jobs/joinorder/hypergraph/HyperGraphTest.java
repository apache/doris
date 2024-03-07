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

import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.HyperGraphBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HyperGraphTest {
    @Test
    void testStarGraph() {
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
        String dottyGraph = hyperGraph.toDottyHyperGraph();
        String target = "digraph G {  # 4 edges\n"
                + "  LOGICAL_OLAP_SCAN0 [label=\"LOGICAL_OLAP_SCAN0 \n"
                + " rowCount=10.00\"];\n"
                + "  LOGICAL_OLAP_SCAN1 [label=\"LOGICAL_OLAP_SCAN1 \n"
                + " rowCount=20.00\"];\n"
                + "  LOGICAL_OLAP_SCAN2 [label=\"LOGICAL_OLAP_SCAN2 \n"
                + " rowCount=30.00\"];\n"
                + "  LOGICAL_OLAP_SCAN3 [label=\"LOGICAL_OLAP_SCAN3 \n"
                + " rowCount=40.00\"];\n"
                + "  LOGICAL_OLAP_SCAN4 [label=\"LOGICAL_OLAP_SCAN4 \n"
                + " rowCount=50.00\"];\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN1 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN2 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN3 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN4 [label=\"1.00\",arrowhead=none]\n"
                + "}\n";

        Assertions.assertEquals(dottyGraph, target);
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
        String dottyGraph = hyperGraph.toDottyHyperGraph();
        String target = "digraph G {  # 5 edges\n"
                + "  LOGICAL_OLAP_SCAN0 [label=\"LOGICAL_OLAP_SCAN0 \n"
                + " rowCount=10.00\"];\n"
                + "  LOGICAL_OLAP_SCAN1 [label=\"LOGICAL_OLAP_SCAN1 \n"
                + " rowCount=20.00\"];\n"
                + "  LOGICAL_OLAP_SCAN2 [label=\"LOGICAL_OLAP_SCAN2 \n"
                + " rowCount=30.00\"];\n"
                + "  LOGICAL_OLAP_SCAN3 [label=\"LOGICAL_OLAP_SCAN3 \n"
                + " rowCount=40.00\"];\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN1 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN1 -> LOGICAL_OLAP_SCAN2 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN2 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN2 -> LOGICAL_OLAP_SCAN3 [label=\"1.00\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN3 [label=\"1.00\",arrowhead=none]\n"
                + "}\n";
        Assertions.assertEquals(dottyGraph, target);
    }

    @Test
    void testRandomQuery() {
        int tableNum = 10;
        int edgeNum = 20;
        // repeat 10 times
        for (int i = 0; i < 10; i++) {
            HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
            HyperGraph hyperGraph = hyperGraphBuilder.randomBuildWith(tableNum, edgeNum);
            Assertions.assertEquals(hyperGraph.getNodes().size(), tableNum);
            Assertions.assertEquals(hyperGraph.getJoinEdges().size(), edgeNum);
        }
    }
}
