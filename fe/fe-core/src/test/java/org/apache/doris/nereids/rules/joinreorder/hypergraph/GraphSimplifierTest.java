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

import org.junit.jupiter.api.Test;

public class GraphSimplifierTest {
    @Test
    void testGraphSimplifier() {
        HyperGraph hyperGraph = new HyperGraphBuilder()
                .init("t1", 10)
                .join(JoinType.INNER_JOIN, "t2", 20)
                .join(JoinType.INNER_JOIN, "t3", 30)
                .join(JoinType.INNER_JOIN, "t4", 40)
                .join(JoinType.INNER_JOIN, "t5", 50)
                .build();
        GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
        graphSimplifier.initFirstStep();
        while (graphSimplifier.applySimplificationStep()) {
        }

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
                + "e0 [shape=circle, width=.001, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN0 -> e0 [arrowhead=none, label=\"0.10\"]\n"
                + "LOGICAL_OLAP_SCAN2 -> e0 [arrowhead=none, label=\"0.10\"]\n"
                + "LOGICAL_OLAP_SCAN3 -> e0 [arrowhead=none, label=\"0.10\"]\n"
                + "LOGICAL_OLAP_SCAN4 -> e0 [arrowhead=none, label=\"0.10\"]\n"
                + "LOGICAL_OLAP_SCAN1 -> e0 [arrowhead=none, label=\"\"]\n"
                + "e1 [shape=circle, width=.001, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN0 -> e1 [arrowhead=none, label=\"0.05\"]\n"
                + "LOGICAL_OLAP_SCAN3 -> e1 [arrowhead=none, label=\"0.05\"]\n"
                + "LOGICAL_OLAP_SCAN4 -> e1 [arrowhead=none, label=\"0.05\"]\n"
                + "LOGICAL_OLAP_SCAN2 -> e1 [arrowhead=none, label=\"\"]\n"
                + "e2 [shape=circle, width=.001, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN0 -> e2 [arrowhead=none, label=\"0.03\"]\n"
                + "LOGICAL_OLAP_SCAN4 -> e2 [arrowhead=none, label=\"0.03\"]\n"
                + "LOGICAL_OLAP_SCAN3 -> e2 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN4 [label=\"0.03\",arrowhead=none]\n"
                + "}\n";
        String dottyGraph = hyperGraph.toDottyHyperGraph();
        assert dottyGraph.equals(target) : dottyGraph;
    }
}
