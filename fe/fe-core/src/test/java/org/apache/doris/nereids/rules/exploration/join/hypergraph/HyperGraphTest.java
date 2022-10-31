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

package org.apache.doris.nereids.rules.exploration.join.hypergraph;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

public class HyperGraphTest {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);
    private final LogicalOlapScan scan5 = PlanConstructor.newLogicalOlapScan(4, "t5", 0);

    @Test
    void testDottyHyperGraph() {
        LogicalPlan joinCluster = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan5, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        HyperGraph hyperGraph = HyperGraph.fromPlan(joinCluster);
        String dottyGraph = hyperGraph.toDottyHyperGraph();
        // This is a star join, which can be transformed to a image by graphviz.
        assert dottyGraph.equals("digraph G {  # 4 edges\n"
            + "  LOGICAL_OLAP_SCAN0 [label=\"LOGICAL_OLAP_SCAN0\"];\n"
            + "  LOGICAL_OLAP_SCAN1 [label=\"LOGICAL_OLAP_SCAN1\"];\n"
            + "  LOGICAL_OLAP_SCAN2 [label=\"LOGICAL_OLAP_SCAN2\"];\n"
            + "  LOGICAL_OLAP_SCAN3 [label=\"LOGICAL_OLAP_SCAN3\"];\n"
            + "  LOGICAL_OLAP_SCAN4 [label=\"LOGICAL_OLAP_SCAN4\"];\n"
            + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN1 [label=\"1.0\",arrowhead=none]\n"
            + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN2 [label=\"1.0\",arrowhead=none]\n"
            + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN3 [label=\"1.0\",arrowhead=none]\n"
            + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN4 [label=\"1.0\",arrowhead=none]\n"
            + "}\n") : dottyGraph;
    }
}
