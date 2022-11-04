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
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

public class HyperGraphTest {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);
    private final LogicalOlapScan scan5 = PlanConstructor.newLogicalOlapScan(4, "t5", 0);

    @Test
    void testHyperGraph() {
        HyperGraph hyperGraph = new HyperGraphBuilder()
                .init("t1", 0)
                .join(JoinType.INNER_JOIN, "t2", 0)
                .join(JoinType.INNER_JOIN, "t3", 0)
                .join(JoinType.INNER_JOIN, "t4", 0)
                .join(JoinType.INNER_JOIN, "t5", 0)
                .build();
        String dottyGraph = hyperGraph.toDottyHyperGraph();
        String target = "digraph G {  # 2 edges\n"
                + "  LOGICAL_OLAP_SCAN0 [label=\"LOGICAL_OLAP_SCAN0 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN1 [label=\"LOGICAL_OLAP_SCAN1 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN2 [label=\"LOGICAL_OLAP_SCAN2 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN3 [label=\"LOGICAL_OLAP_SCAN3 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN4 [label=\"LOGICAL_OLAP_SCAN4 \n"
                + " rowCount=0.00\"];\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN1 [label=\"\",arrowhead=none]\n"
                + "LOGICAL_OLAP_SCAN0 -> LOGICAL_OLAP_SCAN3 [label=\"\",arrowhead=none]\n"
                + "}\n";
        assert dottyGraph.equals(target) : dottyGraph;

    }
}
