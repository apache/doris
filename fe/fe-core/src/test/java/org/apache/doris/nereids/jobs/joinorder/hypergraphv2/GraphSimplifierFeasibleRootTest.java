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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.receiver.Counter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the DPHyp feasible-root guarantee.
 *
 * <p>A successful enumeration must have actually built the full join (the root
 * bitmap). Without this, DPHyp can walk every CSG/CMP without any receiver
 * returning FAIL while never inserting the root — e.g. when the only full split
 * is rejected by the alias-dependency rule (a producer alias whose source spans
 * both children, as in a producer-after-consumer alias chain). The
 * GraphSimplifier would then accept a rootless probe and the final
 * PlanReceiver pass would return a null best plan for the root.
 */
public class GraphSimplifierFeasibleRootTest extends SqlTestBase {

    // Alias chain on the nullable side of an outer join:
    //   x  = T2.score + T3.score          key {T2, T3}     (A, B)
    //   dv = x + T1.score                 key {T2, T3, T1} (A, B, C)
    //   outer: T4 left join (...)          (preserved table, nullable side)
    // Inner join cluster {T2, T3, T1} has edges {T2}--{T3} and {T2}--{T1}.
    private static final String ALIAS_CHAIN_SQL =
            "select T4.id, Sub.dv "
            + "from T4 left join ("
            + "  select Sub2.id, Sub2.x + T1.score as dv "
            + "  from ("
            + "    select T2.id, T2.score + T3.score as x "
            + "    from T2 inner join T3 on T2.id = T3.id"
            + "  ) Sub2 inner join T1 on Sub2.id = T1.id"
            + ") Sub on T4.id = Sub.id";

    /**
     * Builds the DPHyp hypergraph for the join cluster of the analyzed plan.
     */
    private static HyperGraph buildHyperGraph(CascadesContext c1) {
        Group joinGroup = c1.getMemo().getRoot();
        while (!HyperGraph.isValidJoin(joinGroup.getLogicalExpression().getPlan())
                && joinGroup.getLogicalExpression().arity() > 0) {
            joinGroup = joinGroup.getLogicalExpression().child(0);
        }
        HyperGraph.Builder builder = HyperGraph.builderForDPhyper(joinGroup, c1);
        for (AbstractNode node : builder.getNodes()) {
            DPhyperNode dPhyperNode = (DPhyperNode) node;
            builder.updateNode(node.getIndex(), dPhyperNode.getGroup());
        }
        return builder.build();
    }

    /**
     * Forces the producer-after-consumer order and checks that the rootless
     * enumeration is reported as failure.
     *
     * <p>The GraphSimplifier cost-orders A-C before A-B; concretizeSimplificationStep
     * then extends the A-B edge ({T2}--{T3}) to {A,C}--{B} ({T2,T1}--{T3}).
     * After that {A,B} is no longer a connected subgraph, so the only full
     * split of {A,B,C} is {A,C}--{B} — which is rejected by
     * hasUnresolvableAliasDependency (dv references x whose source {A,B} spans
     * both children). The root bitmap is never inserted, and a rootless
     * enumeration must be reported as failure instead of silently succeeding.
     */
    @Test
    void testProducerAfterConsumerOrderRejectsRootlessEnumeration() {
        CascadesContext c1 = createCascadesContext(ALIAS_CHAIN_SQL, connectContext);
        PlanChecker.from(c1).analyze().rewrite();
        HyperGraph hyperGraph = buildHyperGraph(c1);

        // Baseline: without simplification the full plan is reachable.
        Counter counter = new Counter(hyperGraph);
        SubgraphEnumerator enumerator = new SubgraphEnumerator(counter, hyperGraph);
        Assertions.assertTrue(enumerator.enumerate());
        Assertions.assertTrue(counter.contain(hyperGraph.getNodesMap()));

        // Identify the producer edge A-B ({T2}--{T3}) and force the order.
        int a = nodeIndexOfTable(hyperGraph, "T2");
        int b = nodeIndexOfTable(hyperGraph, "T3");
        int c = nodeIndexOfTable(hyperGraph, "T1");
        long aMap = LongBitmap.newBitmap(a);
        long bMap = LongBitmap.newBitmap(b);
        long cMap = LongBitmap.newBitmap(c);
        Edge abEdge = null;
        for (Edge edge : hyperGraph.getJoinEdges()) {
            long ref = edge.getLeftExtendedNodes() | edge.getRightExtendedNodes();
            if (ref == (aMap | bMap)) {
                abEdge = edge;
                break;
            }
        }
        Assertions.assertNotNull(abEdge, "producer edge A-B should exist");
        hyperGraph.modifyEdge(abEdge.getIndex(), aMap | cMap, bMap);

        // The fix: rootless enumeration must be reported as failure so the
        // caller falls back instead of returning a null best plan.
        Counter counter2 = new Counter(hyperGraph);
        SubgraphEnumerator enumerator2 = new SubgraphEnumerator(counter2, hyperGraph);
        Assertions.assertFalse(enumerator2.enumerate());
        Assertions.assertFalse(counter2.contain(hyperGraph.getNodesMap()));
    }

    /**
     * With a low dphyperLimit the GraphSimplifier is forced to simplify. If it
     * produces a producer-after-consumer order whose only full split is
     * rejected, enumeration must not return a null plan — the caller falls back
     * to the original group instead.
     */
    @Test
    void testAliasChainDpHypWithLowLimitProducesValidPlan() {
        connectContext.getSessionVariable().dphyperLimit = 1;
        CascadesContext c1 = createCascadesContext(ALIAS_CHAIN_SQL, connectContext);
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    private static int nodeIndexOfTable(HyperGraph hyperGraph, String table) {
        for (AbstractNode node : hyperGraph.getNodes()) {
            DPhyperNode dn = (DPhyperNode) node;
            Plan p = dn.getGroup().getLogicalExpression().getPlan();
            if (p instanceof LogicalOlapScan) {
                if (table.equals(((LogicalOlapScan) p).getTable().getName())) {
                    return node.getIndex();
                }
            }
        }
        throw new IllegalStateException("table not found: " + table);
    }
}
