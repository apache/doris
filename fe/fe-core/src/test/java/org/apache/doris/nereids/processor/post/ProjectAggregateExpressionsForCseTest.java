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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.MultiCastPlanFragment;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProjectAggregateExpressionsForCseTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("project_aggregate_cse_test");
        createTable("CREATE TABLE project_aggregate_cse_test.t ("
                + "id INT NOT NULL, k INT NOT NULL, a INT NOT NULL, b INT NOT NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
    }

    @Test
    public void testCseIsMergedIntoTheProjectBelowTheDistribute() throws Exception {
        // One-phase aggregate -> distribute -> project(k, a, b) -> scan: the common argument
        // a + b joins the existing project, so the scan computes it and no SelectNode is added.
        Planner planner = planOnePhase("SELECT k, SUM(a + b), MAX(a + b)"
                + " FROM project_aggregate_cse_test.t GROUP BY k");
        String explain = explain(planner);
        Assertions.assertTrue(collectNodes(planner, SelectNode.class).isEmpty(), explain);
        List<OlapScanNode> scans = collectNodes(planner, OlapScanNode.class);
        Assertions.assertEquals(1, scans.size(), explain);
        Assertions.assertEquals(1, countComputedExprs(scans.get(0).getProjectList()), explain);
        assertAggregateArgumentsAreSlots(planner, explain);
    }

    @Test
    public void testCseReferencingAnAliasOfTheProjectBelowTheDistribute() throws Exception {
        // g is produced by the existing project, so the merged CSE expression is rewritten to
        // the project's input: project(k + 1 AS g, a, (k + 1) + a AS cse). The project CSE then
        // computes k + 1 once in an intermediate layer of the same scan projection.
        Planner planner = planOnePhase("SELECT g, SUM(g + a), MAX(g + a)"
                + " FROM (SELECT k + 1 AS g, a FROM project_aggregate_cse_test.t) x GROUP BY g");
        String explain = explain(planner);
        Assertions.assertTrue(collectNodes(planner, SelectNode.class).isEmpty(), explain);
        List<OlapScanNode> scans = collectNodes(planner, OlapScanNode.class);
        Assertions.assertEquals(1, scans.size(), explain);
        Assertions.assertEquals(3, scans.get(0).getProjectList().size(), explain);
        Assertions.assertTrue(countComputedExprs(scans.get(0).getProjectList()) >= 1, explain);
        assertAggregateArgumentsAreSlots(planner, explain);
    }

    @Test
    public void testCseAboveProjectedCteConsumerKeepsOneMulticastProjection() throws Exception {
        // Each consumer of the materialized CTE already has a project (k + N AS g, a, b) between
        // the distribute and the CTE consumer. A multicast sink takes a single projection, so a
        // CSE project stacked on it used to fail the translation with "generate invalid plan".
        Planner planner = planOnePhase("WITH c AS (SELECT k, a, b FROM project_aggregate_cse_test.t WHERE id > 0)"
                + " SELECT g, SUM(a + b), MAX(a + b) FROM (SELECT k + 1 AS g, a, b FROM c) x GROUP BY g"
                + " UNION ALL"
                + " SELECT g, SUM(a + b), MAX(a + b) FROM (SELECT k + 2 AS g, a, b FROM c) y GROUP BY g");
        String explain = explain(planner);
        List<MultiCastDataSink> multiCastSinks = planner.getFragments().stream()
                .filter(MultiCastPlanFragment.class::isInstance)
                .map(fragment -> (MultiCastDataSink) fragment.getSink())
                .collect(Collectors.toList());
        Assertions.assertEquals(1, multiCastSinks.size(), explain);
        List<DataStreamSink> consumerSinks = multiCastSinks.get(0).getDataStreamSinks();
        Assertions.assertEquals(2, consumerSinks.size(), explain);
        for (DataStreamSink consumerSink : consumerSinks) {
            // k + N AS g and the extracted a + b are both computed by the one sink projection.
            Assertions.assertEquals(2, countComputedExprs(consumerSink.getProjections()), explain);
        }
        assertAggregateArgumentsAreSlots(planner, explain);
    }

    @Test
    public void testUnmergeableProjectBelowTheDistributeKeepsTheAggregate() throws Exception {
        // r is volatile and would be referenced twice by the merged project (as r and inside
        // r + a), so the projects cannot be merged: no project is stacked, and the aggregate
        // keeps evaluating its arguments.
        Planner planner = planOnePhase("SELECT k, SUM(r + a), MAX(r + a)"
                + " FROM (SELECT k, a, random() AS r FROM project_aggregate_cse_test.t) x GROUP BY k");
        String explain = explain(planner);
        Assertions.assertTrue(collectNodes(planner, SelectNode.class).isEmpty(), explain);
        List<OlapScanNode> scans = collectNodes(planner, OlapScanNode.class);
        Assertions.assertEquals(1, scans.size(), explain);
        Assertions.assertEquals(1, countComputedExprs(scans.get(0).getProjectList()), explain);
        List<AggregationNode> aggregationNodes = collectNodes(planner, AggregationNode.class);
        Assertions.assertEquals(1, aggregationNodes.size(), explain);
        for (Expr aggregateExpr : aggregateExprs(aggregationNodes.get(0))) {
            Assertions.assertFalse(aggregateExpr.getChild(0) instanceof SlotRef, explain);
        }
    }

    private Planner planOnePhase(String sql) throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.enableBucketedHashAgg = false;
            return getSQLPlanner(sql);
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    private void assertAggregateArgumentsAreSlots(Planner planner, String explain) {
        List<AggregationNode> aggregationNodes = collectNodes(planner, AggregationNode.class);
        Assertions.assertFalse(aggregationNodes.isEmpty(), explain);
        for (AggregationNode aggregationNode : aggregationNodes) {
            for (Expr aggregateExpr : aggregateExprs(aggregationNode)) {
                Assertions.assertTrue(aggregateExpr.getChild(0) instanceof SlotRef, explain);
            }
        }
    }

    private List<FunctionCallExpr> aggregateExprs(AggregationNode aggregationNode) {
        AggregateInfo aggInfo = Deencapsulation.getField(aggregationNode, "aggInfo");
        return aggInfo.getAggregateExprs();
    }

    private long countComputedExprs(List<Expr> projections) {
        Assertions.assertNotNull(projections);
        return projections.stream().filter(expr -> !(expr instanceof SlotRef)).count();
    }

    private String explain(Planner planner) {
        return planner.getFragments().stream()
                .map(fragment -> fragment.getExplainString(TExplainLevel.NORMAL))
                .collect(Collectors.joining("\n"));
    }

    private <T extends PlanNode> List<T> collectNodes(Planner planner, Class<T> nodeClass) {
        Set<PlanNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<T> nodes = Lists.newArrayList();
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root == null) {
                continue;
            }
            List<T> found = Lists.newArrayList();
            root.collect(nodeClass, found);
            for (T node : found) {
                if (seen.add(node)) {
                    nodes.add(node);
                }
            }
        }
        return nodes;
    }
}
