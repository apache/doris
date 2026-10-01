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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.planner.BucketedAggregationNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RecursiveCteNode;
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

/**
 * A recursive union absorbs its children's fragments: visitPhysicalRecursiveUnion merges the child
 * fragments into its own fragment and setPlanRoot then rewrites the fragment ownership of the child
 * plan trees, stopping only at an exchange. It is therefore a fragment-merging node exactly like a
 * join or a set operation, and it translates its children inside
 * PlanTranslatorContext#enterFragmentMergeChild / #exitFragmentMergeChild.
 *
 * <p>Bucketed aggregation fusion deletes the exchange between a one-phase GLOBAL aggregate and its
 * distribute -> olap scan child. Fusing an aggregate that a merging node consumes directly would
 * hand the scan over to the merging parent, so two olap scans could end up in the same fragment and
 * the scan assignment would reject it with "Not supported multiple scan multiple OlapTable but not
 * contains colocate join or bucket shuffle join". A distribute between the merging node and the
 * aggregate is an exchange boundary, though: visitPhysicalDistribute clears the fragment-merge
 * context below it, because its exchange keeps the fused fragment apart from the merging parent.
 * The recursive union requests GATHER from its children, so its base case aggregate always sits
 * below such a gather exchange: it is fused, the exchange survives, and the recursive union
 * fragment keeps a single olap scan. This test pins that contract on the translator itself.
 */
public class RecursiveUnionFragmentMergeContextTest extends TestWithFeService {

    private static final String RECURSIVE_CTE_QUERY = "WITH RECURSIVE cte AS ("
            + " SELECT k, SUM(v) AS sv, CAST(1 AS INT) AS lvl"
            + " FROM recursive_union_fragment_merge_test.base_table GROUP BY k"
            + " UNION ALL"
            + " SELECT k, sv, CAST(lvl + 1 AS INT) AS lvl FROM cte WHERE lvl < 3)"
            + " SELECT k, MAX(sv) AS msv FROM cte GROUP BY k";

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("recursive_union_fragment_merge_test");
        // The group by key is deliberately not the distribution key, so the one-phase GLOBAL
        // aggregate gets a shuffle of its own and is a candidate for bucketed fusion.
        createTable("CREATE TABLE recursive_union_fragment_merge_test.base_table ("
                + "id INT NOT NULL, k INT NOT NULL, v INT NOT NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
    }

    @Test
    public void testRecursiveUnionChildIsFusedBelowItsGatherExchange() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldEnableBucketShuffleJoin = sessionVariable.enableBucketShuffleJoin;
        int oldParallelPipelineTaskNum = sessionVariable.parallelPipelineTaskNum;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.enableBucketShuffleJoin = false;
            sessionVariable.parallelPipelineTaskNum = 1;

            // Positive control: the same aggregate outside a recursive union is fused, which
            // proves bucketed fusion is enabled here. Both queries go through EXPLAIN so that the
            // test only exercises planning.
            Planner plainPlanner = getSQLPlanner("EXPLAIN SELECT k, SUM(v) AS sv"
                    + " FROM recursive_union_fragment_merge_test.base_table GROUP BY k");
            Assertions.assertFalse(collectNodes(plainPlanner, BucketedAggregationNode.class).isEmpty(),
                    "plain single table aggregate should be fused into bucketed aggregation: "
                            + explain(plainPlanner));

            Planner planner = getSQLPlanner("EXPLAIN " + RECURSIVE_CTE_QUERY);
            String explain = explain(planner);
            List<BucketedAggregationNode> bucketedNodes = collectNodes(planner, BucketedAggregationNode.class);
            Assertions.assertEquals(1, bucketedNodes.size(),
                    "the base case aggregate sits below the gather exchange the recursive union requests,"
                            + " so it is fused: " + explain);
            List<RecursiveCteNode> recursiveCteNodes = collectNodes(planner, RecursiveCteNode.class);
            Assertions.assertEquals(1, recursiveCteNodes.size(), explain);
            // The exchange survives, so the fused fragment is not absorbed by the recursive union.
            Assertions.assertNotSame(recursiveCteNodes.get(0).getFragment(), bucketedNodes.get(0).getFragment(),
                    "the fused aggregate must stay in a fragment of its own: " + explain);
            Assertions.assertTrue(bucketedNodes.get(0).getFragment().getDestNode() instanceof ExchangeNode,
                    "the fused fragment must feed the recursive union through an exchange: " + explain);
            for (PlanFragment fragment : planner.getFragments()) {
                PlanNode root = fragment.getPlanRoot();
                if (root != null) {
                    Assertions.assertTrue(countOlapScansInFragment(root) <= 1,
                            "fragment " + fragment.getId() + " has more than one olap scan: " + explain);
                }
            }
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.enableBucketShuffleJoin = oldEnableBucketShuffleJoin;
            sessionVariable.parallelPipelineTaskNum = oldParallelPipelineTaskNum;
        }
    }

    /** Exchange nodes are fragment boundaries and are not descended. */
    private static int countOlapScansInFragment(PlanNode node) {
        if (node instanceof ExchangeNode) {
            return 0;
        }
        int count = node instanceof OlapScanNode ? 1 : 0;
        for (PlanNode child : node.getChildren()) {
            count += countOlapScansInFragment(child);
        }
        return count;
    }

    /**
     * Collects every node of the given type once. The fragments returned by the planner form a tree,
     * so a plain collect over all fragment roots would count an inner node repeatedly.
     */
    private <T extends PlanNode> List<T> collectNodes(Planner planner, Class<T> clazz) {
        Set<PlanNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<T> nodes = Lists.newArrayList();
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root == null) {
                continue;
            }
            List<T> found = Lists.newArrayList();
            root.collect(clazz, found);
            for (T node : found) {
                if (seen.add(node)) {
                    nodes.add(node);
                }
            }
        }
        return nodes;
    }

    private String explain(Planner planner) {
        return planner.getFragments().stream()
                .map(fragment -> fragment.getExplainString(TExplainLevel.NORMAL))
                .collect(Collectors.joining("\n"));
    }
}
