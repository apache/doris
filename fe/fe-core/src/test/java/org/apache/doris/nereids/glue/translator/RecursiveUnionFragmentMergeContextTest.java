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

import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.BucketedAggregationNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
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
 * join or a set operation, and it has to translate its children inside
 * PlanTranslatorContext#forbidExchangeElision / #allowExchangeElision.
 *
 * <p>Bucketed aggregation fusion deletes the exchange between a one-phase GLOBAL aggregate and its
 * distribute -> olap scan child, and that exchange is the only thing that keeps the scan in a
 * fragment of its own. Fusing inside a fragment-merging child hands the scan over to the merging
 * parent, so two olap scans end up in the same fragment, which the scan assignment rejects
 * ("Not supported multiple scan multiple OlapTable but not contains colocate join or bucket shuffle
 * join"). The recursive union is the
 * only merging entry point that did not declare that context, so its base case was fused anyway: the
 * exchange that keeps the merge legal survived only because the property enforcer happens to insert
 * a gather exchange above that child. This test pins the translator contract itself instead of
 * relying on that non-local fact.
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
    public void testRecursiveUnionChildIsNotFusedIntoBucketedAggregation() throws Exception {
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

            // Positive control: the same aggregate outside a recursive union is still fused, which
            // proves bucketed fusion is enabled here and keeps the assertion below from passing
            // vacuously. Both queries go through EXPLAIN so that the test only exercises planning.
            Planner plainPlanner = getSQLPlanner("EXPLAIN SELECT k, SUM(v) AS sv"
                    + " FROM recursive_union_fragment_merge_test.base_table GROUP BY k");
            Assertions.assertFalse(collectNodes(plainPlanner, BucketedAggregationNode.class).isEmpty(),
                    "plain single table aggregate should be fused into bucketed aggregation: "
                            + explain(plainPlanner));

            Planner planner = getSQLPlanner("EXPLAIN " + RECURSIVE_CTE_QUERY);
            Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty(),
                    "the base case of a recursive union is consumed by a fragment merging node, so the"
                            + " exchange that keeps its olap scan in its own fragment must survive: "
                            + explain(planner));
            // Non-vacuity: the base case aggregate is still translated, just as a regular aggregate.
            Assertions.assertFalse(collectNodes(planner, AggregationNode.class).isEmpty(),
                    "base case aggregate should fall back to a regular aggregation: " + explain(planner));
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
