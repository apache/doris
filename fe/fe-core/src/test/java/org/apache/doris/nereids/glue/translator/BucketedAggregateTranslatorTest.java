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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AnalyticEvalNode;
import org.apache.doris.planner.BucketedAggregationNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
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

public class BucketedAggregateTranslatorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("bucketed_aggregate_translator_test");
        createTable("CREATE TABLE bucketed_aggregate_translator_test.agg_group_concat_table ("
                + "kint INT NOT NULL, kbint INT NOT NULL, kstr STRING NOT NULL) "
                + "DISTRIBUTED BY HASH(kint) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
        createFunction("CREATE AGGREGATE FUNCTION bucketed_aggregate_translator_test.py_udaf_sum(INT) "
                + "RETURNS BIGINT PROPERTIES('type'='PYTHON_UDF', 'symbol'='SumUdaf', "
                + "'runtime_version'='3.10.2')");
    }

    @Test
    public void testPythonUdafIsNotFusedIntoBucketedAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            // agg_phase=0 lets the optimizer choose the plan; agg_phase=1 forces the
            // one-phase plan, so only the translator fusion gate can reject the UDAF.
            for (int aggPhase : new int[] {0, 1}) {
                sessionVariable.aggPhase = aggPhase;
                // A builtin aggregate on the same shape is fused, so the UDAF cases below
                // are rejected because of the UDAF rather than the plan shape.
                Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());
                assertUsesRegularAggregation(
                        "bucketed_aggregate_translator_test.py_udaf_sum(kint)");
                assertUsesRegularAggregation(
                        "sum(kint), bucketed_aggregate_translator_test.py_udaf_sum(kint)");
            }
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testAggregateOrderByIsNotFusedIntoBucketedAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldUseOnePhaseAggForGroupConcatWithOrder =
                sessionVariable.useOnePhaseAggForGroupConcatWithOrder;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.useOnePhaseAggForGroupConcatWithOrder = false;

            assertUsesRegularAggregation("group_concat(kstr ORDER BY kint)");
            assertUsesRegularAggregation("multi_distinct_group_concat(kstr ORDER BY kint)");
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.useOnePhaseAggForGroupConcatWithOrder =
                    oldUseOnePhaseAggForGroupConcatWithOrder;
        }
    }

    @Test
    public void testSpillKeepsRegularAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldEnableSpill = sessionVariable.enableSpill;
        boolean oldEnableForceSpill = sessionVariable.enableForceSpill;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.enableSpill = false;
            sessionVariable.enableForceSpill = false;
            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());

            // Bucketed agg cannot spill, so the spillable regular aggregation must be kept.
            sessionVariable.enableSpill = true;
            assertUsesRegularAggregation("sum(kint)");
            sessionVariable.enableSpill = false;
            sessionVariable.enableForceSpill = true;
            assertUsesRegularAggregation("sum(kint)");
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.enableSpill = oldEnableSpill;
            sessionVariable.enableForceSpill = oldEnableForceSpill;
        }
    }

    @Test
    public void testQueryCacheKeepsRegularAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldEnableSpill = sessionVariable.enableSpill;
        boolean oldEnableForceSpill = sessionVariable.enableForceSpill;
        boolean oldEnableQueryCache = sessionVariable.getEnableQueryCache();
        try {
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.enableSpill = false;
            sessionVariable.enableForceSpill = false;
            sessionVariable.setEnableQueryCache(false);
            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());

            // The query cache point is the LOCAL AggregationNode above the scan, which the
            // fused bucketed aggregation does not have.
            sessionVariable.setEnableQueryCache(true);
            Planner planner = planAggregate("sum(kint)");
            Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty());
            List<AggregationNode> aggregationNodes = collectNodes(planner, AggregationNode.class);
            Assertions.assertTrue(aggregationNodes.stream()
                    .anyMatch(node -> node.isQueryCacheCandidate()));
        } finally {
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.enableSpill = oldEnableSpill;
            sessionVariable.enableForceSpill = oldEnableForceSpill;
            sessionVariable.setEnableQueryCache(oldEnableQueryCache);
        }
    }

    @Test
    public void testMixedDistinctDedupAggregateIsNotPlannedAsBucketed() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            // Let the optimizer choose between the one-phase and the multi-phase plans.
            sessionVariable.aggPhase = 0;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            // The dedup aggregate of a mixed DISTINCT / non-DISTINCT query is a one-phase
            // GLOBAL INPUT_TO_RESULT aggregate whose non-distinct functions are partial, so
            // the translator never fuses it. The regulator and the cost model must treat it
            // the same way: neither exempt its one-phase-with-distribute shape from the ban
            // nor discount its cost, otherwise the plan would exchange raw scan rows instead
            // of deduplicating locally before the exchange.
            Planner planner = planAggregate("stddev_pop(distinct kint), sum(kbint)");
            Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty());
            for (ExchangeNode exchange : collectNodes(planner, ExchangeNode.class)) {
                Assertions.assertFalse(exchange.getChild(0) instanceof OlapScanNode,
                        "raw scan rows must not be exchanged: "
                                + planner.getExplainString(new ExplainOptions(false, false, false)));
            }
            Assertions.assertTrue(collectNodes(planner, AggregationNode.class).stream()
                    .anyMatch(node -> node.getChild(0) instanceof OlapScanNode));

            // A fusible aggregate on the same shape is still fused.
            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testUnknownGroupKeyStatisticsKeepBucketedAggregation() throws Exception {
        // The table is never analyzed, so the GROUP BY key has unknown statistics and
        // StatsCalculator only estimates the aggregate output as input rows / 3. That
        // fallback is not a real group cardinality, so the output-ratio gate of
        // ChildrenPropertiesRegulator must not compare it with the default
        // bucketed_agg_high_card_threshold (0.3): 1/3 > 0.3 would otherwise ban
        // bucketed aggregation for every un-analyzed table.
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            // agg_phase=0 lets the optimizer choose, so the regulator's data-volume gates decide
            // whether the one-phase candidate that the translator fuses survives at all.
            sessionVariable.aggPhase = 0;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = new SessionVariable().bucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = true;

            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testAggregateBelowJoinExchangeIsFusedIntoBucketedAggregation() throws Exception {
        // The join key is the aggregate output s, so the shuffle join enforces a hash exchange
        // above the one-phase aggregate. That exchange keeps the aggregate's scan in a fragment
        // of its own, so the fragment-merging join above must not stop the fusion below it;
        // otherwise the plan pays for the raw-row exchange below a regular aggregate and for
        // the enforcer exchange above it, while the cost model already granted the discount.
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            Planner planner = getSQLPlanner("SELECT a.kbint, a.s, b.kint"
                    + " FROM (SELECT kbint, sum(kint) AS s"
                    + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint) a"
                    + " JOIN [shuffle] bucketed_aggregate_translator_test.agg_group_concat_table b"
                    + " ON a.s = b.kbint");
            String explain = explain(planner);
            List<BucketedAggregationNode> bucketedNodes = collectNodes(planner, BucketedAggregationNode.class);
            Assertions.assertEquals(1, bucketedNodes.size(), explain);
            Assertions.assertTrue(collectNodes(planner, AggregationNode.class).isEmpty(), explain);
            List<HashJoinNode> joinNodes = collectNodes(planner, HashJoinNode.class);
            Assertions.assertEquals(1, joinNodes.size(), explain);
            // The fused aggregate stays in its own fragment and feeds the join through the exchange.
            PlanFragment fusedFragment = bucketedNodes.get(0).getFragment();
            Assertions.assertNotSame(joinNodes.get(0).getFragment(), fusedFragment, explain);
            Assertions.assertTrue(fusedFragment.getDestNode() instanceof ExchangeNode, explain);
            assertAtMostOneOlapScanPerFragment(planner);
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testJoinOnGroupKeyConsumesRegularAggregateWithoutExtraExchange() throws Exception {
        // The join key is the GROUP BY key. The enforcer distribute below the aggregate has
        // shuffle type EXECUTION_BUCKETED (EnforceMissingPropertiesHelper), so the output
        // property deriver keeps that hash property instead of ANY: the join consumes the
        // aggregate directly, no exchange is enforced above it, and the translator keeps the
        // regular aggregate because the join would otherwise absorb the fused scan.
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            Planner planner = getSQLPlanner("SELECT a.kbint, a.s, b.kint"
                    + " FROM (SELECT kbint, sum(kint) AS s"
                    + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint) a"
                    + " JOIN [shuffle] bucketed_aggregate_translator_test.agg_group_concat_table b"
                    + " ON a.kbint = b.kint");
            String explain = explain(planner);
            Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty(), explain);
            List<AggregationNode> aggregationNodes = collectNodes(planner, AggregationNode.class);
            Assertions.assertEquals(1, aggregationNodes.size(), explain);
            List<HashJoinNode> joinNodes = collectNodes(planner, HashJoinNode.class);
            Assertions.assertEquals(1, joinNodes.size(), explain);
            // The aggregate output is consumed in the join's fragment: no extra exchange between them.
            Assertions.assertSame(joinNodes.get(0).getFragment(), aggregationNodes.get(0).getFragment(), explain);
            Assertions.assertSame(aggregationNodes.get(0), joinNodes.get(0).getChild(0), explain);
            assertAtMostOneOlapScanPerFragment(planner);
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testSetOperationChildAggregateKeepsOneOlapScanPerFragment() throws Exception {
        // A union absorbs the plan trees of its children's fragments. An aggregate that the
        // union consumes without an exchange in between must therefore keep its own exchange
        // (no fusion), so that no fragment ends up with two olap scans.
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            Planner planner = getSQLPlanner("SELECT kbint, sum(kint)"
                    + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint"
                    + " UNION ALL SELECT kbint, max(kint)"
                    + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint");
            String explain = explain(planner);
            Assertions.assertEquals(2, collectNodes(planner, AggregationNode.class).size()
                    + collectNodes(planner, BucketedAggregationNode.class).size(), explain);
            assertAtMostOneOlapScanPerFragment(planner);
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testParentKeyShuffledAggregateIsNotExemptedAsBucketed() throws Exception {
        // The window partitions by kbint, a strict subset of the GROUP BY keys, so with
        // agg_shuffle_use_parent_key the aggregate also asks its child for HASH(kbint). That
        // distribution satisfies the window without an exchange above the aggregate, but the
        // translator only fuses an aggregate whose distribute child hashes exactly the GROUP BY
        // keys. The parent-key alternative is therefore a regular one-phase aggregate over a
        // raw-row exchange, and the regulator must keep banning it instead of exempting it as a
        // bucketed candidate; otherwise it wins with the bucketed cost discount and without the
        // exchange above the aggregate.
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldAggShuffleUseParentKey = sessionVariable.aggShuffleUseParentKey;
        double oldCboNetWeight = sessionVariable.getCboNetWeight();
        try {
            // Let the optimizer choose between the one-phase and the two-phase plans.
            sessionVariable.aggPhase = 0;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.aggShuffleUseParentKey = true;

            String sql = "SELECT kbint, kstr, s, sum(s) OVER (PARTITION BY kbint)"
                    + " FROM (SELECT kbint, kstr, sum(kint) AS s"
                    + " FROM bucketed_aggregate_translator_test.agg_group_concat_table"
                    + " GROUP BY kbint, kstr) a";
            // The full-key alternative is the one the translator fuses.
            assertFusedBelowWindowExchange(getSQLPlanner(sql));

            // The ban does not depend on the cost: an expensive network makes the exchange
            // above the fused aggregate dearer, which favors the parent-key alternative.
            sessionVariable.setCboNetWeight(100);
            assertFusedBelowWindowExchange(getSQLPlanner(sql));
            sessionVariable.setCboNetWeight(oldCboNetWeight);

            // Without the parent-key request only the full-key alternative exists: same plan.
            sessionVariable.aggShuffleUseParentKey = false;
            assertFusedBelowWindowExchange(getSQLPlanner(sql));
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.aggShuffleUseParentKey = oldAggShuffleUseParentKey;
            sessionVariable.setCboNetWeight(oldCboNetWeight);
        }
    }

    /**
     * Asserts the plan Window <- Exchange <- BucketedAggregation <- OlapScan: the aggregate is
     * fused, and because a fused aggregate is not distributed by the window's partition key the
     * window is fed through an exchange.
     */
    private void assertFusedBelowWindowExchange(Planner planner) {
        String explain = explain(planner);
        Assertions.assertEquals(1, collectNodes(planner, AnalyticEvalNode.class).size(), explain);
        assertNoRawScanExchange(planner);
        List<BucketedAggregationNode> bucketedNodes = collectNodes(planner, BucketedAggregationNode.class);
        Assertions.assertEquals(1, bucketedNodes.size(), explain);
        Assertions.assertTrue(collectNodes(planner, AggregationNode.class).isEmpty(), explain);
        Assertions.assertTrue(bucketedNodes.get(0).getChild(0) instanceof OlapScanNode, explain);
        Assertions.assertTrue(bucketedNodes.get(0).getFragment().getDestNode() instanceof ExchangeNode, explain);
    }

    private void assertNoRawScanExchange(Planner planner) {
        for (ExchangeNode exchange : collectNodes(planner, ExchangeNode.class)) {
            Assertions.assertFalse(exchange.getChild(0) instanceof OlapScanNode,
                    "raw scan rows must not be exchanged: " + explain(planner));
        }
    }

    /**
     * The scan assignment rejects a fragment with several olap scans unless they belong to a
     * colocate or bucket shuffle join, so bucketed fusion must never hand a scan over to a
     * fragment-merging node. Exchange nodes are fragment boundaries and are not descended.
     */
    private void assertAtMostOneOlapScanPerFragment(Planner planner) {
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root != null) {
                Assertions.assertTrue(countOlapScansInFragment(root) <= 1,
                        "fragment " + fragment.getId() + " has more than one olap scan: " + explain(planner));
            }
        }
    }

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

    private String explain(Planner planner) {
        return planner.getFragments().stream()
                .map(fragment -> fragment.getExplainString(TExplainLevel.NORMAL))
                .collect(Collectors.joining("\n"));
    }

    private void assertUsesRegularAggregation(String aggregateFunction) throws Exception {
        Planner planner = planAggregate(aggregateFunction);
        Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty());
        Assertions.assertFalse(collectNodes(planner, AggregationNode.class).isEmpty());
    }

    private List<BucketedAggregationNode> collectBucketedAggregationNodes(String aggregateFunction)
            throws Exception {
        return collectNodes(planAggregate(aggregateFunction), BucketedAggregationNode.class);
    }

    private Planner planAggregate(String aggregateFunction) throws Exception {
        return getSQLPlanner("SELECT " + aggregateFunction
                + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint");
    }

    /**
     * Collects every node of the given type once. The fragments returned by the planner form a
     * tree, so a plain collect over all fragment roots would count an inner node repeatedly.
     */
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
